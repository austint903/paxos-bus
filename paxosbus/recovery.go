package paxosbus

// Failure recovery: the leader heartbeat that doubles as the commit point, the
// view change that runs when that heartbeat stops, and the state transfer both
// of them lean on.
//
// The shape of it: a bus's slot is a local computation from its client's arrival
// line, so replicas keep recording traffic with no leader at all. What a leader
// is actually needed for is deciding — agreeing a commit point, agreeing a no-op
// for a slot nobody received. So a view change never has to re-establish an
// order, only to reconcile which slots hold what, and it can do that from
// metadata: which slots each replica holds (a bitmap), which of them are agreed
// no-ops, and a hash of the committed prefix. Entries themselves move only over
// BusGetState, lazily, after the new view is already running.

import (
	"sort"
	"time"
)

const (
	// suspicionTick is how often a follower checks the heartbeat clock and the
	// lost-connection flag. The flag is set the instant the leader's socket
	// breaks, so this interval is pure added latency on the fast path, and it
	// has to stay well under the spread in when replicas notice a failure
	// (the difference in their one-way delay from the leader, ~10ms on our
	// testbed). Otherwise replicas suspect at times set by their own tick
	// phases rather than by the network, one wakes the others with its request
	// instead of everyone multicasting together, and collecting a quorum of
	// requests costs a full round trip instead of half.
	suspicionTick = 5 * time.Millisecond

	// stateChunkBytes caps a BusNewState by payload size rather than slot count:
	// one bus can carry a thousand requests, so a fixed slot count would swing
	// between a few hundred bytes and tens of megabytes.
	stateChunkBytes    = 1 << 20
	stateEntryOverhead = 32

	stateFetchTimeout = 5 * time.Second
	// stateFetchProbe is how often a blocked fetch rechecks that its peer is
	// still connected. It bounds how long a dead peer can hold the fetch
	// goroutine, so it wants to be well under the round trip a live peer takes
	// to answer — not so fine that an idle fetch spins.
	stateFetchProbe = 50 * time.Millisecond
	// mergeFetchAttempts bounds how many times the new leader re-asks for merged
	// entries a donor failed to produce before declaring those slots no-ops.
	mergeFetchAttempts = 3
)

// ── Commit point ────────────────────────────────────────────────────────────

// syncRound is one outstanding BusSyncPrepare. Guarded by r.mu.
type syncRound struct {
	view uint64
	slot uint64
	acks map[uint32]struct{}
	done bool
}

// syncLoop is the leader's heartbeat and the first phase of the commit point.
// Followers read liveness from its arrival and agreement from its prefix hash,
// so one message carries both and a silent leader is a dead leader.
func (r *Replica) syncLoop() {
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for range ticker.C {
		var (
			prep   BusSyncPrepare
			commit *BusSyncCommit
		)
		r.mu.Lock()
		if !r.AmLeader() || r.status != statusNormal {
			r.sync = nil
			r.mu.Unlock()
			continue
		}
		view := r.view()
		prep = BusSyncPrepare{ViewId: view, SenderIdx: uint32(r.idx)}
		if r.nextExpected > 0 {
			slot := r.nextExpected - 1
			if h, ok := r.prefixHashAtLocked(slot); ok {
				prep.SlotToSync, prep.HasSlot, prep.PrefixHash = slot, true, h
				r.sync = &syncRound{
					view: view,
					slot: slot,
					acks: map[uint32]struct{}{uint32(r.idx): {}},
				}
				commit = r.maybeCommitSyncLocked()
			}
		}
		if !prep.HasSlot {
			// Nothing executed yet: still beat, so followers know we are alive
			// before any client traffic starts.
			r.sync = nil
		}
		r.mu.Unlock()

		r.broadcastToPeers(MsgBusSyncPrepare, &prep)
		if commit != nil {
			r.broadcastToPeers(MsgBusSyncCommit, commit)
		}
	}
}

// maybeCommitSyncLocked promotes the outstanding round once f+1 replicas
// including this one have agreed, and returns the commit to broadcast off-lock.
func (r *Replica) maybeCommitSyncLocked() *BusSyncCommit {
	s := r.sync
	if s == nil || s.done || len(s.acks) < r.config.QuorumSize() {
		return nil
	}
	s.done = true
	r.setStableLocked(s.slot)
	return &BusSyncCommit{ViewId: s.view, StableSlot: s.slot, SenderIdx: uint32(r.idx)}
}

// setStableLocked advances the commit point, never past what this replica has
// actually executed: everything that reads stableSlot — the prune floor, the
// rewind target, the prefix hash a view change is checked against — assumes the
// local log really does run that far.
func (r *Replica) setStableLocked(slot uint64) {
	if r.nextExpected == 0 {
		return
	}
	if top := r.nextExpected - 1; slot > top {
		slot = top
	}
	if !r.haveStable || slot > r.stableSlot {
		r.stableSlot, r.haveStable = slot, true
	}
}

func (r *Replica) handleSyncPrepare(msg *BusSyncPrepare) {
	if msg.ViewId < r.view() {
		return
	}
	if msg.ViewId > r.view() {
		r.requestCatchUp(msg.ViewId)
		return
	}

	r.mu.Lock()
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	if r.status != statusNormal || !msg.HasSlot {
		r.mu.Unlock()
		return
	}
	if r.nextExpected == 0 || r.nextExpected-1 < msg.SlotToSync {
		// Behind. Fetch the missing prefix and skip this round rather than
		// parking a deferred reply — the next heartbeat is one interval away and
		// will find us caught up.
		from := r.nextExpected
		r.mu.Unlock()
		r.enqueueFetch(int(msg.SenderIdx), from, msg.SlotToSync, nil)
		return
	}
	h, known := r.prefixHashAtLocked(msg.SlotToSync)
	stable, haveStable := r.stableSlot, r.haveStable
	r.mu.Unlock()

	switch {
	case !known:
		// The slot has aged out of the rings, meaning we are more than a full
		// window ahead of the leader. We executed it long ago, so agreeing is
		// trivially true; refusing would only stall the commit point.
		Notice("[%s] sync prepare slot=%d below hash window, agreeing unverified",
			r.self, msg.SlotToSync)
	case h != msg.PrefixHash:
		Warning("[%s] PREFIX MISMATCH at slot=%d (ours=%016x leader=%016x) — rewinding to stable and refetching",
			r.self, msg.SlotToSync, h, msg.PrefixHash)
		r.rewindToStableAndRefetch(int(msg.SenderIdx), stable, haveStable)
		return
	}

	r.sendToPeer(int(msg.SenderIdx), MsgBusSyncReply,
		&BusSyncReply{ViewId: msg.ViewId, Slot: msg.SlotToSync, SenderIdx: uint32(r.idx)})
}

func (r *Replica) handleSyncReply(msg *BusSyncReply) {
	var commit *BusSyncCommit
	r.mu.Lock()
	if s := r.sync; s != nil && !s.done && s.view == msg.ViewId && s.slot == msg.Slot {
		s.acks[msg.SenderIdx] = struct{}{}
		commit = r.maybeCommitSyncLocked()
	}
	r.mu.Unlock()
	if commit != nil {
		r.broadcastToPeers(MsgBusSyncCommit, commit)
	}
}

func (r *Replica) handleSyncCommit(msg *BusSyncCommit) {
	if msg.ViewId != r.view() {
		return
	}
	r.mu.Lock()
	if r.status == statusNormal {
		r.setStableLocked(msg.StableSlot)
		r.lastHeartbeatNs = nowNs()
		r.leaderLost = false
	}
	r.mu.Unlock()
}

// rewindToStableAndRefetch drops everything above the commit point and pulls it
// again.
//
// This is the repair for the one divergence a view change cannot detect on its
// own. leaderResolve applies a no-op and advances its own cursor before it has
// f+1 acks (replica.go), so a leader can execute a no-op at a slot no follower
// ever hears about. If that leader is then merely suspected rather than dead, it
// rejoins as a follower still holding the no-op while the merge — seeing the
// real bus at some replica that received it late — kept the entry. It took part
// in the view change, so the no-op list gives it nothing to rewind to.
//
// The next heartbeat catches it: the hashes disagree, and the divergent slot is
// necessarily above this replica's own commit point (a slot below it had f+1
// agreement, so no follower could disagree). Rewinding there and refetching is
// enough. Loud, because the principled fix is for leaderResolve to wait for its
// quorum before executing.
func (r *Replica) rewindToStableAndRefetch(peer int, stable uint64, haveStable bool) {
	if !haveStable {
		return
	}
	r.mu.Lock()
	if !r.rewindToLocked(stable + 1) {
		r.mu.Unlock()
		Warning("[%s] cannot rewind to stable slot %d: outside the hash window", r.self, stable)
		return
	}
	r.clearSlotsAboveLocked(stable + 1)
	top := r.maxSlotSeen
	r.mu.Unlock()
	if top > stable {
		r.enqueueFetch(peer, stable+1, top, nil)
	}
}

// ── Failure detection ───────────────────────────────────────────────────────

func (r *Replica) suspicionLoop() {
	ticker := time.NewTicker(suspicionTick)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		lost, last, st := r.leaderLost, r.lastHeartbeatNs, r.status
		r.mu.Unlock()
		if st != statusNormal || r.AmLeader() {
			continue
		}
		// Missing heartbeats are the only trigger. A closed socket is tempting to
		// act on — it arrives one one-way delay after a kill instead of a whole
		// timeout — but it answers the wrong question: it says this replica's
		// link to the leader broke, which a partition produces just as readily as
		// a crash, and a crash that takes the machine or its power down produces
		// no close at all. Detection that keyed on it would be fast only for the
		// failures polite enough to close their sockets. So the socket state is
		// logged as context and nothing more.
		silentFor := time.Duration(nowNs() - last)
		if silentFor < r.suspectTimeout {
			continue
		}
		sock := "socket up"
		if lost {
			sock = "socket already closed"
		}
		Warning("[%s] SUSPECT leader %d (heartbeat timeout, silent for %v, %s)",
			r.self, r.leaderIdx(), silentFor.Truncate(time.Millisecond), sock)
		r.startViewChange(r.view() + 1)
	}
}

// ── View change ─────────────────────────────────────────────────────────────

// vcState is one view change in progress. Every replica keeps one, because
// every replica has to count BusViewChangeRequests before it may report; only
// the new leader drains reports.
type vcState struct {
	view    uint64
	reports chan *BusViewChange
	abort   chan struct{}

	// requests are the replicas that have asked for this view, including us.
	// A replica sends its own BusViewChange only once a quorum of them has
	// arrived, so no replica ships its suffix on one machine's suspicion alone.
	requests   map[uint32]struct{}
	reportSent bool
}

func newVCState(view uint64, n int) *vcState {
	return &vcState{
		view:     view,
		reports:  make(chan *BusViewChange, n),
		abort:    make(chan struct{}),
		requests: make(map[uint32]struct{}, n),
	}
}

func (r *Replica) startViewChange(newView uint64) {
	r.mu.Lock()
	if newView <= r.view() {
		r.mu.Unlock()
		return
	}
	if r.vc != nil {
		close(r.vc.abort)
		r.vc = nil
	}
	r.viewId.Store(newView)
	r.status = statusViewChange
	r.leaderLost = false
	r.sync = nil
	r.lastHeartbeatNs = nowNs()
	// Retire in-flight gap agreement: those goroutines are negotiating with a
	// leader we have just given up on, and their no-ops would land on slots the
	// merge is about to decide. They time out on their own.
	r.gaps = make(map[gapKey]*gapState)
	r.drainPendingBusesLocked()
	leader := r.config.LeaderIndex(newView)
	vc := newVCState(newView, r.config.N)
	vc.requests[uint32(r.idx)] = struct{}{} // our own suspicion is one request
	r.vc = vc
	stable, next, maxFilled := r.stableSlot, r.nextExpected, r.maxSlotSeen
	r.mu.Unlock()

	Notice("[%s] VIEW-CHANGE start view=%d new_leader=%d stable=%d executed=%d max_filled=%d",
		r.self, newView, leader, stable, next, maxFilled)

	// Everyone who hears this joins the same view immediately instead of waiting
	// out their own timer, so a quorum of requests forms in about half a round
	// trip when replicas notice together (the VR-revisited optimisation).
	r.broadcastToPeers(MsgBusViewChangeRequest,
		&BusViewChangeRequest{ViewId: newView, SenderIdx: uint32(r.idx)})

	if leader == r.idx {
		go r.driveViewChange(vc)
	}
}

// handleViewChangeRequest counts requests for this view and, on the f+1st,
// sends our suffix report to the new leader. Anyone not already in the view
// change joins it here, which is what makes the quorum form in half a round
// trip: a replica that noticed on its own has already multicast its request, so
// everyone holds the quorum one one-way delay after the earliest detection. It
// costs a full round trip only when a single replica notices alone and the
// others have to be woken by its request.
func (r *Replica) handleViewChangeRequest(msg *BusViewChangeRequest) {
	if msg.ViewId > r.view() {
		r.startViewChange(msg.ViewId) // sets ViewChange status, multicasts our own
	}
	if msg.ViewId != r.view() {
		return
	}

	r.mu.Lock()
	vc := r.vc
	if vc == nil || vc.view != msg.ViewId || vc.reportSent {
		r.mu.Unlock()
		return
	}
	vc.requests[msg.SenderIdx] = struct{}{}
	// Our own suspicion is already in the set, so this is f+1 counting ourselves.
	// Any configuration that can survive a failure has f >= 1, so the quorum is
	// never reached before some peer's request arrives here.
	if len(vc.requests) < r.config.QuorumSize() {
		r.mu.Unlock()
		return
	}
	vc.reportSent = true
	nreq := len(vc.requests)
	report := r.buildViewChangeLocked(msg.ViewId)
	leader := r.config.LeaderIndex(msg.ViewId)
	r.mu.Unlock()

	Notice("[%s] view %d: %d view-change requests, sending report to leader %d "+
		"(stable=%d executed=%d max_filled=%d)",
		r.self, msg.ViewId, nreq, leader, report.StableSlot, report.NextExpected,
		report.MaxSlotFilled)

	if leader == r.idx {
		r.deliverViewChange(report)
		return
	}
	r.sendToPeer(leader, MsgBusViewChange, report)
}

func (r *Replica) handleViewChange(msg *BusViewChange) {
	if msg.ViewId < r.view() {
		return
	}
	if msg.ViewId > r.view() {
		// We are the leader of a view we have not joined yet.
		r.startViewChange(msg.ViewId)
	}
	r.mu.Lock()
	st, vc := r.status, r.vc
	installed := r.lastNormalView
	r.mu.Unlock()

	if st == statusNormal && r.AmLeader() && installed == msg.ViewId {
		// A straggler joined this view after we had already installed it. It does
		// not need another round, just the result.
		r.sendStartView(int(msg.SenderIdx))
		return
	}
	if vc != nil && vc.view == msg.ViewId {
		r.deliverViewChange(msg)
	}
}

func (r *Replica) deliverViewChange(msg *BusViewChange) {
	r.mu.Lock()
	vc := r.vc
	r.mu.Unlock()
	if vc == nil || vc.view != msg.ViewId {
		return
	}
	select {
	case vc.reports <- msg:
	default:
	}
}

// handleStateQuery answers a replica that has noticed it is in a stale view.
// It gets the current view's StartView rather than triggering another view
// change: there is a healthy leader, the querier just has not heard from it.
func (r *Replica) handleStateQuery(msg *BusStateQuery) {
	r.mu.Lock()
	ok := r.status == statusNormal && r.lastNormalView == r.view()
	r.mu.Unlock()
	if !ok || !r.AmLeader() || msg.ViewId >= r.view() {
		return
	}
	Notice("[%s] replica %d is in stale view %d, sending start view %d",
		r.self, msg.SenderIdx, msg.ViewId, r.view())
	r.sendStartView(int(msg.SenderIdx))
}

// requestCatchUp asks the cluster for the current view's StartView. Multicast
// because we do not know who leads the view we just heard about.
//
// Hearing a higher view is proof the cluster is alive, so it also counts as a
// heartbeat: suspecting a leader we have simply not met yet, and starting a view
// change from our own stale number, would only disrupt a healthy cluster.
func (r *Replica) requestCatchUp(higherView uint64) {
	r.mu.Lock()
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	r.mu.Unlock()
	Warning("[%s] behind: saw view %d while in %d, requesting catch-up",
		r.self, higherView, r.view())
	r.broadcastToPeers(MsgBusStateQuery,
		&BusStateQuery{ViewId: r.view(), SenderIdx: uint32(r.idx)})
}

// buildViewChangeLocked describes this replica's log to the new leader without
// sending any of it: a bitmap of which suffix slots are filled, the agreed
// no-ops among them, and a hash of the committed prefix.
func (r *Replica) buildViewChangeLocked(newView uint64) *BusViewChange {
	m := &BusViewChange{
		SenderIdx:      uint32(r.idx),
		ViewId:         newView,
		LastNormalView: r.lastNormalView,
		NextExpected:   r.nextExpected,
	}
	if r.haveStable {
		m.StableSlot, m.HasStable = r.stableSlot, true
		if h, ok := r.prefixHashAtLocked(r.stableSlot); ok {
			m.PrefixHash = h
		}
	}

	base := suffixBase(r.stableSlot, r.haveStable)
	if base < r.prunedBelow {
		base = r.prunedBelow
	}
	top := base
	if r.nextExpected > base {
		top = r.nextExpected - 1
	}
	if r.haveMax && r.maxSlotSeen > top {
		top = r.maxSlotSeen
	}
	if top < base {
		m.BitmapBase = base
		return m
	}
	// Keep the newest end if the span is somehow enormous; the merge only cares
	// about slots above the highest reported commit point anyway.
	if span := top - base + 1; span > maxBitmapBytes*8 {
		base = top - (maxBitmapBytes*8 - 1)
	}

	m.BitmapBase = base
	m.FilledBitmap = make([]byte, (top-base+8)/8)
	for s := base; s <= top; s++ {
		st := slotEmpty
		if s < r.nextExpected {
			st = slotReceived // executed, so filled by construction
		}
		if e := r.globalLog[s]; e != nil && e.state != slotEmpty {
			st = e.state
		}
		if st == slotEmpty {
			continue
		}
		setBit(m.FilledBitmap, s-base)
		m.MaxSlotFilled, m.HasMax = s, true
		if st == slotNoOp {
			m.NoOpSlots = append(m.NoOpSlots, s)
		}
	}
	return m
}

// suffixBase is the first slot a view change reasons about: everything on and
// below the commit point is settled and never revisited.
func suffixBase(stable uint64, hasStable bool) uint64 {
	if !hasStable {
		return 0
	}
	return stable + 1
}

func setBit(bm []byte, i uint64) {
	if idx := i / 8; idx < uint64(len(bm)) {
		bm[idx] |= 1 << (i % 8)
	}
}

func bitSet(bm []byte, i uint64) bool {
	idx := i / 8
	return idx < uint64(len(bm)) && bm[idx]&(1<<(i%8)) != 0
}

// ── The merge ───────────────────────────────────────────────────────────────

// mergePlan is what the new leader decides the suffix must contain. It names a
// donor per slot rather than carrying entries, so the leader pulls only what it
// is actually missing.
type mergePlan struct {
	stableSlot uint64
	hasStable  bool
	maxSlot    uint64
	hasMax     bool
	noops      []uint64          // sorted; slots in (stableSlot, maxSlot] agreed empty
	donors     map[uint64]uint32 // slot -> a replica known to hold the entry
	catchUp    uint32            // who to pull the committed prefix from
	hasCatchUp bool
}

// mergeSuffix decides the new view's log from metadata alone.
//
// Only reports from the highest LastNormalView count: an entry recorded in an
// older view can contradict a decision made in the newest one, so a stale report
// is discarded outright rather than merged. Among the survivors the committed
// prefix is the highest commit point anyone reports, and above it a slot holds
// an entry if anyone has one — unless anyone reports it as an agreed no-op, in
// which case the no-op wins. That asymmetry matches setNoOpLocked: in this
// protocol an agreed no-op already overwrites a received entry, because a no-op
// slot has produced no client replies and so loses nothing visible, whereas
// resurrecting a slot other replicas have skipped past would.
func mergeSuffix(reports []*BusViewChange) mergePlan {
	var plan mergePlan
	if len(reports) == 0 {
		return plan
	}

	best := reports[0].LastNormalView
	for _, m := range reports {
		if m.LastNormalView > best {
			best = m.LastNormalView
		}
	}
	survivors := make([]*BusViewChange, 0, len(reports))
	for _, m := range reports {
		if m.LastNormalView == best {
			survivors = append(survivors, m)
		}
	}

	var bestNext uint64
	for _, m := range survivors {
		if m.HasStable && (!plan.hasStable || m.StableSlot > plan.stableSlot) {
			plan.stableSlot, plan.hasStable = m.StableSlot, true
		}
		if m.HasMax && (!plan.hasMax || m.MaxSlotFilled > plan.maxSlot) {
			plan.maxSlot, plan.hasMax = m.MaxSlotFilled, true
		}
		if !plan.hasCatchUp || m.NextExpected > bestNext {
			plan.catchUp, plan.hasCatchUp, bestNext = m.SenderIdx, true, m.NextExpected
		}
	}

	base := suffixBase(plan.stableSlot, plan.hasStable)
	if !plan.hasMax || plan.maxSlot < base {
		return plan
	}

	noop := make(map[uint64]struct{})
	for _, m := range survivors {
		for _, s := range m.NoOpSlots {
			if s >= base && s <= plan.maxSlot {
				noop[s] = struct{}{}
			}
		}
	}
	plan.donors = make(map[uint64]uint32)
	for s := base; s <= plan.maxSlot; s++ {
		if _, isNoOp := noop[s]; isNoOp {
			continue
		}
		found := false
		for _, m := range survivors {
			if s >= m.BitmapBase && bitSet(m.FilledBitmap, s-m.BitmapBase) {
				plan.donors[s] = m.SenderIdx
				found = true
				break
			}
		}
		if !found {
			// Nobody in the quorum holds it, so nobody can have committed it.
			// The new leader is free to close the hole.
			noop[s] = struct{}{}
		}
	}
	plan.noops = make([]uint64, 0, len(noop))
	for s := range noop {
		plan.noops = append(plan.noops, s)
	}
	sort.Slice(plan.noops, func(i, j int) bool { return plan.noops[i] < plan.noops[j] })
	return plan
}

// driveViewChange runs on the new leader: collect a quorum of reports, decide
// the merge, make the local log match it, and only then announce the view.
func (r *Replica) driveViewChange(vc *vcState) {
	deadline := time.After(r.viewChangeTimeout)
	reports := make(map[uint32]*BusViewChange)
	for len(reports) < r.config.QuorumSize() {
		select {
		case m := <-vc.reports:
			if m.ViewId == vc.view {
				reports[m.SenderIdx] = m
			}
		case <-vc.abort:
			return
		case <-deadline:
			Warning("[%s] view %d timed out with %d/%d reports, moving on",
				r.self, vc.view, len(reports), r.config.QuorumSize())
			r.startViewChange(vc.view + 1)
			return
		}
	}

	list := make([]*BusViewChange, 0, len(reports))
	for _, m := range reports {
		list = append(list, m)
	}
	plan := mergeSuffix(list)
	Notice("[%s] view %d merge: quorum=%d stable=%d max=%d noops=%d entries_needed=%d",
		r.self, vc.view, len(reports), plan.stableSlot, plan.maxSlot,
		len(plan.noops), len(plan.donors))

	if !r.fetchMergedState(vc, &plan) {
		return
	}

	r.mu.Lock()
	if r.view() != vc.view {
		r.mu.Unlock()
		return
	}
	r.installViewLocked(vc.view, plan.stableSlot, plan.hasStable, plan.noops, false)
	r.vc = nil
	r.mu.Unlock()

	Notice("[%s] VIEW-CHANGE done view=%d leader=self", r.self, vc.view)
	r.broadcastStartView()
}

// fetchMergedState brings the new leader's own log up to the merge before it
// announces anything: the committed prefix first, then every suffix entry it is
// missing. Slots a donor cannot produce become no-ops — the leader is entitled
// to close a hole nobody can fill, and the alternative is a view change that
// never finishes.
func (r *Replica) fetchMergedState(vc *vcState, plan *mergePlan) bool {
	if plan.hasStable && plan.hasCatchUp && int(plan.catchUp) != r.idx {
		r.mu.Lock()
		from := r.nextExpected
		r.mu.Unlock()
		if from <= plan.stableSlot {
			Notice("[%s] view %d: catching up committed prefix [%d,%d] from replica %d",
				r.self, vc.view, from, plan.stableSlot, plan.catchUp)
			r.fetchRangeBlocking(int(plan.catchUp), from, plan.stableSlot)
		}
	}

	for attempt := 0; attempt < mergeFetchAttempts; attempt++ {
		byDonor := make(map[uint32][2]uint64) // donor -> [lo, hi] of what it still owes
		r.mu.Lock()
		for slot, donor := range plan.donors {
			if e := r.globalLog[slot]; e != nil && e.state != slotEmpty {
				continue
			}
			if slot < r.nextExpected {
				continue
			}
			rng, seen := byDonor[donor]
			if !seen {
				byDonor[donor] = [2]uint64{slot, slot}
				continue
			}
			if slot < rng[0] {
				rng[0] = slot
			}
			if slot > rng[1] {
				rng[1] = slot
			}
			byDonor[donor] = rng
		}
		r.mu.Unlock()
		if len(byDonor) == 0 {
			return true
		}
		select {
		case <-vc.abort:
			return false
		default:
		}
		for donor, rng := range byDonor {
			if int(donor) == r.idx {
				continue
			}
			r.fetchRangeBlocking(int(donor), rng[0], rng[1])
		}
	}

	// Whatever is still missing gets closed as a no-op.
	r.mu.Lock()
	var stuck []uint64
	for slot := range plan.donors {
		if e := r.globalLog[slot]; e == nil || e.state == slotEmpty {
			if slot >= r.nextExpected {
				stuck = append(stuck, slot)
			}
		}
	}
	r.mu.Unlock()
	if len(stuck) > 0 {
		Warning("[%s] view %d: %d merged slots could not be fetched, closing them as no-ops",
			r.self, vc.view, len(stuck))
		plan.noops = append(plan.noops, stuck...)
		sort.Slice(plan.noops, func(i, j int) bool { return plan.noops[i] < plan.noops[j] })
	}
	return true
}

// broadcastStartView announces the installed view. It carries no entries: the
// committed prefix and its hash, how far the merged suffix runs, and which of
// those slots are no-ops. Anything a follower is missing it pulls afterwards.
func (r *Replica) broadcastStartView() {
	msg := r.startViewMsg()
	if msg == nil {
		return
	}
	for j := range r.config.Replicas {
		if j == r.idx {
			continue
		}
		r.sendToPeer(j, MsgBusStartView, msg)
	}
}

func (r *Replica) sendStartView(peer int) {
	if msg := r.startViewMsg(); msg != nil {
		r.sendToPeer(peer, MsgBusStartView, msg)
	}
}

func (r *Replica) startViewMsg() *BusStartView {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != statusNormal {
		return nil
	}
	msg := &BusStartView{
		ViewId:    r.view(),
		SenderIdx: uint32(r.idx),
	}
	if r.haveStable {
		msg.StableSlot, msg.HasStable = r.stableSlot, true
		if h, ok := r.prefixHashAtLocked(r.stableSlot); ok {
			msg.PrefixHash = h
		}
	}
	if r.nextExpected > 0 {
		msg.MaxSlot, msg.HasMax = r.nextExpected-1, true
	}
	base := suffixBase(r.stableSlot, r.haveStable)
	for s := base; msg.HasMax && s <= msg.MaxSlot; s++ {
		if e := r.globalLog[s]; e != nil && e.state == slotNoOp {
			msg.NoOpSlots = append(msg.NoOpSlots, s)
		}
	}
	return msg
}

// handleStartView queues the install. It must not run inline: installing waits
// on a state transfer from the leader, and the reply to that transfer arrives on
// this very connection — blocking the reader here would deadlock against it.
func (r *Replica) handleStartView(msg *BusStartView) {
	cp := *msg
	select {
	case r.startViewQ <- &cp:
	default:
		Warning("[%s] start-view queue full, dropping view %d", r.self, msg.ViewId)
	}
}

// viewInstallLoop serialises view installs so two StartViews cannot rewind the
// log underneath each other.
func (r *Replica) viewInstallLoop() {
	for msg := range r.startViewQ {
		r.installStartView(msg)
	}
}

// installStartView installs a decided view. The replica may be mid-view-change,
// or simply stale and answering its own catch-up query; the difference is
// whether it took part in deciding this view, which is also what decides how
// much of its speculative work it has to give back.
func (r *Replica) installStartView(msg *BusStartView) {
	r.mu.Lock()
	view := r.view()
	if msg.ViewId < view ||
		(msg.ViewId == view && r.status == statusNormal && r.lastNormalView == view) {
		r.mu.Unlock()
		return
	}
	// Having sent a report for exactly this view means we know our suffix was
	// part of the merge, so the no-op list is a complete account of how the
	// result can differ from what we executed. Otherwise — a stale replica being
	// caught up — nothing above our own commit point can be trusted.
	participated := msg.ViewId == view && r.status == statusViewChange
	if r.vc != nil {
		close(r.vc.abort)
		r.vc = nil
	}
	r.sync = nil
	r.gaps = make(map[gapKey]*gapState)
	r.status = statusViewChange
	r.viewId.Store(msg.ViewId)
	r.drainPendingBusesLocked()
	behindBy := int64(0)
	if msg.HasStable && r.nextExpected <= msg.StableSlot {
		behindBy = int64(msg.StableSlot) - int64(r.nextExpected) + 1
	}
	from := r.nextExpected
	r.mu.Unlock()

	// The committed prefix is not optional — nothing may execute past a hole in
	// it — so it is fetched before the view is installed, unlike the suffix.
	if behindBy > 0 {
		Notice("[%s] view %d: fetching committed prefix [%d,%d] from leader %d",
			r.self, msg.ViewId, from, msg.StableSlot, msg.SenderIdx)
		r.fetchRangeBlocking(int(msg.SenderIdx), from, msg.StableSlot)
	}

	r.mu.Lock()
	if r.view() != msg.ViewId {
		r.mu.Unlock()
		return
	}
	if participated && msg.HasStable && r.haveStable && r.stableSlot >= msg.StableSlot {
		if h, ok := r.prefixHashAtLocked(msg.StableSlot); ok && h != msg.PrefixHash {
			Warning("[%s] PREFIX MISMATCH installing view %d at slot=%d (ours=%016x leader=%016x)",
				r.self, msg.ViewId, msg.StableSlot, h, msg.PrefixHash)
			participated = false
		}
	}
	rewound, didRewind := r.installViewLocked(
		msg.ViewId, msg.StableSlot, msg.HasStable, msg.NoOpSlots, !participated)
	next, top := r.nextExpected, msg.MaxSlot
	r.mu.Unlock()

	if didRewind {
		Notice("[%s] view %d: rewound cursor to slot %d", r.self, msg.ViewId, rewound)
	}
	Notice("[%s] VIEW-CHANGE done view=%d leader=%d executed=%d",
		r.self, msg.ViewId, msg.SenderIdx, next)

	// Everything else is lazy: the cursor is already running, and it advances
	// as each chunk of the suffix lands.
	if msg.HasMax && next <= top {
		r.enqueueFetch(int(msg.SenderIdx), next, top, nil)
	}
}

// installViewLocked makes the local log agree with a decided view and returns
// the replica to normal operation.
//
// conservative is for a replica that did not take part in deciding this view:
// it cannot tell which of its speculative slots the merge contradicts, so it
// gives back everything above its own commit point and refetches. A participant
// only has to undo slots the merge turned into no-ops, which is almost always
// none of them — entries are content-determined by (clientId, busSeq) at a slot,
// so two replicas holding the same slot hold the same bus.
func (r *Replica) installViewLocked(view, stable uint64, hasStable bool,
	noops []uint64, conservative bool) (rewound uint64, didRewind bool) {

	target := r.nextExpected
	if conservative {
		// Back to *our* commit point, not the leader's. Ours is the last slot we
		// know a quorum agreed with us on; between it and the leader's, higher,
		// commit point our content was never checked against anyone — and on
		// this path there is no no-op list to tell us where it diverges. Stable
		// slots only ever move forward, so the leader's log agrees with ours
		// through our commit point and everything above it can be refetched.
		if base := suffixBase(r.stableSlot, r.haveStable); r.haveStable && base < target {
			target = base
		} else if !r.haveStable {
			target = 0
		}
	} else {
		for _, s := range noops {
			if s >= r.nextExpected {
				break
			}
			if e := r.globalLog[s]; e != nil && e.state == slotReceived && s < target {
				target = s
			}
		}
	}

	if target < r.nextExpected {
		if r.rewindToLocked(target) {
			rewound, didRewind = target, true
			if conservative {
				r.clearSlotsAboveLocked(target)
			}
		} else {
			Warning("[%s] cannot rewind to slot %d: outside the hash window", r.self, target)
		}
	}

	for _, s := range noops {
		if s < r.nextExpected {
			continue
		}
		if r.setNoOpLocked(s) {
			r.winNoops++
		}
	}

	r.viewId.Store(view)
	r.lastNormalView = view
	r.status = statusNormal
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	// A new view owes the client a reply for every slot above the commit point,
	// in two ranges. What we had already executed was replied to under the old
	// leader and has to be said again; what we had not is drained now and
	// replied to for the first time. Both are stamped with the new view, since
	// viewId is already stored.
	replayFrom, replayTo := suffixBase(r.stableSlot, r.haveStable), r.nextExpected
	if replayFrom < r.prunedBelow {
		replayFrom = r.prunedBelow
	}
	// Unfrozen: this drains every slot recorded during the view change and, for
	// entries the merge handed us that we had never executed, sends the client
	// the reply it never got before the crash.
	r.advanceNextExpectedLocked()
	r.replayRepliesLocked(replayFrom, replayTo)
	if hasStable {
		r.setStableLocked(stable)
	}
	return rewound, didRewind
}

// replayRepliesLocked re-sends the replies for slots this replica had already
// executed above the commit point, now stamped with the new view.
//
// A client counts a request committed on f+1 replies that agree, and requires
// one of them to come from the leader of the view they were stamped with. A
// replica goes on executing for as long as it takes to notice the leader is gone
// — one one-way delay plus a suspicion tick, ~40ms on our testbed — and every
// reply it sends in that window names a leader that is already dead, so those
// replies can never add up. Without this the requests in them are stranded until
// the client's own request timeout re-boards them seconds later, and that does
// not show up in the recovery gap at all: the client is open-loop, so it goes on
// committing newer requests while they wait.
//
// The commit point is the exact floor. A slot at or below it was acked by f+1
// replicas, so the old leader had executed it, and a reply it enqueued before
// dying is still delivered — the kernel flushes what was written before the FIN.
// Above the commit point there is no such guarantee.
//
// Repeats are harmless: the client counts replies in a per-replica bitmask keyed
// by log index and view, so a request that did commit just sees a duplicate.
func (r *Replica) replayRepliesLocked(from, to uint64) {
	if from >= to {
		return
	}
	n := 0
	for s := from; s < to; s++ {
		e := r.globalLog[s]
		if e == nil || e.state != slotReceived {
			continue
		}
		for i := range e.requests {
			req := &e.requests[i]
			li, ok := r.dedup[reqKey{req.ClientId, req.RequestId}]
			if !ok {
				// First assigned below the prune floor, which never rises past the
				// commit point: the request committed long ago.
				continue
			}
			r.enqueueReply(req.ClientId, req.RequestId, s, li)
			n++
		}
	}
	if n > 0 {
		Notice("[%s] view %d: replayed %d replies for executed slots [%d,%d)",
			r.self, r.view(), n, from, to)
	}
}

// rewindToLocked moves the cursor back to target, undoing speculative execution
// above it. The request log list shrinks to the length it had, and every dedup
// entry first assigned up there is released, so re-execution hands out exactly
// the same log indexes again — which is what lets a client's votes for a request
// still add up after a view change. A request first assigned an index below
// target keeps it, which is exactly what re-boarding needs.
//
// The slots being walked are all above the commit point, and the prune floor
// never rises past that, so their request lists are still resident.
func (r *Replica) rewindToLocked(target uint64) bool {
	if target >= r.nextExpected {
		return true
	}
	hash, logIdx, ok := r.prefixStateAtLocked(target)
	if !ok {
		return false
	}
	for s := target; s < r.nextExpected; s++ {
		e := r.globalLog[s]
		if e == nil {
			continue
		}
		for i := range e.requests {
			req := &e.requests[i]
			key := reqKey{req.ClientId, req.RequestId}
			if li, seen := r.dedup[key]; seen && li >= logIdx {
				delete(r.dedup, key)
			}
		}
	}
	r.nextExpected = target
	r.prefixHash = hash
	r.nextLogIndex = logIdx
	return true
}

// clearSlotsAboveLocked discards everything from slot upward so it can be
// refetched from the leader. Only used on the conservative path, where this
// replica cannot tell which of its own entries the new view contradicts.
func (r *Replica) clearSlotsAboveLocked(from uint64) {
	if !r.haveMax {
		return
	}
	for s := from; s <= r.maxSlotSeen; s++ {
		if e := r.globalLog[s]; e != nil {
			r.releaseEntryBytesLocked(e)
		}
		delete(r.globalLog, s)
	}
}

// ── State transfer ──────────────────────────────────────────────────────────

type fetchReq struct {
	peer int
	from uint64
	to   uint64
	done chan struct{}
}

func (r *Replica) enqueueFetch(peer int, from, to uint64, done chan struct{}) {
	if peer == r.idx || from > to {
		if done != nil {
			close(done)
		}
		return
	}
	select {
	case r.fetchQ <- fetchReq{peer: peer, from: from, to: to, done: done}:
	default:
		Warning("[%s] state-fetch queue full, dropping request [%d,%d]", r.self, from, to)
		if done != nil {
			close(done)
		}
	}
}

func (r *Replica) fetchRangeBlocking(peer int, from, to uint64) {
	done := make(chan struct{})
	r.enqueueFetch(peer, from, to, done)
	<-done
}

// stateFetchLoop owns every inbound state transfer. Keeping it on one goroutine
// off r.mu means a multi-megabyte catch-up cannot stall the ordering lock, and
// the sync path, the view-change path and the lazy suffix catch-up all queue
// through the same place.
func (r *Replica) stateFetchLoop() {
	for req := range r.fetchQ {
		r.runFetch(req)
		if req.done != nil {
			close(req.done)
		}
	}
}

// runFetch pulls one slot range, a chunk at a time.
//
// It gives up the moment the peer's connection breaks rather than waiting out
// stateFetchTimeout. A dead peer's reply is never coming, and every fetch shares
// this one goroutine — so a range left blocking on a corpse also blocks whatever
// is queued behind it. That is not hypothetical: a replica that has fallen
// behind is mid-catch-up *from the leader*, so when the leader dies its own
// view change queues behind a five-second wait for the replica it has just
// declared dead. Measured at 5.12s of a 6.9s view change.
func (r *Replica) runFetch(req fetchReq) {
	probe := time.NewTicker(stateFetchProbe)
	defer probe.Stop()
	next := req.from
	for next <= req.to {
		r.sendToPeer(req.peer, MsgBusGetState, &BusGetState{
			ViewId:    r.view(),
			FromSlot:  next,
			ToSlot:    req.to,
			SenderIdx: uint32(r.idx),
		})
		deadline := time.After(stateFetchTimeout)
		for advanced := false; !advanced; {
			select {
			case m := <-r.newStateCh:
				if m.FromSlot != next {
					continue // a reply to an earlier, abandoned request
				}
				r.applyStateEntries(m)
				if m.ToSlot < next {
					return // responder made no progress; give up on this range
				}
				next = m.ToSlot + 1
				advanced = true
			case <-probe.C:
				if !r.peerConnected(req.peer) {
					Warning("[%s] state fetch [%d,%d] from replica %d abandoned at slot %d: "+
						"peer connection lost", r.self, req.from, req.to, req.peer, next)
					return
				}
			case <-deadline:
				Warning("[%s] state fetch [%d,%d] from replica %d timed out at slot %d",
					r.self, req.from, req.to, req.peer, next)
				return
			}
		}
	}
}

func (r *Replica) handleNewState(msg *BusNewState) {
	select {
	case r.newStateCh <- msg:
	default:
		Warning("[%s] dropping unsolicited state transfer [%d,%d]",
			r.self, msg.FromSlot, msg.ToSlot)
	}
}

func (r *Replica) applyStateEntries(m *BusNewState) {
	if len(m.Entries) == 0 {
		return
	}
	r.mu.Lock()
	for i := range m.Entries {
		e := &m.Entries[i]
		if e.IsNoOp {
			if r.setNoOpLocked(e.Slot) {
				r.durableAppendLocked(e.Slot, nil, true)
				r.winNoops++
			}
			continue
		}
		if r.storeRecoveredLocked(e.Slot, e.ClientId, e.ReqId, e.Payload, e.IsBus) {
			r.durableAppendLocked(e.Slot, e.Payload, false)
			r.winRecovered++
		}
	}
	r.advanceNextExpectedLocked()
	r.mu.Unlock()
}

func (r *Replica) handleGetState(msg *BusGetState) {
	cp := *msg
	select {
	case r.serveQ <- &cp:
	default:
		Warning("[%s] state-serve queue full, dropping request from replica %d",
			r.self, msg.SenderIdx)
	}
}

// stateServeLoop answers BusGetState off the connection goroutines: a reply can
// mean reading the durable log and marshalling a megabyte, neither of which
// belongs on a reader or under r.mu.
func (r *Replica) stateServeLoop() {
	for req := range r.serveQ {
		r.serveState(req)
	}
}

func (r *Replica) serveState(req *BusGetState) {
	reply := &BusNewState{
		ViewId:    r.view(),
		FromSlot:  req.FromSlot,
		SenderIdx: uint32(r.idx),
	}
	last := req.FromSlot
	size := 0
	for slot := req.FromSlot; slot <= req.ToSlot; slot++ {
		if ent, ok := r.readSlot(slot); ok {
			reply.Entries = append(reply.Entries, ent)
			size += len(ent.Payload) + stateEntryOverhead
		}
		last = slot
		if size >= stateChunkBytes || len(reply.Entries) >= maxStateEntries {
			break
		}
	}
	reply.ToSlot = last
	r.sendToPeer(int(req.SenderIdx), MsgBusNewState, reply)
}

// readSlot returns one slot's content for a peer, from memory when it is still
// resident and from the durable log when it has been reclaimed.
//
// Only slots at or below the commit point are ever read from disk. Above it a
// slot can still be rewound, and a rewind does not rewrite history on disk — so
// the durable log is authoritative exactly where it can no longer change.
func (r *Replica) readSlot(slot uint64) (StateEntry, bool) {
	r.mu.Lock()
	if e := r.globalLog[slot]; e != nil && e.state != slotEmpty {
		ent := StateEntry{
			Slot:     slot,
			ClientId: e.clientId,
			ReqId:    e.reqId,
			IsNoOp:   e.state == slotNoOp,
		}
		if !e.ownerSet {
			m := r.slotOwnerLocked(slot)
			ent.ClientId, ent.ReqId = m.clientId, m.reqId
		}
		if !ent.IsNoOp {
			ent.Payload, ent.IsBus = r.slotGapPayloadLocked(slot)
		}
		r.mu.Unlock()
		return ent, true
	}
	reclaimed := slot < r.prunedBelow
	settled := r.haveStable && slot <= r.stableSlot
	r.mu.Unlock()

	if !reclaimed || !settled {
		return StateEntry{}, false
	}
	return r.readSlotFromDisk(slot)
}
