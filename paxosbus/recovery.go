package paxosbus

import (
	"sort"
	"time"
)

const (
	suspicionTick = 5 * time.Millisecond

	stateChunkBytes    = 1 << 20
	stateEntryOverhead = 32
	stateFetchSlots    = 1024

	stateFetchTimeout  = 5 * time.Second
	stateFetchProbe    = 50 * time.Millisecond
	mergeFetchAttempts = 3

	recoveryRetryDelay = 100 * time.Millisecond
)

type viewRecovery struct {
	view         uint64
	generation   uint64
	leader       int
	abort        chan struct{}
	stable       uint64
	hasStable    bool
	maxSlot      uint64
	hasMax       bool
	replayFrom   uint64
	replayTo     uint64
	verifyPrefix bool
	prefixHash   uint64
	repairFrom   uint64
}

type syncRound struct {
	prepare BusSyncPrepare
	acks    map[uint32]struct{}
	done    bool
}

func (r *Replica) syncLoop() {
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for range ticker.C {
		r.syncOnce()
	}
}

func (r *Replica) syncOnce() {
	var (
		prep   BusSyncPrepare
		commit *BusSyncCommit
	)
	r.mu.Lock()
	if !r.AmLeader() || r.status != statusNormal {
		r.sync = nil
		r.mu.Unlock()
		return
	}
	view := r.view()
	if r.sync != nil && r.sync.prepare.ViewId != view {
		r.sync = nil
	}
	if r.sync != nil && !r.sync.done {
		prep = r.sync.prepare
	} else {
		prep = BusSyncPrepare{ViewId: view, SenderIdx: uint32(r.idx)}
		if r.nextExpected > 0 {
			slot := r.nextExpected - 1
			if h, ok := r.prefixHashAtLocked(slot); ok {
				prep.SlotToSync, prep.HasSlot, prep.PrefixHash = slot, true, h
				r.sync = &syncRound{
					prepare: prep,
					acks:    map[uint32]struct{}{uint32(r.idx): {}},
				}
				commit = r.maybeCommitSyncLocked()
			}
		}
		if !prep.HasSlot {
			r.sync = nil
		}
	}
	r.mu.Unlock()

	r.broadcastToPeers(MsgBusSyncPrepare, &prep)
	if commit != nil {
		r.broadcastToPeers(MsgBusSyncCommit, commit)
	}
}

func (r *Replica) maybeCommitSyncLocked() *BusSyncCommit {
	s := r.sync
	if s == nil || s.done || len(s.acks) < r.config.QuorumSize() {
		return nil
	}
	s.done = true
	r.setStableLocked(s.prepare.SlotToSync)
	return &BusSyncCommit{
		ViewId:     s.prepare.ViewId,
		StableSlot: s.prepare.SlotToSync,
		SenderIdx:  uint32(r.idx),
	}
}

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
	if !r.validReplicaIndex(msg.SenderIdx) ||
		int(msg.SenderIdx) != r.config.LeaderIndex(msg.ViewId) {
		return
	}
	if msg.ViewId < r.view() {
		return
	}
	if msg.ViewId > r.view() {
		r.requestCatchUp(msg.ViewId)
		return
	}

	r.mu.Lock()
	if msg.ViewId != r.view() {
		r.mu.Unlock()
		return
	}
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	if r.status != statusNormal || !msg.HasSlot {
		r.mu.Unlock()
		return
	}
	if r.nextExpected == 0 || r.nextExpected-1 < msg.SlotToSync {
		leader := int(msg.SenderIdx)
		c := &r.syncCatchup
		var req *fetchReq
		if !c.active || c.view != msg.ViewId || c.leader != leader {
			r.clearSyncCatchupLocked()
			c = &r.syncCatchup
			c.view = msg.ViewId
			c.leader = leader
			c.target = msg.SlotToSync
			c.prepare = *msg
			c.active = true
			fetch := r.newSyncFetchLocked(r.nextExpected, c.target)
			req = &fetch
		} else if msg.SlotToSync > c.target {
			c.target = msg.SlotToSync
			c.prepare = *msg
		}
		r.mu.Unlock()
		if req != nil {
			r.enqueueSyncFetch(*req)
		}
		return
	}
	h, known := r.prefixHashAtLocked(msg.SlotToSync)
	stable, haveStable := r.stableSlot, r.haveStable
	r.mu.Unlock()

	r.answerSyncPrepare(*msg, h, known, stable, haveStable)
}

func (r *Replica) answerSyncPrepare(msg BusSyncPrepare, h uint64, known bool,
	stable uint64, haveStable bool) {
	r.mu.Lock()
	current := r.status == statusNormal && msg.ViewId == r.view() &&
		int(msg.SenderIdx) == r.config.LeaderIndex(msg.ViewId)
	r.mu.Unlock()
	if !current {
		return
	}

	switch {
	case !known:
		Notice("[%s] sync prepare slot=%d below hash window, agreeing unverified",
			r.self, msg.SlotToSync)
	case h != msg.PrefixHash:
		Warning("[%s] PREFIX MISMATCH at slot=%d (ours=%016x leader=%016x) — rewinding to stable and refetching",
			r.self, msg.SlotToSync, h, msg.PrefixHash)
		r.rewindToStableAndRefetch(int(msg.SenderIdx), msg.ViewId, stable, haveStable)
		return
	}

	r.sendToPeer(int(msg.SenderIdx), MsgBusSyncReply,
		&BusSyncReply{ViewId: msg.ViewId, Slot: msg.SlotToSync, SenderIdx: uint32(r.idx)})
}

func (r *Replica) handleSyncReply(msg *BusSyncReply) {
	if !r.validReplicaIndex(msg.SenderIdx) || int(msg.SenderIdx) == r.idx {
		return
	}
	var commit *BusSyncCommit
	r.mu.Lock()
	if s := r.sync; s != nil && !s.done && s.prepare.ViewId == msg.ViewId &&
		s.prepare.SlotToSync == msg.Slot {
		s.acks[msg.SenderIdx] = struct{}{}
		commit = r.maybeCommitSyncLocked()
	}
	r.mu.Unlock()
	if commit != nil {
		r.broadcastToPeers(MsgBusSyncCommit, commit)
	}
}

func (r *Replica) clearSyncCatchupLocked() {
	generation := r.syncCatchup.generation + 1
	r.syncCatchup = syncCatchup{generation: generation}
}

func (r *Replica) handleSyncCommit(msg *BusSyncCommit) {
	if !r.validReplicaIndex(msg.SenderIdx) {
		return
	}
	r.mu.Lock()
	if r.status == statusNormal && msg.ViewId == r.view() &&
		int(msg.SenderIdx) == r.config.LeaderIndex(msg.ViewId) {
		r.setStableLocked(msg.StableSlot)
		r.lastHeartbeatNs = nowNs()
		r.leaderLost = false
	}
	r.mu.Unlock()
}

// Rewind to the local commit point before refetching divergent state.
func (r *Replica) rewindToStableAndRefetch(peer int, view, stable uint64, haveStable bool) {
	if !haveStable {
		return
	}
	r.mu.Lock()
	if r.status != statusNormal || r.view() != view ||
		r.config.LeaderIndex(view) != peer {
		r.mu.Unlock()
		return
	}
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

func (r *Replica) suspicionLoop() {
	ticker := time.NewTicker(suspicionTick)
	defer ticker.Stop()
	for range ticker.C {
		r.suspectLeaderIfTimedOut()
	}
}

func (r *Replica) suspectLeaderIfTimedOut() bool {
	r.mu.Lock()
	view := r.view()
	if r.status != statusNormal || r.config.LeaderIndex(view) == r.idx {
		r.mu.Unlock()
		return false
	}

	// Only missed heartbeats trigger suspicion; a closed socket may reconnect.
	silentFor := time.Duration(nowNs() - r.lastHeartbeatNs)
	if silentFor < r.suspectTimeout {
		r.mu.Unlock()
		return false
	}
	lost := r.leaderLost
	start := r.beginViewChangeLocked(view + 1)
	r.mu.Unlock()
	if start == nil {
		return false
	}
	sock := "socket up"
	if lost {
		sock = "socket already closed"
	}
	Warning("[%s] SUSPECT leader %d (heartbeat timeout, silent for %v, %s)",
		r.self, r.config.LeaderIndex(view), silentFor.Truncate(time.Millisecond), sock)
	r.publishViewChange(start)
	return true
}

type vcState struct {
	view    uint64
	reports chan *BusViewChange
	abort   chan struct{}

	requests   map[uint32]struct{}
	reportSent bool
}

type viewChangeWatchdog struct {
	view       uint64
	generation uint64
	timer      *time.Timer
}

type viewChangeStart struct {
	view      uint64
	leader    int
	stable    uint64
	executed  uint64
	maxFilled uint64
	vc        *vcState
}

func newVCState(view uint64, n int) *vcState {
	return &vcState{
		view:     view,
		reports:  make(chan *BusViewChange, n),
		abort:    make(chan struct{}),
		requests: make(map[uint32]struct{}, n),
	}
}

func (r *Replica) cancelRecoveryLocked() {
	if r.recovery == nil {
		return
	}
	close(r.recovery.abort)
	r.recovery = nil
}

func (r *Replica) cancelViewChangeWatchdogLocked() {
	watchdog := r.viewChangeWatchdog
	if watchdog == nil {
		return
	}
	r.viewChangeWatchdog = nil
	watchdog.timer.Stop()
}

func (r *Replica) armViewChangeWatchdogLocked(view uint64) {
	r.cancelViewChangeWatchdogLocked()
	r.viewChangeWatchdogGen++
	watchdog := &viewChangeWatchdog{
		view:       view,
		generation: r.viewChangeWatchdogGen,
	}
	r.viewChangeWatchdog = watchdog
	watchdog.timer = time.AfterFunc(r.viewChangeFallbackTimeout, func() {
		r.expireViewChangeWatchdog(watchdog)
	})
}

func (r *Replica) beginViewChangeLocked(newView uint64) *viewChangeStart {
	if newView <= r.view() {
		return nil
	}
	if r.vc != nil {
		close(r.vc.abort)
		r.vc = nil
	}
	r.cancelRecoveryLocked()
	r.viewId.Store(newView)
	r.status = statusViewChange
	r.leaderLost = false
	r.sync = nil
	r.clearSyncCatchupLocked()
	r.lastHeartbeatNs = nowNs()
	r.armViewChangeWatchdogLocked(newView)
	r.cancelGapsLocked()
	r.drainPendingBusesLocked()
	leader := r.config.LeaderIndex(newView)
	vc := newVCState(newView, r.config.N)
	vc.requests[uint32(r.idx)] = struct{}{} // our own suspicion is one request
	r.vc = vc
	return &viewChangeStart{
		view:      newView,
		leader:    leader,
		stable:    r.stableSlot,
		executed:  r.nextExpected,
		maxFilled: r.maxSlotSeen,
		vc:        vc,
	}
}

func (r *Replica) publishViewChange(start *viewChangeStart) {
	Notice("[%s] VIEW-CHANGE start view=%d new_leader=%d stable=%d executed=%d max_filled=%d",
		r.self, start.view, start.leader, start.stable, start.executed, start.maxFilled)
	r.pauseClients(start.view)

	r.broadcastToPeers(MsgBusViewChangeRequest,
		&BusViewChangeRequest{ViewId: start.view, SenderIdx: uint32(r.idx)})

	if start.leader == r.idx {
		go r.driveViewChange(start.vc)
	}
}

func (r *Replica) startViewChange(newView uint64) {
	r.mu.Lock()
	start := r.beginViewChangeLocked(newView)
	r.mu.Unlock()
	if start != nil {
		r.publishViewChange(start)
	}
}

func (r *Replica) expireViewChangeWatchdog(watchdog *viewChangeWatchdog) {
	r.mu.Lock()
	if r.viewChangeWatchdog != watchdog ||
		r.viewChangeWatchdogGen != watchdog.generation ||
		r.view() != watchdog.view || r.status != statusViewChange {
		r.mu.Unlock()
		return
	}
	start := r.beginViewChangeLocked(watchdog.view + 1)
	r.mu.Unlock()
	if start == nil {
		return
	}
	Warning("[%s] view-change fallback expired for view %d, moving to view %d",
		r.self, watchdog.view, start.view)
	r.publishViewChange(start)
}

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
		r.startViewChange(msg.ViewId)
	}
	r.mu.Lock()
	st, vc := r.status, r.vc
	installed := r.lastNormalView
	r.mu.Unlock()

	if st == statusNormal && r.AmLeader() && installed == msg.ViewId {
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

type mergePlan struct {
	sourceNormalView uint64
	stableSlot       uint64
	hasStable        bool
	maxSlot          uint64
	hasMax           bool
	noops            []uint64          // sorted; slots in (stableSlot, maxSlot] agreed empty
	donors           map[uint64]uint32 // slot -> a replica known to hold the entry
	selected         []uint32          // reports retained at the highest LastNormalView
	catchUp          uint32            // who to pull the committed prefix from
	hasCatchUp       bool
}

// Merge only reports from the highest LastNormalView; agreed no-ops win.
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
	plan.sourceNormalView = best
	survivors := make([]*BusViewChange, 0, len(reports))
	for _, m := range reports {
		if m.LastNormalView == best {
			survivors = append(survivors, m)
			plan.selected = append(plan.selected, m.SenderIdx)
		}
	}
	sort.Slice(plan.selected, func(i, j int) bool { return plan.selected[i] < plan.selected[j] })

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

	r.mu.Lock()
	watchdog := r.viewChangeWatchdog
	active := r.mergeActiveLocked(vc, watchdog)
	r.mu.Unlock()
	if !active || !r.fetchMergedState(vc, watchdog, &plan) {
		return
	}
	canonicalMax, hasCanonical := mergeCanonicalBoundary(&plan)

	r.mu.Lock()
	if !r.mergeActiveLocked(vc, watchdog) ||
		!r.committedPrefixCompleteLocked(plan.stableSlot, plan.hasStable) {
		r.mu.Unlock()
		return
	}
	r.startViewView = vc.view
	r.startViewSource = plan.sourceNormalView
	r.startViewUsed = append(r.startViewUsed[:0], plan.selected...)
	r.installCanonicalViewLocked(vc.view, plan.stableSlot, plan.hasStable,
		canonicalMax, hasCanonical, plan.noops, false)
	msg := r.initialStartViewMsgLocked(vc.view, &plan, canonicalMax, hasCanonical)
	r.mu.Unlock()

	// Keep the ViewChange fence while multicasting the selected merge.
	r.broadcastStartView(msg)

	r.mu.Lock()
	if !r.mergeActiveLocked(vc, watchdog) ||
		!r.committedPrefixCompleteLocked(plan.stableSlot, plan.hasStable) {
		r.mu.Unlock()
		return
	}
	r.vc = nil
	r.publishNormalViewLocked(vc.view)
	executed := r.nextExpected
	r.mu.Unlock()

	Notice("[%s] VIEW-CHANGE done view=%d leader=self executed=%d", r.self, vc.view, executed)
}

func mergeCanonicalBoundary(plan *mergePlan) (uint64, bool) {
	var maxSlot uint64
	hasMax := false
	if plan.hasStable {
		maxSlot, hasMax = plan.stableSlot, true
	}
	if plan.hasMax && (!hasMax || plan.maxSlot > maxSlot) {
		maxSlot, hasMax = plan.maxSlot, true
	}
	return maxSlot, hasMax
}

func (r *Replica) mergeActiveLocked(vc *vcState, watchdog *viewChangeWatchdog) bool {
	if vc == nil || watchdog == nil {
		return false
	}
	select {
	case <-vc.abort:
		return false
	default:
	}
	return r.vc == vc && r.view() == vc.view && r.status == statusViewChange &&
		r.viewChangeWatchdog == watchdog &&
		r.viewChangeWatchdogGen == watchdog.generation && watchdog.view == vc.view
}

func (r *Replica) mergeActive(vc *vcState, watchdog *viewChangeWatchdog) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mergeActiveLocked(vc, watchdog)
}

func (r *Replica) committedPrefixMissingLocked(stable uint64, hasStable bool) (uint64, bool) {
	if !hasStable || r.nextExpected > stable {
		return 0, false
	}
	for slot := r.nextExpected; ; slot++ {
		if entry := r.globalLog[slot]; entry == nil || entry.state == slotEmpty {
			return slot, true
		}
		if slot == stable {
			return 0, false
		}
	}
}

func (r *Replica) committedPrefixCompleteLocked(stable uint64, hasStable bool) bool {
	_, missing := r.committedPrefixMissingLocked(stable, hasStable)
	return !missing
}

func (r *Replica) waitMergeRetry(vc *vcState, watchdog *viewChangeWatchdog) bool {
	if !r.mergeActive(vc, watchdog) {
		return false
	}
	timer := time.NewTimer(recoveryRetryDelay)
	defer timer.Stop()
	select {
	case <-vc.abort:
		return false
	case <-timer.C:
		return r.mergeActive(vc, watchdog)
	}
}

func (r *Replica) fetchMergedState(vc *vcState, watchdog *viewChangeWatchdog,
	plan *mergePlan) bool {

	if plan.hasStable {
		for {
			r.mu.Lock()
			active := r.mergeActiveLocked(vc, watchdog)
			from, missing := r.committedPrefixMissingLocked(plan.stableSlot, true)
			r.mu.Unlock()
			if !active {
				return false
			}
			if !missing {
				break
			}

			if plan.hasCatchUp && int(plan.catchUp) != r.idx {
				Notice("[%s] view %d: catching up committed prefix [%d,%d] from replica %d",
					r.self, vc.view, from, plan.stableSlot, plan.catchUp)
				r.fetchRangeBlocking(vc, int(plan.catchUp), from, plan.stableSlot, nil)
			} else {
				Warning("[%s] view %d: committed prefix [%d,%d] is missing with no remote donor",
					r.self, vc.view, from, plan.stableSlot)
			}

			r.mu.Lock()
			active = r.mergeActiveLocked(vc, watchdog)
			complete := r.committedPrefixCompleteLocked(plan.stableSlot, true)
			r.mu.Unlock()
			if !active {
				return false
			}
			if complete {
				break
			}
			if !r.waitMergeRetry(vc, watchdog) {
				return false
			}
		}
	}

	for attempt := 0; attempt < mergeFetchAttempts; attempt++ {
		if !r.mergeActive(vc, watchdog) {
			return false
		}
		byDonor := make(map[uint32][]uint64)
		r.mu.Lock()
		for slot, donor := range plan.donors {
			if e := r.globalLog[slot]; e != nil && e.state != slotEmpty {
				continue
			}
			if slot < r.nextExpected {
				continue
			}
			byDonor[donor] = append(byDonor[donor], slot)
		}
		r.mu.Unlock()
		if len(byDonor) == 0 {
			return r.mergeActive(vc, watchdog)
		}
		for donor, slots := range byDonor {
			if int(donor) == r.idx {
				continue
			}
			sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
			r.fetchRangeBlocking(vc, int(donor), slots[0], slots[len(slots)-1], slots)
			if !r.mergeActive(vc, watchdog) {
				return false
			}
		}
	}

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
	return r.mergeActive(vc, watchdog)
}

func (r *Replica) broadcastStartView(msg *BusStartView) {
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

func (r *Replica) initialStartViewMsgLocked(view uint64, plan *mergePlan,
	maxSlot uint64, hasMax bool) *BusStartView {
	msg := &BusStartView{
		ViewId:           view,
		SourceNormalView: plan.sourceNormalView,
		SenderIdx:        uint32(r.idx),
		MaxSlot:          maxSlot,
		HasMax:           hasMax,
		SelectedReports:  append([]uint32(nil), plan.selected...),
		NoOpSlots:        append([]uint64(nil), plan.noops...),
	}
	if plan.hasStable {
		msg.StableSlot, msg.HasStable = plan.stableSlot, true
		if h, ok := r.prefixHashAtLocked(plan.stableSlot); ok {
			msg.PrefixHash = h
		}
	}
	return msg
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
		ViewId:           r.view(),
		SourceNormalView: r.startViewSource,
		SenderIdx:        uint32(r.idx),
		SelectedReports:  append([]uint32(nil), r.startViewUsed...),
	}
	if r.startViewView != msg.ViewId {
		msg.SelectedReports = nil
		msg.SourceNormalView = r.lastNormalView
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

// Install off the reader goroutine; state transfer replies arrive on that connection.
func (r *Replica) handleStartView(msg *BusStartView) {
	cp := *msg
	cp.NoOpSlots = append([]uint64(nil), msg.NoOpSlots...)
	cp.SelectedReports = append([]uint32(nil), msg.SelectedReports...)
	if !r.validStartView(&cp) {
		return
	}

	r.mu.Lock()
	view := r.view()
	if cp.ViewId < view ||
		(cp.ViewId == view && r.status == statusNormal && r.lastNormalView == view) ||
		(r.recovery != nil && cp.ViewId == r.recovery.view) {
		r.mu.Unlock()
		return
	}
	select {
	case r.startViewQ <- &cp:
		if cp.ViewId > r.startViewSeen {
			r.startViewSeen = cp.ViewId
		}
		if r.recovery != nil && cp.ViewId > r.recovery.view {
			r.cancelRecoveryLocked()
		}
		r.mu.Unlock()
	default:
		r.mu.Unlock()
		Warning("[%s] start-view queue full, dropping view %d", r.self, msg.ViewId)
	}
}

func (r *Replica) validStartView(msg *BusStartView) bool {
	if msg.SourceNormalView > msg.ViewId {
		return false
	}
	if int(msg.SenderIdx) >= r.config.N ||
		int(msg.SenderIdx) != r.config.LeaderIndex(msg.ViewId) {
		Warning("[%s] ignoring start view %d from non-leader replica %d",
			r.self, msg.ViewId, msg.SenderIdx)
		return false
	}
	if msg.HasStable && (!msg.HasMax || msg.MaxSlot < msg.StableSlot) {
		Warning("[%s] ignoring malformed start view %d: stable=%d max=%d/%v",
			r.self, msg.ViewId, msg.StableSlot, msg.MaxSlot, msg.HasMax)
		return false
	}
	if msg.HasMax && msg.MaxSlot == ^uint64(0) {
		Warning("[%s] ignoring malformed start view %d: max slot overflows",
			r.self, msg.ViewId)
		return false
	}
	base := suffixBase(msg.StableSlot, msg.HasStable)
	for _, slot := range msg.NoOpSlots {
		if !msg.HasMax || slot < base || slot > msg.MaxSlot {
			Warning("[%s] ignoring malformed start view %d: no-op slot %d outside [%d,%d]",
				r.self, msg.ViewId, slot, base, msg.MaxSlot)
			return false
		}
	}
	seen := make(map[uint32]struct{}, len(msg.SelectedReports))
	for _, replica := range msg.SelectedReports {
		if int(replica) >= r.config.N {
			Warning("[%s] ignoring start view %d with invalid selected replica %d",
				r.self, msg.ViewId, replica)
			return false
		}
		if _, duplicate := seen[replica]; duplicate {
			Warning("[%s] ignoring start view %d with duplicate selected replica %d",
				r.self, msg.ViewId, replica)
			return false
		}
		seen[replica] = struct{}{}
	}
	return true
}

func (r *Replica) viewInstallLoop() {
	for msg := range r.startViewQ {
		r.installStartView(msg)
	}
}

func (r *Replica) installStartView(msg *BusStartView) {
	r.mu.Lock()
	view := r.view()
	if msg.ViewId < view || msg.ViewId < r.startViewSeen ||
		(msg.ViewId == view && r.status == statusNormal && r.lastNormalView == view) {
		r.mu.Unlock()
		return
	}
	if r.recovery != nil {
		r.mu.Unlock()
		return
	}
	retain := r.lastNormalView == msg.SourceNormalView
	if retain && msg.HasStable && r.nextExpected > msg.StableSlot {
		if h, ok := r.prefixHashAtLocked(msg.StableSlot); !ok || h != msg.PrefixHash {
			Warning("[%s] PREFIX MISMATCH installing view %d at slot=%d (ours=%016x leader=%016x)",
				r.self, msg.ViewId, msg.StableSlot, h, msg.PrefixHash)
			retain = false
		}
	}
	if r.vc != nil {
		close(r.vc.abort)
		r.vc = nil
	}
	r.sync = nil
	r.clearSyncCatchupLocked()
	r.cancelGapsLocked()
	r.drainPendingBusesLocked()
	r.viewId.Store(msg.ViewId)
	r.status = statusViewChange
	r.armViewChangeWatchdogLocked(msg.ViewId)
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	r.recoveryGen++
	rec := &viewRecovery{
		view:       msg.ViewId,
		generation: r.recoveryGen,
		leader:     int(msg.SenderIdx),
		abort:      make(chan struct{}),
		stable:     msg.StableSlot,
		hasStable:  msg.HasStable,
		maxSlot:    msg.MaxSlot,
		hasMax:     msg.HasMax,
	}
	r.recovery = rec
	r.startViewView = msg.ViewId
	r.startViewSource = msg.SourceNormalView
	r.startViewUsed = append(r.startViewUsed[:0], msg.SelectedReports...)
	rewound, didRewind, prepared := r.prepareRecoveryLocked(rec, msg, retain)
	r.mu.Unlock()

	if didRewind {
		Notice("[%s] view %d: rewound cursor to slot %d", r.self, msg.ViewId, rewound)
	}
	if !prepared {
		return
	}

	for {
		if r.finishRecoveryIfComplete(rec) {
			return
		}
		from, to, active := r.recoveryMissingRange(rec)
		if !active {
			return
		}
		if from <= to {
			r.fetchRecoveryRange(rec, from, rec.maxSlot)
		}
		if r.finishRecoveryIfComplete(rec) {
			return
		}
		r.mu.Lock()
		progressed := from <= to && r.slotStateLocked(from) != slotEmpty
		r.mu.Unlock()
		if progressed {
			continue
		}
		timer := time.NewTimer(recoveryRetryDelay)
		select {
		case <-rec.abort:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (r *Replica) prepareRecoveryLocked(rec *viewRecovery, msg *BusStartView,
	retain bool) (rewound uint64, didRewind, ok bool) {

	replayFrom := suffixBase(r.stableSlot, r.haveStable)
	if replayFrom < r.prunedBelow {
		replayFrom = r.prunedBelow
	}
	target := r.nextExpected
	canonicalEnd := uint64(0)
	if msg.HasMax {
		canonicalEnd = msg.MaxSlot + 1
	}
	localStableEnd := suffixBase(r.stableSlot, r.haveStable)
	rec.verifyPrefix = retain && msg.HasStable && r.nextExpected <= msg.StableSlot
	rec.prefixHash = msg.PrefixHash
	rec.repairFrom = localStableEnd
	if r.haveStable && (!msg.HasMax || canonicalEnd < localStableEnd) {
		Warning("[%s] cannot prepare recovery for view %d: merged end %d is below local stable frontier %d",
			r.self, rec.view, canonicalEnd, localStableEnd)
		return 0, false, false
	}
	noop := make(map[uint64]struct{}, len(msg.NoOpSlots))
	for _, slot := range msg.NoOpSlots {
		if r.haveStable && slot < localStableEnd && r.slotStateLocked(slot) != slotNoOp {
			Warning("[%s] cannot prepare recovery for view %d: merged no-op at stable slot %d conflicts with local history",
				r.self, rec.view, slot)
			return 0, false, false
		}
		noop[slot] = struct{}{}
	}

	if !msg.HasMax {
		target = 0
	} else if !retain {
		target = localStableEnd
	} else {
		if target > canonicalEnd {
			target = canonicalEnd
		}
		for _, slot := range msg.NoOpSlots {
			if slot < target && r.slotStateLocked(slot) == slotReceived {
				target = slot
			}
		}
		for slot, entry := range r.globalLog {
			if slot < localStableEnd || entry == nil || entry.state != slotNoOp {
				continue
			}
			if _, canonical := noop[slot]; !canonical && slot < target {
				target = slot
			}
		}
	}

	if target < r.nextExpected {
		if !r.rewindToLocked(target) {
			Warning("[%s] cannot prepare recovery for view %d: rewind to slot %d is outside the hash window",
				r.self, rec.view, target)
			return 0, false, false
		}
		rewound, didRewind = target, true
	}

	if !retain {
		r.clearSlotRangeLocked(target, 0, false)
	} else {
		for slot, entry := range r.globalLog {
			if slot < localStableEnd || entry == nil || entry.state != slotNoOp {
				continue
			}
			if _, canonical := noop[slot]; !canonical {
				r.releaseEntryBytesLocked(entry)
				delete(r.globalLog, slot)
			}
		}
	}
	r.recomputeMaxSlotLocked()
	for _, slot := range msg.NoOpSlots {
		if r.setNoOpLocked(slot) {
			r.winNoops++
		}
	}
	rec.replayFrom = replayFrom
	rec.replayTo = r.nextExpected
	return rewound, didRewind, true
}

func (r *Replica) clearSlotRangeLocked(from, to uint64, bounded bool) {
	for slot, entry := range r.globalLog {
		if slot < from || (bounded && slot > to) {
			continue
		}
		r.releaseEntryBytesLocked(entry)
		delete(r.globalLog, slot)
	}
}

func (r *Replica) recomputeMaxSlotLocked() {
	r.haveMax = r.nextExpected > 0
	if r.haveMax {
		r.maxSlotSeen = r.nextExpected - 1
	} else {
		r.maxSlotSeen = 0
	}
	for slot, entry := range r.globalLog {
		if entry == nil || entry.state == slotEmpty {
			continue
		}
		if !r.haveMax || slot > r.maxSlotSeen {
			r.maxSlotSeen, r.haveMax = slot, true
		}
	}
}

func (r *Replica) recoveryMissingRange(rec *viewRecovery) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recovery != rec || r.status != statusViewChange ||
		r.view() != rec.view || r.config.LeaderIndex(rec.view) != rec.leader {
		return 0, 0, false
	}
	if !rec.hasMax || r.nextExpected > rec.maxSlot {
		return 1, 0, true
	}
	from, to := r.missingRangeLocked(r.nextExpected, rec.maxSlot)
	return from, to, true
}

func (r *Replica) missingRangeLocked(from, end uint64) (uint64, uint64) {
	if from < r.nextExpected {
		from = r.nextExpected
	}
	for slot := from; slot <= end; slot++ {
		if entry := r.globalLog[slot]; entry == nil || entry.state == slotEmpty {
			to := slot
			for to < end {
				next := to + 1
				entry := r.globalLog[next]
				if entry != nil && entry.state != slotEmpty {
					break
				}
				to = next
			}
			return slot, to
		}
		if slot == end {
			break
		}
	}
	return 1, 0
}

func (r *Replica) finishRecoveryIfComplete(rec *viewRecovery) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recovery != rec || r.status != statusViewChange || r.view() != rec.view {
		return false
	}
	if rec.hasMax && r.nextExpected <= rec.maxSlot {
		for slot := r.nextExpected; slot <= rec.maxSlot; slot++ {
			if entry := r.globalLog[slot]; entry == nil || entry.state == slotEmpty {
				return false
			}
		}
	}
	if rec.verifyPrefix {
		hash := r.prefixHash
		for slot := r.nextExpected; slot <= rec.stable; slot++ {
			e := r.globalLog[slot]
			clientId, reqId := e.clientId, e.reqId
			if !e.ownerSet {
				owner := r.slotOwnerLocked(slot)
				clientId, reqId = owner.clientId, owner.reqId
			}
			hash = foldSlot(hash, slot, e.state, clientId, reqId)
		}
		if hash != rec.prefixHash {
			if !r.rewindToLocked(rec.repairFrom) {
				return false
			}
			r.clearSlotRangeLocked(rec.repairFrom, rec.stable, true)
			r.recomputeMaxSlotLocked()
			rec.replayTo = r.nextExpected
			Warning("[%s] view %d: retained prefix mismatch, refetching [%d,%d]",
				r.self, rec.view, rec.repairFrom, rec.stable)
			return false
		}
		rec.verifyPrefix = false
	}

	if rec.hasMax {
		r.advanceNextExpectedThroughLocked(rec.maxSlot)
	}
	r.replayRepliesLocked(rec.replayFrom, rec.replayTo)
	if rec.hasStable {
		r.setStableLocked(rec.stable)
	}

	r.lastNormalView = rec.view
	r.cancelViewChangeWatchdogLocked()
	r.status = statusNormal
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	r.recovery = nil
	r.advanceNextExpectedLocked()
	Notice("[%s] VIEW-CHANGE done view=%d leader=%d executed=%d",
		r.self, rec.view, rec.leader, r.nextExpected)
	return true
}

func (r *Replica) installViewLocked(view, stable uint64, hasStable bool,
	maxSlot uint64, hasMax bool, noops []uint64,
	conservative bool) (rewound uint64, didRewind bool) {
	if hasStable && (!hasMax || stable > maxSlot) {
		maxSlot, hasMax = stable, true
	}
	rewound, didRewind = r.installCanonicalViewLocked(view, stable, hasStable,
		maxSlot, hasMax, noops, conservative)
	r.publishNormalViewLocked(view)
	return rewound, didRewind
}

func (r *Replica) installCanonicalViewLocked(view, stable uint64, hasStable bool,
	maxSlot uint64, hasMax bool, noops []uint64,
	conservative bool) (rewound uint64, didRewind bool) {

	target := r.nextExpected
	canonicalNext := uint64(0)
	if hasMax && maxSlot != ^uint64(0) {
		canonicalNext = maxSlot + 1
	}
	if (!hasMax || maxSlot != ^uint64(0)) && target > canonicalNext {
		target = canonicalNext
	}
	if conservative {
		// Rewind to the local commit point; higher slots may differ from the leader.
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
	if !conservative {
		for slot, entry := range r.globalLog {
			outside := !hasMax || (maxSlot != ^uint64(0) && slot > maxSlot)
			if !outside || entry == nil || entry.state != slotNoOp {
				continue
			}
			r.releaseEntryBytesLocked(entry)
			delete(r.globalLog, slot)
		}
		r.recomputeMaxSlotLocked()
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
	replayFrom, replayTo := suffixBase(r.stableSlot, r.haveStable), r.nextExpected
	if replayFrom < r.prunedBelow {
		replayFrom = r.prunedBelow
	}
	if hasMax {
		r.advanceNextExpectedThroughLocked(maxSlot)
	}
	r.replayRepliesLocked(replayFrom, replayTo)
	if hasStable {
		r.setStableLocked(stable)
	}
	return rewound, didRewind
}

func (r *Replica) publishNormalViewLocked(view uint64) {
	r.lastNormalView = view
	r.cancelViewChangeWatchdogLocked()
	r.status = statusNormal
	r.lastHeartbeatNs = nowNs()
	r.leaderLost = false
	r.advanceNextExpectedLocked()
}

// Replay replies above the commit point with the new view ID.
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

func (r *Replica) rewindToLocked(target uint64) bool {
	if target >= r.nextExpected {
		return true
	}
	hash, logIdx, ok := r.prefixStateAtLocked(target)
	if !ok {
		return false
	}
	for s := r.nextExpected; s > target; s-- {
		e := r.globalLog[s-1]
		if e == nil {
			continue
		}
		r.undoWritesLocked(e)
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

type fetchReq struct {
	peer       int
	from       uint64
	to         uint64
	view       uint64
	fetchID    uint64
	syncGen    uint64
	installGen uint64
	vc         *vcState
	cancel     <-chan struct{}
	done       chan bool
	slots      []uint64
}

func (r *Replica) newSyncFetchLocked(from, to uint64) fetchReq {
	c := &r.syncCatchup
	return fetchReq{
		peer:    c.leader,
		from:    from,
		to:      to,
		view:    c.view,
		fetchID: r.fetchSeq.Add(1),
		syncGen: c.generation,
	}
}

func (r *Replica) enqueueSyncFetch(req fetchReq) {
	select {
	case r.fetchQ <- req:
		return
	default:
	}

	r.mu.Lock()
	if r.syncCatchupMatchesLocked(req) {
		r.clearSyncCatchupLocked()
	}
	r.mu.Unlock()
	Warning("[%s] state-fetch queue full, dropping sync request [%d,%d]",
		r.self, req.from, req.to)
}

func (r *Replica) enqueueFetch(peer int, from, to uint64, done chan bool) {
	if peer < 0 || peer >= r.config.N || peer == r.idx || from > to {
		if done != nil {
			done <- peer == r.idx || from > to
		}
		return
	}
	req := fetchReq{
		peer:    peer,
		from:    from,
		to:      to,
		view:    r.view(),
		fetchID: r.fetchSeq.Add(1),
		done:    done,
	}
	select {
	case r.fetchQ <- req:
	default:
		Warning("[%s] state-fetch queue full, dropping request [%d,%d]", r.self, from, to)
		if done != nil {
			done <- false
		}
	}
}

func (r *Replica) fetchRangeBlocking(vc *vcState, peer int, from, to uint64, slots []uint64) bool {
	if peer < 0 || peer >= r.config.N || peer == r.idx || from > to {
		return peer == r.idx || from > to
	}
	done := make(chan bool, 1)
	req := fetchReq{
		peer:    peer,
		from:    from,
		to:      to,
		view:    vc.view,
		fetchID: r.fetchSeq.Add(1),
		vc:      vc,
		slots:   slots,
		cancel:  vc.abort,
		done:    done,
	}
	select {
	case <-vc.abort:
		return false
	case r.fetchQ <- req:
	default:
		Warning("[%s] state-fetch queue full, dropping view-change request [%d,%d]",
			r.self, from, to)
		return false
	}
	select {
	case ok := <-done:
		return ok
	case <-vc.abort:
		return false
	}
}

func (r *Replica) fetchRecoveryRange(rec *viewRecovery, from, to uint64) bool {
	if rec.leader == r.idx || from > to {
		return true
	}
	done := make(chan bool, 1)
	req := fetchReq{
		peer:       rec.leader,
		from:       from,
		to:         to,
		view:       rec.view,
		fetchID:    r.fetchSeq.Add(1),
		installGen: rec.generation,
		cancel:     rec.abort,
		done:       done,
	}
	select {
	case r.fetchQ <- req:
	case <-rec.abort:
		return false
	default:
		Warning("[%s] state-fetch queue full, dropping recovery request [%d,%d]",
			r.self, from, to)
		return false
	}
	select {
	case ok := <-done:
		return ok
	case <-rec.abort:
		return false
	}
}

func (r *Replica) stateFetchLoop() {
	for req := range r.fetchQ {
		ok := r.runFetch(req)
		if req.syncGen != 0 {
			r.finishSyncFetch(req, ok)
		}
		if req.done != nil {
			req.done <- ok
		}
	}
}

func (r *Replica) finishSyncFetch(req fetchReq, ok bool) {
	var (
		prepare    BusSyncPrepare
		hash       uint64
		known      bool
		stable     uint64
		haveStable bool
	)

	r.mu.Lock()
	if !r.syncCatchupActiveLocked(req) {
		r.mu.Unlock()
		return
	}
	if !ok {
		r.clearSyncCatchupLocked()
		r.mu.Unlock()
		return
	}

	c := &r.syncCatchup
	if r.nextExpected == 0 || r.nextExpected-1 < c.target {
		if r.nextExpected <= req.from {
			r.clearSyncCatchupLocked()
			r.mu.Unlock()
			return
		}
		next := r.newSyncFetchLocked(r.nextExpected, c.target)
		r.mu.Unlock()
		r.enqueueSyncFetch(next)
		return
	}

	prepare = c.prepare
	hash, known = r.prefixHashAtLocked(prepare.SlotToSync)
	stable, haveStable = r.stableSlot, r.haveStable
	r.clearSyncCatchupLocked()
	r.mu.Unlock()

	r.answerSyncPrepare(prepare, hash, known, stable, haveStable)
}

func (r *Replica) fetchActive(req fetchReq) bool {
	select {
	case <-req.cancel:
		return false
	default:
	}
	if r.view() != req.view {
		return false
	}
	if req.syncGen != 0 {
		r.mu.Lock()
		active := r.syncCatchupActiveLocked(req)
		r.mu.Unlock()
		return active
	}
	if req.vc != nil {
		r.mu.Lock()
		active := r.fetchActiveLocked(req)
		r.mu.Unlock()
		return active
	}
	if req.installGen == 0 {
		return true
	}
	r.mu.Lock()
	active := r.recovery != nil && r.recovery.generation == req.installGen &&
		r.recovery.view == req.view && r.recovery.leader == req.peer &&
		r.status == statusViewChange
	r.mu.Unlock()
	return active
}

func (r *Replica) missingFetchSlotsLocked(req fetchReq, from uint64) []uint64 {
	var slots []uint64
	i := sort.Search(len(req.slots), func(i int) bool { return req.slots[i] >= from })
	for slot := from; slot <= req.to && len(slots) < stateFetchSlots; slot++ {
		if req.slots != nil {
			if i == len(req.slots) {
				break
			}
			slot = req.slots[i]
			i++
		}
		if slot >= r.nextExpected && r.slotStateLocked(slot) == slotEmpty {
			slots = append(slots, slot)
		}
		if slot == req.to {
			break
		}
	}
	return slots
}

func (r *Replica) runFetch(req fetchReq) bool {
	if req.peer < 0 || req.peer >= r.config.N || req.peer == r.idx || req.from > req.to {
		return req.peer == r.idx || req.from > req.to
	}
	probe := time.NewTicker(stateFetchProbe)
	defer probe.Stop()
	next := req.from
	for next <= req.to {
		if !r.fetchActive(req) {
			return false
		}
		to := req.to
		var slots []uint64
		if req.syncGen != 0 || req.vc != nil || req.installGen != 0 {
			r.mu.Lock()
			slots = r.missingFetchSlotsLocked(req, next)
			r.mu.Unlock()
			if len(slots) == 0 {
				return true
			}
			next, to = slots[0], slots[len(slots)-1]
		}
		r.sendToPeer(req.peer, MsgBusGetState, &BusGetState{
			ViewId:    req.view,
			FromSlot:  next,
			ToSlot:    to,
			FetchId:   req.fetchID,
			SenderIdx: uint32(r.idx),
			Slots:     slots,
		})
		deadline := time.NewTimer(stateFetchTimeout)
		for advanced := false; !advanced; {
			select {
			case m := <-r.newStateCh:
				if m.ViewId != req.view || int(m.SenderIdx) != req.peer ||
					m.FetchId != req.fetchID || m.FromSlot != next ||
					m.ToSlot < next || m.ToSlot > to {
					continue // a reply to an earlier, abandoned request
				}
				valid := true
				for _, entry := range m.Entries {
					if len(slots) > 0 {
						i := sort.Search(len(slots), func(i int) bool { return slots[i] >= entry.Slot })
						valid = valid && i < len(slots) && slots[i] == entry.Slot
					}
				}
				if !valid || !r.applyStateEntries(m, req) {
					if !deadline.Stop() {
						<-deadline.C
					}
					return false
				}
				next = m.ToSlot + 1
				advanced = true
			case <-req.cancel:
				if !deadline.Stop() {
					<-deadline.C
				}
				return false
			case <-probe.C:
				if !r.fetchActive(req) {
					deadline.Stop()
					return false
				}
				if !r.peerConnected(req.peer) {
					Warning("[%s] state fetch [%d,%d] from replica %d abandoned at slot %d: "+
						"peer connection lost", r.self, req.from, req.to, req.peer, next)
					if !deadline.Stop() {
						<-deadline.C
					}
					return false
				}
			case <-deadline.C:
				Warning("[%s] state fetch [%d,%d] from replica %d timed out at slot %d",
					r.self, req.from, req.to, req.peer, next)
				return false
			}
		}
		if !deadline.Stop() {
			select {
			case <-deadline.C:
			default:
			}
		}
	}
	return true
}

func (r *Replica) handleNewState(msg *BusNewState) {
	if int(msg.SenderIdx) >= r.config.N {
		return
	}
	select {
	case r.newStateCh <- msg:
	default:
		Warning("[%s] dropping unsolicited state transfer [%d,%d]",
			r.self, msg.FromSlot, msg.ToSlot)
	}
}

func (r *Replica) applyStateEntries(m *BusNewState, req fetchReq) bool {
	if m.ViewId != req.view || int(m.SenderIdx) != req.peer ||
		m.FetchId != req.fetchID || m.FromSlot < req.from ||
		m.ToSlot < m.FromSlot || m.ToSlot > req.to {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.fetchActiveLocked(req) {
		return false
	}
	seen := make(map[uint64]struct{}, len(m.Entries))
	for i := range m.Entries {
		slot := m.Entries[i].Slot
		if slot < m.FromSlot || slot > m.ToSlot || slot < req.from || slot > req.to {
			return false
		}
		if _, duplicate := seen[slot]; duplicate {
			return false
		}
		seen[slot] = struct{}{}
	}
	for i := range m.Entries {
		e := &m.Entries[i]
		if e.IsNoOp {
			if r.setNoOpLocked(e.Slot) {
				r.winNoops++
			}
			continue
		}
		if r.storeRecoveredLocked(e.Slot, e.ClientId, e.ReqId, e.Payload, e.IsBus) {
			r.winRecovered++
		}
	}
	r.advanceNextExpectedLocked()
	return true
}

func (r *Replica) fetchActiveLocked(req fetchReq) bool {
	if r.view() != req.view {
		return false
	}
	if req.syncGen != 0 {
		return r.syncCatchupActiveLocked(req)
	}
	if req.vc != nil {
		select {
		case <-req.vc.abort:
			return false
		default:
		}
		return r.vc == req.vc && req.vc.view == req.view &&
			r.status == statusViewChange
	}
	if req.installGen == 0 {
		return true
	}
	return r.recovery != nil && r.recovery.generation == req.installGen &&
		r.recovery.view == req.view && r.recovery.leader == req.peer &&
		r.status == statusViewChange
}

func (r *Replica) syncCatchupMatchesLocked(req fetchReq) bool {
	c := &r.syncCatchup
	return req.syncGen != 0 && c.active && c.generation == req.syncGen &&
		c.view == req.view && c.leader == req.peer
}

func (r *Replica) syncCatchupActiveLocked(req fetchReq) bool {
	return r.syncCatchupMatchesLocked(req) && r.status == statusNormal &&
		r.view() == req.view && r.config.LeaderIndex(req.view) == req.peer
}

func (r *Replica) handleGetState(msg *BusGetState) {
	if int(msg.SenderIdx) >= r.config.N || msg.FromSlot > msg.ToSlot || msg.ViewId != r.view() {
		return
	}
	if len(msg.Slots) > stateFetchSlots {
		return
	}
	for i, slot := range msg.Slots {
		if slot < msg.FromSlot || slot > msg.ToSlot || (i > 0 && slot <= msg.Slots[i-1]) {
			return
		}
	}
	cp := *msg
	select {
	case r.serveQ <- &cp:
	default:
		Warning("[%s] state-serve queue full, dropping request from replica %d",
			r.self, msg.SenderIdx)
	}
}

func (r *Replica) stateServeLoop() {
	for req := range r.serveQ {
		r.serveState(req)
	}
}

func (r *Replica) serveState(req *BusGetState) {
	if req.ViewId != r.view() || int(req.SenderIdx) >= r.config.N {
		return
	}
	reply := &BusNewState{
		ViewId:    req.ViewId,
		FromSlot:  req.FromSlot,
		FetchId:   req.FetchId,
		SenderIdx: uint32(r.idx),
	}
	last := req.FromSlot
	size := 0
	for slot, i := req.FromSlot, 0; slot <= req.ToSlot; slot++ {
		if len(req.Slots) > 0 {
			if i == len(req.Slots) {
				break
			}
			slot = req.Slots[i]
			i++
		}
		if ent, ok := r.readSlot(slot); ok {
			reply.Entries = append(reply.Entries, ent)
			size += len(ent.Payload) + stateEntryOverhead
		}
		last = slot
		if size >= stateChunkBytes || len(reply.Entries) >= maxStateEntries || slot == req.ToSlot {
			break
		}
	}
	reply.ToSlot = last
	if req.ViewId != r.view() {
		return
	}
	r.sendToPeer(int(req.SenderIdx), MsgBusNewState, reply)
}

// Disk is authoritative only at or below the commit point.
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
