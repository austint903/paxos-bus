package paxosbus

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// nowNs is a monotonic nanosecond clock (Go's time.Since uses the monotonic
// reading, matching the C++ CLOCK_MONOTONIC usage). It is used for INTRA-process
// durations only (gap-handler latency, client RTT) where a single process
// compares two of its own readings.
var startTime = time.Now()

func nowNs() int64 {
	return time.Since(startTime).Nanoseconds()
}

// wallNs is the SHARED clock for the schedule. Unlike nowNs (per-process
// monotonic, with a different origin in every process), wall-clock nanoseconds
// are comparable across processes: on one host they are literally the same
// clock, and on a WAN they ride NTP/PTP (the slides' "Clock Synced among
// Receivers"). The arrival lines (baseNs, expected arrival) and the
// gap-detection deadline all live on this clock, so every replica computes the
// same predicted arrival y for every message and therefore the same global slot.
func wallNs() int64 {
	return time.Now().UnixNano()
}

// slotState is the per-slot lifecycle used by gap agreement. The mutex
// (Replica.mu) makes every transition atomic; correctness rests on two
// invariants:
//   - NoOp is final: a committed NOOP is never overwritten by later real data.
//   - commit-checks-empty: the leader only commits a NoOp if the slot is still
//     EMPTY (otherwise it keeps the real request).
type slotState uint8

const (
	slotEmpty slotState = iota
	slotReceived
	slotNoOp
)

// clientLine is the replica's per-client AGREED arrival line, taken verbatim
// from the sync message and never re-anchored. Expected arrival of seq N (the
// T = k*x + b line) is baseNs + (N-1)*intervalNs, all on the shared wall clock:
//
//	baseNs     = sync SendTimeNs + StartDelayMs  (b, shared clock)
//	intervalNs = IntervalMs                      (k)
//
// Because both come straight from the sync message's bytes, every replica plugs
// the same numbers into the same formula and computes the identical line — and
// hence the identical global slot for every message — regardless of clock skew.
// The line feeds the global slot formula (computeGlobalSlot) and the per-message
// delta stat; the slot SPACE itself lives in the replica-wide global log below.
type clientLine struct {
	baseNs     int64
	intervalNs int64
	maxSeqSeen uint64
}

// expectedNs returns the predicted (shared-clock) arrival of this client's
// seq n (1-based): y(n) = baseNs + (n-1)*intervalNs.
func (cl *clientLine) expectedNs(n uint64) int64 {
	return cl.baseNs + int64(n-1)*cl.intervalNs
}

// globalEntry is one slot of the replica-wide global log. The slot is the
// message's rank in the merged (predicted-arrival, clientId) order across ALL
// clients — not the per-client request id. clientId/reqId are the provenance of
// whichever message owns the slot, so a recovered/no-op'd slot can still be
// written to the durable log with its origin.
type globalEntry struct {
	clientId uint64
	reqId    uint64
	state    slotState
	op       []byte
}

// slotMetaEntry is the OWNER of a global slot: which (client, seq) ranks there
// and its predicted arrival. It is produced by the merge cursor (genCursorUpTo)
// — the deterministic inverse of computeGlobalSlot — so gap detection can ask
// "whose message should be at slot G, and when was it due?" without it having
// arrived yet.
type slotMetaEntry struct {
	clientId   uint64
	reqId      uint64
	expectedNs int64
}

// computeGlobalSlot returns the global slot of (clientId, reqId): its rank when
// every client's messages are merged and sorted by predicted arrival y, ties
// broken by smaller clientId. It is closed-form and O(numClients) — no running
// state, no coordination — so every replica computes the same answer from the
// same lines. See GLOBAL_LOG.md for the derivation and worked example.
func computeGlobalSlot(lines map[uint64]*clientLine, clientId, reqId uint64) uint64 {
	self := lines[clientId]
	if self == nil {
		return reqId - 1
	}
	y := self.expectedNs(reqId)
	slot := reqId - 1 // this client's own earlier messages all rank before (c, reqId)
	for cid, line := range lines {
		if cid == clientId {
			continue
		}
		slot += countBefore(line, y, cid < clientId)
	}
	return slot
}

// countBefore counts how many of `line`'s messages rank before predicted time y.
// A message n' contributes if y_line(n') < y; when tiePrecedes (the other
// client's id is smaller), an exact tie y_line(n') == y also counts. Closed
// form: with d = y - baseNs and k = intervalNs, the m = n'-1 >= 0 satisfying
// m*k < d number floor((d-1)/k)+1 (for d>0), plus one more if d is an exact
// multiple of k and ties precede.
func countBefore(line *clientLine, y int64, tiePrecedes bool) uint64 {
	d := y - line.baseNs
	k := line.intervalNs
	if k <= 0 {
		return 0
	}
	var cnt uint64
	if d > 0 {
		cnt = uint64((d-1)/k) + 1
	}
	if tiePrecedes && d >= 0 && d%k == 0 {
		cnt++
	}
	return cnt
}

// gapKey identifies an in-flight gap (one GLOBAL slot). The global slot already
// encodes which client/seq it is, so the key needs nothing more.
type gapKey struct {
	slot uint64
}

// gapState coordinates one in-flight gap. Presence of a key in Replica.gaps is
// itself the "gap-in-progress" guard that stops the detector from spawning a
// duplicate handler. The role-specific fields are used only on the side that
// drives that step:
//   - askers/probeReplies/commitAcks: the leader, while resolving.
//   - doneCh: a follower, while waiting for the leader's resolution.
type gapState struct {
	start        int64               // gap-detection time (monotonic), for latency logging
	askers       map[uint32]struct{} // leader: followers awaiting recovered data
	probeReplies chan *BusGapReply   // leader: replies to its probe broadcast
	commitAcks   chan struct{}       // leader: NoOp-commit acks
	doneCh       chan struct{}       // follower: resolution arrived (filled or noop'd)
}

func newGapState(start int64) *gapState {
	return &gapState{
		start:        start,
		askers:       make(map[uint32]struct{}),
		probeReplies: make(chan *BusGapReply, 8),
		commitAcks:   make(chan struct{}, 8),
		doneCh:       make(chan struct{}, 1),
	}
}

// snapshotAskers copies the asker set; callers hold Replica.mu.
func (gs *gapState) snapshotAskers() []uint32 {
	out := make([]uint32, 0, len(gs.askers))
	for a := range gs.askers {
		out = append(out, a)
	}
	return out
}

// Gap-agreement timing. delta is the slack past a slot's expected arrival before
// it is declared a gap (drops are rare on TCP, so this can be generous);
// gapProtocolTimeout bounds how long a handler waits on peer round-trips.
const (
	gapDelta           = 5 * time.Second
	gapProtocolTimeout = 3 * time.Second
)

// dropMode selects which replicas artificially drop incoming requests, used to
// force the (otherwise very rare) gap-agreement paths during a run:
//   - dropLeader: only the leader drops, so it must recover the real op from a
//     follower (leaderResolve Scenario 2).
//   - dropFollowers: the f lowest-index followers drop while the leader keeps the
//     op, so each asks the leader and is served (followerRecover Scenario 1).
//   - dropAll: every replica drops the same slot, so nobody has it and the leader
//     commits a NoOp (leaderResolve Scenario 3).
//
// Every replica is launched with the same mode and decides locally from its role
// and the request id, so all replicas agree on which slots to drop.
type dropMode uint8

const (
	dropNone dropMode = iota
	dropLeader
	dropFollowers
	dropAll
)

// ParseDropMode converts a CLI drop-mode string into a dropMode.
func ParseDropMode(s string) (dropMode, error) {
	switch s {
	case "", "none":
		return dropNone, nil
	case "leader":
		return dropLeader, nil
	case "followers":
		return dropFollowers, nil
	case "all":
		return dropAll, nil
	default:
		return dropNone, fmt.Errorf("unknown drop mode %q (want none|leader|followers|all)", s)
	}
}

func (m dropMode) String() string {
	switch m {
	case dropLeader:
		return "leader"
	case dropFollowers:
		return "followers"
	case dropAll:
		return "all"
	default:
		return "none"
	}
}

type Replica struct {
	config *Config
	idx    int
	viewId uint64
	self   string // log-line identity, e.g. "Replica 1 europe-north1"

	mu      sync.Mutex
	clients map[uint64]*clientLine

	// The replica-wide global log: one ordered slot space holding every client's
	// messages interleaved by predicted arrival. Keyed by global slot. Guarded by
	// mu. nextExpected is the lowest globally-contiguous unfilled slot (so empty
	// slots in [nextExpected, maxSlotSeen] are gap candidates); maxSlotSeen is the
	// highest slot any received message has mapped to.
	globalLog    map[uint64]*globalEntry
	nextExpected uint64
	maxSlotSeen  uint64
	haveMax      bool

	// Merge cursor: the deterministic inverse of computeGlobalSlot. It walks
	// global slots in order, recording each slot's owner (which client/seq ranks
	// there and when it is due), so gap detection can reason about a slot that has
	// not arrived. cursorNextN[c] is the next seq of client c awaiting a slot;
	// cursorSlot is the next slot to assign; slotMeta memoizes slot->owner.
	// Guarded by mu.
	cursorNextN map[uint64]uint64
	cursorSlot  uint64
	slotMeta    map[uint64]slotMetaEntry

	// Persistent outbound connections to every other replica (index r.idx is
	// nil). Gap handlers reply over peerWriters[SenderIdx]. Guarded by mu.
	peerWriters []*lockedWriter

	// In-flight gaps, keyed by global slot. Presence is the duplicate guard.
	// Guarded by mu.
	gaps map[gapKey]*gapState

	// The replica's single durable global log (one file per replica, records
	// carry slot+client+req_id). Separate from the stderr logs above; nil when
	// logDir is empty. Guarded by mu for the open; the writer has its own lock.
	logDir  string
	durable *durableLog

	// Artificial message dropping (gap-agreement testing). dropEvery==0 or
	// dropMode==dropNone disables it; otherwise a slot is dropped when
	// requestId % dropEvery == 0, on the replicas selected by dropMode.
	dropMode  dropMode
	dropEvery uint64

	// 1-second window counters for the periodic summary line (all clients).
	winRecv       uint64
	winDeltaSumUs int64
	winDeltaMinUs int64
	winDeltaMaxUs int64
	winGaps       uint64
	winRecovered  uint64
	winNoops      uint64
	winDropped    uint64
}

func NewReplica(config *Config, idx int, label, logDir string, mode dropMode, every uint64) *Replica {
	self := "Replica " + strconv.Itoa(idx)
	if label != "" {
		self += " " + label
	}
	r := &Replica{
		config:      config,
		idx:         idx,
		viewId:      0,
		self:        self,
		clients:     make(map[uint64]*clientLine),
		globalLog:   make(map[uint64]*globalEntry),
		cursorNextN: make(map[uint64]uint64),
		slotMeta:    make(map[uint64]slotMetaEntry),
		peerWriters: make([]*lockedWriter, config.N),
		gaps:        make(map[gapKey]*gapState),
		logDir:      logDir,
		dropMode:    mode,
		dropEvery:   every,
	}
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			Warning("[%s] cannot create durable log dir %s: %v", self, logDir, err)
			r.logDir = ""
		} else if dl, err := openDurableLog(logDir); err != nil {
			Warning("[%s] cannot open durable global log in %s: %v", self, logDir, err)
			r.logDir = ""
		} else {
			r.durable = dl
			Notice("[%s] durable global log in %s/replica.log", self, logDir)
		}
	}
	leader := "no"
	if r.AmLeader() {
		leader = "yes"
	}
	Notice("[%s] started (view=0, f=%d, quorum=%d, leader=%s)",
		r.self, config.F, config.QuorumSize(), leader)
	if r.dropMode != dropNone && r.dropEvery > 0 {
		Notice("[%s] artificial drop enabled: mode=%s every=%d (drop slot when reqId%%%d==0)",
			r.self, r.dropMode, r.dropEvery, r.dropEvery)
	}
	return r
}

func (r *Replica) AmLeader() bool {
	return r.config.LeaderIndex(r.viewId) == r.idx
}

// followerRankLocked returns this replica's position among the non-leader
// replicas ordered by index (0 = first follower). Used to select the f
// lowest-index followers for dropFollowers. Callers hold r.mu (it reads viewId).
func (r *Replica) followerRankLocked() int {
	leader := r.config.LeaderIndex(r.viewId)
	rank := 0
	for j := 0; j < r.idx; j++ {
		if j != leader {
			rank++
		}
	}
	return rank
}

// shouldDropLocked reports whether this replica should artificially drop the
// request at requestId, per dropMode/dropEvery. Callers hold r.mu.
func (r *Replica) shouldDropLocked(requestId uint64) bool {
	if r.dropEvery == 0 || r.dropMode == dropNone {
		return false
	}
	if requestId%r.dropEvery != 0 {
		return false
	}
	switch r.dropMode {
	case dropAll:
		return true
	case dropLeader:
		return r.AmLeader()
	case dropFollowers:
		// The f lowest-index followers drop; the leader (and any remaining
		// followers) keep the op, so client replies stay >= quorum and the op is
		// always recoverable.
		return !r.AmLeader() && r.followerRankLocked() < r.config.F
	default:
		return false
	}
}

// Run binds the replica port, then serves one reader goroutine per client
// connection. Blocks forever.
func (r *Replica) Run() error {
	l, err := net.Listen("tcp", "0.0.0.0:"+r.config.Port(r.idx))
	if err != nil {
		return err
	}
	go r.statsLoop()
	go r.connectPeers()
	go r.gapDetectLoop()
	if r.durable != nil {
		go r.flushLoop()
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			Warning("[%s] accept error: %v", r.self, err)
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go r.clientListener(conn)
	}
}

// clientListener handles one client connection: a sync message followed by
// the data stream. Replies go back on the same connection, so the writer is
// owned by this goroutine and needs no lock.
func (r *Replica) clientListener(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var (
		syncMsg BusSyncMessage
		reqMsg  BusRequestMessage
		reply   BusReplyMessage
	)

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Notice("[%s] client connection %s closed: %v",
				r.self, conn.RemoteAddr(), err)
			return
		}
		switch msgType {
		case MsgBusSync:
			if err := syncMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync message: %v", r.self, err)
				return
			}
			r.handleSync(&syncMsg)

		case MsgBusRequest:
			if err := reqMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad bus request message: %v", r.self, err)
				return
			}
			slot, ok := r.handleRequest(&reqMsg)
			if !ok {
				continue
			}
			// LogSlotNum is the GLOBAL slot (the message's rank in the merged
			// order), no longer == RequestId. Result is nil: there is no execution
			// layer, so the op is logged but not applied.
			reply = BusReplyMessage{
				ClientId:   reqMsg.ClientId,
				RequestId:  reqMsg.RequestId,
				LogSlotNum: slot,
				ViewId:     r.viewId,
				ReplicaIdx: uint32(r.idx),
				Result:     nil,
			}
			writer.WriteByte(MsgBusReply)
			reply.Marshal(writer)
			if err := writer.Flush(); err != nil {
				Warning("[%s] failed to send reply for req=%d: %v",
					r.self, reqMsg.RequestId, err)
				return
			}

		case MsgBusGapRequest:
			var m BusGapRequest
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap request: %v", r.self, err)
				return
			}
			r.handleGapRequest(&m)

		case MsgBusGapReply:
			var m BusGapReply
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap reply: %v", r.self, err)
				return
			}
			r.handleGapReply(&m)

		case MsgBusGapCommit:
			var m BusGapCommit
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap commit: %v", r.self, err)
				return
			}
			r.handleGapCommit(&m)

		case MsgBusGapCommitReply:
			var m BusGapCommitReply
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap commit reply: %v", r.self, err)
				return
			}
			r.handleGapCommitReply(&m)

		default:
			Warning("[%s] unknown message type %d", r.self, msgType)
			return
		}
	}
}

func (r *Replica) handleSync(msg *BusSyncMessage) {
	r.mu.Lock()
	// The agreed line comes straight from the sync message, on the shared wall
	// clock: base = SendTimeNs + start_delay, k = interval. NOT re-anchored to a
	// local arrival, so expected(N) = base + (N-1)*interval is identical on every
	// replica and the global slot order is consistent across replicas.
	r.clients[msg.ClientId] = &clientLine{
		baseNs:     int64(msg.SendTimeNs) + int64(msg.StartDelayMs)*1e6,
		intervalNs: int64(msg.IntervalMs) * 1e6,
	}
	// A (re)sync changes the client set, which shifts every slot's owner, so the
	// merge cursor is rebuilt. In normal operation all syncs land in the 5 s
	// before any data, so cursorSlot is still 0 and this is a no-op.
	r.resetCursorLocked()
	r.mu.Unlock()
	Notice("[%s] sync from client %d: interval=%dms",
		r.self, msg.ClientId, msg.IntervalMs)
}

// resetCursorLocked discards the merge cursor so it regenerates against the
// current client set. Callers hold r.mu.
func (r *Replica) resetCursorLocked() {
	r.cursorSlot = 0
	r.cursorNextN = make(map[uint64]uint64)
	r.slotMeta = make(map[uint64]slotMetaEntry)
}

// handleRequest maps the request to its global slot, records it under the slot
// state machine, and returns (globalSlot, true). Returns (_, false) if the
// client never synced or the request was artificially dropped (no reply is sent
// then, as in the C++ implementation).
func (r *Replica) handleRequest(msg *BusRequestMessage) (uint64, bool) {
	actualNs := wallNs()
	r.mu.Lock()
	line, ok := r.clients[msg.ClientId]
	if !ok {
		r.mu.Unlock()
		Warning("[%s] request from unsynced client %d, ignoring", r.self, msg.ClientId)
		return 0, false
	}
	// Artificial drop: simulate the request never arriving. Nothing is recorded
	// and no reply is sent, so the slot stays EMPTY and the gap detector picks it
	// up — exactly as a real network drop would.
	if r.shouldDropLocked(msg.RequestId) {
		r.winDropped++
		r.mu.Unlock()
		return 0, false
	}
	if msg.RequestId > line.maxSeqSeen {
		line.maxSeqSeen = msg.RequestId
	}

	// Delta measures arrival drift off the fixed (un-re-anchored) line, both on
	// the shared wall clock.
	expectedNs := line.expectedNs(msg.RequestId)
	deltaUs := (actualNs - expectedNs) / 1000
	if r.winRecv == 0 {
		r.winDeltaMinUs, r.winDeltaMaxUs = deltaUs, deltaUs
	} else {
		if deltaUs < r.winDeltaMinUs {
			r.winDeltaMinUs = deltaUs
		}
		if deltaUs > r.winDeltaMaxUs {
			r.winDeltaMaxUs = deltaUs
		}
	}
	r.winDeltaSumUs += deltaUs
	r.winRecv++

	// Map to the global slot and record it under the state machine. A late
	// request bounces off a committed NoOp (NoOp is final) and a duplicate is
	// ignored; only a fresh EMPTY->RECEIVED transition is durably logged.
	// advanceNextExpected then walks past any contiguous slots a prior gap fill
	// may have completed.
	slot := computeGlobalSlot(r.clients, msg.ClientId, msg.RequestId)
	stored := r.recordReceivedLocked(slot, msg.ClientId, msg.RequestId, msg.Op)
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

	// Durably record the op in the global log at its slot. Global slots interleave
	// across clients, so this slot may arrive out of order; durableLog.record
	// reserves placeholders for skipped slots and patches them in place later. The
	// append only buffers; flushLoop persists it in batches. durableLog has its
	// own lock, so this stays outside r.mu.
	if stored && r.durable != nil {
		r.durable.record(slot, msg.ClientId, msg.RequestId, msg.Op, false)
	}
	return slot, true
}

// slotEntryLocked returns the global log entry for slot, creating an EMPTY one
// on first touch. Callers hold r.mu.
func (r *Replica) slotEntryLocked(slot uint64) *globalEntry {
	e := r.globalLog[slot]
	if e == nil {
		e = &globalEntry{state: slotEmpty}
		r.globalLog[slot] = e
	}
	return e
}

// observeSlotLocked raises maxSlotSeen to include slot. Callers hold r.mu.
func (r *Replica) observeSlotLocked(slot uint64) {
	if !r.haveMax || slot > r.maxSlotSeen {
		r.maxSlotSeen = slot
		r.haveMax = true
	}
}

// recordReceivedLocked applies the data-path slot rule for real data: a
// committed NOOP is final (drop), a RECEIVED slot is idempotent (ignore), and
// an EMPTY slot is filled with the op and its provenance. Returns true only on
// the EMPTY->RECEIVED transition (i.e. when a durable append is warranted).
// Callers hold r.mu.
func (r *Replica) recordReceivedLocked(slot, clientId, reqId uint64, op []byte) bool {
	r.observeSlotLocked(slot)
	e := r.slotEntryLocked(slot)
	switch e.state {
	case slotNoOp, slotReceived:
		return false
	default:
		e.state = slotReceived
		e.clientId = clientId
		e.reqId = reqId
		e.op = op
		return true
	}
}

// setNoOpLocked commits a NoOp at a slot, overwriting EMPTY or RECEIVED (the
// leader has initiative once it broadcasts a commit). The owner provenance is
// kept (from the cursor) so the durable record still identifies the missing
// message. Returns true on a real transition (so the change is logged once).
// Callers hold r.mu.
func (r *Replica) setNoOpLocked(slot uint64) bool {
	r.observeSlotLocked(slot)
	e := r.slotEntryLocked(slot)
	if e.state == slotNoOp {
		return false
	}
	if e.clientId == 0 {
		m := r.slotOwnerLocked(slot)
		e.clientId, e.reqId = m.clientId, m.reqId
	}
	e.state = slotNoOp
	e.op = nil
	return true
}

// slotStateLocked reports a slot's state (EMPTY if unknown). Callers hold r.mu.
func (r *Replica) slotStateLocked(slot uint64) slotState {
	if e := r.globalLog[slot]; e != nil {
		return e.state
	}
	return slotEmpty
}

// slotOpLocked returns a slot's op bytes (nil if absent). Callers hold r.mu.
func (r *Replica) slotOpLocked(slot uint64) []byte {
	if e := r.globalLog[slot]; e != nil {
		return e.op
	}
	return nil
}

// advanceNextExpectedLocked walks nextExpected past every contiguous filled
// (received or noop'd) slot, so the detector's scan window starts at the first
// real hole. Callers hold r.mu.
func (r *Replica) advanceNextExpectedLocked() {
	for {
		e := r.globalLog[r.nextExpected]
		if e == nil || e.state == slotEmpty {
			return
		}
		r.nextExpected++
	}
}

// genCursorUpToLocked extends the merge cursor so every slot in [0, target] has
// a known owner. It is the forward inverse of computeGlobalSlot: at each step it
// picks the client whose next un-assigned seq has the smallest predicted arrival
// (ties broken by smaller clientId) and assigns it the next slot. Callers hold
// r.mu.
func (r *Replica) genCursorUpToLocked(target uint64) {
	for r.cursorSlot <= target {
		var (
			bestCid uint64
			bestN   uint64
			bestY   int64
			found   bool
		)
		for cid, line := range r.clients {
			n := r.cursorNextN[cid]
			if n == 0 {
				n = 1
			}
			y := line.expectedNs(n)
			if !found || y < bestY || (y == bestY && cid < bestCid) {
				bestCid, bestN, bestY, found = cid, n, y, true
			}
		}
		if !found {
			return // no clients yet
		}
		r.slotMeta[r.cursorSlot] = slotMetaEntry{clientId: bestCid, reqId: bestN, expectedNs: bestY}
		r.cursorNextN[bestCid] = bestN + 1
		r.cursorSlot++
	}
}

// slotOwnerLocked returns the owner (client, seq, expected arrival) of a global
// slot, generating the merge cursor up to it as needed. Callers hold r.mu.
func (r *Replica) slotOwnerLocked(slot uint64) slotMetaEntry {
	r.genCursorUpToLocked(slot)
	return r.slotMeta[slot]
}

// durableAppendLocked records a gap resolution (recovered op or NoOp) in the
// global durable log. The slot was overtaken by later arrivals, so
// durableLog.record patches the placeholder reserved at the slot's in-order
// position instead of appending at the tail. Provenance comes from the slot's
// entry if known, else from the merge cursor. Callers hold r.mu.
func (r *Replica) durableAppendLocked(slot uint64, op []byte, noop bool) {
	if r.durable == nil {
		return
	}
	var clientId, reqId uint64
	if e := r.globalLog[slot]; e != nil && e.clientId != 0 {
		clientId, reqId = e.clientId, e.reqId
	} else {
		m := r.slotOwnerLocked(slot)
		clientId, reqId = m.clientId, m.reqId
	}
	r.durable.record(slot, clientId, reqId, op, noop)
}

func (r *Replica) statsLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		r.mu.Lock()
		recv := r.winRecv
		sum, min, max := r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs
		gaps, recovered, noops := r.winGaps, r.winRecovered, r.winNoops
		dropped := r.winDropped
		r.winRecv, r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs = 0, 0, 0, 0
		r.winGaps, r.winRecovered, r.winNoops = 0, 0, 0
		r.winDropped = 0
		r.mu.Unlock()
		if recv == 0 && gaps == 0 && recovered == 0 && noops == 0 && dropped == 0 {
			continue
		}
		var avg int64
		if recv > 0 {
			avg = sum / int64(recv)
		}
		// Same shape as the C++ replica summary; the gap fields now carry real
		// per-window counts so analyze-logs.py parses both implementations alike.
		Notice("[%s] 1s: received=%d dropped=%d delta_avg=%+dus delta_min=%+dus delta_max=%+dus gaps=%d recovered=%d noops=%d",
			r.self, recv, dropped, avg, min, max, gaps, recovered, noops)
	}
}

// flushLoop batches durable-log writes to disk. The per-log lock serializes with
// concurrent appends, so at most one tick of appends is at risk on a crash.
func (r *Replica) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		r.durable.flush()
	}
}

// ── Inter-replica transport ──────────────────────────────────────────────────

// connectPeers dials every other replica, each in its own retry-until-up
// goroutine, so replicas can boot in any order. The resulting connection is
// write-only from this side: this replica sends gap messages on it, and the
// remote reads them in its accept loop. Replies arrive on the remote's own
// outbound connection (its peerWriters[r.idx]).
func (r *Replica) connectPeers() {
	for j := range r.config.Replicas {
		if j == r.idx {
			continue
		}
		go r.dialPeer(j)
	}
}

func (r *Replica) dialPeer(j int) {
	addr := r.config.Replicas[j]
	for {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
			}
			lw := &lockedWriter{w: bufio.NewWriter(conn)}
			r.mu.Lock()
			r.peerWriters[j] = lw
			r.mu.Unlock()
			Notice("[%s] connected to peer replica %d (%s)", r.self, j, addr)
			return
		}
		Warning("[%s] cannot connect to peer replica %d (%s): %v, retrying",
			r.self, j, addr, err)
		time.Sleep(time.Second)
	}
}

// sendToPeer sends one message to replica j over its persistent connection.
func (r *Replica) sendToPeer(j int, code uint8, msg wireMsg) {
	r.mu.Lock()
	lw := r.peerWriters[j]
	r.mu.Unlock()
	if lw == nil {
		Warning("[%s] no peer connection to replica %d yet, dropping message", r.self, j)
		return
	}
	if err := lw.sendMsg(code, msg); err != nil {
		Warning("[%s] send to peer %d failed: %v", r.self, j, err)
	}
}

// broadcastToPeers sends one message to every replica except this one.
func (r *Replica) broadcastToPeers(code uint8, msg wireMsg) {
	for j := range r.config.Replicas {
		if j == r.idx {
			continue
		}
		r.sendToPeer(j, code, msg)
	}
}

// ── Gap detection ────────────────────────────────────────────────────────────

// gapDetectLoop scans the global log's empty slots in [nextExpected, maxSlotSeen]
// once a second. The merge cursor names each slot's owner and its predicted
// arrival; a slot still empty past expected+delta (on the shared clock) is
// declared a gap: a gapState is registered (its presence guards against a
// duplicate handler) and a handler is spawned on its own goroutine, so the
// client data path keeps flowing while the gap is resolved.
func (r *Replica) gapDetectLoop() {
	type spawnInfo struct {
		slot, clientId, reqId uint64
	}
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		wallNow := wallNs()
		monoNow := nowNs()
		var spawn []spawnInfo
		r.mu.Lock()
		if r.haveMax && len(r.clients) > 0 {
			r.genCursorUpToLocked(r.maxSlotSeen)
			for slot := r.nextExpected; slot <= r.maxSlotSeen; slot++ {
				if e := r.globalLog[slot]; e != nil && e.state != slotEmpty {
					continue
				}
				meta, ok := r.slotMeta[slot]
				if !ok {
					continue
				}
				if wallNow <= meta.expectedNs+int64(gapDelta) {
					continue
				}
				key := gapKey{slot}
				if _, busy := r.gaps[key]; busy {
					continue
				}
				r.gaps[key] = newGapState(monoNow)
				r.winGaps++
				spawn = append(spawn, spawnInfo{slot, meta.clientId, meta.reqId})
			}
		}
		r.mu.Unlock()
		for _, s := range spawn {
			Notice("[%s] GAP detected slot=%d client=%d req=%d", r.self, s.slot, s.clientId, s.reqId)
			go r.handleGap(s.slot)
		}
	}
}

// ── Gap protocol ─────────────────────────────────────────────────────────────

// handleGap drives one gap to resolution on its own goroutine. The leader
// coordinates (recover from a peer, else commit a NoOp); a follower asks the
// leader and waits. Network waits never hold r.mu.
func (r *Replica) handleGap(slot uint64) {
	key := gapKey{slot}
	r.mu.Lock()
	gs := r.gaps[key]
	r.mu.Unlock()
	if gs == nil {
		return
	}
	if r.AmLeader() {
		r.leaderResolve(slot, gs)
	} else {
		r.followerRecover(slot, gs)
	}
}

// ownerLog formats a slot's owner provenance for a log line. Acquires r.mu.
func (r *Replica) ownerLog(slot uint64) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.globalLog[slot]; e != nil && e.clientId != 0 {
		return e.clientId, e.reqId
	}
	m := r.slotOwnerLocked(slot)
	return m.clientId, m.reqId
}

// followerRecover (Scenario 1) asks the leader for the slot and waits for the
// resolution to land: either a BusGapReply with the real op (handleGapReply
// fills the slot) or a BusGapCommit NoOp (handleGapCommit applies it). Both
// signal doneCh. On timeout the detector will retry later.
func (r *Replica) followerRecover(slot uint64, gs *gapState) {
	leader := r.config.LeaderIndex(r.viewId)
	r.sendToPeer(leader, MsgBusGapRequest,
		&BusGapRequest{Slot: slot, SenderIdx: uint32(r.idx)})

	select {
	case <-gs.doneCh:
		lat := (nowNs() - gs.start) / 1000
		r.mu.Lock()
		st := r.slotStateLocked(slot)
		delete(r.gaps, gapKey{slot})
		r.mu.Unlock()
		clientId, reqId := r.ownerLog(slot)
		switch st {
		case slotReceived:
			Notice("[%s] recovery_latency=%dus slot=%d client=%d req=%d", r.self, lat, slot, clientId, reqId)
		case slotNoOp:
			Notice("[%s] noop_latency=%dus slot=%d client=%d req=%d", r.self, lat, slot, clientId, reqId)
		}
	case <-time.After(gapProtocolTimeout):
		Warning("[%s] gap recovery timed out slot=%d", r.self, slot)
		r.mu.Lock()
		delete(r.gaps, gapKey{slot})
		r.mu.Unlock()
	}
}

// leaderResolve coordinates a gap from the leader. It first checks whether the
// slot is already resolved, then probes peers for the real op (Scenario 2), and
// finally commits a NoOp (Scenario 3) if nobody has it.
func (r *Replica) leaderResolve(slot uint64, gs *gapState) {
	key := gapKey{slot}

	// Already resolved (data arrived, or a NoOp was already committed)?
	r.mu.Lock()
	if st := r.slotStateLocked(slot); st != slotEmpty {
		op := r.slotOpLocked(slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		if st == slotReceived {
			r.answerAskers(slot, askers, op)
		}
		return
	}
	r.mu.Unlock()

	// Scenario 2: probe all peers for the real op.
	r.broadcastToPeers(MsgBusGapRequest,
		&BusGapRequest{Slot: slot, SenderIdx: uint32(r.idx)})
	var recovered []byte
	timeout := time.After(gapProtocolTimeout)
probe:
	for {
		select {
		case reply := <-gs.probeReplies:
			if reply.Found {
				recovered = reply.Op
				break probe
			}
		case <-timeout:
			break probe
		}
	}

	if recovered != nil {
		r.mu.Lock()
		m := r.slotOwnerLocked(slot)
		stored := r.recordReceivedLocked(slot, m.clientId, m.reqId, recovered)
		if stored {
			r.durableAppendLocked(slot, recovered, false)
			r.winRecovered++
		}
		r.advanceNextExpectedLocked()
		op := r.slotOpLocked(slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		r.answerAskers(slot, askers, op)
		Notice("[%s] recovery_latency=%dus slot=%d client=%d req=%d",
			r.self, (nowNs()-gs.start)/1000, slot, m.clientId, m.reqId)
		return
	}

	// Scenario 3: nobody has it. Commit a NoOp, but only if the slot is still
	// EMPTY (commit-checks-empty); if the real request arrived during the probe,
	// keep it and answer the askers with the real data instead.
	r.mu.Lock()
	if r.slotStateLocked(slot) == slotReceived {
		op := r.slotOpLocked(slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		r.answerAskers(slot, askers, op)
		return
	}
	if r.setNoOpLocked(slot) {
		r.durableAppendLocked(slot, nil, true)
		r.winNoops++
	}
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

	// Broadcast the commit and wait for f+1 acks (counting this leader).
	r.broadcastToPeers(MsgBusGapCommit,
		&BusGapCommit{Slot: slot, SenderIdx: uint32(r.idx), ViewId: r.viewId})
	acks := 1
	timeout = time.After(gapProtocolTimeout)
collect:
	for acks < r.config.QuorumSize() {
		select {
		case <-gs.commitAcks:
			acks++
		case <-timeout:
			Warning("[%s] noop commit got only %d/%d acks slot=%d",
				r.self, acks, r.config.QuorumSize(), slot)
			break collect
		}
	}
	clientId, reqId := r.ownerLog(slot)
	r.mu.Lock()
	delete(r.gaps, key)
	r.mu.Unlock()
	Notice("[%s] noop_latency=%dus slot=%d client=%d req=%d",
		r.self, (nowNs()-gs.start)/1000, slot, clientId, reqId)
}

// answerAskers sends the recovered op to every follower that asked the leader
// for this slot (pull-based: only the askers, no rebroadcast).
func (r *Replica) answerAskers(slot uint64, askers []uint32, op []byte) {
	for _, idx := range askers {
		r.sendToPeer(int(idx), MsgBusGapReply,
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: true, Op: op})
	}
}

// handleGapRequest answers a peer's "do you have this slot?". On the leader it
// is a follower's recovery request (Scenario 1): serve the op if present, send a
// NoOp commit if already NoOp'd, or start leader resolution and register the
// asker if the leader also lacks it. On a follower it is the leader's probe
// (Scenario 2): reply Found with the op, or not-found.
func (r *Replica) handleGapRequest(msg *BusGapRequest) {
	slot := msg.Slot
	r.mu.Lock()
	st := r.slotStateLocked(slot)
	op := r.slotOpLocked(slot)
	r.mu.Unlock()

	if !r.AmLeader() {
		reply := &BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: st == slotReceived}
		if reply.Found {
			reply.Op = op
		}
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply, reply)
		return
	}

	switch st {
	case slotReceived:
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply,
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: true, Op: op})
	case slotNoOp:
		// Already NoOp'd: tell the asker to apply the NoOp directly.
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommit,
			&BusGapCommit{Slot: slot, SenderIdx: uint32(r.idx), ViewId: r.viewId})
	default:
		r.ensureLeaderResolve(slot, msg.SenderIdx)
	}
}

// ensureLeaderResolve starts leader resolution for a slot the leader is missing
// (registering the asking follower), or just adds the asker if resolution is
// already underway.
func (r *Replica) ensureLeaderResolve(slot uint64, asker uint32) {
	key := gapKey{slot}
	r.mu.Lock()
	gs := r.gaps[key]
	spawn := false
	if gs == nil {
		gs = newGapState(nowNs())
		r.gaps[key] = gs
		r.winGaps++
		spawn = true
	}
	gs.askers[asker] = struct{}{}
	m := r.slotOwnerLocked(slot)
	r.mu.Unlock()
	if spawn {
		Notice("[%s] GAP detected slot=%d client=%d req=%d (peer request)", r.self, slot, m.clientId, m.reqId)
		go r.handleGap(slot)
	}
}

// handleGapReply routes a BusGapReply. On the leader it is a probe answer
// (Scenario 2), forwarded to the waiting resolver. On a follower it is the
// leader's answer to its own request: fill the slot with the recovered op and
// signal the waiter.
func (r *Replica) handleGapReply(msg *BusGapReply) {
	key := gapKey{msg.Slot}
	r.mu.Lock()
	gs := r.gaps[key]
	r.mu.Unlock()
	if gs == nil {
		return
	}
	if r.AmLeader() {
		select {
		case gs.probeReplies <- msg:
		default:
		}
		return
	}
	if msg.Found {
		r.mu.Lock()
		m := r.slotOwnerLocked(msg.Slot)
		if r.recordReceivedLocked(msg.Slot, m.clientId, m.reqId, msg.Op) {
			r.durableAppendLocked(msg.Slot, msg.Op, false)
			r.winRecovered++
		}
		r.advanceNextExpectedLocked()
		r.mu.Unlock()
		select {
		case gs.doneCh <- struct{}{}:
		default:
		}
	}
}

// handleGapCommit applies the leader's authoritative NoOp (overwriting EMPTY or
// RECEIVED — the leader has initiative), acks it, and signals any waiter.
func (r *Replica) handleGapCommit(msg *BusGapCommit) {
	key := gapKey{msg.Slot}
	r.mu.Lock()
	if r.setNoOpLocked(msg.Slot) {
		r.durableAppendLocked(msg.Slot, nil, true)
		r.winNoops++
	}
	r.advanceNextExpectedLocked()
	gs := r.gaps[key]
	r.mu.Unlock()
	if gs != nil {
		select {
		case gs.doneCh <- struct{}{}:
		default:
		}
	}
	r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommitReply,
		&BusGapCommitReply{Slot: msg.Slot, SenderIdx: uint32(r.idx)})
}

// handleGapCommitReply forwards a follower's NoOp-commit ack to the waiting
// leader resolver.
func (r *Replica) handleGapCommitReply(msg *BusGapCommitReply) {
	r.mu.Lock()
	gs := r.gaps[gapKey{msg.Slot}]
	r.mu.Unlock()
	if gs == nil {
		return
	}
	select {
	case gs.commitAcks <- struct{}{}:
	default:
	}
}
