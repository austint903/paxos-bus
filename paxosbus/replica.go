package paxosbus

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// nowNs is a monotonic nanosecond clock (Go's time.Since uses the monotonic
// reading, matching the C++ CLOCK_MONOTONIC usage).
var startTime = time.Now()

func nowNs() int64 {
	return time.Since(startTime).Nanoseconds()
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

// slotEntry holds one client slot: its state and, for a received slot, the op
// bytes (so a peer can serve the exact request during recovery).
type slotEntry struct {
	state slotState
	op    []byte
}

// clientStream is the replica's per-client arrival model. Expected arrival of
// seq N is base_recv_ns + (N-1)*interval_ns (the T = m*x + b line: m=interval,
// b=base). base is provisionally anchored from the sync schedule, then pinned
// ONCE through the first observed data arrival. The line stays fixed after
// that, so delta measures cumulative drift from a stable baseline. (Valid
// locally where there are no packet drops to break the seq->time mapping.)
//
// log is the queryable, back-fillable per-slot map gap agreement repairs;
// nextExpected is the lowest slot not yet contiguously filled (received or
// noop'd), so empty slots in [nextExpected, maxSeqSeen] are gap candidates.
type clientStream struct {
	baseRecvNs   int64
	intervalNs   int64
	anchored     bool
	nextExpected uint64
	maxSeqSeen   uint64
	log          map[uint64]*slotEntry
}

// slot returns the entry for seq n, creating an EMPTY one on first touch.
func (s *clientStream) slot(n uint64) *slotEntry {
	e := s.log[n]
	if e == nil {
		e = &slotEntry{state: slotEmpty}
		s.log[n] = e
	}
	return e
}

// gapKey identifies an in-flight gap (one client slot).
type gapKey struct {
	clientId uint64
	slot     uint64
}

// gapState coordinates one in-flight gap. Presence of a key in Replica.gaps is
// itself the "gap-in-progress" guard that stops the detector from spawning a
// duplicate handler. The role-specific fields are used only on the side that
// drives that step:
//   - askers/probeReplies/commitAcks: the leader, while resolving.
//   - doneCh: a follower, while waiting for the leader's resolution.
type gapState struct {
	start        int64               // gap-detection time, for latency logging
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

type Replica struct {
	config *Config
	idx    int
	viewId uint64
	self   string // log-line identity, e.g. "Replica 1 europe-north1"

	mu      sync.Mutex
	clients map[uint64]*clientStream

	// Persistent outbound connections to every other replica (index r.idx is
	// nil). Gap handlers reply over peerWriters[SenderIdx]. Guarded by mu.
	peerWriters []*lockedWriter

	// In-flight gaps, keyed by (client, slot). Presence is the duplicate guard.
	// Guarded by mu.
	gaps map[gapKey]*gapState

	// Durable per-client logs (slot == client seq). Separate from the stderr
	// logs above; empty logDir disables them. Guarded by mu.
	logDir     string
	clientLogs map[uint64]*clientLog

	// 1-second window counters for the periodic summary line (all clients).
	winRecv       uint64
	winDeltaSumUs int64
	winDeltaMinUs int64
	winDeltaMaxUs int64
	winGaps       uint64
	winRecovered  uint64
	winNoops      uint64
}

func NewReplica(config *Config, idx int, label, logDir string) *Replica {
	self := "Replica " + strconv.Itoa(idx)
	if label != "" {
		self += " " + label
	}
	r := &Replica{
		config:      config,
		idx:         idx,
		viewId:      0,
		self:        self,
		clients:     make(map[uint64]*clientStream),
		peerWriters: make([]*lockedWriter, config.N),
		gaps:        make(map[gapKey]*gapState),
		logDir:      logDir,
		clientLogs:  make(map[uint64]*clientLog),
	}
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			Warning("[%s] cannot create durable log dir %s: %v", self, logDir, err)
			r.logDir = ""
		} else {
			Notice("[%s] durable per-client logs in %s", self, logDir)
		}
	}
	leader := "no"
	if r.AmLeader() {
		leader = "yes"
	}
	Notice("[%s] started (view=0, f=%d, quorum=%d, leader=%s)",
		r.self, config.F, config.QuorumSize(), leader)
	return r
}

func (r *Replica) AmLeader() bool {
	return r.config.LeaderIndex(r.viewId) == r.idx
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
	if r.logDir != "" {
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
			if !r.handleRequest(&reqMsg) {
				continue
			}
			// LogSlotNum == RequestId until gap agreement makes them diverge.
			// Result is nil: there is no execution layer, so the op is logged
			// but not applied.
			reply = BusReplyMessage{
				ClientId:   reqMsg.ClientId,
				RequestId:  reqMsg.RequestId,
				LogSlotNum: reqMsg.RequestId,
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
	recvNs := nowNs()
	r.mu.Lock()
	// Anchor baseline to when the first data message is expected to arrive:
	// base = sync_recv + start_delay, so expected(N) = base + (N-1)*interval.
	// Provisional; re-anchored to observed arrivals in handleRequest.
	r.clients[msg.ClientId] = &clientStream{
		baseRecvNs:   recvNs + int64(msg.StartDelayMs)*1e6,
		intervalNs:   int64(msg.IntervalMs) * 1e6,
		anchored:     false,
		nextExpected: 1,
		maxSeqSeen:   0,
		log:          make(map[uint64]*slotEntry),
	}
	r.mu.Unlock()
	Notice("[%s] sync from client %d: interval=%dms",
		r.self, msg.ClientId, msg.IntervalMs)
}

// handleRequest updates the arrival model and stats; returns false if the
// client never synced (no reply is sent then, as in the C++ implementation).
func (r *Replica) handleRequest(msg *BusRequestMessage) bool {
	actualNs := nowNs()
	r.mu.Lock()
	s, ok := r.clients[msg.ClientId]
	if !ok {
		r.mu.Unlock()
		Warning("[%s] request from unsynced client %d, ignoring", r.self, msg.ClientId)
		return false
	}
	if msg.RequestId > s.maxSeqSeen {
		s.maxSeqSeen = msg.RequestId
	}

	// Anchor once: pin b through the first observed arrival so expected(this
	// req) == actual, then leave the line fixed. Subsequent deltas are measured
	// against that fixed line, so they capture cumulative drift, not per-message
	// jitter off a sliding anchor.
	var deltaUs int64
	if !s.anchored {
		s.baseRecvNs = actualNs - int64(msg.RequestId-1)*s.intervalNs
		s.anchored = true
	} else {
		expectedNs := s.baseRecvNs + int64(msg.RequestId-1)*s.intervalNs
		deltaUs = (actualNs - expectedNs) / 1000
	}
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

	// Record the slot under the state machine. A late request bounces off a
	// committed NoOp (NoOp is final) and a duplicate is ignored; only a fresh
	// EMPTY->RECEIVED transition is durably logged. advanceNextExpected then
	// walks past any contiguous slots a prior gap fill may have completed.
	stored := r.recordReceivedLocked(msg.ClientId, msg.RequestId, msg.Op)
	r.advanceNextExpectedLocked(msg.ClientId)
	// Lazily open the durable log on the first request (the sync message itself
	// is never logged — only client->replica operations get a slot).
	cl := r.clientLogs[msg.ClientId]
	if cl == nil && r.logDir != "" {
		var err error
		if cl, err = openClientLog(r.logDir, msg.ClientId); err != nil {
			Warning("[%s] cannot open durable log for client %d: %v",
				r.self, msg.ClientId, err)
		} else {
			r.clientLogs[msg.ClientId] = cl
		}
	}
	r.mu.Unlock()

	// Durably record the op in this client's per-slot log (slot == req id). The
	// append only buffers; flushLoop persists it in batches. clientLog has its
	// own lock, so this stays outside r.mu.
	if stored && cl != nil {
		cl.append(msg.RequestId, msg.RequestId, msg.Op, false)
	}
	return true
}

// recordReceivedLocked applies the data-path slot rule for real data: a
// committed NOOP is final (drop), a RECEIVED slot is idempotent (ignore), and
// an EMPTY slot is filled with the op. Returns true only on the EMPTY->RECEIVED
// transition (i.e. when a durable append is warranted). Callers hold r.mu.
func (r *Replica) recordReceivedLocked(clientId, slot uint64, op []byte) bool {
	s := r.clients[clientId]
	if s == nil {
		return false
	}
	e := s.slot(slot)
	if slot > s.maxSeqSeen {
		s.maxSeqSeen = slot
	}
	switch e.state {
	case slotNoOp, slotReceived:
		return false
	default:
		e.state = slotReceived
		e.op = op
		return true
	}
}

// setNoOpLocked commits a NoOp at a slot, overwriting EMPTY or RECEIVED (the
// leader has initiative once it broadcasts a commit). Returns true on a real
// transition (so the change is logged once). Callers hold r.mu.
func (r *Replica) setNoOpLocked(clientId, slot uint64) bool {
	s := r.clients[clientId]
	if s == nil {
		return false
	}
	e := s.slot(slot)
	if slot > s.maxSeqSeen {
		s.maxSeqSeen = slot
	}
	if e.state == slotNoOp {
		return false
	}
	e.state = slotNoOp
	e.op = nil
	return true
}

// slotStateLocked reports a slot's state (EMPTY if the client/slot is unknown).
// Callers hold r.mu.
func (r *Replica) slotStateLocked(clientId, slot uint64) slotState {
	s := r.clients[clientId]
	if s == nil {
		return slotEmpty
	}
	if e := s.log[slot]; e != nil {
		return e.state
	}
	return slotEmpty
}

// advanceNextExpectedLocked walks nextExpected past every contiguous filled
// (received or noop'd) slot, so the detector's scan window starts at the first
// real hole. Callers hold r.mu.
func (r *Replica) advanceNextExpectedLocked(clientId uint64) {
	s := r.clients[clientId]
	if s == nil {
		return
	}
	for {
		e := s.log[s.nextExpected]
		if e == nil || e.state == slotEmpty {
			return
		}
		s.nextExpected++
	}
}

// durableAppendLocked records a gap resolution (recovered op or NoOp) in the
// client's durable log, lazily opening it. Used only on the rare gap path, so
// the buffered append under r.mu is acceptable. Callers hold r.mu.
func (r *Replica) durableAppendLocked(clientId, slot, reqId uint64, op []byte, noop bool) {
	if r.logDir == "" {
		return
	}
	cl := r.clientLogs[clientId]
	if cl == nil {
		var err error
		if cl, err = openClientLog(r.logDir, clientId); err != nil {
			Warning("[%s] cannot open durable log for client %d: %v", r.self, clientId, err)
			return
		}
		r.clientLogs[clientId] = cl
	}
	cl.append(slot, reqId, op, noop)
}

func (r *Replica) statsLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		r.mu.Lock()
		recv := r.winRecv
		sum, min, max := r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs
		gaps, recovered, noops := r.winGaps, r.winRecovered, r.winNoops
		r.winRecv, r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs = 0, 0, 0, 0
		r.winGaps, r.winRecovered, r.winNoops = 0, 0, 0
		r.mu.Unlock()
		if recv == 0 && gaps == 0 && recovered == 0 && noops == 0 {
			continue
		}
		var avg int64
		if recv > 0 {
			avg = sum / int64(recv)
		}
		// Same shape as the C++ replica summary; the gap fields now carry real
		// per-window counts so analyze-logs.py parses both implementations alike.
		Notice("[%s] 1s: received=%d dropped=0 delta_avg=%+dus delta_min=%+dus delta_max=%+dus gaps=%d recovered=%d noops=%d",
			r.self, recv, avg, min, max, gaps, recovered, noops)
	}
}

// flushLoop batches durable-log writes to disk. Every tick it snapshots the set
// of client logs under mu, then flushes each (the per-log lock serializes with
// concurrent appends), so at most one tick of appends is at risk on a crash.
func (r *Replica) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		r.mu.Lock()
		logs := make([]*clientLog, 0, len(r.clientLogs))
		for _, cl := range r.clientLogs {
			logs = append(logs, cl)
		}
		r.mu.Unlock()
		for _, cl := range logs {
			cl.flush()
		}
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

// gapDetectLoop scans every client's empty slots in [nextExpected, maxSeqSeen]
// once a second. A slot still empty past expected(slot)+delta is declared a gap:
// a gapState is registered (its presence guards against a duplicate handler) and
// a handler is spawned on its own goroutine, so the client data path keeps
// flowing while the gap is resolved.
func (r *Replica) gapDetectLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		now := nowNs()
		var spawn []gapKey
		r.mu.Lock()
		for clientId, s := range r.clients {
			if !s.anchored {
				continue
			}
			for seq := s.nextExpected; seq <= s.maxSeqSeen; seq++ {
				if e := s.log[seq]; e != nil && e.state != slotEmpty {
					continue
				}
				deadline := s.baseRecvNs + int64(seq-1)*s.intervalNs + int64(gapDelta)
				if now <= deadline {
					continue
				}
				key := gapKey{clientId, seq}
				if _, busy := r.gaps[key]; busy {
					continue
				}
				r.gaps[key] = newGapState(now)
				r.winGaps++
				spawn = append(spawn, key)
			}
		}
		r.mu.Unlock()
		for _, key := range spawn {
			Notice("[%s] GAP detected seq=%d client=%d", r.self, key.slot, key.clientId)
			go r.handleGap(key.clientId, key.slot)
		}
	}
}

// ── Gap protocol ─────────────────────────────────────────────────────────────

// handleGap drives one gap to resolution on its own goroutine. The leader
// coordinates (recover from a peer, else commit a NoOp); a follower asks the
// leader and waits. Network waits never hold r.mu.
func (r *Replica) handleGap(clientId, slot uint64) {
	key := gapKey{clientId, slot}
	r.mu.Lock()
	gs := r.gaps[key]
	r.mu.Unlock()
	if gs == nil {
		return
	}
	if r.AmLeader() {
		r.leaderResolve(clientId, slot, gs)
	} else {
		r.followerRecover(clientId, slot, gs)
	}
}

// followerRecover (Scenario 1) asks the leader for the slot and waits for the
// resolution to land: either a BusGapReply with the real op (handleGapReply
// fills the slot) or a BusGapCommit NoOp (handleGapCommit applies it). Both
// signal doneCh. On timeout the detector will retry later.
func (r *Replica) followerRecover(clientId, slot uint64, gs *gapState) {
	leader := r.config.LeaderIndex(r.viewId)
	r.sendToPeer(leader, MsgBusGapRequest,
		&BusGapRequest{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx)})

	select {
	case <-gs.doneCh:
		lat := (nowNs() - gs.start) / 1000
		r.mu.Lock()
		st := r.slotStateLocked(clientId, slot)
		delete(r.gaps, gapKey{clientId, slot})
		r.mu.Unlock()
		switch st {
		case slotReceived:
			Notice("[%s] recovery_latency=%dus client=%d seq=%d", r.self, lat, clientId, slot)
		case slotNoOp:
			Notice("[%s] noop_latency=%dus client=%d seq=%d", r.self, lat, clientId, slot)
		}
	case <-time.After(gapProtocolTimeout):
		Warning("[%s] gap recovery timed out client=%d seq=%d", r.self, clientId, slot)
		r.mu.Lock()
		delete(r.gaps, gapKey{clientId, slot})
		r.mu.Unlock()
	}
}

// leaderResolve coordinates a gap from the leader. It first checks whether the
// slot is already resolved, then probes peers for the real op (Scenario 2), and
// finally commits a NoOp (Scenario 3) if nobody has it.
func (r *Replica) leaderResolve(clientId, slot uint64, gs *gapState) {
	key := gapKey{clientId, slot}

	// Already resolved (data arrived, or a NoOp was already committed)?
	r.mu.Lock()
	if st := r.slotStateLocked(clientId, slot); st != slotEmpty {
		op := r.slotOpLocked(clientId, slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		if st == slotReceived {
			r.answerAskers(clientId, slot, askers, op)
		}
		return
	}
	r.mu.Unlock()

	// Scenario 2: probe all peers for the real op.
	r.broadcastToPeers(MsgBusGapRequest,
		&BusGapRequest{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx)})
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
		stored := r.recordReceivedLocked(clientId, slot, recovered)
		if stored {
			r.durableAppendLocked(clientId, slot, slot, recovered, false)
			r.winRecovered++
		}
		r.advanceNextExpectedLocked(clientId)
		op := r.slotOpLocked(clientId, slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		r.answerAskers(clientId, slot, askers, op)
		Notice("[%s] recovery_latency=%dus client=%d seq=%d",
			r.self, (nowNs()-gs.start)/1000, clientId, slot)
		return
	}

	// Scenario 3: nobody has it. Commit a NoOp, but only if the slot is still
	// EMPTY (commit-checks-empty); if the real request arrived during the probe,
	// keep it and answer the askers with the real data instead.
	r.mu.Lock()
	if r.slotStateLocked(clientId, slot) == slotReceived {
		op := r.slotOpLocked(clientId, slot)
		askers := gs.snapshotAskers()
		delete(r.gaps, key)
		r.mu.Unlock()
		r.answerAskers(clientId, slot, askers, op)
		return
	}
	if r.setNoOpLocked(clientId, slot) {
		r.durableAppendLocked(clientId, slot, 0, nil, true)
		r.winNoops++
	}
	r.advanceNextExpectedLocked(clientId)
	r.mu.Unlock()

	// Broadcast the commit and wait for f+1 acks (counting this leader).
	r.broadcastToPeers(MsgBusGapCommit,
		&BusGapCommit{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx), ViewId: r.viewId})
	acks := 1
	timeout = time.After(gapProtocolTimeout)
collect:
	for acks < r.config.QuorumSize() {
		select {
		case <-gs.commitAcks:
			acks++
		case <-timeout:
			Warning("[%s] noop commit got only %d/%d acks client=%d seq=%d",
				r.self, acks, r.config.QuorumSize(), clientId, slot)
			break collect
		}
	}
	r.mu.Lock()
	delete(r.gaps, key)
	r.mu.Unlock()
	Notice("[%s] noop_latency=%dus client=%d seq=%d",
		r.self, (nowNs()-gs.start)/1000, clientId, slot)
}

// slotOpLocked returns a slot's op bytes (nil if absent). Callers hold r.mu.
func (r *Replica) slotOpLocked(clientId, slot uint64) []byte {
	s := r.clients[clientId]
	if s == nil {
		return nil
	}
	if e := s.log[slot]; e != nil {
		return e.op
	}
	return nil
}

// answerAskers sends the recovered op to every follower that asked the leader
// for this slot (pull-based: only the askers, no rebroadcast).
func (r *Replica) answerAskers(clientId, slot uint64, askers []uint32, op []byte) {
	for _, idx := range askers {
		r.sendToPeer(int(idx), MsgBusGapReply,
			&BusGapReply{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx), Found: true, Op: op})
	}
}

// handleGapRequest answers a peer's "do you have this slot?". On the leader it
// is a follower's recovery request (Scenario 1): serve the op if present, send a
// NoOp commit if already NoOp'd, or start leader resolution and register the
// asker if the leader also lacks it. On a follower it is the leader's probe
// (Scenario 2): reply Found with the op, or not-found.
func (r *Replica) handleGapRequest(msg *BusGapRequest) {
	clientId, slot := msg.ClientId, msg.Slot
	r.mu.Lock()
	st := r.slotStateLocked(clientId, slot)
	op := r.slotOpLocked(clientId, slot)
	r.mu.Unlock()

	if !r.AmLeader() {
		reply := &BusGapReply{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx), Found: st == slotReceived}
		if reply.Found {
			reply.Op = op
		}
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply, reply)
		return
	}

	switch st {
	case slotReceived:
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply,
			&BusGapReply{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx), Found: true, Op: op})
	case slotNoOp:
		// Already NoOp'd: tell the asker to apply the NoOp directly.
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommit,
			&BusGapCommit{ClientId: clientId, Slot: slot, SenderIdx: uint32(r.idx), ViewId: r.viewId})
	default:
		r.ensureLeaderResolve(clientId, slot, msg.SenderIdx)
	}
}

// ensureLeaderResolve starts leader resolution for a slot the leader is missing
// (registering the asking follower), or just adds the asker if resolution is
// already underway.
func (r *Replica) ensureLeaderResolve(clientId, slot uint64, asker uint32) {
	key := gapKey{clientId, slot}
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
	r.mu.Unlock()
	if spawn {
		Notice("[%s] GAP detected seq=%d client=%d (peer request)", r.self, slot, clientId)
		go r.handleGap(clientId, slot)
	}
}

// handleGapReply routes a BusGapReply. On the leader it is a probe answer
// (Scenario 2), forwarded to the waiting resolver. On a follower it is the
// leader's answer to its own request: fill the slot with the recovered op and
// signal the waiter.
func (r *Replica) handleGapReply(msg *BusGapReply) {
	key := gapKey{msg.ClientId, msg.Slot}
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
		if r.recordReceivedLocked(msg.ClientId, msg.Slot, msg.Op) {
			r.durableAppendLocked(msg.ClientId, msg.Slot, msg.Slot, msg.Op, false)
			r.winRecovered++
		}
		r.advanceNextExpectedLocked(msg.ClientId)
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
	key := gapKey{msg.ClientId, msg.Slot}
	r.mu.Lock()
	if r.setNoOpLocked(msg.ClientId, msg.Slot) {
		r.durableAppendLocked(msg.ClientId, msg.Slot, 0, nil, true)
		r.winNoops++
	}
	r.advanceNextExpectedLocked(msg.ClientId)
	gs := r.gaps[key]
	r.mu.Unlock()
	if gs != nil {
		select {
		case gs.doneCh <- struct{}{}:
		default:
		}
	}
	r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommitReply,
		&BusGapCommitReply{ClientId: msg.ClientId, Slot: msg.Slot, SenderIdx: uint32(r.idx)})
}

// handleGapCommitReply forwards a follower's NoOp-commit ack to the waiting
// leader resolver.
func (r *Replica) handleGapCommitReply(msg *BusGapCommitReply) {
	r.mu.Lock()
	gs := r.gaps[gapKey{msg.ClientId, msg.Slot}]
	r.mu.Unlock()
	if gs == nil {
		return
	}
	select {
	case gs.commitAcks <- struct{}{}:
	default:
	}
}
