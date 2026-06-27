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

var startTime = time.Now()

func nowNs() int64 {
	return time.Since(startTime).Nanoseconds()
}

func wallNs() int64 {
	return time.Now().UnixNano()
}

type slotState uint8

const (
	slotEmpty slotState = iota
	slotReceived
	slotNoOp
)

type clientLine struct {
	baseNs     int64
	intervalNs int64
	maxSeqSeen uint64
}

func (cl *clientLine) expectedNs(n uint64) int64 {
	return cl.baseNs + int64(n-1)*cl.intervalNs
}

type globalEntry struct {
	clientId uint64
	reqId    uint64
	state    slotState
	op       []byte
	requests []RequestMessage
	isBus    bool
}

type reqKey struct {
	clientId  uint64
	requestId uint64
}

type logEntry struct {
	clientId  uint64
	requestId uint64
	op        []byte
}

type pendingReply struct {
	clientId  uint64
	requestId uint64
	busSlot   uint64
	logIndex  uint64
	viewId    uint64
}

type slotMetaEntry struct {
	clientId   uint64
	reqId      uint64
	expectedNs int64
}

func computeGlobalSlot(lines map[uint64]*clientLine, clientId, reqId uint64) uint64 {
	self := lines[clientId]
	if self == nil {
		return reqId - 1
	}
	y := self.expectedNs(reqId)
	slot := reqId - 1
	for cid, line := range lines {
		if cid == clientId {
			continue
		}
		slot += countBefore(line, y, cid < clientId)
	}
	return slot
}

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

type gapKey struct {
	slot uint64
}

type gapState struct {
	start        int64
	askers       map[uint32]struct{}
	probeReplies chan *BusGapReply
	commitAcks   chan struct{}
	doneCh       chan struct{}
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

func (gs *gapState) snapshotAskers() []uint32 {
	out := make([]uint32, 0, len(gs.askers))
	for a := range gs.askers {
		out = append(out, a)
	}
	return out
}

const (
	gapDelta           = 5 * time.Second
	gapProtocolTimeout = 3 * time.Second
)

type dropMode uint8

const (
	dropNone dropMode = iota
	dropLeader
	dropFollowers
	dropAll
)

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
	self   string

	mu      sync.Mutex
	clients map[uint64]*clientLine

	globalLog    map[uint64]*globalEntry
	nextExpected uint64
	maxSlotSeen  uint64
	haveMax      bool

	logList []logEntry
	dedup   map[reqKey]uint64
	busMode bool

	pendingBuses []*BusMessage

	cwMu          sync.Mutex
	clientWriters map[uint64]*lockedWriter
	replyCh       chan pendingReply

	cursorNextN map[uint64]uint64
	cursorSlot  uint64
	slotMeta    map[uint64]slotMetaEntry

	peerWriters []*lockedWriter

	gaps map[gapKey]*gapState

	logDir  string
	durable *durableLog

	dropMode  dropMode
	dropEvery uint64

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
		config:        config,
		idx:           idx,
		viewId:        0,
		self:          self,
		clients:       make(map[uint64]*clientLine),
		globalLog:     make(map[uint64]*globalEntry),
		dedup:         make(map[reqKey]uint64),
		clientWriters: make(map[uint64]*lockedWriter),
		replyCh:       make(chan pendingReply, 1<<16),
		cursorNextN:   make(map[uint64]uint64),
		slotMeta:      make(map[uint64]slotMetaEntry),
		peerWriters:   make([]*lockedWriter, config.N),
		gaps:          make(map[gapKey]*gapState),
		logDir:        logDir,
		dropMode:      mode,
		dropEvery:     every,
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
		return !r.AmLeader() && r.followerRankLocked() < r.config.F
	default:
		return false
	}
}

func (r *Replica) Run() error {
	l, err := net.Listen("tcp", "0.0.0.0:"+r.config.Port(r.idx))
	if err != nil {
		return err
	}
	go r.statsLoop()
	go r.connectPeers()
	go r.gapDetectLoop()
	go r.replyLoop()
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

func (r *Replica) clientListener(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	lw := &lockedWriter{w: bufio.NewWriter(conn)}

	var (
		syncMsg BusSyncMessage
		reqMsg  BusRequestMessage
		busMsg  BusMessage
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
			reply = BusReplyMessage{
				ClientId:   reqMsg.ClientId,
				RequestId:  reqMsg.RequestId,
				LogSlotNum: slot,
				ViewId:     r.viewId,
				ReplicaIdx: uint32(r.idx),
				Result:     nil,
			}
			if err := lw.sendMsg(MsgBusReply, &reply); err != nil {
				Warning("[%s] failed to send reply for req=%d: %v",
					r.self, reqMsg.RequestId, err)
				return
			}

		case MsgBus:
			if err := busMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad bus message: %v", r.self, err)
				return
			}
			r.handleBus(&busMsg, lw)

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
	r.clients[msg.ClientId] = &clientLine{
		baseNs:     int64(msg.SendTimeNs) + int64(msg.StartDelayMs)*1e6,
		intervalNs: int64(msg.IntervalMs) * 1e6,
	}
	r.resetCursorLocked()
	r.mu.Unlock()
	Notice("[%s] sync from client %d: interval=%dms",
		r.self, msg.ClientId, msg.IntervalMs)
}

func (r *Replica) resetCursorLocked() {
	r.cursorSlot = 0
	r.cursorNextN = make(map[uint64]uint64)
	r.slotMeta = make(map[uint64]slotMetaEntry)
}

func (r *Replica) handleRequest(msg *BusRequestMessage) (uint64, bool) {
	actualNs := wallNs()
	r.mu.Lock()
	line, ok := r.clients[msg.ClientId]
	if !ok {
		r.mu.Unlock()
		Warning("[%s] request from unsynced client %d, ignoring", r.self, msg.ClientId)
		return 0, false
	}
	if r.shouldDropLocked(msg.RequestId) {
		r.winDropped++
		r.mu.Unlock()
		return 0, false
	}
	if msg.RequestId > line.maxSeqSeen {
		line.maxSeqSeen = msg.RequestId
	}

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

	slot := computeGlobalSlot(r.clients, msg.ClientId, msg.RequestId)
	stored := r.recordReceivedLocked(slot, msg.ClientId, msg.RequestId, msg.Op)
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

	if stored && r.durable != nil {
		r.durable.record(slot, msg.ClientId, msg.RequestId, msg.Op, false)
	}
	return slot, true
}

func (r *Replica) handleBus(msg *BusMessage, lw *lockedWriter) {
	actualNs := wallNs()
	r.cwMu.Lock()
	r.clientWriters[msg.ClientId] = lw
	r.cwMu.Unlock()

	r.mu.Lock()
	line, ok := r.clients[msg.ClientId]
	if !ok {
		r.mu.Unlock()
		Warning("[%s] bus from unsynced client %d, ignoring", r.self, msg.ClientId)
		return
	}
	if r.shouldDropLocked(msg.BusSeqNum) {
		r.winDropped++
		r.mu.Unlock()
		return
	}
	if msg.BusSeqNum > line.maxSeqSeen {
		line.maxSeqSeen = msg.BusSeqNum
	}

	expectedNs := line.expectedNs(msg.BusSeqNum)
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

	r.busMode = true

	if len(r.gaps) > 0 {
		slot := computeGlobalSlot(r.clients, msg.ClientId, msg.BusSeqNum)
		if gs, isGap := r.gaps[gapKey{slot}]; isGap {
			r.recordBusReceivedLocked(slot, msg.ClientId, msg.BusSeqNum, msg.Requests)
			r.advanceNextExpectedLocked()
			if gs != nil {
				select {
				case gs.doneCh <- struct{}{}:
				default:
				}
			}
			r.mu.Unlock()
			return
		}
		cp := *msg
		r.pendingBuses = append(r.pendingBuses, &cp)
		r.mu.Unlock()
		return
	}

	slot := computeGlobalSlot(r.clients, msg.ClientId, msg.BusSeqNum)
	r.recordBusReceivedLocked(slot, msg.ClientId, msg.BusSeqNum, msg.Requests)
	r.advanceNextExpectedLocked()
	r.mu.Unlock()
}

func (r *Replica) finishGapLocked(key gapKey) {
	delete(r.gaps, key)
	if len(r.gaps) == 0 {
		r.drainPendingBusesLocked()
	}
}

func (r *Replica) drainPendingBusesLocked() {
	if len(r.pendingBuses) == 0 {
		return
	}
	buses := r.pendingBuses
	r.pendingBuses = nil
	for _, b := range buses {
		slot := computeGlobalSlot(r.clients, b.ClientId, b.BusSeqNum)
		r.recordBusReceivedLocked(slot, b.ClientId, b.BusSeqNum, b.Requests)
	}
	r.advanceNextExpectedLocked()
}

func (r *Replica) recordBusReceivedLocked(slot, clientId, busSeq uint64, reqs []RequestMessage) bool {
	r.observeSlotLocked(slot)
	e := r.slotEntryLocked(slot)
	switch e.state {
	case slotNoOp, slotReceived:
		return false
	default:
		e.state = slotReceived
		e.clientId = clientId
		e.reqId = busSeq
		e.requests = reqs
		e.isBus = true
		return true
	}
}

func (r *Replica) slotGapPayloadLocked(slot uint64) ([]byte, bool) {
	e := r.globalLog[slot]
	if e == nil {
		return nil, false
	}
	if e.isBus {
		return marshalRequests(e.requests), true
	}
	return e.op, false
}

func (r *Replica) storeRecoveredLocked(slot, clientId, reqId uint64, payload []byte, isBus bool) bool {
	if isBus {
		reqs, err := unmarshalRequests(payload)
		if err != nil {
			Warning("[%s] cannot decode recovered bus for slot=%d: %v", r.self, slot, err)
			return false
		}
		return r.recordBusReceivedLocked(slot, clientId, reqId, reqs)
	}
	return r.recordReceivedLocked(slot, clientId, reqId, payload)
}

func (r *Replica) slotEntryLocked(slot uint64) *globalEntry {
	e := r.globalLog[slot]
	if e == nil {
		e = &globalEntry{state: slotEmpty}
		r.globalLog[slot] = e
	}
	return e
}

func (r *Replica) observeSlotLocked(slot uint64) {
	if !r.haveMax || slot > r.maxSlotSeen {
		r.maxSlotSeen = slot
		r.haveMax = true
	}
}

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

func (r *Replica) slotStateLocked(slot uint64) slotState {
	if e := r.globalLog[slot]; e != nil {
		return e.state
	}
	return slotEmpty
}

func (r *Replica) slotOpLocked(slot uint64) []byte {
	if e := r.globalLog[slot]; e != nil {
		return e.op
	}
	return nil
}

func (r *Replica) advanceNextExpectedLocked() {
	for {
		slot := r.nextExpected
		e := r.globalLog[slot]
		if e == nil || e.state == slotEmpty {
			return
		}
		r.appendBusToLogListLocked(slot)
		if r.busMode && r.durable != nil {
			r.durableRecordCursorLocked(slot, e)
		}
		r.nextExpected++
	}
}

func (r *Replica) durableRecordCursorLocked(slot uint64, e *globalEntry) {
	clientId, reqId := e.clientId, e.reqId
	if clientId == 0 {
		m := r.slotOwnerLocked(slot)
		clientId, reqId = m.clientId, m.reqId
	}
	r.durable.recordBus(slot, clientId, reqId, e.requests, e.state == slotNoOp)
}

func (r *Replica) appendBusToLogListLocked(slot uint64) {
	e := r.globalLog[slot]
	if e == nil || e.state == slotNoOp {
		return
	}
	for i := range e.requests {
		req := &e.requests[i]
		key := reqKey{req.ClientId, req.RequestId}
		li, ok := r.dedup[key]
		if !ok {
			li = uint64(len(r.logList))
			r.logList = append(r.logList, logEntry{req.ClientId, req.RequestId, req.Op})
			r.dedup[key] = li
		}
		r.enqueueReply(req.ClientId, req.RequestId, slot, li)
	}
}

func (r *Replica) enqueueReply(clientId, requestId, busSlot, logIndex uint64) {
	r.replyCh <- pendingReply{
		clientId:  clientId,
		requestId: requestId,
		busSlot:   busSlot,
		logIndex:  logIndex,
		viewId:    r.viewId,
	}
}

func (r *Replica) replyLoop() {
	for pr := range r.replyCh {
		r.cwMu.Lock()
		lw := r.clientWriters[pr.clientId]
		r.cwMu.Unlock()
		if lw == nil {
			continue
		}
		msg := &RequestReplyMessage{
			ClientId:   pr.clientId,
			RequestId:  pr.requestId,
			BusSlotNum: pr.busSlot,
			LogIndex:   pr.logIndex,
			ViewId:     pr.viewId,
			ReplicaIdx: uint32(r.idx),
		}
		if err := lw.sendMsg(MsgRequestReply, msg); err != nil {
			Warning("[%s] failed to send request reply to client %d: %v",
				r.self, pr.clientId, err)
		}
	}
}

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
			return
		}
		r.slotMeta[r.cursorSlot] = slotMetaEntry{clientId: bestCid, reqId: bestN, expectedNs: bestY}
		r.cursorNextN[bestCid] = bestN + 1
		r.cursorSlot++
	}
}

func (r *Replica) slotOwnerLocked(slot uint64) slotMetaEntry {
	r.genCursorUpToLocked(slot)
	return r.slotMeta[slot]
}

func (r *Replica) durableAppendLocked(slot uint64, op []byte, noop bool) {
	if r.durable == nil || r.busMode {
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
		Notice("[%s] 1s: received=%d dropped=%d delta_avg=%+dus delta_min=%+dus delta_max=%+dus gaps=%d recovered=%d noops=%d",
			r.self, recv, dropped, avg, min, max, gaps, recovered, noops)
	}
}

func (r *Replica) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		r.durable.flush()
	}
}

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

func (r *Replica) broadcastToPeers(code uint8, msg wireMsg) {
	for j := range r.config.Replicas {
		if j == r.idx {
			continue
		}
		r.sendToPeer(j, code, msg)
	}
}

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

func (r *Replica) ownerLog(slot uint64) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.globalLog[slot]; e != nil && e.clientId != 0 {
		return e.clientId, e.reqId
	}
	m := r.slotOwnerLocked(slot)
	return m.clientId, m.reqId
}

func (r *Replica) followerRecover(slot uint64, gs *gapState) {
	leader := r.config.LeaderIndex(r.viewId)
	r.sendToPeer(leader, MsgBusGapRequest,
		&BusGapRequest{Slot: slot, SenderIdx: uint32(r.idx)})

	select {
	case <-gs.doneCh:
		lat := (nowNs() - gs.start) / 1000
		r.mu.Lock()
		st := r.slotStateLocked(slot)
		r.finishGapLocked(gapKey{slot})
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
		r.finishGapLocked(gapKey{slot})
		r.mu.Unlock()
	}
}

func (r *Replica) leaderResolve(slot uint64, gs *gapState) {
	key := gapKey{slot}

	r.mu.Lock()
	if st := r.slotStateLocked(slot); st != slotEmpty {
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key)
		r.mu.Unlock()
		if st == slotReceived {
			r.answerAskers(slot, askers, op, isBus)
		}
		return
	}
	r.mu.Unlock()

	r.broadcastToPeers(MsgBusGapRequest,
		&BusGapRequest{Slot: slot, SenderIdx: uint32(r.idx)})
	var recovered []byte
	var recoveredBus bool
	timeout := time.After(gapProtocolTimeout)
probe:
	for {
		select {
		case reply := <-gs.probeReplies:
			if reply.Found {
				recovered = reply.Op
				recoveredBus = reply.Bus
				break probe
			}
		case <-timeout:
			break probe
		}
	}

	if recovered != nil {
		r.mu.Lock()
		m := r.slotOwnerLocked(slot)
		stored := r.storeRecoveredLocked(slot, m.clientId, m.reqId, recovered, recoveredBus)
		if stored {
			r.durableAppendLocked(slot, recovered, false)
			r.winRecovered++
		}
		r.advanceNextExpectedLocked()
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key)
		r.mu.Unlock()
		r.answerAskers(slot, askers, op, isBus)
		Notice("[%s] recovery_latency=%dus slot=%d client=%d req=%d",
			r.self, (nowNs()-gs.start)/1000, slot, m.clientId, m.reqId)
		return
	}

	r.mu.Lock()
	if r.slotStateLocked(slot) == slotReceived {
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key)
		r.mu.Unlock()
		r.answerAskers(slot, askers, op, isBus)
		return
	}
	if r.setNoOpLocked(slot) {
		r.durableAppendLocked(slot, nil, true)
		r.winNoops++
	}
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

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
	r.finishGapLocked(key)
	r.mu.Unlock()
	Notice("[%s] noop_latency=%dus slot=%d client=%d req=%d",
		r.self, (nowNs()-gs.start)/1000, slot, clientId, reqId)
}

func (r *Replica) answerAskers(slot uint64, askers []uint32, op []byte, isBus bool) {
	for _, idx := range askers {
		r.sendToPeer(int(idx), MsgBusGapReply,
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: true, Bus: isBus, Op: op})
	}
}

func (r *Replica) handleGapRequest(msg *BusGapRequest) {
	slot := msg.Slot
	r.mu.Lock()
	st := r.slotStateLocked(slot)
	op, isBus := r.slotGapPayloadLocked(slot)
	r.mu.Unlock()

	if !r.AmLeader() {
		reply := &BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: st == slotReceived}
		if reply.Found {
			reply.Op = op
			reply.Bus = isBus
		}
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply, reply)
		return
	}

	switch st {
	case slotReceived:
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapReply,
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), Found: true, Bus: isBus, Op: op})
	case slotNoOp:
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommit,
			&BusGapCommit{Slot: slot, SenderIdx: uint32(r.idx), ViewId: r.viewId})
	default:
		r.ensureLeaderResolve(slot, msg.SenderIdx)
	}
}

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
		if r.storeRecoveredLocked(msg.Slot, m.clientId, m.reqId, msg.Op, msg.Bus) {
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
