package paxosbus

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imdea-software/swiftpaxos/state"
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
	baseNs          int64
	intervalNs      int64
	maxSeqSeen      uint64
	resumeSeq       uint64
	arrivalOffsetNs int64
}

func (cl *clientLine) expectedNs(n uint64) int64 {
	return cl.baseNs + int64(n-1)*cl.intervalNs
}

func (cl *clientLine) arrivalNs(n uint64) int64 {
	t := cl.expectedNs(n)
	if cl.resumeSeq != 0 && n >= cl.resumeSeq {
		t += cl.arrivalOffsetNs
	}
	return t
}

type globalEntry struct {
	clientId uint64
	reqId    uint64
	state    slotState
	op       []byte
	requests []RequestMessage
	isBus    bool
	ownerSet bool // clientId/reqId hold this slot's real owner (client id 0 is valid)

	// [logIdxLo, logIdxHi) owns the dedup entries first assigned in this slot.
	logIdxLo      uint64
	logIdxHi      uint64
	prefixHash    uint64
	logIndexAfter uint64

	undo []writeUndo

	sizeBytes uint64 // this slot's share of residentBytes
}

type writeUndo struct {
	key     state.Key
	value   state.Value
	present bool
}

const requestOverheadBytes = 48

func busSizeBytes(reqs []RequestMessage) uint64 {
	n := uint64(len(reqs)) * requestOverheadBytes
	for i := range reqs {
		n += uint64(len(reqs[i].Op))
	}
	return n
}

type reqKey struct {
	clientId  uint64
	requestId uint64
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
	view uint64
	slot uint64
}

type gapState struct {
	view         uint64
	start        int64
	askers       map[uint32]struct{}
	probeReplies chan *BusGapReply
	commitAcks   chan uint32
	doneCh       chan struct{}
	abortCh      chan struct{}
	abortOnce    sync.Once
}

func newGapState(start int64, view uint64) *gapState {
	return &gapState{
		view:         view,
		start:        start,
		askers:       make(map[uint32]struct{}),
		probeReplies: make(chan *BusGapReply, 8),
		commitAcks:   make(chan uint32, 8),
		doneCh:       make(chan struct{}, 1),
		abortCh:      make(chan struct{}),
	}
}

func (gs *gapState) cancel() {
	gs.abortOnce.Do(func() { close(gs.abortCh) })
}

func (gs *gapState) cancelled() bool {
	select {
	case <-gs.abortCh:
		return true
	default:
		return false
	}
}

func (gs *gapState) snapshotAskers() []uint32 {
	out := make([]uint32, 0, len(gs.askers))
	for a := range gs.askers {
		out = append(out, a)
	}
	return out
}

const defaultGapDeltaMs = 5000

const gapRecoveryTimeout = 3 * time.Second

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

// ViewChange records buses but fences execution and client replies.
type replicaStatus uint8

const (
	statusNormal replicaStatus = iota
	statusViewChange
)

func (s replicaStatus) String() string {
	switch s {
	case statusViewChange:
		return "view-change"
	default:
		return "normal"
	}
}

// generation rejects results from an obsolete sync transfer.
type syncCatchup struct {
	generation uint64
	view       uint64
	leader     int
	target     uint64
	prepare    BusSyncPrepare
	active     bool
}

type Replica struct {
	config *Config
	idx    int
	viewId atomic.Uint64
	self   string

	mu              sync.Mutex
	clients         map[uint64]*clientLine
	clientControls  map[uint64]*clientControlState // guarded by r.mu
	status          replicaStatus
	lastNormalView  uint64
	recovery        *viewRecovery
	recoveryGen     uint64
	startViewSeen   uint64
	startViewView   uint64
	startViewSource uint64
	startViewUsed   []uint32

	globalLog    map[uint64]*globalEntry
	nextExpected uint64
	prunedBelow  uint64
	maxSlotSeen  uint64
	haveMax      bool

	// stableSlot is the committed, durable prefix and the rewind floor.
	stableSlot uint64
	haveStable bool

	// The rings retain prefix hashes and log lengths needed for rewind.
	prefixHash  uint64
	hashRing    []uint64
	logIdxRing  []uint64
	retainSlots uint64
	ringSize    uint64

	residentBytes uint64
	retainBytes   uint64

	nextLogIndex uint64
	dedup        map[reqKey]uint64
	State        *state.State

	pendingBuses []*BusMessage

	cwMu           sync.Mutex
	replySenders   map[uint64]*replySender
	pendingReplies []pendingReply // guarded by r.mu; drained by replyLoop off-lock so a slow client can't freeze r.mu
	replyWake      chan struct{}  // cap-1 signal that pendingReplies is non-empty

	cursorNextN map[uint64]uint64
	cursorSlot  uint64
	slotMeta    map[uint64]slotMetaEntry

	peerWriters []*lockedWriter
	peerRedial  []chan struct{} // cap-1 wake for dialPeer after a send failure
	leaderLost  bool            // peer connection to the leader broke; diagnostic only, never triggers suspicion

	gaps map[gapKey]*gapState

	syncInterval              time.Duration
	suspectTimeout            time.Duration
	viewChangeTimeout         time.Duration
	viewChangeFallbackTimeout time.Duration
	gapRetryTimeout           time.Duration
	lastHeartbeatNs           int64
	sync                      *syncRound
	syncCatchup               syncCatchup
	vc                        *vcState
	viewChangeWatchdog        *viewChangeWatchdog
	viewChangeWatchdogGen     uint64
	fetchQ                    chan fetchReq
	serveQ                    chan *BusGetState
	newStateCh                chan *BusNewState
	startViewQ                chan *BusStartView
	fetchSeq                  atomic.Uint64

	logDir     string
	durable    *durableLog // BusMessage Log: slot -> bus (replica.log)
	reqListLog *durableLog // Request Log List: log_index -> deduped request (requestlist.log)

	dropMode   dropMode
	dropEvery  uint64
	gapDeltaNs int64

	winRecv       uint64
	winDeltaSumUs int64
	winDeltaMinUs int64
	winDeltaMaxUs int64
	winGaps       uint64
	winRecovered  uint64
	winNoops      uint64
	winDropped    uint64
	winReplyMax   int
}

type RecoveryOptions struct {
	SyncIntervalMs              uint64
	SuspectTimeoutMs            uint64
	ViewChangeTimeoutMs         uint64
	ViewChangeFallbackTimeoutMs uint64
	GapRetryTimeoutMs           uint64
	RetainSlots                 uint64
	RetainBytes                 uint64
}

const (
	defaultSyncIntervalMs              = 500
	defaultSuspectTimeoutMs            = 2000
	defaultViewChangeTimeoutMs         = 15000
	defaultViewChangeFallbackTimeoutMs = 20000
	defaultGapRetryTimeoutMs           = 1500
	defaultRetainBytes                 = 256 << 20
)

func (o *RecoveryOptions) withDefaults() RecoveryOptions {
	out := *o
	if out.SyncIntervalMs == 0 {
		out.SyncIntervalMs = defaultSyncIntervalMs
	}
	if out.SuspectTimeoutMs == 0 {
		out.SuspectTimeoutMs = defaultSuspectTimeoutMs
	}
	if out.ViewChangeTimeoutMs == 0 {
		out.ViewChangeTimeoutMs = defaultViewChangeTimeoutMs
	}
	if out.ViewChangeFallbackTimeoutMs == 0 {
		out.ViewChangeFallbackTimeoutMs = defaultViewChangeFallbackTimeoutMs
	}
	if out.GapRetryTimeoutMs == 0 {
		out.GapRetryTimeoutMs = defaultGapRetryTimeoutMs
	}
	if out.RetainSlots == 0 {
		out.RetainSlots = defaultRetainSlots
	}
	if out.RetainBytes == 0 {
		out.RetainBytes = defaultRetainBytes
	}
	return out
}

func NewReplica(config *Config, idx int, label, logDir string, mode dropMode, every uint64,
	gapDeltaMs uint64, opts RecoveryOptions) *Replica {
	self := "Replica " + strconv.Itoa(idx)
	if label != "" {
		self += " " + label
	}
	if gapDeltaMs == 0 {
		gapDeltaMs = defaultGapDeltaMs
	}
	opts = opts.withDefaults()
	redial := make([]chan struct{}, config.N)
	for i := range redial {
		redial[i] = make(chan struct{}, 1)
	}
	r := &Replica{
		config:                    config,
		idx:                       idx,
		self:                      self,
		gapDeltaNs:                int64(gapDeltaMs) * 1e6,
		clients:                   make(map[uint64]*clientLine),
		globalLog:                 make(map[uint64]*globalEntry),
		dedup:                     make(map[reqKey]uint64),
		State:                     state.InitState(),
		replySenders:              make(map[uint64]*replySender),
		replyWake:                 make(chan struct{}, 1),
		cursorNextN:               make(map[uint64]uint64),
		slotMeta:                  make(map[uint64]slotMetaEntry),
		peerWriters:               make([]*lockedWriter, config.N),
		peerRedial:                redial,
		gaps:                      make(map[gapKey]*gapState),
		logDir:                    logDir,
		dropMode:                  mode,
		dropEvery:                 every,
		retainSlots:               opts.RetainSlots,
		retainBytes:               opts.RetainBytes,
		ringSize:                  opts.RetainSlots + 2,
		hashRing:                  make([]uint64, opts.RetainSlots+2),
		logIdxRing:                make([]uint64, opts.RetainSlots+2),
		prefixHash:                fnvOffset64,
		syncInterval:              time.Duration(opts.SyncIntervalMs) * time.Millisecond,
		suspectTimeout:            time.Duration(opts.SuspectTimeoutMs) * time.Millisecond,
		viewChangeTimeout:         time.Duration(opts.ViewChangeTimeoutMs) * time.Millisecond,
		viewChangeFallbackTimeout: time.Duration(opts.ViewChangeFallbackTimeoutMs) * time.Millisecond,
		gapRetryTimeout:           time.Duration(opts.GapRetryTimeoutMs) * time.Millisecond,
		fetchQ:                    make(chan fetchReq, 64),
		serveQ:                    make(chan *BusGetState, 64),
		newStateCh:                make(chan *BusNewState, 8),
		startViewQ:                make(chan *BusStartView, 8),
	}
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			Warning("[%s] cannot create durable log dir %s: %v", self, logDir, err)
			r.logDir = ""
		} else if busLog, err := openDurableLog(logDir, "replica.log"); err != nil {
			Warning("[%s] cannot open durable bus-message log in %s: %v", self, logDir, err)
			r.logDir = ""
		} else if reqLog, err := openDurableLog(logDir, "requestlist.log"); err != nil {
			Warning("[%s] cannot open durable request-list log in %s: %v", self, logDir, err)
			busLog.close()
			r.logDir = ""
		} else {
			r.durable = busLog
			r.reqListLog = reqLog
			Notice("[%s] durable logs in %s: replica.log (bus-message log) + requestlist.log (request log list)", self, logDir)
		}
	}
	leader := "no"
	if r.AmLeader() {
		leader = "yes"
	}
	Notice("[%s] started (view=0, f=%d, quorum=%d, leader=%s, gap_delta=%dms)",
		r.self, config.F, config.QuorumSize(), leader, r.gapDeltaNs/1e6)
	Notice("[%s] recovery: sync=%v suspect=%v view_change_timeout=%v view_change_fallback_timeout=%v gap_retry_timeout=%v retain_slots=%d",
		r.self, r.syncInterval, r.suspectTimeout, r.viewChangeTimeout,
		r.viewChangeFallbackTimeout, r.gapRetryTimeout, r.retainSlots)
	if r.dropMode != dropNone && r.dropEvery > 0 {
		Notice("[%s] artificial drop enabled: mode=%s every=%d (drop slot when reqId%%%d==0)",
			r.self, r.dropMode, r.dropEvery, r.dropEvery)
	}
	return r
}

func (r *Replica) view() uint64 { return r.viewId.Load() }

func (r *Replica) leaderIdx() int { return r.config.LeaderIndex(r.view()) }

func (r *Replica) AmLeader() bool {
	return r.leaderIdx() == r.idx
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func fnvMix(h, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= v & 0xff
		h *= fnvPrime64
		v >>= 8
	}
	return h
}

func foldSlot(h, slot uint64, st slotState, clientId, reqId uint64) uint64 {
	h = fnvMix(h, slot)
	h = fnvMix(h, uint64(st))
	h = fnvMix(h, clientId)
	return fnvMix(h, reqId)
}

func (r *Replica) foldExecutedLocked(slot uint64, e *globalEntry) {
	clientId, reqId := e.clientId, e.reqId
	if !e.ownerSet {
		m := r.slotOwnerLocked(slot)
		clientId, reqId = m.clientId, m.reqId
	}
	r.prefixHash = foldSlot(r.prefixHash, slot, e.state, clientId, reqId)
	e.prefixHash, e.logIndexAfter = r.prefixHash, r.nextLogIndex
	i := slot % r.ringSize
	r.hashRing[i] = r.prefixHash
	r.logIdxRing[i] = r.nextLogIndex
}

func (r *Replica) ringValidLocked(slot uint64) bool {
	if slot >= r.nextExpected {
		return false
	}
	return slot+r.ringSize >= r.nextExpected
}

func (r *Replica) prefixHashAtLocked(slot uint64) (uint64, bool) {
	if !r.ringValidLocked(slot) {
		if e := r.globalLog[slot]; slot >= r.prunedBelow && slot < r.nextExpected && e != nil {
			return e.prefixHash, true
		}
		return 0, false
	}
	return r.hashRing[slot%r.ringSize], true
}

func (r *Replica) logIndexAfterLocked(slot uint64) (uint64, bool) {
	if !r.ringValidLocked(slot) {
		if e := r.globalLog[slot]; slot >= r.prunedBelow && slot < r.nextExpected && e != nil {
			return e.logIndexAfter, true
		}
		return 0, false
	}
	return r.logIdxRing[slot%r.ringSize], true
}

func (r *Replica) prefixStateAtLocked(slot uint64) (hash, logIdx uint64, ok bool) {
	if slot == 0 {
		return fnvOffset64, 0, true
	}
	h, ok1 := r.prefixHashAtLocked(slot - 1)
	li, ok2 := r.logIndexAfterLocked(slot - 1)
	return h, li, ok1 && ok2
}

func (r *Replica) followerRankLocked() int {
	leader := r.leaderIdx()
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
	r.mu.Lock()
	r.lastHeartbeatNs = nowNs()
	r.mu.Unlock()
	go r.statsLoop()
	go r.connectPeers()
	go r.gapDetectLoop()
	go r.replyLoop()
	go r.syncLoop()
	go r.suspicionLoop()
	// State transfer runs off connection readers and r.mu.
	go r.stateServeLoop()
	go r.stateFetchLoop()
	go r.viewInstallLoop()
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
	lw := &lockedWriter{w: bufio.NewWriter(conn), conn: conn}

	peerIdx := -1
	defer func() {
		if peerIdx >= 0 {
			r.peerConnLost(peerIdx)
		}
	}()

	var (
		syncMsg BusSyncMessage
		busMsg  BusMessage
	)

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Notice("[%s] client connection %s closed: %v",
				r.self, conn.RemoteAddr(), err)
			return
		}
		switch msgType {
		case MsgClientStatusQuery, MsgClientResume:
			var m clientControlMessage
			if err := m.Unmarshal(reader); err != nil {
				return
			}
			r.handleClientControl(msgType, &m, lw)

		case MsgBusSync:
			if err := syncMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync message: %v", r.self, err)
				return
			}
			r.handleSync(&syncMsg)

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
			peerIdx = int(m.SenderIdx)
			r.handleGapRequest(&m)

		case MsgBusGapReply:
			var m BusGapReply
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap reply: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleGapReply(&m)

		case MsgBusGapCommit:
			var m BusGapCommit
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap commit: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleGapCommit(&m)

		case MsgBusGapCommitReply:
			var m BusGapCommitReply
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad gap commit reply: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleGapCommitReply(&m)

		case MsgBusSyncPrepare:
			var m BusSyncPrepare
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync prepare: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleSyncPrepare(&m)

		case MsgBusSyncReply:
			var m BusSyncReply
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync reply: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleSyncReply(&m)

		case MsgBusSyncCommit:
			var m BusSyncCommit
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync commit: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleSyncCommit(&m)

		case MsgBusViewChangeRequest:
			var m BusViewChangeRequest
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad view change request: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleViewChangeRequest(&m)

		case MsgBusViewChange:
			var m BusViewChange
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad view change: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleViewChange(&m)

		case MsgBusStartView:
			var m BusStartView
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad start view: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleStartView(&m)

		case MsgBusStateQuery:
			var m BusStateQuery
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad state query: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleStateQuery(&m)

		case MsgBusGetState:
			var m BusGetState
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad get state: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleGetState(&m)

		case MsgBusNewState:
			var m BusNewState
			if err := m.Unmarshal(reader); err != nil {
				Warning("[%s] bad new state: %v", r.self, err)
				return
			}
			peerIdx = int(m.SenderIdx)
			r.handleNewState(&m)

		default:
			Warning("[%s] unknown message type %d", r.self, msgType)
			return
		}
	}
}

func (r *Replica) handleSync(msg *BusSyncMessage) {
	r.mu.Lock()
	r.clients[msg.ClientId] = &clientLine{
		baseNs:     int64(msg.FirstMsgNs),
		intervalNs: int64(msg.IntervalMs) * 1e6,
	}
	r.resetCursorLocked()
	r.mu.Unlock()
	Notice("[%s] sync from client %d: first msg expected at %dns (in %dms), interval=%dms",
		r.self, msg.ClientId, msg.FirstMsgNs, (int64(msg.FirstMsgNs)-wallNs())/1e6, msg.IntervalMs)
}

func (r *Replica) resetCursorLocked() {
	r.cursorSlot = 0
	r.cursorNextN = make(map[uint64]uint64)
	r.slotMeta = make(map[uint64]slotMetaEntry)
}

type admission uint8

const (
	admitted      admission = iota
	admitUnsynced           // sender never announced an arrival line
	admitDropped            // swallowed by the fault injector
)

func (r *Replica) admitLocked(clientId, seq uint64, actualNs int64) admission {
	line, ok := r.clients[clientId]
	if !ok {
		return admitUnsynced
	}
	if seq > line.maxSeqSeen {
		line.maxSeqSeen = seq
	}
	if r.shouldDropLocked(seq) {
		r.winDropped++
		return admitDropped
	}
	r.observeArrivalLocked(line, seq, actualNs)
	return admitted
}

func (r *Replica) observeArrivalLocked(line *clientLine, seq uint64, actualNs int64) {
	deltaUs := (actualNs - line.arrivalNs(seq)) / 1000
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
}

func (r *Replica) handleBus(msg *BusMessage, lw *lockedWriter) {
	actualNs := wallNs()
	r.bindReplySender(msg.ClientId, lw)

	r.mu.Lock()
	switch r.admitLocked(msg.ClientId, msg.BusSeqNum, actualNs) {
	case admitUnsynced:
		r.mu.Unlock()
		Warning("[%s] bus from unsynced client %d, ignoring", r.self, msg.ClientId)
		return
	case admitDropped:
		r.mu.Unlock()
		return
	}
	if len(r.gaps) > 0 {
		r.applyDuringRecoveryLocked(msg)
	} else {
		slot := computeGlobalSlot(r.clients, msg.ClientId, msg.BusSeqNum)
		r.recordBusReceivedLocked(slot, msg.ClientId, msg.BusSeqNum, msg.Requests)
		r.advanceNextExpectedLocked()
	}
	r.mu.Unlock()
}

func (r *Replica) bindReplySender(clientId uint64, lw *lockedWriter) {
	r.cwMu.Lock()
	defer r.cwMu.Unlock()
	if rs := r.replySenders[clientId]; rs != nil {
		rs.setWriter(lw)
		return
	}
	r.replySenders[clientId] = newReplySender(r, lw)
}

func (r *Replica) applyDuringRecoveryLocked(msg *BusMessage) {
	slot := computeGlobalSlot(r.clients, msg.ClientId, msg.BusSeqNum)
	stored := r.recordBusReceivedLocked(slot, msg.ClientId, msg.BusSeqNum, msg.Requests)
	r.advanceNextExpectedLocked()
	key := gapKey{view: r.view(), slot: slot}
	if gs := r.gaps[key]; stored && gs != nil {
		select {
		case gs.doneCh <- struct{}{}:
		default:
		}
	}
}

func (r *Replica) gapActiveLocked(key gapKey, gs *gapState) bool {
	return gs != nil && !gs.cancelled() && gs.view == key.view &&
		r.status == statusNormal && r.view() == key.view && r.gaps[key] == gs
}

func (r *Replica) leaderGapActiveLocked(key gapKey, gs *gapState) bool {
	return r.gapActiveLocked(key, gs) && r.config.LeaderIndex(key.view) == r.idx
}

// Only the current gap state may be removed by its waiter.
func (r *Replica) finishGapLocked(key gapKey, gs *gapState) {
	if r.gaps[key] != gs {
		return
	}
	gs.cancel()
	delete(r.gaps, key)
	if len(r.gaps) == 0 {
		r.drainPendingBusesLocked()
	}
}

func (r *Replica) cancelGapsLocked() {
	for _, gs := range r.gaps {
		gs.cancel()
	}
	r.gaps = make(map[gapKey]*gapState)
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
	e := r.claimSlotLocked(slot)
	if e == nil {
		return false
	}
	sz := busSizeBytes(reqs)
	*e = globalEntry{
		clientId:  clientId,
		reqId:     busSeq,
		state:     slotReceived,
		requests:  reqs,
		isBus:     true,
		ownerSet:  true,
		sizeBytes: sz,
	}
	r.residentBytes += sz
	return true
}

func (r *Replica) slotGapPayloadLocked(slot uint64) (payload []byte, isBus bool) {
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
	if !isBus {
		return r.recordReceivedLocked(slot, clientId, reqId, payload)
	}
	reqs, err := unmarshalRequests(payload)
	if err != nil {
		Warning("[%s] cannot decode recovered bus for slot=%d: %v", r.self, slot, err)
		return false
	}
	return r.recordBusReceivedLocked(slot, clientId, reqId, reqs)
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

func (r *Replica) claimSlotLocked(slot uint64) *globalEntry {
	r.observeSlotLocked(slot)
	e := r.slotEntryLocked(slot)
	if e.state != slotEmpty {
		return nil
	}
	return e
}

func (r *Replica) recordReceivedLocked(slot, clientId, reqId uint64, op []byte) bool {
	e := r.claimSlotLocked(slot)
	if e == nil {
		return false
	}
	sz := uint64(len(op)) + requestOverheadBytes
	*e = globalEntry{
		clientId:  clientId,
		reqId:     reqId,
		state:     slotReceived,
		op:        op,
		ownerSet:  true,
		sizeBytes: sz,
	}
	r.residentBytes += sz
	return true
}

// An agreed no-op may replace a bus only before that slot executes.
func (r *Replica) setNoOpLocked(slot uint64) bool {
	if r.executedLocked(slot) {
		Warning("[%s] refusing no-op at slot=%d below executed frontier %d",
			r.self, slot, r.nextExpected)
		return false
	}
	r.observeSlotLocked(slot)
	e := r.slotEntryLocked(slot)
	if e.state == slotNoOp {
		return false
	}
	if !e.ownerSet {
		m := r.slotOwnerLocked(slot)
		e.clientId, e.reqId = m.clientId, m.reqId
		e.ownerSet = true
	}
	e.state = slotNoOp
	e.op = nil
	e.requests = nil
	e.isBus = false
	r.releaseEntryBytesLocked(e)
	return true
}

func (r *Replica) releaseEntryBytesLocked(e *globalEntry) {
	if e.sizeBytes > r.residentBytes {
		r.residentBytes = 0
	} else {
		r.residentBytes -= e.sizeBytes
	}
	e.sizeBytes = 0
}

func (r *Replica) slotStateLocked(slot uint64) slotState {
	if e := r.globalLog[slot]; e != nil {
		return e.state
	}
	return slotEmpty
}

func (r *Replica) executedLocked(slot uint64) bool {
	return slot < r.nextExpected
}

func (r *Replica) slotOpLocked(slot uint64) []byte {
	if e := r.globalLog[slot]; e != nil {
		return e.op
	}
	return nil
}

func (r *Replica) advanceNextExpectedLocked() {
	if r.status != statusNormal {
		r.pruneCommittedLocked()
		return
	}
	for r.advanceNextExpectedOneLocked() {
	}
	r.pruneCommittedLocked()
}

// View installation may advance the cursor only through its canonical range.
func (r *Replica) advanceNextExpectedThroughLocked(maxSlot uint64) {
	for r.nextExpected <= maxSlot && r.advanceNextExpectedOneLocked() {
	}
	r.pruneCommittedLocked()
}

func (r *Replica) advanceNextExpectedOneLocked() bool {
	slot := r.nextExpected
	e := r.globalLog[slot]
	if e == nil || e.state == slotEmpty {
		return false
	}
	logIdxs := r.appendBusToLogListLocked(slot)
	if r.durable != nil {
		r.durableRecordCursorLocked(slot, e, logIdxs)
	}
	r.nextExpected++
	r.foldExecutedLocked(slot, e)
	return true
}

const defaultRetainSlots = 1 << 14

func (r *Replica) pruneCommittedLocked() {
	limit := r.nextExpected
	if r.haveStable && r.stableSlot < limit {
		limit = r.stableSlot
	}

	var target uint64
	if r.nextExpected > r.retainSlots {
		target = r.nextExpected - r.retainSlots
	}
	if target > limit {
		target = limit
	}
	for s := r.prunedBelow; s < target; s++ {
		r.pruneSlotLocked(s)
	}
	if target > r.prunedBelow {
		r.prunedBelow = target
	}

	for r.retainBytes > 0 && r.residentBytes > r.retainBytes && r.prunedBelow < limit {
		r.pruneSlotLocked(r.prunedBelow)
		r.prunedBelow++
	}
}

func (r *Replica) pruneSlotLocked(slot uint64) {
	r.pruneDedupForSlotLocked(slot)
	if e := r.globalLog[slot]; e != nil {
		r.releaseEntryBytesLocked(e)
	}
	delete(r.globalLog, slot)
	delete(r.slotMeta, slot)
}

// Release dedup entries only from the slot where each request was first assigned.
func (r *Replica) pruneDedupForSlotLocked(slot uint64) {
	e := r.globalLog[slot]
	if e == nil || len(e.requests) == 0 {
		return
	}
	for i := range e.requests {
		req := &e.requests[i]
		key := reqKey{req.ClientId, req.RequestId}
		if li, seen := r.dedup[key]; seen && li >= e.logIdxLo && li < e.logIdxHi {
			delete(r.dedup, key)
		}
	}
}

func (r *Replica) durableRecordCursorLocked(slot uint64, e *globalEntry, logIdxs []uint64) {
	clientId, reqId := e.clientId, e.reqId
	if !e.ownerSet {
		m := r.slotOwnerLocked(slot)
		clientId, reqId = m.clientId, m.reqId
	}
	r.durable.recordBus(slot, clientId, reqId, logIdxs, e.state == slotNoOp)
}

func (r *Replica) executeLocked(req *RequestMessage, logIndex uint64) {
	cmd := state.Command{Op: state.PUT, K: state.Key(req.ClientId), V: state.Value(req.Op)}
	cmd.Execute(r.State)
}

func (r *Replica) rememberWriteLocked(e *globalEntry, key state.Key) {
	for _, u := range e.undo {
		if u.key == key {
			return
		}
	}
	value, present := r.State.Store.Get(key)
	u := writeUndo{key: key, present: present}
	if present {
		u.value = value.(state.Value)
	}
	e.undo = append(e.undo, u)
	size := uint64(40 + len(u.value))
	e.sizeBytes += size
	r.residentBytes += size
}

func (r *Replica) undoWritesLocked(e *globalEntry) {
	for _, u := range e.undo {
		if u.present {
			r.State.Store.Put(u.key, u.value)
		} else {
			r.State.Store.Remove(u.key)
		}
		size := uint64(40 + len(u.value))
		e.sizeBytes -= size
		r.residentBytes -= size
	}
	e.undo = nil
}

func (r *Replica) appendBusToLogListLocked(slot uint64) []uint64 {
	e := r.globalLog[slot]
	if e == nil || e.state == slotNoOp {
		return nil
	}
	logIdxs := make([]uint64, 0, len(e.requests))
	e.logIdxLo = r.nextLogIndex
	for i := range e.requests {
		req := &e.requests[i]
		key := reqKey{req.ClientId, req.RequestId}
		li := r.nextLogIndex
		r.nextLogIndex++
		if r.reqListLog != nil {
			r.reqListLog.recordReq(li, req.ClientId, req.RequestId, req.Op)
		}
		execIdx, seen := r.dedup[key]
		if !seen {
			r.dedup[key] = li
			execIdx = li
			r.rememberWriteLocked(e, state.Key(req.ClientId))
			r.executeLocked(req, li)
		}
		logIdxs = append(logIdxs, li)
		r.enqueueReply(req.ClientId, req.RequestId, slot, execIdx)
	}
	e.logIdxHi = r.nextLogIndex
	return logIdxs
}

func (r *Replica) enqueueReply(clientId, requestId, busSlot, logIndex uint64) {
	r.pendingReplies = append(r.pendingReplies, pendingReply{
		clientId:  clientId,
		requestId: requestId,
		busSlot:   busSlot,
		logIndex:  logIndex,
		viewId:    r.view(),
	})
	if n := len(r.pendingReplies); n > r.winReplyMax {
		r.winReplyMax = n
	}
	select {
	case r.replyWake <- struct{}{}:
	default:
	}
}

func (r *Replica) replyLoop() {
	for range r.replyWake {
		for {
			r.mu.Lock()
			batch := r.pendingReplies
			r.pendingReplies = nil
			r.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			for i := range batch {
				pr := &batch[i]
				r.cwMu.Lock()
				rs := r.replySenders[pr.clientId]
				r.cwMu.Unlock()
				if rs == nil {
					continue
				}
				rs.enqueue(*pr)
			}
		}
	}
}

type replySender struct {
	r *Replica

	mu        sync.Mutex
	lw        *lockedWriter
	buf       []pendingReply
	errLogged bool
	wake      chan struct{} // cap-1 signal that buf is non-empty
}

func newReplySender(r *Replica, lw *lockedWriter) *replySender {
	rs := &replySender{r: r, lw: lw, wake: make(chan struct{}, 1)}
	go rs.loop()
	return rs
}

func (rs *replySender) setWriter(lw *lockedWriter) {
	rs.mu.Lock()
	if rs.lw != lw {
		rs.lw = lw
		rs.errLogged = false
	}
	rs.mu.Unlock()
}

func (rs *replySender) enqueue(pr pendingReply) {
	rs.mu.Lock()
	rs.buf = append(rs.buf, pr)
	rs.mu.Unlock()
	select {
	case rs.wake <- struct{}{}:
	default:
	}
}

func (rs *replySender) loop() {
	for range rs.wake {
		for {
			rs.mu.Lock()
			batch := rs.buf
			rs.buf = nil
			lw := rs.lw
			rs.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			lw.mu.Lock()
			for i := range batch {
				pr := &batch[i]
				msg := RequestReplyMessage{
					ClientId:   pr.clientId,
					RequestId:  pr.requestId,
					BusSlotNum: pr.busSlot,
					LogIndex:   pr.logIndex,
					ViewId:     pr.viewId,
					ReplicaIdx: uint32(rs.r.idx),
				}
				lw.w.WriteByte(MsgRequestReply)
				msg.Marshal(lw.w)
			}
			err := lw.w.Flush()
			lw.mu.Unlock()
			if err != nil {
				rs.mu.Lock()
				logIt := !rs.errLogged
				rs.errLogged = true
				rs.mu.Unlock()
				if logIt {
					Warning("[%s] failed to send request replies to client %d: %v",
						rs.r.self, batch[0].clientId, err)
				}
			}
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

func (r *Replica) statsLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		r.mu.Lock()
		recv := r.winRecv
		sum, min, max := r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs
		gaps, recovered, noops := r.winGaps, r.winRecovered, r.winNoops
		dropped := r.winDropped
		replyMax := r.winReplyMax
		r.winRecv, r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs = 0, 0, 0, 0
		r.winGaps, r.winRecovered, r.winNoops = 0, 0, 0
		r.winDropped = 0
		r.winReplyMax = 0
		status, stable, haveStable := r.status, r.stableSlot, r.haveStable
		executed, resident, dedupSize := r.nextExpected, len(r.globalLog), len(r.dedup)
		residentMB := r.residentBytes >> 20
		entryLogLen, prunedBelow := r.nextLogIndex, r.prunedBelow
		busLogHigh := uint64(0)
		if r.haveMax {
			busLogHigh = r.maxSlotSeen + 1
		}
		r.mu.Unlock()
		durMax := 0
		if r.durable != nil {
			if m := r.durable.backlogMax(); m > durMax {
				durMax = m
			}
		}
		if r.reqListLog != nil {
			if m := r.reqListLog.backlogMax(); m > durMax {
				durMax = m
			}
		}
		Notice("[%s] 1s: bus_log_len=%d bus_log_high=%d entry_log_len=%d pruned_below=%d",
			r.self, executed, busLogHigh, entryLogLen, prunedBelow)
		if recv == 0 && gaps == 0 && recovered == 0 && noops == 0 && dropped == 0 {
			continue
		}
		var avg int64
		if recv > 0 {
			avg = sum / int64(recv)
		}
		stableStr := "none"
		if haveStable {
			stableStr = strconv.FormatUint(stable, 10)
		}
		Notice("[%s] 1s: received=%d dropped=%d delta_avg=%+dus delta_min=%+dus delta_max=%+dus gaps=%d recovered=%d noops=%d reply_backlog_max=%d durable_backlog_max=%d",
			r.self, recv, dropped, avg, min, max, gaps, recovered, noops, replyMax, durMax)
		Notice("[%s] 1s: view=%d status=%s executed=%d stable=%s resident_slots=%d resident_mb=%d dedup=%d",
			r.self, r.view(), status, executed, stableStr, resident, residentMB, dedupSize)
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
		if err != nil {
			Warning("[%s] cannot connect to peer replica %d (%s): %v, retrying",
				r.self, j, addr, err)
			time.Sleep(time.Second)
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		lw := &lockedWriter{w: bufio.NewWriter(conn), conn: conn}
		r.mu.Lock()
		r.peerWriters[j] = lw
		r.mu.Unlock()
		Notice("[%s] connected to peer replica %d (%s)", r.self, j, addr)

		<-r.peerRedial[j]
		conn.Close()
		Notice("[%s] peer replica %d connection retired, redialing", r.self, j)
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *Replica) sendToPeer(j int, code uint8, msg wireMsg) {
	if j < 0 || j >= len(r.peerWriters) {
		return
	}
	r.mu.Lock()
	lw := r.peerWriters[j]
	r.mu.Unlock()
	if lw == nil {
		return // dialPeer is already retrying; logging here would just spam
	}
	if err := lw.sendMsg(code, msg); err != nil {
		Warning("[%s] send to peer %d failed: %v", r.self, j, err)
		r.retirePeer(j, lw)
	}
}

func (r *Replica) validReplicaIndex(idx uint32) bool {
	return uint64(idx) < uint64(r.config.N) && int(idx) < len(r.peerWriters)
}

func (r *Replica) peerConnected(j int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peerWriters[j] != nil
}

func (r *Replica) retirePeer(j int, lw *lockedWriter) {
	r.mu.Lock()
	cleared := false
	if lw == nil || r.peerWriters[j] == lw {
		cleared = r.peerWriters[j] != nil
		r.peerWriters[j] = nil
	}
	// A broken leader socket is diagnostic; only missed heartbeats trigger suspicion.
	if j != r.idx && j == r.leaderIdx() {
		r.leaderLost = true
	}
	r.mu.Unlock()
	if cleared {
		select {
		case r.peerRedial[j] <- struct{}{}:
		default:
		}
	}
}

func (r *Replica) peerConnLost(j int) {
	if j < 0 || j >= r.config.N || j == r.idx {
		return
	}
	Notice("[%s] lost inbound connection from peer replica %d", r.self, j)
	r.retirePeer(j, nil)
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
		key             gapKey
		gs              *gapState
		clientId, reqId uint64
	}
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		wallNow := wallNs()
		monoNow := nowNs()
		var spawn []spawnInfo
		r.mu.Lock()
		if r.status == statusNormal && r.haveMax && len(r.clients) > 0 {
			view := r.view()
			r.genCursorUpToLocked(r.maxSlotSeen)
			for slot := r.nextExpected; slot <= r.maxSlotSeen; slot++ {
				if e := r.globalLog[slot]; e != nil && e.state != slotEmpty {
					continue
				}
				meta, ok := r.slotMeta[slot]
				if !ok {
					continue
				}
				if !r.clientGapDueLocked(meta, wallNow) {
					continue
				}
				key := gapKey{view: view, slot: slot}
				if _, busy := r.gaps[key]; busy {
					continue
				}
				gs := newGapState(monoNow, view)
				r.gaps[key] = gs
				r.winGaps++
				spawn = append(spawn, spawnInfo{key: key, gs: gs, clientId: meta.clientId, reqId: meta.reqId})
			}
		}
		r.mu.Unlock()
		for _, s := range spawn {
			Notice("[%s] GAP detected slot=%d client=%d req=%d", r.self, s.key.slot, s.clientId, s.reqId)
			go r.handleGap(s.key, s.gs)
		}
	}
}

func (r *Replica) handleGap(key gapKey, gs *gapState) {
	r.mu.Lock()
	active := r.gapActiveLocked(key, gs)
	leader := r.config.LeaderIndex(key.view)
	r.mu.Unlock()
	if !active {
		return
	}
	if leader == r.idx {
		r.leaderResolve(key, gs)
	} else {
		r.followerRecover(key, gs)
	}
}

func (r *Replica) ownerLog(slot uint64) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.globalLog[slot]; e != nil && e.ownerSet {
		return e.clientId, e.reqId
	}
	m := r.slotOwnerLocked(slot)
	return m.clientId, m.reqId
}

func (r *Replica) followerRecover(key gapKey, gs *gapState) {
	leader := r.config.LeaderIndex(key.view)
	r.sendToPeer(leader, MsgBusGapRequest,
		&BusGapRequest{Slot: key.slot, SenderIdx: uint32(r.idx), ViewId: key.view})

	timer := time.NewTimer(gapRecoveryTimeout)
	defer timer.Stop()
	select {
	case <-gs.doneCh:
		lat := (nowNs() - gs.start) / 1000
		r.mu.Lock()
		if !r.gapActiveLocked(key, gs) {
			r.mu.Unlock()
			return
		}
		st := r.slotStateLocked(key.slot)
		r.finishGapLocked(key, gs)
		r.mu.Unlock()
		clientId, reqId := r.ownerLog(key.slot)
		switch st {
		case slotReceived:
			Notice("[%s] recovery_latency=%dus slot=%d client=%d req=%d", r.self, lat, key.slot, clientId, reqId)
		case slotNoOp:
			Notice("[%s] noop_latency=%dus slot=%d client=%d req=%d", r.self, lat, key.slot, clientId, reqId)
		}
	case <-timer.C:
		Warning("[%s] gap recovery timed out slot=%d", r.self, key.slot)
		r.mu.Lock()
		r.finishGapLocked(key, gs)
		r.mu.Unlock()
	case <-gs.abortCh:
		return
	}
}

func (r *Replica) leaderResolve(key gapKey, gs *gapState) {
	slot := key.slot
	r.mu.Lock()
	if !r.leaderGapActiveLocked(key, gs) {
		r.mu.Unlock()
		return
	}
	if st := r.slotStateLocked(slot); st != slotEmpty {
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key, gs)
		r.mu.Unlock()
		if st == slotReceived {
			r.answerAskers(key.view, slot, askers, op, isBus)
		}
		return
	}
	r.mu.Unlock()

	r.broadcastToPeers(MsgBusGapRequest,
		&BusGapRequest{Slot: slot, SenderIdx: uint32(r.idx), ViewId: key.view})
	var recovered []byte
	var recoveredBus bool
	recoveredFound := false
	notFound := make(map[uint32]struct{}, r.config.N-1)
	timer := time.NewTimer(gapRecoveryTimeout)
probe:
	for {
		select {
		case reply := <-gs.probeReplies:
			if reply.Found {
				recovered = reply.Op
				recoveredBus = reply.Bus
				recoveredFound = true
				break probe
			}
			if r.validReplicaIndex(reply.SenderIdx) && int(reply.SenderIdx) != r.idx {
				notFound[reply.SenderIdx] = struct{}{}
				if len(notFound) == r.config.N-1 {
					break probe
				}
			}
		case <-timer.C:
			break probe
		case <-gs.abortCh:
			timer.Stop()
			return
		}
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	if recoveredFound {
		r.mu.Lock()
		if !r.leaderGapActiveLocked(key, gs) {
			r.mu.Unlock()
			return
		}
		m := r.slotOwnerLocked(slot)
		stored := r.storeRecoveredLocked(slot, m.clientId, m.reqId, recovered, recoveredBus)
		if stored {
			r.winRecovered++
		}
		r.advanceNextExpectedLocked()
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key, gs)
		r.mu.Unlock()
		r.answerAskers(key.view, slot, askers, op, isBus)
		Notice("[%s] recovery_latency=%dus slot=%d client=%d req=%d",
			r.self, (nowNs()-gs.start)/1000, slot, m.clientId, m.reqId)
		return
	}

	r.mu.Lock()
	if !r.leaderGapActiveLocked(key, gs) {
		r.mu.Unlock()
		return
	}
	if r.slotStateLocked(slot) == slotReceived {
		op, isBus := r.slotGapPayloadLocked(slot)
		askers := gs.snapshotAskers()
		r.finishGapLocked(key, gs)
		r.mu.Unlock()
		r.answerAskers(key.view, slot, askers, op, isBus)
		return
	}
	if r.setNoOpLocked(slot) {
		r.winNoops++
	}
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

	acked := r.collectNoOpQuorum(key, gs)

	r.mu.Lock()
	r.finishGapLocked(key, gs)
	r.mu.Unlock()
	if !acked {
		return
	}
	clientId, reqId := r.ownerLog(slot)
	Notice("[%s] noop_latency=%dus slot=%d client=%d req=%d",
		r.self, (nowNs()-gs.start)/1000, slot, clientId, reqId)
}

func (r *Replica) collectNoOpQuorum(key gapKey, gs *gapState) bool {
	r.mu.Lock()
	active := r.leaderGapActiveLocked(key, gs)
	r.mu.Unlock()
	if !active {
		return false
	}
	commit := &BusGapCommit{Slot: key.slot, SenderIdx: uint32(r.idx), ViewId: key.view}
	r.broadcastToPeers(MsgBusGapCommit, commit)

	acked := make(map[uint32]struct{}, r.config.N)
	timer := time.NewTimer(r.gapRetryTimeout)
	defer timer.Stop()
	for round := 1; ; {
		if len(acked)+1 >= r.config.QuorumSize() {
			r.mu.Lock()
			active := r.leaderGapActiveLocked(key, gs)
			r.mu.Unlock()
			return active
		}
		select {
		case idx := <-gs.commitAcks:
			if r.validReplicaIndex(idx) && int(idx) != r.idx {
				acked[idx] = struct{}{}
			}
		case <-timer.C:
			r.mu.Lock()
			active := r.leaderGapActiveLocked(key, gs)
			r.mu.Unlock()
			if !active {
				Warning("[%s] abandoning noop commit slot=%d after %d rounds: no longer leading a normal view",
					r.self, key.slot, round)
				return false
			}
			round++
			Warning("[%s] noop commit round %d for slot=%d: %d/%d acks, retrying",
				r.self, round, key.slot, len(acked)+1, r.config.QuorumSize())
			r.broadcastToPeers(MsgBusGapCommit, commit)
			timer.Reset(r.gapRetryTimeout)
		case <-gs.abortCh:
			return false
		}
	}
}

func (r *Replica) answerAskers(view, slot uint64, askers []uint32, op []byte, isBus bool) {
	for _, idx := range askers {
		r.sendToPeer(int(idx), MsgBusGapReply,
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), ViewId: view, Found: true, Bus: isBus, Op: op})
	}
}

func (r *Replica) handleGapRequest(msg *BusGapRequest) {
	if !r.validReplicaIndex(msg.SenderIdx) {
		return
	}
	slot := msg.Slot
	r.mu.Lock()
	if r.status != statusNormal || msg.ViewId != r.view() {
		r.mu.Unlock()
		return
	}
	leader := r.config.LeaderIndex(msg.ViewId)
	amLeader := leader == r.idx
	if !amLeader && int(msg.SenderIdx) != leader {
		r.mu.Unlock()
		return
	}
	st := r.slotStateLocked(slot)
	op, isBus := r.slotGapPayloadLocked(slot)
	reclaimed := st == slotEmpty && r.executedLocked(slot)
	r.mu.Unlock()

	if !amLeader {
		reply := &BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), ViewId: msg.ViewId, Found: st == slotReceived}
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
			&BusGapReply{Slot: slot, SenderIdx: uint32(r.idx), ViewId: msg.ViewId, Found: true, Bus: isBus, Op: op})
	case slotNoOp:
		r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommit,
			&BusGapCommit{Slot: slot, SenderIdx: uint32(r.idx), ViewId: msg.ViewId})
	default:
		if reclaimed {
			return
		}
		r.ensureLeaderResolve(msg.ViewId, slot, msg.SenderIdx)
	}
}

func (r *Replica) ensureLeaderResolve(view, slot uint64, asker uint32) {
	key := gapKey{view: view, slot: slot}
	r.mu.Lock()
	if r.status != statusNormal || r.view() != view || r.config.LeaderIndex(view) != r.idx {
		r.mu.Unlock()
		return
	}
	gs := r.gaps[key]
	spawn := false
	if gs == nil {
		gs = newGapState(nowNs(), view)
		r.gaps[key] = gs
		r.winGaps++
		spawn = true
	}
	gs.askers[asker] = struct{}{}
	m := r.slotOwnerLocked(slot)
	r.mu.Unlock()
	if spawn {
		Notice("[%s] GAP detected slot=%d client=%d req=%d (peer request)", r.self, slot, m.clientId, m.reqId)
		go r.handleGap(key, gs)
	}
}

func (r *Replica) handleGapReply(msg *BusGapReply) {
	if !r.validReplicaIndex(msg.SenderIdx) {
		return
	}
	key := gapKey{view: msg.ViewId, slot: msg.Slot}
	r.mu.Lock()
	gs := r.gaps[key]
	if !r.gapActiveLocked(key, gs) {
		r.mu.Unlock()
		return
	}
	leader := r.config.LeaderIndex(msg.ViewId)
	if leader == r.idx {
		if int(msg.SenderIdx) == r.idx {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		select {
		case gs.probeReplies <- msg:
		case <-gs.abortCh:
		default:
		}
		return
	}
	if int(msg.SenderIdx) != leader || !msg.Found {
		r.mu.Unlock()
		return
	}
	m := r.slotOwnerLocked(msg.Slot)
	if r.storeRecoveredLocked(msg.Slot, m.clientId, m.reqId, msg.Op, msg.Bus) {
		r.winRecovered++
	}
	r.advanceNextExpectedLocked()
	r.mu.Unlock()
	select {
	case gs.doneCh <- struct{}{}:
	case <-gs.abortCh:
	default:
	}
}

func (r *Replica) handleGapCommit(msg *BusGapCommit) {
	if !r.validReplicaIndex(msg.SenderIdx) {
		return
	}
	key := gapKey{view: msg.ViewId, slot: msg.Slot}
	r.mu.Lock()
	if r.status != statusNormal || msg.ViewId != r.view() ||
		int(msg.SenderIdx) != r.config.LeaderIndex(msg.ViewId) {
		r.mu.Unlock()
		return
	}
	ack := r.applyGapCommitLocked(msg.Slot)
	gs := r.gaps[key]
	active := r.gapActiveLocked(key, gs)
	r.mu.Unlock()
	if active {
		select {
		case gs.doneCh <- struct{}{}:
		case <-gs.abortCh:
		default:
		}
	}
	if !ack {
		return
	}
	r.sendToPeer(int(msg.SenderIdx), MsgBusGapCommitReply,
		&BusGapCommitReply{Slot: msg.Slot, SenderIdx: uint32(r.idx), ViewId: msg.ViewId})
}

func (r *Replica) applyGapCommitLocked(slot uint64) bool {
	if r.slotStateLocked(slot) == slotNoOp {
		return true
	}
	if r.executedLocked(slot) {
		Warning("[%s] ignoring no-op commit for settled slot=%d (executed=%d)",
			r.self, slot, r.nextExpected)
		return false
	}
	if r.setNoOpLocked(slot) {
		r.winNoops++
	}
	r.advanceNextExpectedLocked()
	return true
}

func (r *Replica) handleGapCommitReply(msg *BusGapCommitReply) {
	if !r.validReplicaIndex(msg.SenderIdx) || int(msg.SenderIdx) == r.idx {
		return
	}
	key := gapKey{view: msg.ViewId, slot: msg.Slot}
	r.mu.Lock()
	gs := r.gaps[key]
	if !r.leaderGapActiveLocked(key, gs) {
		r.mu.Unlock()
		return
	}
	select {
	case gs.commitAcks <- msg.SenderIdx:
	default:
	}
	r.mu.Unlock()
}
