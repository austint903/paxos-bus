package paxosbus

import (
	"bufio"
	"bytes"
	crand "crypto/rand"
	"math/bits"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	defaultStartDelayMs     = 5000
	defaultRequestTimeoutMs = 5000
	DefaultCommandSize      = 16
)

// Count quorum votes within one view; replies from different views cannot combine.
type voteKey struct {
	logIndex uint64
	viewId   uint64
}

type reqInflight struct {
	sendTimeNs  int64
	firstSendNs int64
	op          []byte
	votes       map[voteKey]uint32
	committed   bool
}

const gcCommittedNs = 2 * int64(time.Second)

type lockedWriter struct {
	mu   sync.Mutex
	w    *bufio.Writer
	conn net.Conn // control and peer writes use a bounded write deadline
}

func (lw *lockedWriter) sendMsg(code uint8, msg wireMsg) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.conn != nil {
		lw.conn.SetWriteDeadline(time.Now().Add(clientControlWriteTimeout))
		defer lw.conn.SetWriteDeadline(time.Time{})
	}
	if err := lw.w.WriteByte(code); err != nil {
		return err
	}
	msg.Marshal(lw.w)
	return lw.w.Flush()
}

func (lw *lockedWriter) sendRawBatch(bufs [][]byte) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	for _, b := range bufs {
		if _, err := lw.w.Write(b); err != nil {
			return err
		}
	}
	return lw.w.Flush()
}

type connSender struct {
	lw   *lockedWriter
	self string
	idx  int

	mu   sync.Mutex
	buf  [][]byte
	dead bool
	wake chan struct{}
}

func newConnSender(lw *lockedWriter, self string, idx int) *connSender {
	s := &connSender{lw: lw, self: self, idx: idx, wake: make(chan struct{}, 1)}
	go s.loop()
	return s
}

func (s *connSender) enqueue(b []byte) {
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return
	}
	s.buf = append(s.buf, b)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *connSender) loop() {
	for range s.wake {
		for {
			s.mu.Lock()
			batch := s.buf
			s.buf = nil
			s.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			if err := s.lw.sendRawBatch(batch); err != nil {
				Warning("[%s] send bus to replica %d failed: %v (dropping further buses on this conn)",
					s.self, s.idx, err)
				s.mu.Lock()
				s.dead = true
				s.buf = nil
				s.mu.Unlock()
				return
			}
		}
	}
}

type Client struct {
	config     *Config
	clientId   uint64
	intervalMs uint64
	resendMs   uint64
	self       string

	genIntervalUs    uint64
	requestOp        []byte // immutable write value, shared by this client's requests
	reqTimeoutNs     int64
	verbose          bool
	startDelayMs     uint64
	syncWallNs       int64
	scheduleMu       sync.Mutex
	desiredInterval  uint64
	firstSchedule    uint64
	scheduleStart    uint64
	pauseWindowAfter time.Duration
	windows          map[uint64]HsReply
	submitted        map[uint64]map[uint64]bool
	handshakeReplies chan HsReply
	arrivalNs        uint64

	// maxOwdNs is the worst measured one-way delay to a replica.
	maxOwdNs int64
	owdAuto  bool

	conns      []net.Conn
	readers    []*bufio.Reader
	writers    []*lockedWriter
	busSenders []*connSender

	mu sync.Mutex

	pendingMu sync.Mutex
	pending   []RequestMessage

	retryMu sync.Mutex
	retry   []RequestMessage

	busSeqNum uint64
	rInflight map[uint64]*reqInflight

	committedCount uint64
	totalRttUs     uint64
	resendCount    uint64

	winSent      uint64
	winCommitted uint64
	winResends   uint64
	winRttSumUs  uint64

	pauseMu         sync.Mutex
	paused          bool
	activeView      uint64
	pauseView       uint64
	viewHint        uint64
	controlStatus   []clientControlMessage
	startupComplete bool
}

func NewClient(config *Config, clientId, intervalMs, resendMs uint64, label string,
	genIntervalUs uint64, verbose bool, startDelayMs uint64,
	maxOwdMs float64, commandSize int) *Client {
	self := "Client " + strconv.FormatUint(clientId, 10)
	if label != "" {
		self += " " + label
	}
	if startDelayMs == 0 {
		startDelayMs = defaultStartDelayMs
	}
	if resendMs == 0 {
		resendMs = defaultRequestTimeoutMs
	}
	requestOp := make([]byte, commandSize)
	if _, err := crand.Read(requestOp); err != nil {
		panic("cannot generate write payload: " + err.Error())
	}
	c := &Client{
		config:           config,
		clientId:         clientId,
		intervalMs:       intervalMs,
		desiredInterval:  intervalMs,
		windows:          make(map[uint64]HsReply),
		submitted:        make(map[uint64]map[uint64]bool),
		handshakeReplies: make(chan HsReply, 64),
		resendMs:         resendMs,
		self:             self,
		genIntervalUs:    genIntervalUs,
		requestOp:        requestOp,
		reqTimeoutNs:     int64(resendMs) * 1e6,
		verbose:          verbose,
		startDelayMs:     startDelayMs,
		maxOwdNs:         int64(maxOwdMs * 1e6),
		owdAuto:          maxOwdMs == 0,
		conns:            make([]net.Conn, config.N),
		readers:          make([]*bufio.Reader, config.N),
		writers:          make([]*lockedWriter, config.N),
		busSenders:       make([]*connSender, config.N),
		rInflight:        make(map[uint64]*reqInflight),
		controlStatus:    make([]clientControlMessage, config.N),
	}
	resend := ""
	if resendMs > 0 {
		resend = "  resend=on"
	}
	Notice("[%s] started  request-gen  gen=%dus  bus=%dms  replicas=%d  f=%d  quorum=%d (f+1, must include leader)%s",
		c.self, genIntervalUs, intervalMs, config.N, config.F, config.QuorumSize(), resend)
	Notice("[%s] workload writes=100 command-size=%d key=%d", c.self, commandSize, clientId)
	if resendMs > 0 {
		Notice("[%s] req-timeout=%dms", c.self, resendMs)
	}
	return c
}

func (c *Client) Connect() error {
	var maxDialRtt time.Duration
	for i, addr := range c.config.Replicas {
		var dialRtt time.Duration
		for {
			t0 := time.Now()
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err == nil {
				dialRtt = time.Since(t0)
				c.conns[i] = conn
				break
			}
			Warning("[%s] cannot connect to replica %d (%s): %v, retrying",
				c.self, i, addr, err)
			time.Sleep(time.Second)
		}
		if dialRtt > maxDialRtt {
			maxDialRtt = dialRtt
		}
		if tc, ok := c.conns[i].(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		c.readers[i] = bufio.NewReader(c.conns[i])
		c.writers[i] = &lockedWriter{w: bufio.NewWriter(c.conns[i])}
		c.busSenders[i] = newConnSender(c.writers[i], c.self, i)
		Notice("[%s] connected to replica %d (%s)  dial_rtt=%.2fms",
			c.self, i, addr, float64(dialRtt)/1e6)
	}
	if c.owdAuto {
		c.maxOwdNs = int64(maxDialRtt) / 2
	}
	src := "-owd override"
	if c.owdAuto {
		src = "max dial RTT / 2"
	}
	Notice("[%s] max one-way delay %.2fms (%s): buses depart that far ahead of the announced arrival schedule",
		c.self, float64(c.maxOwdNs)/1e6, src)
	return nil
}

func (c *Client) Run() {
	for i := range c.readers {
		go c.receiveLoop(i)
	}
	c.startClientControl()
	c.awaitInitialSchedule()

	if sleep := c.firstSendWallNs() - wallNs(); sleep > 0 {
		time.Sleep(time.Duration(sleep))
	}

	Notice("[%s] sync wait done, starting request-gen data phase (gen=%dus bus=%dms)",
		c.self, c.genIntervalUs, c.intervalMs)
	if c.reqTimeoutNs > 0 {
		go c.reqTimeoutLoop()
	}
	go c.janitorLoop()
	go c.genLoop()
	c.busLoop()
}

func (c *Client) genLoop() {
	intervalNs := int64(c.genIntervalUs) * 1000
	if intervalNs <= 0 {
		intervalNs = 1000
	}
	next := nowNs()
	var rid uint64
	for {
		if !c.generationEnabled() {
			time.Sleep(time.Millisecond)
			next = nowNs()
			continue
		}
		now := nowNs()
		for now >= next {
			if c.isPaused() || !c.generationEnabled() {
				break
			}
			rid++
			c.pendingMu.Lock()
			c.pending = append(c.pending, RequestMessage{
				ClientId:   c.clientId,
				RequestId:  rid,
				SendTimeNs: uint64(now), // generation time; per-request latency clock starts here
				Op:         c.requestOp,
			})
			c.pendingMu.Unlock()
			next += intervalNs
			now = nowNs()
		}
		if sleep := next - nowNs(); sleep > 0 {
			time.Sleep(time.Duration(sleep))
		}
	}
}

func (c *Client) dataPhaseStartWallNs() int64 {
	return c.syncWallNs + int64(c.startDelayMs)*1e6
}

func (c *Client) firstSendWallNs() int64 {
	return c.dataPhaseStartWallNs() - c.maxOwdNs
}

func (c *Client) runOnSchedule(base, intervalNs int64, tick func()) {
	next := base
	curEpoch := int64(0)
	for {
		now := wallNs()
		for now >= next {
			tick()
			next += intervalNs
			if epoch := (next - base) / int64(time.Second); epoch != curEpoch {
				c.emitStats()
				curEpoch = epoch
			}
			now = wallNs()
		}
		if sleep := next - wallNs(); sleep > 0 {
			time.Sleep(time.Duration(sleep))
		}
	}
}

func (c *Client) sendBus() {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.paused {
		return
	}
	c.pendingMu.Lock()
	batch := c.pending
	c.pending = nil
	c.pendingMu.Unlock()

	c.retryMu.Lock()
	retry := c.retry
	c.retry = nil
	c.retryMu.Unlock()

	now := nowNs()
	reqs := make([]RequestMessage, 0, len(retry)+len(batch))
	reqs = append(reqs, retry...)
	reqs = append(reqs, batch...)

	c.mu.Lock()
	c.busSeqNum = c.arrivalNs
	seq := c.busSeqNum
	for i := range reqs {
		rid := reqs[i].RequestId
		e := c.rInflight[rid]
		if e == nil {
			genNs := int64(reqs[i].SendTimeNs)
			if genNs == 0 {
				genNs = now
			}
			e = &reqInflight{firstSendNs: genNs, op: reqs[i].Op, votes: make(map[voteKey]uint32)}
			c.rInflight[rid] = e
		}
		e.sendTimeNs = now
		reqs[i].SendTimeNs = uint64(now)
	}
	c.winSent += uint64(len(reqs))
	c.mu.Unlock()

	msg := BusMessage{
		ClientId:   c.clientId,
		BusSeqNum:  seq,
		SendTimeNs: uint64(now),
		Requests:   reqs,
	}
	var wire bytes.Buffer
	wire.WriteByte(MsgBus)
	msg.Marshal(&wire)
	b := wire.Bytes()
	for i := range c.busSenders {
		c.busSenders[i].enqueue(b)
	}
}

func (c *Client) reqTimeoutLoop() {
	tick := time.Duration(c.resendMs) * time.Millisecond / 4
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	for range ticker.C {
		if !c.generationEnabled() {
			continue
		}
		now := nowNs()
		var reboard []RequestMessage
		c.mu.Lock()
		for rid, e := range c.rInflight {
			if e.committed || now-e.sendTimeNs < c.reqTimeoutNs {
				continue
			}
			e.sendTimeNs = now
			c.resendCount++
			c.winResends++
			reboard = append(reboard, RequestMessage{
				ClientId: c.clientId, RequestId: rid, Op: e.op,
			})
		}
		c.mu.Unlock()
		if len(reboard) > 0 {
			c.retryMu.Lock()
			c.retry = append(c.retry, reboard...)
			c.retryMu.Unlock()
			Notice("[%s] REQ-TIMEOUT reboarding=%d requests", c.self, len(reboard))
		}
	}
}

func (c *Client) receiveLoop(rid int) {
	reader := c.readers[rid]
	var reqReply RequestReplyMessage
	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Warning("[%s] connection to replica %d lost: %v", c.self, rid, err)
			return
		}
		switch msgType {
		case MsgRequestReply:
			if err := reqReply.Unmarshal(reader); err != nil {
				Warning("[%s] bad request reply from replica %d: %v", c.self, rid, err)
				return
			}
			c.handleRequestReply(&reqReply)
		default:
			Warning("[%s] unknown message type %d from replica %d",
				c.self, msgType, rid)
			return
		}
	}
}

func (c *Client) quorumReached(mask uint32, viewId uint64) bool {
	return bits.OnesCount32(mask) >= c.config.QuorumSize() &&
		mask&(uint32(1)<<c.config.LeaderIndex(viewId)) != 0
}

func (c *Client) recordCommitLocked(latencyUs int64) {
	c.committedCount++
	c.totalRttUs += uint64(latencyUs)
	c.winCommitted++
	c.winRttSumUs += uint64(latencyUs)
}

func (c *Client) handleRequestReply(msg *RequestReplyMessage) {
	now := nowNs()

	c.mu.Lock()
	e, ok := c.rInflight[msg.RequestId]
	if !ok {
		c.mu.Unlock()
		return
	}
	bit := uint32(1) << msg.ReplicaIdx
	vk := voteKey{logIndex: msg.LogIndex, viewId: msg.ViewId}
	mask := e.votes[vk]
	if mask&bit != 0 {
		c.mu.Unlock()
		return
	}
	mask |= bit
	e.votes[vk] = mask
	replyRttUs := (now - e.sendTimeNs) / 1000

	justCommitted := !e.committed && c.quorumReached(mask, msg.ViewId)
	var rttUs, totalUs int64
	if justCommitted {
		e.committed = true
		rttUs = (now - e.sendTimeNs) / 1000
		totalUs = (now - e.firstSendNs) / 1000 // generation -> commit
		c.recordCommitLocked(totalUs)
	}
	c.mu.Unlock()

	if c.verbose {
		Notice("[%s] REPLY from replica=%d  rtt=%dus  req=%d  bus_slot=%d  log_index=%d",
			c.self, msg.ReplicaIdx, replyRttUs, msg.RequestId, msg.BusSlotNum, msg.LogIndex)
	}
	if justCommitted {
		Notice("[%s] COMMITTED req=%d log_index=%d rtt=%dus total=%dus",
			c.self, msg.RequestId, msg.LogIndex, rttUs, totalUs)
	}
}

func (c *Client) janitorLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		now := nowNs()
		c.mu.Lock()
		for rid, e := range c.rInflight {
			if e.committed && now-e.sendTimeNs >= gcCommittedNs {
				delete(c.rInflight, rid)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) emitStats() {
	c.mu.Lock()
	sent, committed, resends := c.winSent, c.winCommitted, c.winResends
	rttSum := c.winRttSumUs
	inflight := len(c.rInflight)
	cumCommitted, cumRttSum := c.committedCount, c.totalRttUs
	c.winSent, c.winCommitted, c.winResends, c.winRttSumUs = 0, 0, 0, 0
	c.mu.Unlock()

	if sent == 0 && committed == 0 && resends == 0 {
		return
	}
	var winAvgUs, cumAvgUs uint64
	if committed > 0 {
		winAvgUs = rttSum / committed
	}
	if cumCommitted > 0 {
		cumAvgUs = cumRttSum / cumCommitted
	}
	Notice("[%s] 1s: sent=%d committed=%d resends=%d inflight=%d lat_avg=%dus  cumulative: committed=%d lat_avg=%dus",
		c.self, sent, committed, resends, inflight, winAvgUs, cumCommitted, cumAvgUs)
}
