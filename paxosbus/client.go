package paxosbus

import (
	"bufio"
	"bytes"
	"math/bits"
	"net"
	"strconv"
	"sync"
	"time"
)

const defaultStartDelayMs = 5000

type inflightEntry struct {
	sendTimeNs      int64
	firstSendTimeNs int64
	replicaMask     uint32
	replyCount      int
	origReqId       uint64
	attempts        uint32
	committed       bool
}

type reqInflight struct {
	sendTimeNs  int64
	firstSendNs int64
	op          []byte
	votes       map[uint64]uint32
	committed   bool
}

const gcCommittedNs = 2 * int64(time.Second)

type lockedWriter struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func (lw *lockedWriter) send(code uint8, msg *BusRequestMessage) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.w.WriteByte(code)
	msg.Marshal(lw.w)
	return lw.w.Flush()
}

func (lw *lockedWriter) sendMsg(code uint8, msg wireMsg) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.w.WriteByte(code)
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

// connSender decouples the bus loop from one replica connection: sendBus
// enqueues the pre-marshaled bus and never blocks on the network, so TCP
// backpressure from one stalled replica cannot delay buses to the healthy
// ones. A per-connection goroutine drains the buffer in FIFO order; a send
// error kills the sender (the TCP stream is broken anyway — receiveLoop dies
// on the same conn) and further buses to it are dropped.
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

	requestGen    bool
	genIntervalUs uint64
	reqTimeoutNs  int64
	verbose       bool
	startDelayMs  uint64
	syncWallNs    int64

	conns      []net.Conn
	readers    []*bufio.Reader
	writers    []*lockedWriter
	busSenders []*connSender

	mu        sync.Mutex
	inflight  map[uint64]*inflightEntry
	requestId uint64

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
}

func NewClient(config *Config, clientId, intervalMs, resendMs uint64, label string,
	requestGen bool, genIntervalUs uint64, verbose bool, startDelayMs uint64) *Client {
	self := "Client " + strconv.FormatUint(clientId, 10)
	if label != "" {
		self += " " + label
	}
	if startDelayMs == 0 {
		startDelayMs = defaultStartDelayMs
	}
	c := &Client{
		config:        config,
		clientId:      clientId,
		intervalMs:    intervalMs,
		resendMs:      resendMs,
		self:          self,
		requestGen:    requestGen,
		genIntervalUs: genIntervalUs,
		reqTimeoutNs:  int64(resendMs) * 1e6,
		verbose:       verbose,
		startDelayMs:  startDelayMs,
		conns:         make([]net.Conn, config.N),
		readers:       make([]*bufio.Reader, config.N),
		writers:       make([]*lockedWriter, config.N),
		busSenders:    make([]*connSender, config.N),
		inflight:      make(map[uint64]*inflightEntry),
		rInflight:     make(map[uint64]*reqInflight),
	}
	resend := ""
	if resendMs > 0 {
		resend = "  resend=on"
	}
	if requestGen {
		Notice("[%s] started  request-gen  gen=%dus  bus=%dms  replicas=%d  f=%d  quorum=%d (f+1, must include leader)%s",
			c.self, genIntervalUs, intervalMs, config.N, config.F, config.QuorumSize(), resend)
		if resendMs > 0 {
			Notice("[%s] req-timeout=%dms", c.self, resendMs)
		}
		return c
	}
	Notice("[%s] started  interval=%dms  replicas=%d  f=%d  quorum=%d (f+1, must include leader)%s",
		c.self, intervalMs, config.N, config.F, config.QuorumSize(), resend)
	if resendMs > 0 {
		Notice("[%s] resend-on-no-quorum timeout=%dms", c.self, resendMs)
	}
	return c
}

func (c *Client) Connect() error {
	for i, addr := range c.config.Replicas {
		for {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err == nil {
				c.conns[i] = conn
				break
			}
			Warning("[%s] cannot connect to replica %d (%s): %v, retrying",
				c.self, i, addr, err)
			time.Sleep(time.Second)
		}
		if tc, ok := c.conns[i].(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		c.readers[i] = bufio.NewReader(c.conns[i])
		c.writers[i] = &lockedWriter{w: bufio.NewWriter(c.conns[i])}
		c.busSenders[i] = newConnSender(c.writers[i], c.self, i)
		Notice("[%s] connected to replica %d (%s)", c.self, i, addr)
	}
	return nil
}

func (c *Client) Run() {
	c.syncWallNs = wallNs()
	syncMsg := BusSyncMessage{
		ClientId:     c.clientId,
		SendTimeNs:   uint64(c.syncWallNs),
		IntervalMs:   c.intervalMs,
		StartDelayMs: c.startDelayMs,
	}
	for i, lw := range c.writers {
		lw.mu.Lock()
		lw.w.WriteByte(MsgBusSync)
		syncMsg.Marshal(lw.w)
		err := lw.w.Flush()
		lw.mu.Unlock()
		if err != nil {
			Panic("[%s] failed to send sync to replica %d: %v", c.self, i, err)
		}
	}
	Notice("[%s] sync sent, waiting %dms before data phase", c.self, c.startDelayMs)

	for i := range c.readers {
		go c.receiveLoop(i)
	}

	// Sleep until the promised data-phase start on the same wall clock the
	// replicas use for expected arrival times: base = syncWallNs + startDelay.
	// Sleeping a fixed duration from "after sync send" instead would shift every
	// actual departure a few ms past the promised schedule, and with multiple
	// clients each replica's in-order log append waits for the LATEST client.
	if sleep := c.dataPhaseStartWallNs() - wallNs(); sleep > 0 {
		time.Sleep(time.Duration(sleep))
	}

	if c.requestGen {
		Notice("[%s] sync wait done, starting request-gen data phase (gen=%dus bus=%dms)",
			c.self, c.genIntervalUs, c.intervalMs)
		if c.reqTimeoutNs > 0 {
			go c.reqTimeoutLoop()
		}
		go c.janitorLoop()
		go c.genLoop()
		c.busLoop()
		return
	}

	Notice("[%s] sync wait done, starting open-loop data phase (interval=%dms)",
		c.self, c.intervalMs)

	if c.resendMs > 0 {
		go c.resendLoop()
	}
	go c.janitorLoop()
	c.sendLoop()
}

func (c *Client) genLoop() {
	intervalNs := int64(c.genIntervalUs) * 1000
	if intervalNs <= 0 {
		intervalNs = 1000
	}
	next := nowNs()
	var rid uint64
	for {
		now := nowNs()
		for now >= next {
			rid++
			c.pendingMu.Lock()
			c.pending = append(c.pending, RequestMessage{
				ClientId:   c.clientId,
				RequestId:  rid,
				SendTimeNs: uint64(now), // generation time; per-request latency clock starts here
				Op:         []byte("hello"),
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

// dataPhaseStartWallNs is the wall-clock instant promised to the replicas in
// the sync message: bus/request n is expected at start + (n-1)*interval.
func (c *Client) dataPhaseStartWallNs() int64 {
	return c.syncWallNs + int64(c.startDelayMs)*1e6
}

// busLoop targets the promised wall-clock schedule directly (base + k*interval)
// rather than free-running from whenever the 5s sleep happened to wake, so
// actual departures track the schedule the replicas order by to within sleep
// jitter instead of drifting several ms late.
func (c *Client) busLoop() {
	intervalNs := int64(c.intervalMs) * 1e6
	base := c.dataPhaseStartWallNs()
	next := base
	curEpoch := int64(0)
	for {
		now := wallNs()
		for now >= next {
			c.sendBus()
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
	c.busSeqNum++
	seq := c.busSeqNum
	for i := range reqs {
		rid := reqs[i].RequestId
		e := c.rInflight[rid]
		if e == nil {
			// firstSendNs = generation time (stamped in genLoop) so per-request
			// latency includes the wait in c.pending before boarding this bus.
			genNs := int64(reqs[i].SendTimeNs)
			if genNs == 0 {
				genNs = now
			}
			e = &reqInflight{firstSendNs: genNs, op: reqs[i].Op, votes: make(map[uint64]uint32)}
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
	// Marshal once; every sender reads the same byte slice (never mutated).
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

// sendLoop targets the promised wall-clock schedule for the same reason as
// busLoop (see there).
func (c *Client) sendLoop() {
	intervalNs := int64(c.intervalMs) * 1e6
	base := c.dataPhaseStartWallNs()
	nextSendNs := base
	curEpoch := int64(0)
	for {
		now := wallNs()
		for now >= nextSendNs {
			c.mu.Lock()
			c.requestId++
			reqId := c.requestId
			c.mu.Unlock()
			c.sendRequest(reqId, reqId, 0, 1)
			nextSendNs += intervalNs

			if epoch := (nextSendNs - base) / int64(time.Second); epoch != curEpoch {
				c.emitStats()
				curEpoch = epoch
			}
			now = wallNs()
		}
		if sleep := nextSendNs - wallNs(); sleep > 0 {
			time.Sleep(time.Duration(sleep))
		}
	}
}

func (c *Client) sendRequest(reqId, origReqId uint64, firstSendNs int64, attempts uint32) {
	now := nowNs()
	if firstSendNs == 0 {
		firstSendNs = now
	}
	c.mu.Lock()
	c.inflight[reqId] = &inflightEntry{
		sendTimeNs:      now,
		firstSendTimeNs: firstSendNs,
		origReqId:       origReqId,
		attempts:        attempts,
	}
	c.winSent++
	c.mu.Unlock()

	msg := BusRequestMessage{
		ClientId:   c.clientId,
		RequestId:  reqId,
		SendTimeNs: uint64(now),
		Op:         []byte("hello"),
	}
	for i, lw := range c.writers {
		if err := lw.send(MsgBusRequest, &msg); err != nil {
			Warning("[%s] send to replica %d failed: %v", c.self, i, err)
		}
	}
}

func (c *Client) receiveLoop(rid int) {
	reader := c.readers[rid]
	var (
		busReply BusReplyMessage
		reqReply RequestReplyMessage
	)
	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Warning("[%s] connection to replica %d lost: %v", c.self, rid, err)
			return
		}
		switch msgType {
		case MsgBusReply:
			if err := busReply.Unmarshal(reader); err != nil {
				Warning("[%s] bad reply from replica %d: %v", c.self, rid, err)
				return
			}
			c.handleBusReply(&busReply)
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

func (c *Client) handleRequestReply(msg *RequestReplyMessage) {
	now := nowNs()

	c.mu.Lock()
	e, ok := c.rInflight[msg.RequestId]
	if !ok {
		c.mu.Unlock()
		return
	}
	bit := uint32(1) << msg.ReplicaIdx
	mask := e.votes[msg.LogIndex]
	if mask&bit != 0 {
		c.mu.Unlock()
		return
	}
	mask |= bit
	e.votes[msg.LogIndex] = mask
	replyRttUs := (now - e.sendTimeNs) / 1000

	leaderIdx := c.config.LeaderIndex(msg.ViewId)
	justCommitted := !e.committed &&
		bits.OnesCount32(mask) >= c.config.QuorumSize() &&
		mask&(uint32(1)<<leaderIdx) != 0

	var rttUs, totalUs int64
	if justCommitted {
		e.committed = true
		rttUs = (now - e.sendTimeNs) / 1000
		totalUs = (now - e.firstSendNs) / 1000 // generation -> commit
		c.committedCount++
		c.totalRttUs += uint64(totalUs)
		c.winCommitted++
		c.winRttSumUs += uint64(totalUs)
	}
	c.mu.Unlock()

	// Per-reply lines are gated: at thousands of requests/s this is 3 log
	// syscalls per request through the global log mutex, which the receive
	// goroutines serialize on. COMMITTED (with total= gen->quorum latency) is
	// always logged — it is what the stats aggregation parses.
	if c.verbose {
		Notice("[%s] REPLY from replica=%d  rtt=%dus  req=%d  bus_slot=%d  log_index=%d",
			c.self, msg.ReplicaIdx, replyRttUs, msg.RequestId, msg.BusSlotNum, msg.LogIndex)
	}
	if justCommitted {
		Notice("[%s] COMMITTED req=%d log_index=%d rtt=%dus total=%dus",
			c.self, msg.RequestId, msg.LogIndex, rttUs, totalUs)
	}
}

func (c *Client) handleBusReply(msg *BusReplyMessage) {
	now := nowNs()

	c.mu.Lock()
	e, ok := c.inflight[msg.RequestId]
	if !ok {
		c.mu.Unlock()
		return
	}
	bit := uint32(1) << msg.ReplicaIdx
	if e.replicaMask&bit != 0 {
		c.mu.Unlock()
		return
	}
	e.replicaMask |= bit
	e.replyCount++
	replyRttUs := (now - e.sendTimeNs) / 1000

	leaderIdx := c.config.LeaderIndex(msg.ViewId)
	justCommitted := !e.committed &&
		e.replyCount >= c.config.QuorumSize() &&
		e.replicaMask&(uint32(1)<<leaderIdx) != 0

	var rttUs, totalUs int64
	var origReqId uint64
	var attempts uint32
	if justCommitted {
		e.committed = true
		rttUs = (now - e.sendTimeNs) / 1000
		totalUs = (now - e.firstSendTimeNs) / 1000
		origReqId, attempts = e.origReqId, e.attempts
		c.committedCount++
		c.totalRttUs += uint64(rttUs)
		c.winCommitted++
		c.winRttSumUs += uint64(rttUs)
	}
	postQuorum := 0
	if e.committed && !justCommitted {
		postQuorum = 1
	}
	if e.replyCount >= c.config.N {
		delete(c.inflight, msg.RequestId)
	}
	c.mu.Unlock()

	if c.verbose {
		Notice("[%s] REPLY from replica=%d  rtt=%dus  req=%d  slot=%d  post_quorum=%d",
			c.self, msg.ReplicaIdx, replyRttUs, msg.RequestId, msg.LogSlotNum, postQuorum)
	}
	if justCommitted {
		Notice("[%s] COMMITTED req=%d slot=%d rtt=%dus total=%dus attempts=%d",
			c.self, origReqId, msg.LogSlotNum, rttUs, totalUs, attempts)
	}
}

func (c *Client) resendLoop() {
	resendNs := int64(c.resendMs) * 1e6
	tick := time.Duration(c.resendMs) * time.Millisecond / 4
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	for range ticker.C {
		now := nowNs()
		type resend struct {
			oldReqId, newReqId, origReqId uint64
			firstSendNs                   int64
			attempts                      uint32
			totalResends                  uint64
		}
		var expired []resend

		c.mu.Lock()
		for reqId, e := range c.inflight {
			if e.committed {
				continue
			}
			if now-e.sendTimeNs < resendNs {
				continue
			}
			c.requestId++
			newReqId := c.requestId
			c.resendCount++
			c.winResends++
			expired = append(expired, resend{
				oldReqId: reqId, newReqId: newReqId, origReqId: e.origReqId,
				firstSendNs: e.firstSendTimeNs, attempts: e.attempts,
				totalResends: c.resendCount,
			})
			delete(c.inflight, reqId)
		}
		c.mu.Unlock()

		for _, rs := range expired {
			Notice("[%s] NO-QUORUM req=%d  resending as req=%d  attempt=%d  total_resends=%d",
				c.self, rs.oldReqId, rs.newReqId, rs.attempts+1, rs.totalResends)
			c.sendRequest(rs.newReqId, rs.origReqId, rs.firstSendNs, rs.attempts+1)
		}
	}
}

func (c *Client) janitorLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		now := nowNs()
		c.mu.Lock()
		for seq, e := range c.inflight {
			if e.committed && now-e.sendTimeNs >= gcCommittedNs {
				delete(c.inflight, seq)
			}
		}
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
	inflight := len(c.inflight)
	if c.requestGen {
		inflight = len(c.rInflight)
	}
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
