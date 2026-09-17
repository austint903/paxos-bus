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
	defaultRecoveryWaitMs   = 2500
	defaultRequestTimeoutMs = 5000
	statusPollInterval      = 350 * time.Millisecond
	// DefaultCommandSize is the value size in bytes, matching the GCP baselines.
	DefaultCommandSize = 16
)

// Votes are counted per view, never across them. A view change can hand a
// replica an entry it had logged but never acknowledged, so it replies again in
// the new view — and those replies must not be added to the ones the request
// collected before the old leader died. Two half-quorums from two views are not
// a quorum: the request simply has not committed, and the resend path re-boards
// it (dedup hands back its original log index, so the votes it earns next are
// for the same entry). Where a request really did commit in the old view, the
// sticky committed flag makes the new view's extra replies a no-op.
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
	mu sync.Mutex
	w  *bufio.Writer
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
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

	genIntervalUs  uint64
	requestOp      []byte // immutable write value, shared by this client's requests
	reqTimeoutNs   int64
	verbose        bool
	startDelayMs   uint64
	recoveryWaitMs uint64
	syncWallNs     int64

	// maxOwdNs is the worst one-way delay from this client to any replica.
	// The sync message announces an ARRIVAL schedule, so every bus departs
	// maxOwdNs before its announced line instant — by that instant it has
	// reached even the farthest replica. 0 until Connect() measures it (or a
	// -owd override is given); replicas ordering on the same lines then stop
	// paying this client's inbound delay before appending later slots.
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

	pauseMu       sync.Mutex
	paused        bool
	activeView    uint64
	pauseView     uint64
	pauseEpoch    uint64
	pauseCh       chan struct{}
	resumeCh      chan struct{}
	resumeSendNs  int64
	statusReplies chan ClientStatusReply
	statusQueryId uint64

	committedCount uint64
	totalRttUs     uint64
	resendCount    uint64

	winSent      uint64
	winCommitted uint64
	winResends   uint64
	winRttSumUs  uint64
}

func NewClient(config *Config, clientId, intervalMs, resendMs uint64, label string,
	genIntervalUs uint64, verbose bool, startDelayMs uint64,
	recoveryWaitMs uint64, maxOwdMs float64, commandSize int) *Client {
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
	if recoveryWaitMs == 0 {
		recoveryWaitMs = defaultRecoveryWaitMs
	}
	requestOp := make([]byte, commandSize)
	if _, err := crand.Read(requestOp); err != nil {
		panic("cannot generate write payload: " + err.Error())
	}
	c := &Client{
		config:         config,
		clientId:       clientId,
		intervalMs:     intervalMs,
		resendMs:       resendMs,
		self:           self,
		genIntervalUs:  genIntervalUs,
		requestOp:      requestOp,
		reqTimeoutNs:   int64(resendMs) * 1e6,
		verbose:        verbose,
		startDelayMs:   startDelayMs,
		recoveryWaitMs: recoveryWaitMs,
		maxOwdNs:       int64(maxOwdMs * 1e6),
		owdAuto:        maxOwdMs == 0,
		conns:          make([]net.Conn, config.N),
		readers:        make([]*bufio.Reader, config.N),
		writers:        make([]*lockedWriter, config.N),
		busSenders:     make([]*connSender, config.N),
		rInflight:      make(map[uint64]*reqInflight),
		pauseCh:        make(chan struct{}),
		statusReplies:  make(chan ClientStatusReply, config.N*2),
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
	// The TCP handshake (SYN -> SYN-ACK) is one round trip, so the slowest
	// dial estimates the worst RTT to any replica without extra protocol.
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
	// The sync message anchors the arrival-prediction line at NextBusSeq.
	// FirstMsgNs is a true ARRIVAL instant: the bus departs maxOwdNs earlier, so
	// it reaches the farthest replica right on its line
	// and nearer replicas early. Ordering by send instants instead made every
	// in-order append (and thus every reply) wait out the slowest inbound
	// region's one-way delay past the line — the straggler penalty; now the
	// replica-side Δ only has to absorb jitter around the line, not the delay.
	c.syncWallNs = wallNs()
	syncMsg := BusSyncMessage{
		ClientId:   c.clientId,
		FirstMsgNs: uint64(c.dataPhaseStartWallNs()),
		IntervalMs: c.intervalMs,
		NextBusSeq: 1,
	}
	if !c.broadcastSync(&syncMsg) {
		Panic("[%s] failed to send initial sync", c.self)
	}
	Notice("[%s] sync sent, waiting %dms before data phase", c.self, c.startDelayMs)

	for i := range c.readers {
		go c.receiveLoop(i)
	}

	// Sleep until maxOwdNs BEFORE the FirstMsgNs instant announced in the sync
	// message, on the same wall clock the replicas use for expected arrival
	// times. Sleeping a fixed duration from "after sync send" instead would
	// shift every actual arrival past the announced schedule, and with multiple
	// clients each replica's in-order log append waits for the LATEST client.
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

func (c *Client) broadcastSync(msg *BusSyncMessage) bool {
	ok := true
	for i, lw := range c.writers {
		if err := lw.sendMsg(MsgBusSync, msg); err != nil {
			Warning("[%s] failed to send sync to replica %d: %v", c.self, i, err)
			ok = false
		}
	}
	return ok
}

// genLoop produces requests at a fixed rate into c.pending, stamping SendTimeNs
// so that latency counts each request's wait for a bus
func (c *Client) genLoop() {
	intervalNs := int64(c.genIntervalUs) * 1000
	if intervalNs <= 0 {
		intervalNs = 1000
	}
	next := nowNs()
	var rid uint64
	for {
		paused, changed, _ := c.pauseState()
		if paused {
			<-changed
			next = nowNs()
			continue
		}
		now := nowNs()
		for now >= next {
			if c.isPaused() {
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
			timer := time.NewTimer(time.Duration(sleep))
			select {
			case <-changed:
				stopTimer(timer)
				next = nowNs()
			case <-timer.C:
			}
		}
	}
}

// dataPhaseStartWallNs is the wall-clock instant announced to the replicas in
// the sync message, where bus n is expected to arrive at start + (n-1)*interval
func (c *Client) dataPhaseStartWallNs() int64 {
	return c.syncWallNs + int64(c.startDelayMs)*1e6
}

// firstSendWallNs is when buses actually start departing, early enough that bus
// n reaches even the farthest replica by its line instant instead of one one-way
// delay after it
func (c *Client) firstSendWallNs() int64 {
	return c.dataPhaseStartWallNs() - c.maxOwdNs
}

// busLoop departs one bus per interval on the announced schedule
func (c *Client) busLoop() {
	intervalNs := int64(c.intervalMs) * 1e6
	next := c.firstSendWallNs()
	lastStats := wallNs()
	resetSchedule := false
	for {
		paused, changed, resumeAt := c.pauseState()
		if paused {
			resetSchedule = true
			<-changed
			continue
		}
		if resetSchedule {
			next = resumeAt
			resetSchedule = false
		}
		now := wallNs()
		if now < next {
			timer := time.NewTimer(time.Duration(next - now))
			select {
			case <-changed:
				stopTimer(timer)
				resetSchedule = true
				continue
			case <-timer.C:
			}
		}
		if !c.isPaused() {
			c.sendBus()
		}
		next += intervalNs
		if now := wallNs(); now-lastStats >= int64(time.Second) {
			c.emitStats()
			lastStats = now
		}
	}
}

// sendBus drains the pending and retry buffers into one bus, marshals it once,
// and hands the same bytes to every replica's sender
func (c *Client) sendBus() {
	if c.isPaused() {
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
	c.busSeqNum++
	seq := c.busSeqNum
	for i := range reqs {
		rid := reqs[i].RequestId
		e := c.rInflight[rid]
		if e == nil {
			// firstSendNs is the generation time
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
	// The bus is marshaled once, since every sender reads the same bytes
	var wire bytes.Buffer
	wire.WriteByte(MsgBus)
	msg.Marshal(&wire)
	b := wire.Bytes()
	for i := range c.busSenders {
		c.busSenders[i].enqueue(b)
	}
}

// reqTimeoutLoop re-boards requests that missed quorum onto the next bus, and
// the request id is kept so that dedup returns the log index it already had
func (c *Client) reqTimeoutLoop() {
	tick := time.Duration(c.resendMs) * time.Millisecond / 4
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	for range ticker.C {
		if c.isPaused() {
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

// receiveLoop reads replies from one replica, with one goroutine per connection
func (c *Client) receiveLoop(rid int) {
	reader := c.readers[rid]
	var reqReply RequestReplyMessage
	var pause ClientPauseMessage
	var status ClientStatusReply
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
		case MsgClientPause:
			if err := pause.Unmarshal(reader); err != nil {
				Warning("[%s] bad pause message from replica %d: %v", c.self, rid, err)
				return
			}
			c.handlePause(&pause)
		case MsgClientStatusReply:
			if err := status.Unmarshal(reader); err != nil {
				Warning("[%s] bad status reply from replica %d: %v", c.self, rid, err)
				return
			}
			select {
			case c.statusReplies <- status:
			default:
			}
		default:
			Warning("[%s] unknown message type %d from replica %d",
				c.self, msgType, rid)
			return
		}
	}
}

func (c *Client) pauseState() (bool, <-chan struct{}, int64) {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.paused {
		return true, c.resumeCh, c.resumeSendNs
	}
	return false, c.pauseCh, c.resumeSendNs
}

func (c *Client) isPaused() bool {
	c.pauseMu.Lock()
	paused := c.paused
	c.pauseMu.Unlock()
	return paused
}

func (c *Client) handlePause(msg *ClientPauseMessage) {
	c.pauseMu.Lock()
	if msg.ViewId <= c.activeView {
		c.pauseMu.Unlock()
		return
	}
	started := false
	if !c.paused {
		c.paused = true
		c.pauseView = msg.ViewId
		c.pauseEpoch++
		close(c.pauseCh)
		c.resumeCh = make(chan struct{})
		started = true
	} else if msg.ViewId > c.pauseView {
		c.pauseView = msg.ViewId
		c.pauseEpoch++
	}
	c.pauseMu.Unlock()
	if !started {
		return
	}
	Notice("[%s] pausing for view change at replica %d view=%d",
		c.self, msg.ReplicaIdx, msg.ViewId)
	go c.recoveryLoop()
}

func (c *Client) recoveryLoop() {
	for {
		c.pauseMu.Lock()
		if !c.paused {
			c.pauseMu.Unlock()
			return
		}
		minView, epoch := c.pauseView, c.pauseEpoch
		c.pauseMu.Unlock()

		view := c.waitForNormalQuorum(minView)
		c.pauseMu.Lock()
		stale := !c.paused || c.pauseEpoch != epoch || view < c.pauseView
		c.pauseMu.Unlock()
		if stale {
			continue
		}

		c.mu.Lock()
		nextSeq := c.busSeqNum + 1
		c.mu.Unlock()
		arrivalNs := wallNs() + int64(c.recoveryWaitMs)*1e6
		msg := BusSyncMessage{
			ClientId: c.clientId, FirstMsgNs: uint64(arrivalNs),
			IntervalMs: c.intervalMs, NextBusSeq: nextSeq,
		}
		c.broadcastSync(&msg)
		Notice("[%s] normal quorum in view=%d; sync sent, waiting %dms before resuming",
			c.self, view, c.recoveryWaitMs)

		resumeSendNs := arrivalNs - c.maxOwdNs
		if sleep := resumeSendNs - wallNs(); sleep > 0 {
			time.Sleep(time.Duration(sleep))
		}

		c.pauseMu.Lock()
		if !c.paused || c.pauseEpoch != epoch || view < c.pauseView {
			c.pauseMu.Unlock()
			continue
		}
		c.mu.Lock()
		now := nowNs()
		for _, req := range c.rInflight {
			if !req.committed {
				req.sendTimeNs = now
			}
		}
		c.mu.Unlock()
		c.activeView = view
		c.resumeSendNs = resumeSendNs
		c.paused = false
		c.pauseCh = make(chan struct{})
		close(c.resumeCh)
		c.pauseMu.Unlock()
		Notice("[%s] resumed in view=%d at bus=%d", c.self, view, nextSeq)
		return
	}
}

func (c *Client) waitForNormalQuorum(minView uint64) uint64 {
	for {
		for {
			select {
			case <-c.statusReplies:
			default:
				goto drained
			}
		}
	drained:
		c.statusQueryId++
		queryId := c.statusQueryId
		query := ClientStatusQuery{QueryId: queryId}
		for i, lw := range c.writers {
			if err := lw.sendMsg(MsgClientStatusQuery, &query); err != nil {
				Warning("[%s] status query to replica %d failed: %v", c.self, i, err)
			}
		}

		votes := make(map[uint64]map[uint32]struct{})
		timer := time.NewTimer(statusPollInterval)
		for {
			select {
			case reply := <-c.statusReplies:
				if reply.QueryId != queryId || !reply.Normal || reply.ViewId < minView ||
					int(reply.ReplicaIdx) >= c.config.N {
					continue
				}
				set := votes[reply.ViewId]
				if set == nil {
					set = make(map[uint32]struct{})
					votes[reply.ViewId] = set
				}
				set[reply.ReplicaIdx] = struct{}{}
				if len(set) >= c.config.QuorumSize() {
					stopTimer(timer)
					return reply.ViewId
				}
			case <-timer.C:
				goto retry
			}
		}
	retry:
	}
}

// quorumReached reports whether mask holds a quorum that includes the leader of
// viewId
func (c *Client) quorumReached(mask uint32, viewId uint64) bool {
	return bits.OnesCount32(mask) >= c.config.QuorumSize() &&
		mask&(uint32(1)<<c.config.LeaderIndex(viewId)) != 0
}

// recordCommitLocked adds one commit's latency to the cumulative and per-second
// counters
func (c *Client) recordCommitLocked(latencyUs int64) {
	c.committedCount++
	c.totalRttUs += uint64(latencyUs)
	c.winCommitted++
	c.winRttSumUs += uint64(latencyUs)
}

// handleRequestReply counts one replica's vote, keyed by log index because a
// re-boarded request can land at different indexes on different replicas
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
