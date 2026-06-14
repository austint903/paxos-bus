package paxosbus

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"time"
)

const syncStartDelayMs = 5000

// inflightEntry tracks one open-loop request: seq -> per-request state.
type inflightEntry struct {
	sendTimeNs      int64  // this attempt's send time
	firstSendTimeNs int64  // first attempt's send time (stable across resends)
	replicaMask     uint32 // bit i set => replica i has replied
	replyCount      int
	appReqId        uint64 // stable logical id across resends
	attempts        uint32 // 1 on first send, +1 per resend
}

// lockedWriter serializes writes to one replica connection: the sender
// goroutine and the resend scanner both write to the same TCP stream.
type lockedWriter struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func (lw *lockedWriter) send(code uint8, msg *DataMessage) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.w.WriteByte(code)
	msg.Marshal(lw.w)
	return lw.w.Flush()
}

// Client runs the PaxosBus open loop with two decoupled sides sharing one
// mutex-protected inflight map:
//   - the send goroutine paces DataMessages to all replicas at a fixed
//     interval (absolute deadlines, so the rate never exceeds 1/interval);
//   - per-replica receive goroutines tally DataReplyMessages, check the
//     leader-inclusive f+1 quorum, and delete committed entries.
type Client struct {
	config     *Config
	clientId   uint64
	intervalMs uint64
	resendMs   uint64
	self       string // log-line identity, e.g. "Client 1 asia-east1"

	conns   []net.Conn
	readers []*bufio.Reader
	writers []*lockedWriter

	mu       sync.Mutex
	inflight map[uint64]*inflightEntry
	seqNum   uint64
	appReqId uint64

	// cumulative latency stats (microseconds)
	committedCount uint64
	totalRttUs     uint64
	resendCount    uint64

	// 1-second window counters for the periodic summary line
	winSent      uint64
	winCommitted uint64
	winResends   uint64
	winRttSumUs  uint64
}

func NewClient(config *Config, clientId, intervalMs, resendMs uint64, label string) *Client {
	self := "Client " + strconv.FormatUint(clientId, 10)
	if label != "" {
		self += " " + label
	}
	c := &Client{
		config:     config,
		clientId:   clientId,
		intervalMs: intervalMs,
		resendMs:   resendMs,
		self:       self,
		conns:      make([]net.Conn, config.N),
		readers:    make([]*bufio.Reader, config.N),
		writers:    make([]*lockedWriter, config.N),
		inflight:   make(map[uint64]*inflightEntry),
	}
	resend := ""
	if resendMs > 0 {
		resend = "  resend=on"
	}
	Notice("[%s] started  interval=%dms  replicas=%d  f=%d  quorum=%d (f+1, must include leader)%s",
		c.self, intervalMs, config.N, config.F, config.QuorumSize(), resend)
	if resendMs > 0 {
		Notice("[%s] resend-on-no-quorum timeout=%dms", c.self, resendMs)
	}
	return c
}

// Connect dials every replica, retrying until each is up (replicas may still
// be binding when clients launch).
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
		Notice("[%s] connected to replica %d (%s)", c.self, i, addr)
	}
	return nil
}

// Run executes the sync phase and then the open-loop data phase. Blocks
// forever (the send loop never exits).
func (c *Client) Run() {
	syncMsg := SyncMessage{
		ClientId:     c.clientId,
		SendTimeNs:   uint64(nowNs()),
		IntervalMs:   c.intervalMs,
		StartDelayMs: syncStartDelayMs,
	}
	for i, lw := range c.writers {
		lw.mu.Lock()
		lw.w.WriteByte(MsgSync)
		syncMsg.Marshal(lw.w)
		err := lw.w.Flush()
		lw.mu.Unlock()
		if err != nil {
			Panic("[%s] failed to send sync to replica %d: %v", c.self, i, err)
		}
	}
	Notice("[%s] sync sent, waiting 5s before data phase", c.self)

	// Replies only flow once data flows, but start the receive side now so
	// nothing is ever queued behind the sync wait.
	for i := range c.readers {
		go c.receiveLoop(i)
	}

	time.Sleep(syncStartDelayMs * time.Millisecond)
	Notice("[%s] sync wait done, starting open-loop data phase (interval=%dms)",
		c.self, c.intervalMs)

	if c.resendMs > 0 {
		go c.resendLoop()
	}
	c.sendLoop()
}

// sendLoop is the send goroutine: every interval, lock the map, register the
// next request, unlock, and broadcast it. Absolute deadlines (nextSendNs)
// mean late wakeups are repaid as a small catch-up burst instead of
// permanently lowering the rate — and the average rate never exceeds
// 1/interval.
func (c *Client) sendLoop() {
	intervalNs := int64(c.intervalMs) * 1e6
	t0 := nowNs()
	nextSendNs := t0
	curEpoch := int64(0)
	for {
		now := nowNs()
		for now >= nextSendNs {
			c.mu.Lock()
			c.seqNum++
			c.appReqId++
			seq, appReqId := c.seqNum, c.appReqId
			c.mu.Unlock()
			c.sendData(seq, appReqId, 0, 1)
			nextSendNs += intervalNs

			// Emit the 1s summary on the send grid itself rather than from a
			// separate wall-clock ticker. Send deadlines are exactly
			// t0 + k*interval, so every completed 1-second epoch holds exactly
			// 1000/interval sends — the reported send count has no sampling
			// jitter. This only changes when the stats line fires; the send
			// timing, wire messages, and quorum logic are untouched.
			if epoch := (nextSendNs - t0) / int64(time.Second); epoch != curEpoch {
				c.emitStats()
				curEpoch = epoch
			}
			now = nowNs()
		}
		time.Sleep(time.Duration(nextSendNs - now))
	}
}

// sendData registers seq in the inflight map and broadcasts the DataMessage.
// The entry is inserted before the send so a reply can never race the map.
func (c *Client) sendData(seq, appReqId uint64, firstSendNs int64, attempts uint32) {
	now := nowNs()
	if firstSendNs == 0 {
		firstSendNs = now
	}
	c.mu.Lock()
	c.inflight[seq] = &inflightEntry{
		sendTimeNs:      now,
		firstSendTimeNs: firstSendNs,
		appReqId:        appReqId,
		attempts:        attempts,
	}
	c.winSent++
	c.mu.Unlock()

	msg := DataMessage{
		ClientId:   c.clientId,
		SeqNum:     seq,
		SendTimeNs: uint64(now),
		AppReqId:   appReqId,
		Payload:    []byte("hello"),
	}
	for i, lw := range c.writers {
		if err := lw.send(MsgData, &msg); err != nil {
			Warning("[%s] send to replica %d failed: %v", c.self, i, err)
		}
	}
}

// receiveLoop is the receive side for one replica connection: read a reply,
// lock the map, tally it, run the quorum check, and remove committed entries.
func (c *Client) receiveLoop(rid int) {
	reader := c.readers[rid]
	var reply DataReplyMessage
	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Warning("[%s] connection to replica %d lost: %v", c.self, rid, err)
			return
		}
		if msgType != MsgDataReply {
			Warning("[%s] unknown message type %d from replica %d",
				c.self, msgType, rid)
			return
		}
		if err := reply.Unmarshal(reader); err != nil {
			Warning("[%s] bad reply from replica %d: %v", c.self, rid, err)
			return
		}
		c.handleDataReply(&reply)
	}
}

func (c *Client) handleDataReply(msg *DataReplyMessage) {
	now := nowNs()

	c.mu.Lock()
	e, ok := c.inflight[msg.SeqNum]
	if !ok {
		c.mu.Unlock()
		return // already committed (or abandoned by a resend)
	}
	bit := uint32(1) << msg.ReplicaIdx
	if e.replicaMask&bit != 0 {
		c.mu.Unlock()
		return // duplicate
	}
	e.replicaMask |= bit
	e.replyCount++
	replyRttUs := (now - e.sendTimeNs) / 1000

	// Commit requires f+1 replies INCLUDING the leader's (as in NOPaxos): the
	// leader's log is authoritative during gap agreement, so a quorum without
	// it could later be overwritten by a NoOp commit.
	leaderIdx := c.config.LeaderIndex(msg.ViewId)
	committed := e.replyCount >= c.config.QuorumSize() &&
		e.replicaMask&(uint32(1)<<leaderIdx) != 0

	var rttUs, totalUs int64
	var appReqId uint64
	var attempts uint32
	if committed {
		rttUs = (now - e.sendTimeNs) / 1000
		totalUs = (now - e.firstSendTimeNs) / 1000
		appReqId, attempts = e.appReqId, e.attempts
		c.committedCount++
		c.totalRttUs += uint64(rttUs)
		c.winCommitted++
		c.winRttSumUs += uint64(rttUs)
		delete(c.inflight, msg.SeqNum)
	}
	c.mu.Unlock()

	// Per-replica RTT measurement line (one per first reply from each
	// replica; run-gcp.sh's summary and analyze-logs.py both parse this).
	Notice("[%s] REPLY from replica=%d  rtt=%dus  seq=%d",
		c.self, msg.ReplicaIdx, replyRttUs, msg.SeqNum)
	if committed {
		Notice("[%s] COMMITTED seq=%d app_req=%d rtt=%dus total=%dus attempts=%d",
			c.self, msg.SeqNum, appReqId, rttUs, totalUs, attempts)
	}
}

// resendLoop scans for requests that missed their quorum deadline and
// resends the same logical request at a fresh seq (the old slot is presumed
// NoOp'd or its leader reply lost — either way no leader-inclusive quorum).
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
			oldSeq, newSeq, appReqId uint64
			firstSendNs              int64
			attempts                 uint32
			totalResends             uint64
		}
		var expired []resend

		c.mu.Lock()
		for seq, e := range c.inflight {
			if now-e.sendTimeNs < resendNs {
				continue
			}
			c.seqNum++ // the resend occupies a brand-new slot; appReqId is unchanged
			newSeq := c.seqNum
			c.resendCount++
			c.winResends++
			expired = append(expired, resend{
				oldSeq: seq, newSeq: newSeq, appReqId: e.appReqId,
				firstSendNs: e.firstSendTimeNs, attempts: e.attempts,
				totalResends: c.resendCount,
			})
			delete(c.inflight, seq)
		}
		c.mu.Unlock()

		for _, rs := range expired {
			Notice("[%s] NO-QUORUM seq=%d app_req=%d  resending as seq=%d  attempt=%d  total_resends=%d",
				c.self, rs.oldSeq, rs.appReqId, rs.newSeq, rs.attempts+1, rs.totalResends)
			c.sendData(rs.newSeq, rs.appReqId, rs.firstSendNs, rs.attempts+1)
		}
	}
}

// emitStats snapshots the 1-second window counters and prints the summary
// line. It is driven by the send grid (see sendLoop), not a wall-clock ticker,
// so each completed 1-second window reports an exact send count.
func (c *Client) emitStats() {
	c.mu.Lock()
	sent, committed, resends := c.winSent, c.winCommitted, c.winResends
	rttSum := c.winRttSumUs
	inflight := len(c.inflight)
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
	Notice("[%s] 1s: sent=%d committed=%d resends=%d inflight=%d rtt_avg=%dus  cumulative: committed=%d rtt_avg=%dus",
		c.self, sent, committed, resends, inflight, winAvgUs, cumCommitted, cumAvgUs)
}
