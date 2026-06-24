package paxosbus

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"time"
)

const syncStartDelayMs = 5000

type inflightEntry struct {
	sendTimeNs      int64
	firstSendTimeNs int64
	replicaMask     uint32
	replyCount      int
	origReqId       uint64
	attempts        uint32
	committed       bool
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

type Client struct {
	config     *Config
	clientId   uint64
	intervalMs uint64
	resendMs   uint64
	self       string

	conns   []net.Conn
	readers []*bufio.Reader
	writers []*lockedWriter

	mu        sync.Mutex
	inflight  map[uint64]*inflightEntry
	requestId uint64

	committedCount uint64
	totalRttUs     uint64
	resendCount    uint64

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

func (c *Client) Run() {
	syncMsg := BusSyncMessage{
		ClientId:     c.clientId,
		SendTimeNs:   uint64(wallNs()),
		IntervalMs:   c.intervalMs,
		StartDelayMs: syncStartDelayMs,
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
	Notice("[%s] sync sent, waiting 5s before data phase", c.self)

	for i := range c.readers {
		go c.receiveLoop(i)
	}

	time.Sleep(syncStartDelayMs * time.Millisecond)
	Notice("[%s] sync wait done, starting open-loop data phase (interval=%dms)",
		c.self, c.intervalMs)

	if c.resendMs > 0 {
		go c.resendLoop()
	}
	go c.janitorLoop()
	c.sendLoop()
}

func (c *Client) sendLoop() {
	intervalNs := int64(c.intervalMs) * 1e6
	t0 := nowNs()
	nextSendNs := t0
	curEpoch := int64(0)
	for {
		now := nowNs()
		for now >= nextSendNs {
			c.mu.Lock()
			c.requestId++
			reqId := c.requestId
			c.mu.Unlock()
			c.sendRequest(reqId, reqId, 0, 1)
			nextSendNs += intervalNs

			if epoch := (nextSendNs - t0) / int64(time.Second); epoch != curEpoch {
				c.emitStats()
				curEpoch = epoch
			}
			now = nowNs()
		}
		time.Sleep(time.Duration(nextSendNs - now))
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
	var reply BusReplyMessage
	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Warning("[%s] connection to replica %d lost: %v", c.self, rid, err)
			return
		}
		if msgType != MsgBusReply {
			Warning("[%s] unknown message type %d from replica %d",
				c.self, msgType, rid)
			return
		}
		if err := reply.Unmarshal(reader); err != nil {
			Warning("[%s] bad reply from replica %d: %v", c.self, rid, err)
			return
		}
		c.handleBusReply(&reply)
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

	Notice("[%s] REPLY from replica=%d  rtt=%dus  req=%d  slot=%d  post_quorum=%d",
		c.self, msg.ReplicaIdx, replyRttUs, msg.RequestId, msg.LogSlotNum, postQuorum)
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
		c.mu.Unlock()
	}
}

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
