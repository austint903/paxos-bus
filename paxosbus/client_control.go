package paxosbus

// Client flow control travels on separate TCP connections: data/reply queues
// must not delay the message that asks the producer to stop growing them.
// Resume changes arrival predictions, never the immutable ordering lines.

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
	"net"
	"time"
)

const clientControlPoll = 250 * time.Millisecond
const clientControlWriteTimeout = 2 * time.Second

type clientControlMessage struct {
	ClientId, ViewId, Token, NextSeq, ArrivalNs uint64
	ReplicaIdx                                  uint32
	Normal                                      bool
}

func (m *clientControlMessage) Marshal(w io.Writer) {
	var b [45]byte
	for i, v := range [...]uint64{m.ClientId, m.ViewId, m.Token, m.NextSeq, m.ArrivalNs} {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	binary.LittleEndian.PutUint32(b[40:], m.ReplicaIdx)
	if m.Normal {
		b[44] = 1
	}
	w.Write(b[:])
}

func (m *clientControlMessage) Unmarshal(r io.Reader) error {
	var b [45]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:])
	m.ViewId = binary.LittleEndian.Uint64(b[8:])
	m.Token = binary.LittleEndian.Uint64(b[16:])
	m.NextSeq = binary.LittleEndian.Uint64(b[24:])
	m.ArrivalNs = binary.LittleEndian.Uint64(b[32:])
	m.ReplicaIdx = binary.LittleEndian.Uint32(b[40:])
	m.Normal = b[44] == 1
	return nil
}

func sendClientControl(lw *lockedWriter, code uint8, m *clientControlMessage) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.conn != nil {
		lw.conn.SetWriteDeadline(time.Now().Add(clientControlWriteTimeout))
		defer lw.conn.SetWriteDeadline(time.Time{})
	}
	if err := lw.w.WriteByte(code); err != nil {
		return err
	}
	m.Marshal(lw.w)
	err := lw.w.Flush()
	if err != nil && lw.conn != nil {
		lw.conn.Close()
	}
	return err
}

type clientControlState struct {
	writer    *lockedWriter
	resume    clientControlMessage
	holdView  uint64
	holdUntil int64
}

func (r *Replica) pauseClients(view uint64) {
	r.mu.Lock()
	writers := make(map[uint64]*lockedWriter, len(r.clientControls))
	for id, s := range r.clientControls {
		s.holdView = view
		s.holdUntil = wallNs() + int64(r.viewChangeFallbackTimeout)
		writers[id] = s.writer
	}
	r.mu.Unlock()
	for id, w := range writers {
		msg := clientControlMessage{ClientId: id, ViewId: view, ReplicaIdx: uint32(r.idx)}
		go sendClientControl(w, MsgClientPause, &msg)
	}
}

// A silent/failed client cannot suppress gap detection indefinitely. Before
// its resume fence arrives, allow one recovery interval for the handshake.
func (r *Replica) clientGapDueLocked(meta slotMetaEntry, now int64) bool {
	if s := r.clientControls[meta.clientId]; s != nil &&
		s.resume.ViewId < r.view() && now < s.holdUntil {
		return false
	}
	expected := meta.expectedNs
	if line := r.clients[meta.clientId]; line != nil {
		expected = line.arrivalNs(meta.reqId)
	}
	return now > expected+r.gapDeltaNs
}

func (r *Replica) clientControlReplyLocked(code uint8, m *clientControlMessage, lw *lockedWriter) clientControlMessage {
	if r.clientControls == nil {
		r.clientControls = make(map[uint64]*clientControlState)
	}
	s := r.clientControls[m.ClientId]
	if s == nil {
		s = &clientControlState{}
		r.clientControls[m.ClientId] = s
	}
	s.writer = lw
	view := r.view()
	if s.holdView < view {
		s.holdView = view
		s.holdUntil = wallNs() + int64(r.viewChangeFallbackTimeout)
	}
	reply := *m
	reply.ViewId, reply.ReplicaIdx = view, uint32(r.idx)
	reply.Normal = r.status == statusNormal
	if code != MsgClientResume {
		return reply
	}
	reply.Normal = false
	line := r.clients[m.ClientId]
	if r.status != statusNormal || m.ViewId != view || line == nil ||
		m.Token == 0 || m.NextSeq == 0 || m.ArrivalNs > math.MaxInt64 ||
		line.intervalNs <= 0 || line.baseNs < 0 ||
		m.NextSeq-1 > uint64((math.MaxInt64-line.baseNs)/line.intervalNs) {
		return reply
	}
	// An ACK also fences the old data stream: every bus already issued by this
	// client must have reached this replica before that client starts a new one.
	if m.NextSeq-1 > line.maxSeqSeen {
		return reply
	}
	if s.resume.ViewId == view && s.resume.Token != 0 {
		if m.Token < s.resume.Token || m.NextSeq < s.resume.NextSeq {
			return reply
		}
		if m.Token == s.resume.Token && (m.NextSeq != s.resume.NextSeq || m.ArrivalNs != s.resume.ArrivalNs) {
			return reply
		}
	}
	line.resumeSeq = m.NextSeq
	line.arrivalOffsetNs = int64(m.ArrivalNs) - line.expectedNs(m.NextSeq)
	s.resume = *m
	reply.Normal = true
	return reply
}

func (r *Replica) handleClientControl(code uint8, m *clientControlMessage, lw *lockedWriter) {
	r.mu.Lock()
	reply := r.clientControlReplyLocked(code, m, lw)
	r.mu.Unlock()
	response := MsgClientStatusReply
	if code == MsgClientResume {
		response = MsgClientResumeAck
	}
	sendClientControl(lw, response, &reply)
}

type clientControlReply struct {
	code uint8
	msg  clientControlMessage
}

type clientControlPeer struct{ out chan clientControlReply }

// ConfigureClientPause is called before Connect/Run. Pause is enabled by
// default; the switch permits paired benchmark runs with identical binaries.
func (c *Client) ConfigureClientPause(enabled bool, resumeLead time.Duration) {
	c.pauseEnabled = enabled
	if resumeLead > 0 {
		c.resumeWait = resumeLead
	}
}

func (c *Client) startClientControl() {
	c.controlPeers = make([]*clientControlPeer, c.config.N)
	for i := range c.controlPeers {
		c.controlPeers[i] = &clientControlPeer{out: make(chan clientControlReply, 1)}
	}
	for i := range c.controlPeers {
		go c.controlPeerLoop(i)
	}
}

func (c *Client) controlPeerLoop(idx int) {
	p := c.controlPeers[idx]
	for {
		conn, err := net.DialTimeout("tcp", c.config.Replicas[idx], time.Second)
		if err != nil {
			time.Sleep(clientControlPoll)
			continue
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(true)
		}
		lw := &lockedWriter{w: bufio.NewWriter(conn), conn: conn}
		stop, sent := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(sent)
			ticker := time.NewTicker(clientControlPoll)
			defer ticker.Stop()
			for {
				frame := clientControlReply{code: MsgClientStatusQuery, msg: clientControlMessage{ClientId: c.clientId}}
				select {
				case <-stop:
					return
				case frame = <-p.out:
				case <-ticker.C:
				}
				if sendClientControl(lw, frame.code, &frame.msg) != nil {
					return
				}
			}
		}()
		reader := bufio.NewReader(conn)
		for {
			code, err := reader.ReadByte()
			if err != nil {
				break
			}
			if code != MsgClientPause && code != MsgClientStatusReply && code != MsgClientResumeAck {
				break
			}
			var m clientControlMessage
			if err := m.Unmarshal(reader); err != nil {
				break
			}
			if m.ClientId != c.clientId || int(m.ReplicaIdx) != idx {
				continue
			}
			c.handleControlReply(code, m)
		}
		conn.Close()
		close(stop)
		<-sent
		time.Sleep(clientControlPoll)
	}
}

func (c *Client) handleControlReply(code uint8, m clientControlMessage) {
	// Polls also deliver a missed pause (including after control reconnection).
	if code == MsgClientPause || code == MsgClientStatusReply {
		c.pauseForView(m.ViewId)
	}
	if code != MsgClientPause && m.Token != 0 {
		select {
		case c.controlReplies <- clientControlReply{code, m}:
		default:
		}
	}
}

func (c *Client) pauseForView(view uint64) {
	c.pauseMu.Lock()
	if !c.pauseEnabled || view <= c.activeView || (c.paused && view <= c.pauseView) {
		c.pauseMu.Unlock()
		return
	}
	start := !c.paused
	c.paused, c.pauseView = true, view
	close(c.pauseChanged)
	c.pauseChanged = make(chan struct{})
	c.pauseMu.Unlock()
	Notice("[%s] CLIENT-PAUSE view=%d", c.self, view)
	if start {
		go c.clientRecoveryLoop()
	}
}

func (c *Client) isPaused() bool {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	return c.paused
}

func (c *Client) waitWhilePaused() bool {
	waited := false
	for {
		c.pauseMu.Lock()
		paused, changed := c.paused, c.pauseChanged
		c.pauseMu.Unlock()
		if !paused {
			return waited
		}
		waited = true
		<-changed
	}
}

func (c *Client) waitUnlessPause(delay time.Duration) {
	c.pauseMu.Lock()
	changed := c.pauseChanged
	c.pauseMu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-changed:
	}
}

func (c *Client) broadcastControl(code uint8, m clientControlMessage) {
	for _, p := range c.controlPeers {
		// At most one unsent control request per peer. Retries supersede older
		// tokens, so a dead replica cannot build a queue or hold up the quorum.
		select {
		case <-p.out:
		default:
		}
		select {
		case p.out <- clientControlReply{code, m}:
		default:
		}
	}
}

func (c *Client) controlQuorum(code uint8, m clientControlMessage, duration time.Duration) bool {
	c.pauseMu.Lock()
	changed, current := c.pauseChanged, c.pauseView
	c.pauseMu.Unlock()
	if current != m.ViewId {
		return false
	}
	c.broadcastControl(code, m)
	want := MsgClientStatusReply
	if code == MsgClientResume {
		want = MsgClientResumeAck
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	var mask uint32
	for {
		select {
		case <-changed:
			return false
		case <-timer.C:
			return false
		case reply := <-c.controlReplies:
			r := reply.msg
			if reply.code != want || !r.Normal || r.Token != m.Token || r.ViewId != m.ViewId ||
				r.ClientId != c.clientId || int(r.ReplicaIdx) >= c.config.N {
				continue
			}
			if code == MsgClientResume && (r.NextSeq != m.NextSeq || r.ArrivalNs != m.ArrivalNs) {
				continue
			}
			mask |= uint32(1) << r.ReplicaIdx
			if c.quorumReached(mask, m.ViewId) {
				return true
			}
		}
	}
}

func (c *Client) clientRecoveryLoop() {
	for {
		c.pauseMu.Lock()
		view := c.pauseView
		c.pauseMu.Unlock()
		c.controlSeq++
		m := clientControlMessage{ClientId: c.clientId, ViewId: view, Token: c.controlSeq}
		if !c.controlQuorum(MsgClientStatusQuery, m, 2*clientControlPoll) {
			continue
		}
		c.mu.Lock()
		m.NextSeq = c.busSeqNum + 1
		c.mu.Unlock()
		lead := c.resumeWait
		if minimum := time.Duration(2*c.maxOwdNs) + clientControlPoll; lead < minimum {
			lead = minimum
		}
		m.ArrivalNs = uint64(wallNs() + int64(lead) + c.maxOwdNs)
		c.controlSeq++
		m.Token = c.controlSeq
		if !c.controlQuorum(MsgClientResume, m, lead) {
			continue
		}
		depart := int64(m.ArrivalNs) - c.maxOwdNs
		if delay := depart - wallNs(); delay > 0 {
			c.waitUnlessPause(time.Duration(delay))
		} else {
			continue
		}
		c.pauseMu.Lock()
		if c.pauseView != view {
			c.pauseMu.Unlock()
			continue
		}
		c.activeView, c.resumeSendNs = view, depart
		c.paused = false
		close(c.pauseChanged)
		c.pauseChanged = make(chan struct{})
		c.pauseMu.Unlock()
		Notice("[%s] CLIENT-RESUME view=%d next_bus=%d schedule_acked=true", c.self, view, m.NextSeq)
		return
	}
}
