package paxosbus

// Control messages use separate connections so data backpressure cannot delay a pause.

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"
)

const clientControlPoll = 250 * time.Millisecond
const clientControlWriteTimeout = 2 * time.Second

type clientControlMessage struct {
	ClientId, ViewId uint64
	ReplicaIdx       uint32
	Normal           bool
}

func (m *clientControlMessage) Marshal(w io.Writer) {
	var b [21]byte
	for i, v := range [...]uint64{m.ClientId, m.ViewId} {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	binary.LittleEndian.PutUint32(b[16:], m.ReplicaIdx)
	if m.Normal {
		b[20] = 1
	}
	w.Write(b[:])
}

func (m *clientControlMessage) Unmarshal(r io.Reader) error {
	var b [21]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:])
	m.ViewId = binary.LittleEndian.Uint64(b[8:])
	m.ReplicaIdx = binary.LittleEndian.Uint32(b[16:])
	m.Normal = b[20] == 1
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

func (r *Replica) clientGapDueLocked(meta slotMetaEntry, now int64) bool {
	if s := r.clientControls[meta.clientId]; s != nil &&
		r.view() > 0 && now < s.holdUntil {
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
	if s.holdView < view || !r.scheduleActiveLocked() {
		s.holdView = view
		s.holdUntil = wallNs() + int64(r.viewChangeFallbackTimeout)
	}
	reply := *m
	reply.ViewId, reply.ReplicaIdx = view, uint32(r.idx)
	reply.Normal = r.status == statusNormal
	return reply
}

func (r *Replica) handleClientControl(code uint8, m *clientControlMessage, lw *lockedWriter) {
	r.mu.Lock()
	reply := r.clientControlReplyLocked(code, m, lw)
	r.mu.Unlock()
	sendClientControl(lw, MsgClientStatusReply, &reply)
}

func (c *Client) startClientControl() {
	for i := 0; i < c.config.N; i++ {
		go c.controlPeerLoop(i)
	}
}

func (c *Client) controlPeerLoop(idx int) {
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
				message := clientControlMessage{ClientId: c.clientId}
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				if sendClientControl(lw, MsgClientStatusQuery, &message) != nil {
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
			if code != MsgClientPause && code != MsgClientStatusReply {
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
	c.pauseMu.Lock()
	if int(m.ReplicaIdx) < len(c.controlStatus) {
		c.controlStatus[m.ReplicaIdx] = m
	}
	best := c.viewHint
	for _, candidateReport := range c.controlStatus {
		candidate := candidateReport.ViewId
		if candidate <= best {
			continue
		}
		normal := 0
		leaderNormal := false
		for i := range c.controlStatus {
			report := c.controlStatus[i]
			if report.Normal && report.ViewId == candidate {
				normal++
				leaderNormal = leaderNormal || i == c.config.LeaderIndex(candidate)
			}
		}
		if normal >= c.config.QuorumSize() && leaderNormal {
			best = candidate
		}
	}
	c.viewHint = best
	canPause := c.startupComplete
	c.pauseMu.Unlock()
	if canPause && (code == MsgClientPause || code == MsgClientStatusReply) {
		c.pauseForView(m.ViewId)
	}

}

func (c *Client) pauseForView(view uint64) {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if view <= c.activeView || view <= c.pauseView {
		return
	}
	c.paused, c.pauseView = true, view
}

func (c *Client) isPaused() bool {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	return c.paused
}
