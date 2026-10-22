package paxosbus

import (
	"bufio"
	"io"
	"net"
	"sort"
	"time"
)

func (r *Replica) sendSchedulePeer(peer int, code uint8, msg wireMsg) {
	if peer < 0 || peer >= len(r.schedulePeers) || peer == r.idx {
		return
	}
	r.schedulePeerMu[peer].Lock()
	defer r.schedulePeerMu[peer].Unlock()
	lw := r.schedulePeers[peer]
	if lw == nil {
		conn, err := net.DialTimeout("tcp", r.config.Replicas[peer], time.Second)
		if err != nil {
			return
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(true)
		}
		lw = &lockedWriter{w: bufio.NewWriter(conn), conn: conn}
		r.schedulePeers[peer] = lw
	}
	lw.conn.SetWriteDeadline(time.Now().Add(time.Second))
	if lw.sendMsg(code, msg) != nil {
		lw.conn.Close()
		r.schedulePeers[peer] = nil
	}
}

type scheduleEnvelope struct {
	code uint8
	msg  wireMsg
}

func (r *Replica) scheduleSender(peer int) {
	for m := range r.scheduleSendQueues[peer] {
		r.sendSchedulePeer(peer, m.code, m.msg)
	}
}
func (r *Replica) broadcastSchedule(code uint8, msg wireMsg) {
	for peer := range r.scheduleSendQueues {
		if peer != r.idx {
			select {
			case r.scheduleSendQueues[peer] <- scheduleEnvelope{code, msg}:
			default:
			}
		}
	}
}
func (cl *clientLine) validSequence(seq, end uint64) bool {
	if len(cl.windows) == 0 {
		return true
	}
	i := sort.Search(len(cl.windows), func(i int) bool { return cl.windows[i].EndTime > seq })
	if i < len(cl.windows) {
		w := cl.windows[i]
		return w.IntervalMs != 0 && seq >= w.FirstMsgNs && w.EndTime <= end && (seq-w.FirstMsgNs)%(w.IntervalMs*1e6) == 0
	}
	return false
}
func (cl *clientLine) nextSequence(seq uint64) uint64 {
	i := sort.Search(len(cl.windows), func(i int) bool { return cl.windows[i].EndTime > seq })
	for _, w := range cl.windows[i:] {
		if w.IntervalMs == 0 {
			continue
		}
		if seq <= w.FirstMsgNs {
			return w.FirstMsgNs
		}
		step := w.IntervalMs * 1e6
		n := w.FirstMsgNs + ((seq-w.FirstMsgNs-1)/step+1)*step
		if n < w.EndTime {
			return n
		}
	}
	return 0
}

type scheduleBatch struct {
	snapshot  BusScheduleInstall
	prepare   SchdPrepare
	committed bool
}

func (m *scheduleBatch) Marshal(w io.Writer) {
	m.snapshot.Marshal(w)
	w.Write([]byte{MsgSchdPrepare})
	m.prepare.Marshal(w)
	if m.committed {
		w.Write([]byte{MsgSchdCommit})
		c := SchdCommit{ReplicaId: m.prepare.ReplicaId, ViewId: m.prepare.ViewId, StartTime: m.prepare.StartTime, EndTime: m.prepare.EndTime}
		c.Marshal(w)
	}
}
