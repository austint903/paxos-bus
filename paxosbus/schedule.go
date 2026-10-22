package paxosbus

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"time"
)

const scheduleRetryInterval = 250 * time.Millisecond

func canonicalSchedule(announcements map[uint64]BusSyncMessage) []BusSyncMessage {
	out := make([]BusSyncMessage, 0, len(announcements))
	for _, m := range announcements {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientId < out[j].ClientId })
	return out
}
func scheduleDigest(schedule []BusSyncMessage) uint64 {
	h := fnv.New64a()
	var b [32]byte
	for _, m := range schedule {
		binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
		binary.LittleEndian.PutUint64(b[8:16], m.FirstMsgNs)
		binary.LittleEndian.PutUint64(b[16:24], m.IntervalMs)
		binary.LittleEndian.PutUint64(b[24:32], m.EndTime)
		h.Write(b[:])
	}
	return h.Sum64()
}
func (r *Replica) validateSchedule(schedule []BusSyncMessage) error {
	if len(schedule) > maxScheduleHistoryLen {
		return fmt.Errorf("schedule history exceeds limit")
	}
	for i, m := range schedule {
		if m.FirstMsgNs > math.MaxInt64 || m.EndTime > math.MaxInt64 || m.EndTime <= m.FirstMsgNs || m.IntervalMs > math.MaxInt64/1_000_000 {
			return fmt.Errorf("invalid schedule timing")
		}
		if i > 0 {
			p := schedule[i-1]
			if m.FirstMsgNs < p.FirstMsgNs || (m.FirstMsgNs == p.FirstMsgNs && (m.EndTime != p.EndTime || m.ClientId <= p.ClientId)) || (m.FirstMsgNs > p.FirstMsgNs && m.FirstMsgNs < p.EndTime) {
				return fmt.Errorf("overlapping or unordered schedule")
			}
		}
	}
	return nil
}
func schedulesEqual(a, b []BusSyncMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func schedulePrefix(a, b []BusSyncMessage) bool {
	return len(a) <= len(b) && schedulesEqual(a, b[:len(a)])
}
func (r *Replica) installScheduleLocked(schedule []BusSyncMessage, epoch uint64, ready bool) {
	if schedulesEqual(r.schedule, schedule) {
		r.scheduleEpoch = epoch
		r.scheduleReady = ready
		if ready {
			r.commitScheduleHistoryLocked()
		}
		return
	}
	appendOnly := schedulePrefix(r.schedule, schedule)
	lines := make(map[uint64]*clientLine)
	for _, m := range schedule {
		line := lines[m.ClientId]
		if line == nil {
			line = &clientLine{windowCounts: []uint64{0}, baseNs: int64(m.FirstMsgNs), intervalNs: int64(m.IntervalMs) * 1e6}
			if old := r.clients[m.ClientId]; old != nil {
				line.maxSeqSeen = old.maxSeqSeen
			}
			lines[m.ClientId] = line
		}
		line.windows = append(line.windows, m)
		count := countBefore(&clientLine{baseNs: int64(m.FirstMsgNs), intervalNs: int64(m.IntervalMs) * 1e6}, int64(m.EndTime), false)
		line.windowCounts = append(line.windowCounts, line.windowCounts[len(line.windowCounts)-1]+count)
	}
	r.clients = lines
	r.schedule = append([]BusSyncMessage(nil), schedule...)
	r.scheduleEpoch = epoch
	r.scheduleReady = ready
	if ready {
		r.commitScheduleHistoryLocked()
	}
	if !appendOnly {
		r.resetCursorLocked()
	}
}
func (r *Replica) commitScheduleHistoryLocked() {
	if len(r.schedule) > 0 {
		r.scheduleCommittedEnd = r.schedule[len(r.schedule)-1].EndTime
	}
}
func (r *Replica) scheduleActiveLocked() bool {
	return r.scheduleReady && r.scheduleCompleteForViewLocked(r.view())
}
func (r *Replica) scheduleCompleteForViewLocked(view uint64) bool {
	return r.scheduleEpoch == view && len(r.schedule) > 0
}
func (r *Replica) handleHsRequest(m *HsRequest, lw *lockedWriter) {
	w := uint64(r.config.ScheduleWindow())
	now := uint64(wallNs())
	if m.StartTime > math.MaxInt64-w || m.StartTime%w != 0 || m.IntervalMs > math.MaxInt64/1_000_000 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.idx != r.leaderIdx() || r.status != statusNormal {
		return
	}
	for _, s := range r.schedule {
		if s.FirstMsgNs == m.StartTime && s.ClientId == m.ClientId && s.EndTime <= r.scheduleCommittedEnd {
			reply := HsReply{ReplicaId: uint32(r.idx), ViewId: r.view(), IntervalMs: s.IntervalMs, StartTime: s.FirstMsgNs, EndTime: s.EndTime}
			go lw.sendMsg(MsgHsReply, &reply)
			return
		}
	}
	if m.StartTime <= now+w || m.StartTime > now+3*w || m.StartTime < r.scheduleCommittedEnd {
		return
	}
	if r.scheduleRequests == nil {
		r.scheduleRequests = make(map[uint64]map[uint64]HsRequest)
		r.scheduleWindowWaiters = make(map[uint64]map[uint64]*lockedWriter)
	}
	if r.scheduleRequests[m.StartTime] == nil {
		r.scheduleRequests[m.StartTime] = make(map[uint64]HsRequest)
		r.scheduleWindowWaiters[m.StartTime] = make(map[uint64]*lockedWriter)
	}
	requests := r.scheduleRequests[m.StartTime]
	if _, ok := requests[m.ClientId]; !ok && len(requests) >= r.config.ExpectedClients {
		return
	}
	requests[m.ClientId] = *m
	r.scheduleWindowWaiters[m.StartTime][m.ClientId] = lw
}
func (r *Replica) scheduleLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		if r.status != statusNormal || r.idx != r.leaderIdx() {
			r.mu.Unlock()
			continue
		}
		r.expireScheduleRequestsLocked(uint64(wallNs()))
		if !r.scheduleInstallActive {
			var start uint64
			now := uint64(wallNs())
			w := uint64(r.config.ScheduleWindow())
			for t := range r.scheduleRequests {
				if t <= now+w && (start == 0 || t < start) {
					start = t
				}
			}
			if start != 0 {
				requests := r.scheduleRequests[start]
				delete(r.scheduleRequests, start)
				if start >= r.scheduleCommittedEnd {
					list := make([]HsRequest, 0, len(requests))
					for _, q := range requests {
						list = append(list, q)
					}
					sort.Slice(list, func(i, j int) bool { return list[i].ClientId < list[j].ClientId })
					msg := SchdPrepare{ReplicaId: uint32(r.idx), ViewId: r.view(), HsReqList: list, StartTime: start, EndTime: start + w}
					if r.acceptSchedulePrepareLocked(&msg) {
						r.scheduleInstallActive = true
						r.scheduleProposal = &msg
						r.schedulePrepared = map[uint32]bool{uint32(r.idx): true}
						r.scheduleLastSend = time.Time{}
					}
				}
			}
		}
		if r.scheduleProposal == nil {
			history := r.committedScheduleLocked()
			view := r.view()
			send := len(history) > 0 && time.Since(r.scheduleLastSend) >= scheduleRetryInterval
			if send {
				r.scheduleLastSend = time.Now()
			}
			r.mu.Unlock()
			if send {
				r.broadcastSchedule(MsgBusScheduleInstall, &BusScheduleInstall{Epoch: view, Digest: scheduleDigest(history), SenderIdx: uint32(r.idx), Schedule: history})
			}
			continue
		}
		msg := *r.scheduleProposal
		committed := len(r.schedulePrepared) >= r.config.QuorumSize()
		if committed {
			r.scheduleCommittedEnd = msg.EndTime
			r.scheduleReady = true
			r.scheduleInstallActive = false
			r.scheduleProposal = nil
		}
		send := committed || time.Since(r.scheduleLastSend) >= scheduleRetryInterval
		if send {
			r.scheduleLastSend = time.Now()
		}
		history := r.committedScheduleLocked()
		waiters := r.scheduleWindowWaiters[msg.StartTime]
		if committed {
			delete(r.scheduleWindowWaiters, msg.StartTime)
		}
		r.mu.Unlock()
		if !send {
			continue
		}
		snapshot := BusScheduleInstall{Epoch: msg.ViewId, Digest: scheduleDigest(history), SenderIdx: uint32(r.idx), Schedule: history}
		r.broadcastSchedule(MsgBusScheduleInstall, &scheduleBatch{snapshot: snapshot, prepare: msg, committed: committed})
		if committed {
			for _, q := range msg.HsReqList {
				if lw := waiters[q.ClientId]; lw != nil {
					reply := HsReply{ReplicaId: uint32(r.idx), ViewId: msg.ViewId, IntervalMs: q.IntervalMs, StartTime: msg.StartTime, EndTime: msg.EndTime}
					go lw.sendMsg(MsgHsReply, &reply)
				}
			}
		}
	}
}
func (r *Replica) committedScheduleLocked() []BusSyncMessage {
	var out []BusSyncMessage
	for _, m := range r.schedule {
		if m.EndTime <= r.scheduleCommittedEnd {
			out = append(out, m)
		}
	}
	return out
}
func (r *Replica) acceptSchedulePrepareLocked(m *SchdPrepare) bool {
	if m.ViewId != r.view() || r.status != statusNormal || int(m.ReplicaId) != r.leaderIdx() || m.EndTime-m.StartTime != uint64(r.config.ScheduleWindow()) || m.EndTime <= m.StartTime || len(m.HsReqList) == 0 || len(m.HsReqList) > r.config.ExpectedClients {
		return false
	}
	entries := make([]BusSyncMessage, len(m.HsReqList))
	for i, q := range m.HsReqList {
		if q.StartTime != m.StartTime {
			return false
		}
		entries[i] = BusSyncMessage{ClientId: q.ClientId, FirstMsgNs: q.StartTime, IntervalMs: q.IntervalMs, EndTime: m.EndTime}
	}
	if r.validateSchedule(entries) != nil {
		return false
	}
	for i, s := range r.schedule {
		if s.FirstMsgNs == m.StartTime {
			end := i
			for end < len(r.schedule) && r.schedule[end].FirstMsgNs == m.StartTime {
				end++
			}
			return schedulesEqual(r.schedule[i:end], entries)
		}
	}
	if len(r.schedule) > 0 && r.schedule[len(r.schedule)-1].EndTime > m.StartTime {
		return false
	}
	if len(r.schedule)+len(entries) > maxScheduleHistoryLen {
		return false
	}
	r.installScheduleLocked(append(append([]BusSyncMessage(nil), r.schedule...), entries...), m.ViewId, false)
	r.scheduleReady = r.scheduleCommittedEnd > 0
	return true
}
func (r *Replica) handleSchdPrepare(m *SchdPrepare) {
	r.mu.Lock()
	ok := r.acceptSchedulePrepareLocked(m)
	r.mu.Unlock()
	if ok {
		r.sendSchedulePeer(int(m.ReplicaId), MsgSchdPrepareReply, &SchdPrepareReply{ReplicaId: uint32(r.idx), ViewId: m.ViewId, StartTime: m.StartTime, EndTime: m.EndTime})
	}
}
func (r *Replica) handleSchdPrepareReply(m *SchdPrepareReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.scheduleProposal
	if p != nil && r.status == statusNormal && r.idx == r.leaderIdx() && m.ViewId == p.ViewId && m.StartTime == p.StartTime && m.EndTime == p.EndTime && int(m.ReplicaId) < r.config.N {
		r.schedulePrepared[m.ReplicaId] = true
	}
}
func (r *Replica) handleSchdCommit(m *SchdCommit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.ViewId != r.view() || r.status != statusNormal || int(m.ReplicaId) != r.leaderIdx() {
		return
	}
	for _, s := range r.schedule {
		if s.FirstMsgNs == m.StartTime && s.EndTime == m.EndTime {
			if m.EndTime > r.scheduleCommittedEnd {
				r.scheduleCommittedEnd = m.EndTime
			}
			r.scheduleReady = true
			return
		}
	}
}
func (r *Replica) handleScheduleInstall(m *BusScheduleInstall) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.Epoch != r.view() || r.status != statusNormal || int(m.SenderIdx) != r.leaderIdx() || m.Digest != scheduleDigest(m.Schedule) || r.validateSchedule(m.Schedule) != nil {
		return false
	}
	if len(m.Schedule) == 0 {
		return len(r.schedule) == 0 || r.scheduleCommittedEnd == 0
	}
	if schedulePrefix(r.schedule, m.Schedule) {
		r.installScheduleLocked(m.Schedule, m.Epoch, true)
		return true
	}
	if schedulePrefix(m.Schedule, r.schedule) {
		end := m.Schedule[len(m.Schedule)-1].EndTime
		if end > r.scheduleCommittedEnd {
			r.scheduleCommittedEnd = end
		}
		r.scheduleReady = true
		return true
	}
	return false
}

func (r *Replica) expireScheduleRequestsLocked(now uint64) {
	w := uint64(r.config.ScheduleWindow())
	for start := range r.scheduleRequests {
		if start <= now && w <= now-start {
			delete(r.scheduleRequests, start)
			delete(r.scheduleWindowWaiters, start)
		}
	}
}
