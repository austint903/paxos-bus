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
	var b [24]byte
	for i := range schedule {
		binary.LittleEndian.PutUint64(b[0:8], schedule[i].ClientId)
		binary.LittleEndian.PutUint64(b[8:16], schedule[i].FirstMsgNs)
		binary.LittleEndian.PutUint64(b[16:24], schedule[i].IntervalMs)
		h.Write(b[:])
	}
	return h.Sum64()
}

func (r *Replica) validateSchedule(schedule []BusSyncMessage) error {
	if len(schedule) != r.config.ExpectedClients {
		return fmt.Errorf("got %d clients, want %d", len(schedule), r.config.ExpectedClients)
	}
	for i := range schedule {
		m := schedule[i]
		if m.FirstMsgNs > math.MaxInt64 || m.IntervalMs == 0 || m.IntervalMs > math.MaxInt64/1_000_000 {
			return fmt.Errorf("invalid timing for client %d", m.ClientId)
		}
		if i > 0 && schedule[i-1].ClientId >= m.ClientId {
			return fmt.Errorf("client IDs are not unique and ordered")
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

// installScheduleLocked atomically replaces the slot-mapping inputs. Runtime
// counters are retained when a view change reinstalls an unchanged client line.
func (r *Replica) installScheduleLocked(schedule []BusSyncMessage, epoch uint64, ready bool) {
	if schedulesEqual(r.schedule, schedule) {
		r.scheduleEpoch = epoch
		r.scheduleReady = ready
		return
	}
	lines := make(map[uint64]*clientLine, len(schedule))
	for i := range schedule {
		m := schedule[i]
		line := &clientLine{baseNs: int64(m.FirstMsgNs), intervalNs: int64(m.IntervalMs) * 1e6}
		if old := r.clients[m.ClientId]; old != nil && old.baseNs == line.baseNs && old.intervalNs == line.intervalNs {
			line.maxSeqSeen = old.maxSeqSeen
			line.resumeSeq = old.resumeSeq
			line.arrivalOffsetNs = old.arrivalOffsetNs
		}
		lines[m.ClientId] = line
	}
	r.clients = lines
	r.schedule = append(r.schedule[:0], schedule...)
	r.scheduleEpoch = epoch
	r.scheduleReady = ready
	r.resetCursorLocked()
}

func (r *Replica) scheduleActiveLocked() bool {
	return r.scheduleReady && r.scheduleCompleteForViewLocked(r.view())
}

func (r *Replica) scheduleCompleteForViewLocked(view uint64) bool {
	return r.scheduleEpoch == view && len(r.schedule) == r.config.ExpectedClients
}

func (r *Replica) handleSync(msg *BusSyncMessage, lw *lockedWriter) {
	if msg.IntervalMs == 0 || msg.FirstMsgNs > math.MaxInt64 || msg.IntervalMs > math.MaxInt64/1_000_000 {
		Warning("[%s] rejected invalid schedule announcement from client %d", r.self, msg.ClientId)
		return
	}
	r.mu.Lock()
	view := r.view()
	if r.idx != r.config.LeaderIndex(view) || r.status != statusNormal {
		r.mu.Unlock()
		Warning("[%s] rejected client %d schedule: not the normal-view leader", r.self, msg.ClientId)
		return
	}
	if r.scheduleReady {
		line := r.clients[msg.ClientId]
		if line != nil && line.intervalNs == int64(msg.IntervalMs)*1e6 {
			ready := BusScheduleReady{ClientId: msg.ClientId, Epoch: r.scheduleEpoch, FirstMsgNs: uint64(line.baseNs)}
			r.mu.Unlock()
			sendScheduleReady(lw, &ready)
			return
		}
		r.mu.Unlock()
		Warning("[%s] rejected unknown or incompatible client %d after schedule commit", r.self, msg.ClientId)
		return
	}
	if r.scheduleInstallActive {
		line := r.clients[msg.ClientId]
		if line != nil && line.baseNs == int64(msg.FirstMsgNs) && line.intervalNs == int64(msg.IntervalMs)*1e6 {
			r.scheduleWaiters[msg.ClientId] = lw
		}
		r.mu.Unlock()
		return
	}
	_, alreadyAnnounced := r.scheduleAnnouncements[msg.ClientId]
	r.scheduleAnnouncements[msg.ClientId] = *msg
	r.scheduleWaiters[msg.ClientId] = lw
	announcementCount := len(r.scheduleAnnouncements)
	complete := announcementCount == r.config.ExpectedClients && !r.scheduleInstallActive
	if complete {
		r.scheduleInstallActive = true
		schedule := canonicalSchedule(r.scheduleAnnouncements)
		if err := r.validateSchedule(schedule); err != nil {
			r.scheduleInstallActive = false
			r.mu.Unlock()
			Warning("[%s] cannot install client schedule: %v", r.self, err)
			return
		}
		r.installScheduleLocked(schedule, view, false)
		r.mu.Unlock()
		go r.coordinateInitialSchedule(view, schedule)
		return
	}
	r.mu.Unlock()
	if !alreadyAnnounced {
		Notice("[%s] schedule announcement from client %d (%d/%d)", r.self, msg.ClientId,
			announcementCount, r.config.ExpectedClients)
	}
}

func sendScheduleReady(lw *lockedWriter, msg *BusScheduleReady) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if err := lw.w.WriteByte(MsgBusScheduleReady); err != nil {
		return err
	}
	msg.Marshal(lw.w)
	return lw.w.Flush()
}

func (r *Replica) coordinateInitialSchedule(view uint64, schedule []BusSyncMessage) {
	digest := scheduleDigest(schedule)
	acked := make(map[uint32]bool, r.config.N)
	acked[uint32(r.idx)] = true
	installActive := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.view() == view && r.status == statusNormal && r.scheduleInstallActive
	}
	if !r.replicateSchedule(view, digest, schedule, acked, r.config.QuorumSize(), installActive) {
		r.mu.Lock()
		r.scheduleInstallActive = false
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	if r.view() != view || r.status != statusNormal || !schedulesEqual(r.schedule, schedule) {
		r.scheduleInstallActive = false
		r.mu.Unlock()
		return
	}
	r.scheduleReady = true
	r.scheduleInstallActive = false
	waiters := make(map[uint64]*lockedWriter, len(r.scheduleWaiters))
	for id, lw := range r.scheduleWaiters {
		waiters[id] = lw
	}
	r.mu.Unlock()
	go r.finishScheduleDissemination(view, digest, schedule, acked)

	firstByID := make(map[uint64]uint64, len(schedule))
	for i := range schedule {
		firstByID[schedule[i].ClientId] = schedule[i].FirstMsgNs
	}
	for id, lw := range waiters {
		firstMsgNs, ok := firstByID[id]
		if !ok {
			continue
		}
		ready := BusScheduleReady{ClientId: id, Epoch: view, FirstMsgNs: firstMsgNs}
		if err := sendScheduleReady(lw, &ready); err != nil {
			Warning("[%s] failed to release client %d after schedule commit: %v", r.self, id, err)
		}
	}
	Notice("[%s] client schedule committed: epoch=%d clients=%d", r.self, view, len(schedule))
}

func (r *Replica) replicateSchedule(view, digest uint64, schedule []BusSyncMessage,
	acked map[uint32]bool, target int, active func() bool) bool {
	ticker := time.NewTicker(scheduleRetryInterval)
	defer ticker.Stop()
	send := func() {
		msg := &BusScheduleInstall{Epoch: view, Digest: digest, SenderIdx: uint32(r.idx), Schedule: schedule}
		for peer := range r.config.Replicas {
			if peer != r.idx && !acked[uint32(peer)] {
				r.sendToPeer(peer, MsgBusScheduleInstall, msg)
			}
		}
	}
	send()
	for len(acked) < target {
		select {
		case ack := <-r.scheduleAckCh:
			if ack.Epoch == view && ack.Digest == digest && int(ack.SenderIdx) < r.config.N {
				acked[ack.SenderIdx] = true
			}
		case <-ticker.C:
			if !active() {
				return false
			}
			send()
		}
	}
	return true
}

// A quorum is enough to release clients safely: replicas without the exact
// schedule remain fenced. Continue dissemination so recovered peers can rejoin
// without putting startup availability behind the slowest replica.
func (r *Replica) finishScheduleDissemination(view, digest uint64, schedule []BusSyncMessage,
	acked map[uint32]bool) {
	active := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.view() == view && r.status == statusNormal && r.scheduleActiveLocked()
	}
	r.replicateSchedule(view, digest, schedule, acked, r.config.N, active)
}

func (r *Replica) handleScheduleInstall(msg *BusScheduleInstall) {
	r.mu.Lock()
	if msg.Epoch != r.view() || r.status != statusNormal || int(msg.SenderIdx) != r.config.LeaderIndex(msg.Epoch) ||
		msg.Digest != scheduleDigest(msg.Schedule) {
		r.mu.Unlock()
		return
	}
	if err := r.validateSchedule(msg.Schedule); err != nil {
		r.mu.Unlock()
		Warning("[%s] rejected invalid schedule from leader: %v", r.self, err)
		return
	}
	if len(r.schedule) != 0 && !schedulesEqual(r.schedule, msg.Schedule) {
		r.mu.Unlock()
		Warning("[%s] rejected conflicting schedule for epoch %d", r.self, msg.Epoch)
		return
	}
	r.installScheduleLocked(msg.Schedule, msg.Epoch, true)
	r.mu.Unlock()
	r.sendToPeer(int(msg.SenderIdx), MsgBusScheduleAck, &BusScheduleAck{
		Epoch: msg.Epoch, Digest: msg.Digest, SenderIdx: uint32(r.idx),
	})
}

func (r *Replica) handleScheduleAck(msg *BusScheduleAck) {
	if int(msg.SenderIdx) >= r.config.N {
		return
	}
	select {
	case r.scheduleAckCh <- *msg:
	default:
	}
}
