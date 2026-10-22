package paxosbus

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"
)

func scheduleReplica() *Replica {
	return NewReplica(&Config{N: 3, F: 1, ExpectedClients: 2, ScheduleWindowMs: 10, Replicas: []string{"a", "b", "c"}}, 0, "", "", dropNone, 0, 1, RecoveryOptions{RetainSlots: 16})
}
func TestWindowSlotMapping(t *testing.T) {
	r := scheduleReplica()
	history := []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10e6, IntervalMs: 2, EndTime: 20e6}, {ClientId: 1, FirstMsgNs: 10e6, IntervalMs: 5, EndTime: 20e6}, {ClientId: 0, FirstMsgNs: 20e6, IntervalMs: 0, EndTime: 30e6}, {ClientId: 1, FirstMsgNs: 20e6, IntervalMs: 2, EndTime: 30e6}, {ClientId: 0, FirstMsgNs: 30e6, IntervalMs: 5, EndTime: 40e6}, {ClientId: 1, FirstMsgNs: 30e6, IntervalMs: 5, EndTime: 40e6}}
	r.installScheduleLocked(history, 0, true)
	expected := []struct{ id, seq uint64 }{{0, 10e6}, {1, 10e6}, {0, 12e6}, {0, 14e6}, {1, 15e6}, {0, 16e6}, {0, 18e6}, {1, 20e6}, {1, 22e6}, {1, 24e6}, {1, 26e6}, {1, 28e6}, {0, 30e6}, {1, 30e6}, {0, 35e6}, {1, 35e6}}
	for slot, want := range expected {
		got := computeGlobalSlot(r.clients, want.id, want.seq)
		if got != uint64(slot) {
			t.Fatalf("slot %d got %d", slot, got)
		}
		owner := r.slotOwnerLocked(uint64(slot))
		if owner.clientId != want.id || owner.reqId != want.seq {
			t.Fatalf("owner %d: %+v", slot, owner)
		}
	}
	for _, bad := range []struct{ id, seq uint64 }{{0, 0}, {0, 11e6}, {0, 20e6}, {0, 40e6}, {1, 29e6}} {
		if r.clients[bad.id].validSequence(bad.seq, r.scheduleCommittedEnd) {
			t.Fatalf("accepted invalid bus %+v", bad)
		}
	}
}
func TestSchedulePrepareCommitFence(t *testing.T) {
	r := scheduleReplica()
	p := SchdPrepare{ReplicaId: 0, ViewId: 0, StartTime: 10e6, EndTime: 20e6, HsReqList: []HsRequest{{ClientId: 0, StartTime: 10e6, IntervalMs: 2}}}
	if !r.acceptSchedulePrepareLocked(&p) {
		t.Fatal("prepare rejected")
	}
	if r.clients[0].validSequence(10e6, r.scheduleCommittedEnd) || r.scheduleActiveLocked() {
		t.Fatal("prepared schedule executed before commit")
	}
	r.handleSchdCommit(&SchdCommit{ReplicaId: 1, ViewId: 0, StartTime: 10e6, EndTime: 20e6})
	if r.scheduleCommittedEnd != 0 {
		t.Fatal("nonleader commit accepted")
	}
	r.handleSchdCommit(&SchdCommit{ReplicaId: 0, ViewId: 0, StartTime: 10e6, EndTime: 20e6})
	if !r.clients[0].validSequence(10e6, r.scheduleCommittedEnd) {
		t.Fatal("committed schedule fenced")
	}
	p.HsReqList[0].IntervalMs = 3
	if r.acceptSchedulePrepareLocked(&p) {
		t.Fatal("conflicting prepare accepted")
	}
	p.HsReqList[0].IntervalMs = 2
	p.ViewId = 1
	if r.acceptSchedulePrepareLocked(&p) {
		t.Fatal("future-view prepare accepted")
	}
}
func TestPrepareAcknowledgementQuorum(t *testing.T) {
	r := scheduleReplica()
	r.scheduleProposal = &SchdPrepare{ViewId: 0, StartTime: 10, EndTime: 20}
	r.schedulePrepared = map[uint32]bool{0: true}
	for _, ack := range []SchdPrepareReply{{ReplicaId: 9, StartTime: 10, EndTime: 20}, {ReplicaId: 1, ViewId: 1, StartTime: 10, EndTime: 20}, {ReplicaId: 1, StartTime: 11, EndTime: 20}, {ReplicaId: 0, StartTime: 10, EndTime: 20}} {
		r.handleSchdPrepareReply(&ack)
	}
	if len(r.schedulePrepared) != 1 {
		t.Fatal("invalid or repeated ack counted")
	}
	r.handleSchdPrepareReply(&SchdPrepareReply{ReplicaId: 1, StartTime: 10, EndTime: 20})
	r.handleSchdPrepareReply(&SchdPrepareReply{ReplicaId: 1, StartTime: 10, EndTime: 20})
	if len(r.schedulePrepared) != 2 {
		t.Fatal("quorum incorrectly counted")
	}
}
func TestHistoryRecoveryPrefix(t *testing.T) {
	a := []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10, IntervalMs: 1, EndTime: 20}}
	b := append(append([]BusSyncMessage(nil), a...), BusSyncMessage{ClientId: 0, FirstMsgNs: 20, IntervalMs: 0, EndTime: 30})
	p := mergeSuffix([]*BusViewChange{{SenderIdx: 0, Schedule: a}, {SenderIdx: 1, Schedule: b}})
	if p.scheduleConflict || !schedulesEqual(p.schedule, b) {
		t.Fatal("compatible history lost")
	}
	b[0].IntervalMs = 2
	p = mergeSuffix([]*BusViewChange{{SenderIdx: 0, Schedule: a}, {SenderIdx: 1, Schedule: b}})
	if !p.scheduleConflict {
		t.Fatal("conflicting history accepted")
	}
}
func TestHandshakeCollectionBoundary(t *testing.T) {
	r := scheduleReplica()
	w := uint64(r.config.ScheduleWindow())
	start := (uint64(time.Now().UnixNano())/w + 2) * w
	var b bytes.Buffer
	lw := &lockedWriter{w: bufio.NewWriter(&b)}
	r.handleHsRequest(&HsRequest{ClientId: 0, StartTime: start, IntervalMs: 0}, lw)
	if len(r.scheduleRequests[start]) != 1 {
		t.Fatal("disabled client not collected")
	}
	r.handleHsRequest(&HsRequest{ClientId: 1, StartTime: start - w, IntervalMs: 1}, lw)
	if len(r.scheduleRequests[start-w]) != 0 {
		t.Fatal("late request accepted")
	}
}
func TestAppendingHistoryPreservesCursor(t *testing.T) {
	r := scheduleReplica()
	a := []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10e6, IntervalMs: 5, EndTime: 20e6}}
	r.installScheduleLocked(a, 0, true)
	r.genCursorUpToLocked(1)
	before := r.cursorSlot
	a = append(a, BusSyncMessage{ClientId: 0, FirstMsgNs: 20e6, IntervalMs: 5, EndTime: 30e6})
	r.installScheduleLocked(a, 0, true)
	if r.cursorSlot != before {
		t.Fatal("cursor restarted")
	}
	owner := r.slotOwnerLocked(2)
	if owner.reqId != 20e6 {
		t.Fatal("cursor did not extend")
	}
}

func TestPrepareRequiresSnapshotOnConnection(t *testing.T) {
	r := scheduleReplica()
	r.idx = 1
	p := SchdPrepare{ReplicaId: 0, StartTime: 20e6, EndTime: 30e6, HsReqList: []HsRequest{{ClientId: 0, StartTime: 20e6, IntervalMs: 5}}}
	deliver := func(code byte, m wireMsg) {
		a, b := net.Pipe()
		done := make(chan struct{})
		go func() { r.clientListener(b); close(done) }()
		var buf bytes.Buffer
		buf.WriteByte(code)
		m.Marshal(&buf)
		if _, err := a.Write(buf.Bytes()); err != nil {
			t.Fatal(err)
		}
		a.Close()
		<-done
	}
	deliver(MsgSchdPrepare, &p)
	if len(r.schedule) != 0 {
		t.Fatal("accepted prepare without predecessor snapshot")
	}
	history := []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10e6, IntervalMs: 5, EndTime: 20e6}}
	snapshot := BusScheduleInstall{Epoch: 0, SenderIdx: 0, Schedule: history, Digest: scheduleDigest(history)}
	deliver(MsgBusScheduleInstall, &scheduleBatch{snapshot: snapshot, prepare: p})
	if len(r.schedule) != 2 || computeGlobalSlot(r.clients, 0, 20e6) != 2 {
		t.Fatal("predecessor history was not installed before prepare")
	}
	snapshot.Schedule = []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10e6, IntervalMs: 2, EndTime: 20e6}}
	snapshot.Digest = scheduleDigest(snapshot.Schedule)
	p.StartTime = 30e6
	p.EndTime = 40e6
	p.HsReqList[0].StartTime = 30e6
	deliver(MsgBusScheduleInstall, &scheduleBatch{snapshot: snapshot, prepare: p})
	if len(r.schedule) != 2 {
		t.Fatal("prepare after rejected snapshot accepted")
	}
}

func TestSnapshotRejectedDuringRecovery(t *testing.T) {
	r := scheduleReplica()
	r.idx = 1
	r.status = statusViewChange
	history := []BusSyncMessage{{ClientId: 0, FirstMsgNs: 10e6, IntervalMs: 5, EndTime: 20e6}}
	snapshot := BusScheduleInstall{Epoch: 0, SenderIdx: 0, Schedule: history, Digest: scheduleDigest(history)}
	if r.handleScheduleInstall(&snapshot) {
		t.Fatal("recovery snapshot authorized prepare")
	}
	r.status = statusNormal
	if len(r.schedule) != 0 || r.scheduleCommittedEnd != 0 {
		t.Fatal("ignored snapshot changed history")
	}
	r.installScheduleLocked(history, 1, false)
	r.viewId.Store(1)
	r.publishNormalViewLocked(1)
	if r.scheduleCommittedEnd != 20e6 || !r.scheduleActiveLocked() {
		t.Fatal("recovered authoritative schedule was not committed")
	}
}

func TestRecoveryCursorStartsAtRetainedPrefix(t *testing.T) {
	r := scheduleReplica()
	start := uint64(10e6)
	end := start + 3_000_000*1e6
	history := []BusSyncMessage{{ClientId: 0, FirstMsgNs: start, IntervalMs: 1, EndTime: end}, {ClientId: 1, FirstMsgNs: start, IntervalMs: 1, EndTime: end}}
	pending := append(append([]BusSyncMessage(nil), history...), BusSyncMessage{ClientId: 0, FirstMsgNs: end, IntervalMs: 1, EndTime: end + 10e6})
	r.installScheduleLocked(pending, 0, true)
	r.prunedBelow = 5_000_001
	r.nextExpected = r.prunedBelow + 20
	r.installScheduleLocked(history, 1, false)
	if r.cursorSlot != r.prunedBelow || len(r.slotMeta) != 0 {
		t.Fatal("cursor did not start at retained prefix")
	}
	first := r.slotOwnerLocked(r.prunedBelow)
	if first.clientId != 1 || first.reqId != start+2_500_000*1e6 {
		t.Fatalf("retained owner: %+v", first)
	}
	next := r.slotOwnerLocked(r.prunedBelow + 1)
	if next.clientId != 0 || next.reqId != first.reqId+1e6 {
		t.Fatalf("next owner: %+v", next)
	}
	if len(r.slotMeta) != 2 {
		t.Fatalf("regenerated %d historical slots", len(r.slotMeta))
	}
	r.slotOwnerLocked(r.prunedBelow - 1)
	if len(r.slotMeta) != 2 {
		t.Fatal("recreated pruned metadata")
	}
}
func TestHandshakeReturnsAuthoritativeCommittedInterval(t *testing.T) {
	r := scheduleReplica()
	w := uint64(r.config.ScheduleWindow())
	start := (uint64(time.Now().UnixNano()) / w) * w
	r.installScheduleLocked([]BusSyncMessage{{ClientId: 0, FirstMsgNs: start, IntervalMs: 1, EndTime: start + w}}, 0, true)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	a.SetReadDeadline(time.Now().Add(time.Second))
	r.handleHsRequest(&HsRequest{ClientId: 0, StartTime: start, IntervalMs: 2}, &lockedWriter{w: bufio.NewWriter(b), conn: b})
	reader := bufio.NewReader(a)
	code, err := reader.ReadByte()
	if err != nil || code != MsgHsReply {
		t.Fatalf("missing authoritative reply: %v", err)
	}
	var reply HsReply
	if err := reply.Unmarshal(reader); err != nil {
		t.Fatal(err)
	}
	if reply.IntervalMs != 1 || reply.StartTime != start || reply.EndTime != start+w {
		t.Fatalf("wrong committed reply %+v", reply)
	}
}
func TestExpiredCollectionsDiscarded(t *testing.T) {
	r := scheduleReplica()
	r.scheduleRequests = map[uint64]map[uint64]HsRequest{10e6: {0: {ClientId: 0}}, 20e6: {0: {ClientId: 0}}}
	r.scheduleWindowWaiters = map[uint64]map[uint64]*lockedWriter{10e6: {}, 20e6: {}}
	r.scheduleProposal = &SchdPrepare{StartTime: 1, EndTime: 2}
	r.expireScheduleRequestsLocked(25e6)
	if len(r.scheduleRequests) != 1 || r.scheduleRequests[20e6] == nil || len(r.scheduleWindowWaiters) != 1 || r.scheduleProposal == nil {
		t.Fatal("expired collection cleanup changed live state")
	}
}
