package paxosbus

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// ── The merge ───────────────────────────────────────────────────────────────

// report builds a BusViewChange the way buildViewChangeLocked would, from a
// literal list of which suffix slots the replica holds.
func report(idx uint32, lastNormalView, stable uint64, filled []uint64, noops []uint64) *BusViewChange {
	m := &BusViewChange{
		SenderIdx:      idx,
		ViewId:         1,
		LastNormalView: lastNormalView,
		StableSlot:     stable,
		HasStable:      true,
		NextExpected:   stable + 1,
		BitmapBase:     stable + 1,
		NoOpSlots:      noops,
	}
	all := append(append([]uint64{}, filled...), noops...)
	for _, s := range all {
		if !m.HasMax || s > m.MaxSlotFilled {
			m.MaxSlotFilled, m.HasMax = s, true
		}
	}
	if m.HasMax {
		m.FilledBitmap = make([]byte, (m.MaxSlotFilled-m.BitmapBase+8)/8)
		for _, s := range all {
			setBit(m.FilledBitmap, s-m.BitmapBase)
		}
	}
	return m
}

func TestMergeTakesHighestStableAndMax(t *testing.T) {
	plan := mergeSuffix([]*BusViewChange{
		report(0, 3, 10, []uint64{11, 12}, nil),
		report(1, 3, 14, []uint64{15}, nil),
		report(2, 3, 12, []uint64{13}, nil),
	})
	if !plan.hasStable || plan.stableSlot != 14 {
		t.Fatalf("stable = %d/%v, want 14", plan.stableSlot, plan.hasStable)
	}
	if !plan.hasMax || plan.maxSlot != 15 {
		t.Fatalf("max = %d/%v, want 15", plan.maxSlot, plan.hasMax)
	}
	// Only slot 15 sits above the commit point, and replica 1 holds it.
	if got := plan.donors[15]; got != 1 {
		t.Errorf("donor for slot 15 = %d, want 1", got)
	}
	if len(plan.noops) != 0 {
		t.Errorf("noops = %v, want none", plan.noops)
	}
}

func TestMergeNoOpWinsOverEntry(t *testing.T) {
	// Replica 0 received a real bus at slot 6; replica 1 agreed a no-op there
	// with the old leader before it died. The no-op is the decision that stands.
	plan := mergeSuffix([]*BusViewChange{
		report(0, 2, 5, []uint64{6, 7}, nil),
		report(1, 2, 5, []uint64{7}, []uint64{6}),
	})
	if !reflect.DeepEqual(plan.noops, []uint64{6}) {
		t.Fatalf("noops = %v, want [6]", plan.noops)
	}
	if _, hasDonor := plan.donors[6]; hasDonor {
		t.Error("slot 6 got a donor even though the merge made it a no-op")
	}
	if got := plan.donors[7]; got != 0 {
		t.Errorf("donor for slot 7 = %d, want 0", got)
	}
}

func TestMergeIgnoresStaleLastNormalView(t *testing.T) {
	// Replica 2 never learned view 3. Its report must not contribute — neither
	// its higher commit point nor the entries it still holds.
	plan := mergeSuffix([]*BusViewChange{
		report(0, 3, 5, []uint64{6}, nil),
		report(1, 3, 5, []uint64{6}, nil),
		report(2, 1, 9, []uint64{10, 11}, nil),
	})
	if plan.stableSlot != 5 {
		t.Errorf("stable = %d, want 5 (stale report must not raise it)", plan.stableSlot)
	}
	if plan.maxSlot != 6 {
		t.Errorf("max = %d, want 6 (stale report must not extend it)", plan.maxSlot)
	}
	if !reflect.DeepEqual(plan.selected, []uint32{0, 1}) {
		t.Errorf("selected reports = %v, want [0 1]", plan.selected)
	}
}

func TestMergeClosesSlotsNobodyHolds(t *testing.T) {
	// Slot 7 is missing everywhere in the quorum, so it cannot have committed
	// and the new leader is free to close it.
	plan := mergeSuffix([]*BusViewChange{
		report(0, 1, 5, []uint64{6, 8}, nil),
		report(1, 1, 5, []uint64{6}, nil),
	})
	if !reflect.DeepEqual(plan.noops, []uint64{7}) {
		t.Fatalf("noops = %v, want [7]", plan.noops)
	}
	if _, ok := plan.donors[8]; !ok {
		t.Error("slot 8 should still have a donor")
	}
}

func TestMergeEmpty(t *testing.T) {
	if plan := mergeSuffix(nil); plan.hasStable || plan.hasMax {
		t.Errorf("empty merge produced %+v", plan)
	}
}

func TestStartViewAndStateTransferRoundTripRecoveryFences(t *testing.T) {
	t.Run("start view", func(t *testing.T) {
		want := &BusStartView{
			ViewId: 4, StableSlot: 8, HasStable: true, MaxSlot: 12, HasMax: true,
			PrefixHash: 99, SenderIdx: 1, NoOpSlots: []uint64{9, 11},
			SelectedReports: []uint32{0, 1, 3},
		}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusStartView
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})

	t.Run("get state", func(t *testing.T) {
		want := &BusGetState{ViewId: 4, FromSlot: 9, ToSlot: 12, FetchId: 77, SenderIdx: 2}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusGetState
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})

	t.Run("new state", func(t *testing.T) {
		want := &BusNewState{
			ViewId: 4, FromSlot: 9, ToSlot: 9, FetchId: 77, SenderIdx: 1,
			Entries: []StateEntry{{Slot: 9, ClientId: 3, ReqId: 5, IsBus: true, Payload: []byte("bus")}},
		}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusNewState
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})
}

// ── Prefix hash ─────────────────────────────────────────────────────────────

// testReplica builds a replica wired up enough to execute slots, with no
// network, no durable log and one client line.
func testReplica(idx int) *Replica {
	cfg := &Config{N: 3, F: 1, Replicas: []string{"a:1", "b:2", "c:3"}}
	r := NewReplica(cfg, idx, "", "", dropNone, 0, 0, RecoveryOptions{RetainSlots: 64})
	r.clients[1] = &clientLine{baseNs: 0, intervalNs: ms}
	r.busMode = true
	return r
}

func busAt(r *Replica, slot, busSeq uint64, reqIds ...uint64) {
	reqs := make([]RequestMessage, len(reqIds))
	for i, id := range reqIds {
		reqs[i] = RequestMessage{ClientId: 1, RequestId: id, Op: []byte("x")}
	}
	r.recordBusReceivedLocked(slot, 1, busSeq, reqs)
}

func TestPrefixHashIndependentOfArrivalOrder(t *testing.T) {
	a, b := testReplica(0), testReplica(1)

	a.mu.Lock()
	for s := uint64(0); s < 6; s++ {
		busAt(a, s, s+1, s*10+1, s*10+2)
		a.advanceNextExpectedLocked()
	}
	a.mu.Unlock()

	// Same buses, delivered back to front: the cursor cannot move until the
	// hole at slot 0 is filled, so everything lands in one sweep at the end.
	b.mu.Lock()
	for s := int64(5); s >= 0; s-- {
		u := uint64(s)
		busAt(b, u, u+1, u*10+1, u*10+2)
		b.advanceNextExpectedLocked()
	}
	b.mu.Unlock()

	if a.nextExpected != 6 || b.nextExpected != 6 {
		t.Fatalf("executed a=%d b=%d, want 6 each", a.nextExpected, b.nextExpected)
	}
	if a.prefixHash != b.prefixHash {
		t.Errorf("prefix hashes differ: %016x vs %016x", a.prefixHash, b.prefixHash)
	}
	for s := uint64(0); s < 6; s++ {
		ha, _ := a.prefixHashAtLocked(s)
		hb, _ := b.prefixHashAtLocked(s)
		if ha != hb {
			t.Errorf("slot %d: hash %016x vs %016x", s, ha, hb)
		}
	}
}

func TestPrefixHashDistinguishesNoOp(t *testing.T) {
	a, b := testReplica(0), testReplica(1)
	a.mu.Lock()
	busAt(a, 0, 1, 1)
	a.advanceNextExpectedLocked()
	a.mu.Unlock()

	b.mu.Lock()
	b.setNoOpLocked(0)
	b.advanceNextExpectedLocked()
	b.mu.Unlock()

	if a.prefixHash == b.prefixHash {
		t.Error("a received entry and a no-op at the same slot hash the same")
	}
}

// ── Rewind ──────────────────────────────────────────────────────────────────

func TestRewindRestoresLogIndexesExactly(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()

	for s := uint64(0); s < 5; s++ {
		busAt(r, s, s+1, s*10+1, s*10+2)
	}
	r.advanceNextExpectedLocked()
	if r.nextExpected != 5 || r.nextLogIndex != 10 {
		t.Fatalf("setup: executed=%d logIndex=%d, want 5/10", r.nextExpected, r.nextLogIndex)
	}
	hashAt2, _ := r.prefixHashAtLocked(2)

	// Rewind to slot 3: slots 3 and 4 are given back.
	if !r.rewindToLocked(3) {
		t.Fatal("rewind refused")
	}
	if r.nextExpected != 3 {
		t.Errorf("nextExpected = %d, want 3", r.nextExpected)
	}
	if r.nextLogIndex != 6 {
		t.Errorf("nextLogIndex = %d, want 6", r.nextLogIndex)
	}
	if r.prefixHash != hashAt2 {
		t.Errorf("prefixHash = %016x, want %016x", r.prefixHash, hashAt2)
	}
	for _, rid := range []uint64{31, 32, 41, 42} {
		if _, ok := r.dedup[reqKey{1, rid}]; ok {
			t.Errorf("dedup still holds rolled-back request %d", rid)
		}
	}
	for _, rid := range []uint64{1, 2, 21, 22} {
		if _, ok := r.dedup[reqKey{1, rid}]; !ok {
			t.Errorf("dedup dropped request %d from below the rewind point", rid)
		}
	}

	// Re-executing must hand out exactly the same indexes, which is what lets a
	// client's votes for a request still add up across a view change.
	r.advanceNextExpectedLocked()
	if r.nextExpected != 5 || r.nextLogIndex != 10 {
		t.Fatalf("re-execute: executed=%d logIndex=%d, want 5/10", r.nextExpected, r.nextLogIndex)
	}
	if got := r.dedup[reqKey{1, 31}]; got != 6 {
		t.Errorf("request 31 reassigned index %d, want 6", got)
	}
	if got := r.dedup[reqKey{1, 42}]; got != 9 {
		t.Errorf("request 42 reassigned index %d, want 9", got)
	}
}

func TestRewindRefusedOutsideHashWindow(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := uint64(0); s < 200; s++ { // ringSize is 66 here
		busAt(r, s, s+1, s+1)
		r.advanceNextExpectedLocked()
	}
	if r.rewindToLocked(2) {
		t.Error("rewind to a slot the rings have forgotten should be refused")
	}
}

func TestInstallViewAppliesNoOpAndRewinds(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := uint64(0); s < 4; s++ {
		busAt(r, s, s+1, s+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(1)

	// The merge says slot 2 is a no-op, but we executed a real bus there.
	rewound, did := r.installViewLocked(1, 1, true, 3, true, []uint64{2}, false)
	if !did || rewound != 2 {
		t.Fatalf("rewound=%v to %d, want true to 2", did, rewound)
	}
	if r.status != statusNormal || r.lastNormalView != 1 || r.view() != 1 {
		t.Errorf("not installed: status=%v lnv=%d view=%d", r.status, r.lastNormalView, r.view())
	}
	if st := r.slotStateLocked(2); st != slotNoOp {
		t.Errorf("slot 2 state = %v, want no-op", st)
	}
	if r.nextExpected != 4 {
		t.Errorf("cursor stopped at %d, want it to sweep back to 4", r.nextExpected)
	}
	// Slot 2's passengers are gone with the no-op; slot 3's are re-indexed
	// behind them.
	if _, ok := r.dedup[reqKey{1, 3}]; ok {
		t.Error("request 3 survived its slot becoming a no-op")
	}
	if got := r.dedup[reqKey{1, 4}]; got != 2 {
		t.Errorf("request 4 index = %d, want 2", got)
	}
}

// A replica that did not take part in deciding a view cannot tell which of its
// speculative slots the merge contradicts, so it gives everything above its own
// commit point back — not the leader's, which is higher and covers slots nobody
// ever checked against it.
func TestInstallViewConservativeRewindsToOwnCommitPoint(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := uint64(0); s < 10; s++ {
		busAt(r, s, s+1, s+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(3) // our commit point; the leader's below is higher

	rewound, did := r.installViewLocked(2, 6, true, 9, true, nil, true)
	if !did || rewound != 4 {
		t.Fatalf("rewound=%v to %d, want true to 4 (our stable 3, +1)", did, rewound)
	}
	if r.nextExpected != 4 {
		t.Errorf("cursor at %d, want 4 — nothing above should be executable yet", r.nextExpected)
	}
	for s := uint64(4); s < 10; s++ {
		if _, ok := r.globalLog[s]; ok {
			t.Errorf("slot %d survived the conservative rewind; it must be refetched", s)
		}
	}
	if r.view() != 2 || r.status != statusNormal {
		t.Errorf("not installed: view=%d status=%v", r.view(), r.status)
	}
}

// repliesBySlot summarises what installing a view queued for the client, and
// fails the test if any of it still names the view we just left.
func repliesBySlot(t *testing.T, r *Replica, wantView uint64) map[uint64]int {
	t.Helper()
	out := make(map[uint64]int)
	for _, pr := range r.pendingReplies {
		if pr.viewId != wantView {
			t.Errorf("reply for slot %d stamped view %d, want %d",
				pr.busSlot, pr.viewId, wantView)
		}
		if got, ok := r.dedup[reqKey{pr.clientId, pr.requestId}]; !ok || got != pr.logIndex {
			t.Errorf("reply for request %d carries log index %d, want %d (present=%v)",
				pr.requestId, pr.logIndex, got, ok)
		}
		out[pr.busSlot]++
	}
	return out
}

func queuedReplySlots(r *Replica) []uint64 {
	slots := make([]uint64, len(r.pendingReplies))
	for i := range r.pendingReplies {
		slots[i] = r.pendingReplies[i].busSlot
	}
	return slots
}

// firstWriteGate lets a test stop the initial StartView in the peer send itself,
// exposing the exact state between canonical installation and Normal
// publication without ticker sleeps or production hooks.
type firstWriteGate struct {
	first       chan []byte
	release     chan struct{}
	blockOnce   sync.Once
	releaseOnce sync.Once
}

func newFirstWriteGate() *firstWriteGate {
	return &firstWriteGate{
		first:   make(chan []byte, 1),
		release: make(chan struct{}),
	}
}

func (w *firstWriteGate) Write(p []byte) (int, error) {
	block := false
	w.blockOnce.Do(func() { block = true })
	if block {
		cp := append([]byte(nil), p...)
		w.first <- cp
		<-w.release
	}
	return len(p), nil
}

func (w *firstWriteGate) open() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func blockLeaderStartView(t *testing.T, r *Replica, vc *vcState,
	reports ...*BusViewChange) (*firstWriteGate, []byte, <-chan struct{}) {
	t.Helper()
	gate := newFirstWriteGate()
	t.Cleanup(gate.open)
	r.mu.Lock()
	r.peerWriters[0] = &lockedWriter{w: bufio.NewWriter(gate)}
	r.mu.Unlock()
	for _, report := range reports {
		vc.reports <- report
	}
	done := make(chan struct{})
	go func() {
		r.driveViewChange(vc)
		close(done)
	}()
	select {
	case wire := <-gate.first:
		return gate, wire, done
	case <-time.After(time.Second):
		t.Fatal("leader never reached the initial StartView multicast")
		return nil, nil, nil
	}
}

func decodeStartViewWrite(t *testing.T, wire []byte) *BusStartView {
	t.Helper()
	if len(wire) == 0 || wire[0] != MsgBusStartView {
		t.Fatalf("first peer message code=%v, want StartView %d", wire, MsgBusStartView)
	}
	var msg BusStartView
	if err := msg.Unmarshal(bytes.NewReader(wire[1:])); err != nil {
		t.Fatalf("decode initial StartView: %v", err)
	}
	return &msg
}

func waitForViewChangeDrive(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leader did not finish view change after multicast returned")
	}
}

func TestLeaderMulticastsCanonicalStartViewBeforeNormal(t *testing.T) {
	r := testReplica(1) // replica 1 leads view 1
	cleanupViewChangeTest(t, r)
	var laterPeer bytes.Buffer

	r.mu.Lock()
	busAt(r, 0, 1, 1)
	busAt(r, 1, 2, 2)
	busAt(r, 2, 3, 3)
	r.setNoOpLocked(3)
	r.advanceNextExpectedLocked()
	if r.nextExpected != 4 {
		r.mu.Unlock()
		t.Fatalf("setup cursor=%d, want 4", r.nextExpected)
	}
	r.setStableLocked(0)
	r.pendingReplies = nil
	r.viewId.Store(1)
	r.status = statusViewChange
	vc := newVCState(1, r.config.N)
	r.vc = vc
	r.armViewChangeWatchdogLocked(vc.view)
	r.peerWriters[2] = &lockedWriter{w: bufio.NewWriter(&laterPeer)}
	r.mu.Unlock()

	gate, wire, done := blockLeaderStartView(t, r, vc,
		report(1, 0, 0, []uint64{1}, nil),
		report(2, 0, 0, []uint64{1}, nil))
	msg := decodeStartViewWrite(t, wire)
	if !msg.HasMax || msg.MaxSlot != 1 {
		t.Fatalf("initial StartView max=%d/%v, want decided boundary 1/true", msg.MaxSlot, msg.HasMax)
	}

	r.mu.Lock()
	if r.status != statusViewChange || r.lastNormalView != 0 {
		r.mu.Unlock()
		t.Fatalf("before multicast returned status=%v lastNormal=%d, want ViewChange/0",
			r.status, r.lastNormalView)
	}
	if r.nextExpected != 2 {
		r.mu.Unlock()
		t.Fatalf("before multicast returned cursor=%d, want canonical end 2", r.nextExpected)
	}
	if got := queuedReplySlots(r); !reflect.DeepEqual(got, []uint64{1}) {
		r.mu.Unlock()
		t.Fatalf("before multicast returned reply slots=%v, want merged suffix [1]", got)
	}
	if _, executed := r.dedup[reqKey{1, 3}]; executed {
		r.mu.Unlock()
		t.Fatal("already-executed post-merge bus remained executed before Normal")
	}
	if r.slotStateLocked(2) != slotReceived {
		r.mu.Unlock()
		t.Fatal("already-executed post-merge real bus was not retained for the later sweep")
	}
	if r.slotStateLocked(3) != slotEmpty {
		r.mu.Unlock()
		t.Fatal("stale post-merge no-op survived the canonical install")
	}
	// This bus arrives while the first peer send is blocked. It must remain
	// frozen and absent from the immutable StartView snapshot too.
	busAt(r, 3, 4, 4)
	if r.nextExpected != 2 {
		r.mu.Unlock()
		t.Fatalf("bus arriving during multicast advanced cursor to %d", r.nextExpected)
	}
	r.mu.Unlock()

	// A deterministic heartbeat tick at this boundary is ineligible: the
	// multicast has not returned and status is still ViewChange.
	r.syncOnce()
	if laterPeer.Len() != 0 {
		t.Fatal("heartbeat reached a later peer before StartView publication")
	}

	gate.open()
	waitForViewChangeDrive(t, done)

	r.mu.Lock()
	if r.status != statusNormal || r.lastNormalView != 1 {
		r.mu.Unlock()
		t.Fatalf("after multicast status=%v lastNormal=%d, want Normal/1",
			r.status, r.lastNormalView)
	}
	if r.nextExpected != 4 {
		r.mu.Unlock()
		t.Fatalf("post-Normal cursor=%d, want post-merge buses drained through 3", r.nextExpected)
	}
	if got, want := queuedReplySlots(r), []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		r.mu.Unlock()
		t.Fatalf("reply order=%v, want merged suffix before post-merge buses %v", got, want)
	}
	r.mu.Unlock()

	// Later catch-up responses retain the established behavior: after the
	// post-merge sweep they describe the leader's current full executed prefix.
	later := r.startViewMsg()
	if later == nil || !later.HasMax || later.MaxSlot != 3 {
		t.Fatalf("later StartView max=%v, want current boundary 3", later)
	}

	beforeHeartbeat := laterPeer.Len()
	if beforeHeartbeat == 0 {
		t.Fatal("initial StartView never reached the later peer")
	}
	r.syncOnce()
	afterHeartbeat := laterPeer.Bytes()
	if len(afterHeartbeat) <= beforeHeartbeat || afterHeartbeat[beforeHeartbeat] != MsgBusSyncPrepare {
		t.Fatal("next eligible heartbeat tick did not send SyncPrepare after Normal publication")
	}
}

func TestLeaderStableOnlyMergeAdvertisesCommittedBoundary(t *testing.T) {
	r := testReplica(1)
	cleanupViewChangeTest(t, r)
	r.mu.Lock()
	for slot := uint64(0); slot < 3; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(1)
	r.pendingReplies = nil
	r.viewId.Store(1)
	r.status = statusViewChange
	vc := newVCState(1, r.config.N)
	r.vc = vc
	r.armViewChangeWatchdogLocked(vc.view)
	r.mu.Unlock()

	gate, wire, done := blockLeaderStartView(t, r, vc,
		report(1, 0, 1, nil, nil),
		report(2, 0, 1, nil, nil))
	msg := decodeStartViewWrite(t, wire)
	if !msg.HasStable || msg.StableSlot != 1 || !msg.HasMax || msg.MaxSlot != 1 {
		t.Fatalf("stable-only StartView stable=%d/%v max=%d/%v, want 1/true and 1/true",
			msg.StableSlot, msg.HasStable, msg.MaxSlot, msg.HasMax)
	}
	if !r.validStartView(msg) {
		t.Fatal("stable-only StartView is not valid for follower installation")
	}
	r.mu.Lock()
	if r.status != statusViewChange || r.nextExpected != 2 {
		r.mu.Unlock()
		t.Fatalf("stable-only merge exposed status=%v cursor=%d before multicast returned",
			r.status, r.nextExpected)
	}
	r.mu.Unlock()

	gate.open()
	waitForViewChangeDrive(t, done)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != statusNormal || r.nextExpected != 3 {
		t.Fatalf("stable-only publication status=%v cursor=%d, want Normal/3", r.status, r.nextExpected)
	}
	if got := queuedReplySlots(r); !reflect.DeepEqual(got, []uint64{2}) {
		t.Fatalf("stable-only post-merge replies=%v, want [2]", got)
	}
}

func TestStaleLeaderDriveCannotPublishNormalAfterNewerView(t *testing.T) {
	r := testReplica(1)
	cleanupViewChangeTest(t, r)
	r.mu.Lock()
	for slot := uint64(0); slot < 3; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(0)
	r.pendingReplies = nil
	r.viewId.Store(1)
	r.status = statusViewChange
	vc := newVCState(1, r.config.N)
	r.vc = vc
	r.armViewChangeWatchdogLocked(vc.view)
	r.mu.Unlock()

	gate, _, done := blockLeaderStartView(t, r, vc,
		report(1, 0, 0, []uint64{1}, nil),
		report(2, 0, 0, []uint64{1}, nil))

	// The old drive retains its local writer while blocked. Remove it from the
	// replica so the real startViewChange path can publish view 2 without its own
	// multicast waiting behind that writer.
	r.mu.Lock()
	r.peerWriters[0] = nil
	r.mu.Unlock()
	r.startViewChange(2)
	gate.open()
	waitForViewChangeDrive(t, done)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.view() != 2 || r.status != statusViewChange || r.lastNormalView != 0 {
		t.Fatalf("stale drive published view 1: view=%d status=%v lastNormal=%d",
			r.view(), r.status, r.lastNormalView)
	}
	if r.nextExpected != 2 {
		t.Fatalf("stale drive swept post-merge bus in newer view; cursor=%d, want 2", r.nextExpected)
	}
	if got := queuedReplySlots(r); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("stale drive queued post-merge replies=%v, want canonical [1] only", got)
	}
}

// A replica keeps executing for as long as it takes to notice the leader died,
// and those replies name a leader that will never answer — the client's quorum
// rule discards them. Installing the new view has to say them again before it
// replies for traffic that arrived beyond the decided merge boundary.
func TestInstallViewQueuesMergedRepliesBeforePostMergeBus(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()

	for s := uint64(0); s < 4; s++ {
		busAt(r, s, s+1, s*10+1, s*10+2)
	}
	r.advanceNextExpectedLocked() // executed under the old leader, replies in view 0
	r.setStableLocked(2)
	r.pendingReplies = nil // the view-0 replies; the client could never count them

	// Slot 4 belongs to the decided merge; slot 5 arrived after its boundary.
	// Both were recorded with the cursor frozen.
	r.status = statusViewChange
	busAt(r, 4, 5, 41, 42)
	busAt(r, 5, 6, 51, 52)

	r.installViewLocked(1, 2, true, 4, true, nil, false)

	got := repliesBySlot(t, r, 1)
	want := map[uint64]int{3: 2, 4: 2, 5: 2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replies by slot = %v, want %v", got, want)
	}
	// Newly executed canonical entries retain the existing enqueue-before-replay
	// behavior; both canonical groups must precede the post-merge bus.
	wantOrder := []uint64{4, 4, 3, 3, 5, 5}
	if gotOrder := queuedReplySlots(r); !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Errorf("reply slot order = %v, want canonical replies before post-merge bus %v",
			gotOrder, wantOrder)
	}
	if r.nextExpected != 6 {
		t.Errorf("cursor at %d, want 6", r.nextExpected)
	}
	if r.status != statusNormal || r.lastNormalView != 1 {
		t.Errorf("status=%v lastNormalView=%d, want direct ViewChange to Normal/1",
			r.status, r.lastNormalView)
	}
}

// The replay range is taken after the rewind, so a slot the merge contradicted
// is replied for by the re-executing cursor and not by the replay as well.
func TestInstallViewReplayDoesNotDoubleCountRewoundSlots(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()

	for s := uint64(0); s < 6; s++ {
		busAt(r, s, s+1, s*10+1, s*10+2)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(2)
	r.pendingReplies = nil

	// The merge says slot 4 is a no-op: the cursor rewinds there, slot 4 loses
	// its passengers, and slot 5 is re-executed behind it.
	r.installViewLocked(1, 2, true, 5, true, []uint64{4}, false)

	got := repliesBySlot(t, r, 1)
	want := map[uint64]int{3: 2, 5: 2} // 3 replayed, 5 re-executed, 4 gone
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replies by slot = %v, want %v", got, want)
	}
}

// Nothing at or below the commit point is worth saying again: it was acked by a
// quorum, so the old leader had executed it and its reply was already on the
// wire before it died.
func TestInstallViewReplaySkipsCommittedPrefix(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()

	for s := uint64(0); s < 4; s++ {
		busAt(r, s, s+1, s*10+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(3) // everything executed is committed
	r.pendingReplies = nil

	r.installViewLocked(1, 3, true, 3, true, nil, false)

	if n := len(r.pendingReplies); n != 0 {
		t.Errorf("%d replies queued for a fully committed log, want none", n)
	}
}

func prepareFollowerRecoveryForTest(t *testing.T, r *Replica, msg *BusStartView,
	selected bool) *viewRecovery {
	t.Helper()
	r.mu.Lock()
	r.viewId.Store(msg.ViewId)
	r.status = statusViewChange
	r.recoveryGen++
	rec := &viewRecovery{
		view:       msg.ViewId,
		generation: r.recoveryGen,
		leader:     int(msg.SenderIdx),
		abort:      make(chan struct{}),
		stable:     msg.StableSlot,
		hasStable:  msg.HasStable,
		maxSlot:    msg.MaxSlot,
		hasMax:     msg.HasMax,
	}
	r.recovery = rec
	if _, _, ok := r.prepareRecoveryLocked(rec, msg, selected); !ok {
		r.mu.Unlock()
		t.Fatal("could not prepare follower recovery")
	}
	r.mu.Unlock()
	return rec
}

func TestSelectedFollowerRetainsMergedSuffixAndLateBus(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	for slot := uint64(0); slot < 7; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(2)
	r.pendingReplies = nil
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 2, HasStable: true,
		MaxSlot: 4, HasMax: true, SelectedReports: []uint32{0, 1},
	}
	rec := prepareFollowerRecoveryForTest(t, r, msg, true)

	r.mu.Lock()
	if r.status != statusViewChange || r.lastNormalView != 0 {
		t.Fatalf("status=%v lastNormalView=%d, want view-change with last normal view 0",
			r.status, r.lastNormalView)
	}
	for _, slot := range []uint64{3, 4} {
		if r.slotStateLocked(slot) != slotReceived {
			t.Errorf("selected suffix slot %d was not retained", slot)
		}
	}
	for _, slot := range []uint64{5, 6} {
		if _, ok := r.globalLog[slot]; ok {
			t.Errorf("pre-StartView slot %d beyond merged max survived", slot)
		}
	}
	// This bus arrives after the one-time cleanup. Finishing recovery must not
	// erase it merely because it is beyond the old merged MaxSlot, and its reply
	// must follow all replies owed by the canonical suffix.
	busAt(r, 5, 6, 70)
	r.mu.Unlock()

	if !r.finishRecoveryIfComplete(rec) {
		t.Fatal("complete selected suffix did not finish recovery")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != statusNormal || r.lastNormalView != 1 {
		t.Errorf("status=%v lastNormalView=%d, want normal in view 1", r.status, r.lastNormalView)
	}
	if r.slotStateLocked(5) != slotReceived {
		t.Error("bus arriving after recovery began was deleted")
	}
	if r.nextExpected != 6 {
		t.Errorf("cursor=%d, want canonical range and post-merge bus through slot 5", r.nextExpected)
	}
	if got, want := repliesBySlot(t, r, 1), map[uint64]int{3: 1, 4: 1, 5: 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("replies by slot = %v, want %v", got, want)
	}
	if gotOrder, wantOrder := queuedReplySlots(r), []uint64{3, 4, 5}; !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Errorf("reply slot order = %v, want canonical replay before post-merge bus %v",
			gotOrder, wantOrder)
	}
}

func TestUnselectedFollowerDiscardsSpeculativeSuffix(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	for slot := uint64(0); slot < 7; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(2)
	r.pendingReplies = nil
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 2, HasStable: true,
		MaxSlot: 5, HasMax: true, NoOpSlots: []uint64{4}, SelectedReports: []uint32{1, 2},
	}
	prepareFollowerRecoveryForTest(t, r, msg, false)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextExpected != 3 {
		t.Errorf("cursor=%d, want rewind to own stable frontier 3", r.nextExpected)
	}
	if r.slotStateLocked(3) != slotEmpty || r.slotStateLocked(5) != slotEmpty {
		t.Error("unselected speculative real entries survived conservative rewind")
	}
	if r.slotStateLocked(4) != slotNoOp {
		t.Errorf("canonical slot 4 state=%v, want no-op", r.slotStateLocked(4))
	}
}

func TestRecoveryNeverRewindsBelowLocalStableFrontier(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	for slot := uint64(0); slot < 5; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(3)
	r.viewId.Store(1)
	r.status = statusViewChange
	rec := &viewRecovery{
		view: 1, generation: 1, leader: 1, abort: make(chan struct{}),
		stable: 1, hasStable: true, maxSlot: 2, hasMax: true,
	}
	r.recovery = rec
	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 1, HasStable: true,
		MaxSlot: 2, HasMax: true,
	}
	_, _, ok := r.prepareRecoveryLocked(rec, msg, false)
	if ok {
		r.mu.Unlock()
		t.Fatal("accepted a merged log ending below the local stable frontier")
	}
	if r.nextExpected != 5 || r.stableSlot != 3 {
		next, stable := r.nextExpected, r.stableSlot
		r.mu.Unlock()
		t.Fatalf("failed recovery rewound stable history: next=%d stable=%d, want 5/3", next, stable)
	}
	for slot := uint64(0); slot < 5; slot++ {
		if r.slotStateLocked(slot) != slotReceived {
			r.mu.Unlock()
			t.Fatalf("failed recovery changed stable slot %d", slot)
		}
	}
	r.mu.Unlock()
}

func TestIncompleteOrFailedStartViewInstallStaysInViewChange(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	busAt(r, 0, 1, 1)
	r.advanceNextExpectedLocked()
	r.setStableLocked(0)
	r.pendingReplies = nil
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 0, HasStable: true,
		MaxSlot: 2, HasMax: true, NoOpSlots: []uint64{2}, SelectedReports: []uint32{0, 1},
	}
	rec := prepareFollowerRecoveryForTest(t, r, msg, true)
	if r.finishRecoveryIfComplete(rec) {
		t.Fatal("recovery completed with missing slot 1")
	}

	ok := r.runFetch(fetchReq{
		peer: 1, from: 1, to: 2, view: 1, fetchID: 10,
		installGen: rec.generation, cancel: rec.abort,
	})
	if ok {
		t.Fatal("fetch from disconnected leader reported success")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != statusViewChange || r.lastNormalView != 0 {
		t.Errorf("failed fetch exposed replica as %v in lastNormalView %d",
			r.status, r.lastNormalView)
	}
}

func TestRecoveryFetchesOnlyContiguousMissingRuns(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	for slot := uint64(0); slot < 3; slot++ {
		busAt(r, slot, slot+1, slot+1)
	}
	r.advanceNextExpectedLocked()
	r.status = statusViewChange
	busAt(r, 4, 5, 5)
	busAt(r, 6, 7, 7)
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, MaxSlot: 6, HasMax: true,
		SelectedReports: []uint32{0, 1},
	}
	rec := prepareFollowerRecoveryForTest(t, r, msg, true)

	from, to, active := r.recoveryMissingRange(rec)
	if !active || from != 3 || to != 3 {
		t.Fatalf("first missing range=%d..%d active=%v, want 3..3", from, to, active)
	}
	r.mu.Lock()
	busAt(r, 3, 4, 4)
	r.mu.Unlock()
	from, to, active = r.recoveryMissingRange(rec)
	if !active || from != 5 || to != 5 {
		t.Fatalf("second missing range=%d..%d active=%v, want 5..5", from, to, active)
	}
}

func TestStaleRecoveryTokenCannotInstallState(t *testing.T) {
	r := testReplica(0)
	msg := &BusStartView{ViewId: 1, SenderIdx: 1, MaxSlot: 0, HasMax: true}
	rec := prepareFollowerRecoveryForTest(t, r, msg, false)
	state := &BusNewState{
		ViewId: 1, FromSlot: 0, ToSlot: 0, FetchId: 8, SenderIdx: 1,
		Entries: []StateEntry{{
			Slot: 0, ClientId: 1, ReqId: 1, IsBus: true,
			Payload: marshalRequests([]RequestMessage{{ClientId: 1, RequestId: 1, Op: []byte("x")}}),
		}},
	}
	wrongLeader := fetchReq{peer: 2, from: 0, to: 0, view: 1, fetchID: 8, installGen: rec.generation}
	if r.applyStateEntries(state, wrongLeader) {
		t.Fatal("state from a non-leader peer was accepted during follower recovery")
	}
	staleView := fetchReq{peer: 1, from: 0, to: 0, view: 0, fetchID: 8, installGen: rec.generation}
	if r.applyStateEntries(state, staleView) {
		t.Fatal("state from a stale view was accepted")
	}
	stale := fetchReq{peer: 1, from: 0, to: 0, view: 1, fetchID: 8, installGen: rec.generation + 1}
	if r.applyStateEntries(state, stale) {
		t.Fatal("state from stale recovery token was accepted")
	}
	r.mu.Lock()
	if r.slotStateLocked(0) != slotEmpty {
		r.mu.Unlock()
		t.Fatal("stale recovery token mutated the log")
	}
	r.mu.Unlock()

	active := stale
	active.installGen = rec.generation
	if !r.applyStateEntries(state, active) {
		t.Fatal("state for active recovery token was rejected")
	}
	if !r.finishRecoveryIfComplete(rec) {
		t.Fatal("complete active recovery did not finish")
	}
}

func TestViewChangeFetchCannotCrossIntoNewView(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	oldVC := newVCState(4, r.config.N)
	r.mu.Lock()
	r.viewId.Store(oldVC.view)
	r.status = statusViewChange
	r.vc = oldVC
	r.armViewChangeWatchdogLocked(oldVC.view)
	watchdog := r.viewChangeWatchdog
	r.mu.Unlock()

	plan := mergePlan{
		donors: map[uint64]uint32{
			0: 1,
			1: 2,
		},
	}
	result := make(chan bool, 1)
	go func() {
		result <- r.fetchMergedState(oldVC, watchdog, &plan)
	}()

	var first fetchReq
	select {
	case first = <-r.fetchQ:
	case <-time.After(time.Second):
		t.Fatal("view-change merge did not queue its first donor fetch")
	}
	if first.view != oldVC.view {
		t.Errorf("first fetch view=%d, want immutable originating view %d",
			first.view, oldVC.view)
	}
	if first.vc != oldVC {
		t.Errorf("first fetch owner=%p, want exact vcState %p", first.vc, oldVC)
	}
	if first.cancel != oldVC.abort {
		t.Error("first fetch did not carry the originating cancellation channel")
	}

	// Retire view 4 while its first donor is still outstanding. Completing that
	// fetch afterward reproduces the old race: the abandoned loop used to move
	// to its next donor and snapshot the now-current view 5.
	newVC := newVCState(5, r.config.N)
	r.mu.Lock()
	close(oldVC.abort)
	r.viewId.Store(newVC.view)
	r.vc = newVC
	r.mu.Unlock()
	first.done <- false

	select {
	case stale := <-r.fetchQ:
		// Release an implementation without cancellation-aware waiting before
		// failing, so the test does not strand its goroutine on the second fetch.
		stale.done <- false
		<-result
		t.Fatalf("retired view-4 merge queued another donor fetch as view %d", stale.view)
	case ok := <-result:
		if ok {
			t.Fatal("retired view-change merge reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("retired view-change merge did not stop promptly")
	}
	if queued := len(r.fetchQ); queued != 0 {
		t.Fatalf("retired view-change merge left %d donor fetches queued", queued)
	}
}

func TestLeaderRetriesCommittedPrefixBeforePublishingStartView(t *testing.T) {
	r := testReplica(1) // replica 1 leads view 1
	cleanupViewChangeTest(t, r)
	var peer bytes.Buffer
	r.mu.Lock()
	busAt(r, 0, 1, 1)
	r.advanceNextExpectedLocked()
	r.setStableLocked(0)
	r.releaseEntryBytesLocked(r.globalLog[0])
	delete(r.globalLog, 0) // an executed/reclaimed slot still counts as present
	r.prunedBelow = 1
	r.peerWriters[0] = &lockedWriter{w: bufio.NewWriter(&peer)}
	start := r.beginViewChangeLocked(1)
	watchdog := r.viewChangeWatchdog
	r.mu.Unlock()
	if start == nil || watchdog == nil {
		t.Fatal("could not start leader view change")
	}

	// The new leader has executed and reclaimed slot 0. Replica 2 proves two
	// more slots are committed and is the furthest-executed selected donor.
	start.vc.reports <- &BusViewChange{
		ViewId: 1, SenderIdx: 1, LastNormalView: 0,
		StableSlot: 0, HasStable: true, NextExpected: 1, BitmapBase: 1,
	}
	start.vc.reports <- &BusViewChange{
		ViewId: 1, SenderIdx: 2, LastNormalView: 0,
		StableSlot: 2, HasStable: true, NextExpected: 3, BitmapBase: 3,
	}

	done := make(chan struct{})
	go func() {
		r.driveViewChange(start.vc)
		close(done)
	}()

	var first fetchReq
	select {
	case first = <-r.fetchQ:
	case <-time.After(time.Second):
		t.Fatal("leader did not request its missing committed prefix")
	}
	if first.peer != 2 || first.from != 1 || first.to != 2 {
		t.Fatalf("first committed-prefix fetch=%+v, want peer 2 range [1,2]", first)
	}
	first.done <- false // timeout, disconnect, or queue worker failure

	r.mu.Lock()
	status, currentWatchdog := r.status, r.viewChangeWatchdog
	r.mu.Unlock()
	if status != statusViewChange || currentWatchdog != watchdog {
		t.Fatalf("failed fetch published status=%v watchdog=%p, want view-change with original %p",
			status, currentWatchdog, watchdog)
	}
	if peer.Len() != 0 {
		t.Fatal("leader broadcast StartView after the failed committed-prefix fetch")
	}

	var retry fetchReq
	select {
	case retry = <-r.fetchQ:
	case <-time.After(time.Second):
		t.Fatal("leader did not retry its missing committed prefix")
	}
	if retry.peer != 2 || retry.from != 1 || retry.to != 2 || retry.fetchID == first.fetchID {
		t.Fatalf("retry fetch=%+v, want a new peer-2 request for [1,2]", retry)
	}
	reply := &BusNewState{
		ViewId: 1, FromSlot: 1, ToSlot: 2, FetchId: retry.fetchID, SenderIdx: 2,
		Entries: []StateEntry{
			{Slot: 1, ClientId: 1, ReqId: 2, Payload: []byte("one")},
			{Slot: 2, ClientId: 1, ReqId: 3, Payload: []byte("two")},
		},
	}
	if !r.applyStateEntries(reply, retry) {
		t.Fatal("active retry response was rejected")
	}
	retry.done <- true

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leader did not finish after receiving the committed prefix")
	}
	r.mu.Lock()
	status, view, lastNormal := r.status, r.view(), r.lastNormalView
	next, stable, haveStable := r.nextExpected, r.stableSlot, r.haveStable
	currentWatchdog = r.viewChangeWatchdog
	r.mu.Unlock()
	if status != statusNormal || view != 1 || lastNormal != 1 ||
		next != 3 || !haveStable || stable != 2 || currentWatchdog != nil {
		t.Fatalf("completed merge state=%v view=%d lastNormal=%d next=%d stable=%d/%v watchdog=%p",
			status, view, lastNormal, next, stable, haveStable, currentWatchdog)
	}
	code, err := peer.ReadByte()
	if err != nil {
		t.Fatalf("leader did not broadcast StartView after completing the prefix: %v", err)
	}
	if code != MsgBusStartView {
		t.Fatalf("published message code=%d, want StartView %d", code, MsgBusStartView)
	}
	var msg BusStartView
	if err := msg.Unmarshal(&peer); err != nil {
		t.Fatalf("bad published StartView: %v", err)
	}
	if msg.ViewId != 1 || !msg.HasStable || msg.StableSlot != 2 {
		t.Fatalf("published StartView=%+v, want view 1 stable slot 2", msg)
	}
}

func TestCommittedPrefixRetryStopsAtViewChangeWatchdog(t *testing.T) {
	r := testReplica(1) // replica 1 leads view 1, but not view 2
	cleanupViewChangeTest(t, r)
	var peer bytes.Buffer
	r.mu.Lock()
	r.peerWriters[0] = &lockedWriter{w: bufio.NewWriter(&peer)}
	start := r.beginViewChangeLocked(1)
	watchdog := r.viewChangeWatchdog
	r.mu.Unlock()
	if start == nil || watchdog == nil {
		t.Fatal("could not start leader view change")
	}
	start.vc.reports <- &BusViewChange{
		ViewId: 1, SenderIdx: 1, LastNormalView: 0, NextExpected: 0,
	}
	start.vc.reports <- &BusViewChange{
		ViewId: 1, SenderIdx: 2, LastNormalView: 0,
		StableSlot: 1, HasStable: true, NextExpected: 2, BitmapBase: 2,
	}

	done := make(chan struct{})
	go func() {
		r.driveViewChange(start.vc)
		close(done)
	}()
	select {
	case <-r.fetchQ:
	case <-time.After(time.Second):
		t.Fatal("leader did not begin committed-prefix recovery")
	}

	// Expiry starts view 2 and closes the exact vcState the blocked fetch owns.
	// It must not install or announce the incomplete view 1 on its way out.
	r.expireViewChangeWatchdog(watchdog)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("committed-prefix retry did not stop when its watchdog expired")
	}
	r.mu.Lock()
	status, view, lastNormal := r.status, r.view(), r.lastNormalView
	startViewView, currentVC := r.startViewView, r.vc
	currentWatchdog := r.viewChangeWatchdog
	r.mu.Unlock()
	if status != statusViewChange || view != 2 || lastNormal != 0 || startViewView == 1 {
		t.Fatalf("expired merge published state=%v view=%d lastNormal=%d startViewView=%d",
			status, view, lastNormal, startViewView)
	}
	if currentVC == nil || currentWatchdog == nil {
		t.Fatalf("watchdog did not create fresh view-change state: vc=%p watchdog=%p",
			currentVC, currentWatchdog)
	}
	if currentVC == start.vc || currentVC.view != 2 ||
		currentWatchdog == watchdog || currentWatchdog.view != 2 {
		t.Fatalf("watchdog did not fence merge into a fresh view: vc=%p/%v watchdog=%p/%v",
			currentVC, currentVC.view, currentWatchdog, currentWatchdog.view)
	}
	code, err := peer.ReadByte()
	if err != nil {
		t.Fatalf("fallback did not publish the next view-change request: %v", err)
	}
	if code != MsgBusViewChangeRequest {
		t.Fatalf("message after expiry=%d, want only view-change request %d",
			code, MsgBusViewChangeRequest)
	}
	var request BusViewChangeRequest
	if err := request.Unmarshal(&peer); err != nil {
		t.Fatalf("bad fallback view-change request: %v", err)
	}
	if request.ViewId != 2 || peer.Len() != 0 {
		t.Fatalf("fallback request=%+v trailing_bytes=%d; incomplete view likely published",
			request, peer.Len())
	}
	if queued := len(r.fetchQ); queued != 0 {
		t.Fatalf("expired committed-prefix merge queued %d retries", queued)
	}
}

func TestViewChangeFetchRequiresExactVCState(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	retired := newVCState(4, r.config.N)
	active := newVCState(4, r.config.N)
	r.mu.Lock()
	r.viewId.Store(active.view)
	r.status = statusViewChange
	r.vc = active
	r.mu.Unlock()

	state := &BusNewState{
		ViewId: 4, FromSlot: 0, ToSlot: 0, FetchId: 91, SenderIdx: 1,
		Entries: []StateEntry{{
			Slot: 0, ClientId: 1, ReqId: 1, IsBus: true,
			Payload: marshalRequests([]RequestMessage{{ClientId: 1, RequestId: 1, Op: []byte("x")}}),
		}},
	}
	staleReq := fetchReq{
		peer: 1, from: 0, to: 0, view: 4, fetchID: 91,
		vc: retired, cancel: retired.abort,
	}
	if r.applyStateEntries(state, staleReq) {
		t.Fatal("response owned by a replaced vcState was accepted")
	}
	r.mu.Lock()
	if got := r.slotStateLocked(0); got != slotEmpty {
		r.mu.Unlock()
		t.Fatalf("replaced vcState mutated slot 0 to %v", got)
	}
	r.mu.Unlock()

	activeReq := staleReq
	activeReq.vc = active
	activeReq.cancel = active.abort
	if !r.applyStateEntries(state, activeReq) {
		t.Fatal("response owned by the exact active vcState was rejected")
	}
}

func TestNewViewDuringRecoveryKeepsPreviousLastNormalView(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	busAt(r, 0, 1, 1)
	r.advanceNextExpectedLocked()
	r.setStableLocked(0)
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 0, HasStable: true,
		MaxSlot: 1, HasMax: true, SelectedReports: []uint32{0, 1},
	}
	rec := prepareFollowerRecoveryForTest(t, r, msg, true)
	r.startViewChange(2)
	select {
	case <-rec.abort:
	default:
		t.Fatal("starting view 2 did not cancel the incomplete view-1 recovery")
	}

	r.mu.Lock()
	incomplete := r.buildViewChangeLocked(2)
	status, lastNormal := r.status, r.lastNormalView
	r.mu.Unlock()
	if status != statusViewChange || lastNormal != 0 || incomplete.LastNormalView != 0 {
		t.Fatalf("status=%v local/report lastNormal=%d/%d, want viewchange and 0/0",
			status, lastNormal, incomplete.LastNormalView)
	}

	// A complete replica from the previous normal view still contributes slot
	// 1. The incomplete StartView install report must not claim view 1 and outrank it.
	complete := report(2, 0, 0, []uint64{1}, nil)
	complete.ViewId = 2
	plan := mergeSuffix([]*BusViewChange{incomplete, complete})
	if !plan.hasMax || plan.maxSlot != 1 || plan.donors[1] != 2 {
		t.Fatalf("next merge lost complete replica's slot 1: %+v", plan)
	}
}

func TestRecoveredRepliesUseNewView(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	busAt(r, 0, 1, 1)
	busAt(r, 1, 2, 2)
	r.advanceNextExpectedLocked()
	r.setStableLocked(0)
	r.pendingReplies = nil
	r.mu.Unlock()

	msg := &BusStartView{
		ViewId: 1, SenderIdx: 1, StableSlot: 0, HasStable: true,
		MaxSlot: 2, HasMax: true, SelectedReports: []uint32{0, 1},
	}
	rec := prepareFollowerRecoveryForTest(t, r, msg, true)
	state := &BusNewState{
		ViewId: 1, FromSlot: 2, ToSlot: 2, FetchId: 9, SenderIdx: 1,
		Entries: []StateEntry{{
			Slot: 2, ClientId: 1, ReqId: 3, IsBus: true,
			Payload: marshalRequests([]RequestMessage{{ClientId: 1, RequestId: 3, Op: []byte("x")}}),
		}},
	}
	req := fetchReq{
		peer: 1, from: 2, to: 2, view: 1, fetchID: 9,
		installGen: rec.generation, cancel: rec.abort,
	}
	if !r.applyStateEntries(state, req) {
		t.Fatal("could not install recovered suffix entry")
	}
	r.mu.Lock()
	if r.status != statusViewChange || r.lastNormalView != 0 {
		r.mu.Unlock()
		t.Fatal("follower left ViewChange before finishing the canonical suffix")
	}
	busAt(r, 3, 4, 4) // arrived beyond the BusStartView boundary
	r.mu.Unlock()
	if !r.finishRecoveryIfComplete(rec) {
		t.Fatal("could not finish complete recovered suffix")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	got := repliesBySlot(t, r, 1)
	want := map[uint64]int{1: 1, 2: 1, 3: 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recovery replies by slot=%v, want %v", got, want)
	}
	if gotOrder, wantOrder := queuedReplySlots(r), []uint64{2, 1, 3}; !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Errorf("reply slot order = %v, want canonical execution/replay before post-merge bus %v",
			gotOrder, wantOrder)
	}
}

func TestStartViewDoesNotBecomeNormalBeforeStateArrives(t *testing.T) {
	r := testReplica(0)
	var peer bytes.Buffer
	r.mu.Lock()
	r.peerWriters[1] = &lockedWriter{w: bufio.NewWriter(&peer)}
	r.mu.Unlock()

	go r.stateFetchLoop()
	done := make(chan struct{})
	go func() {
		r.installStartView(&BusStartView{
			ViewId: 1, SenderIdx: 1, MaxSlot: 0, HasMax: true,
			SelectedReports: []uint32{1},
		})
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		status, lastNormal := r.status, r.lastNormalView
		r.mu.Unlock()
		if status == statusViewChange && r.fetchSeq.Load() > 0 {
			if lastNormal != 0 {
				t.Fatalf("lastNormalView=%d before state arrived, want 0", lastNormal)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica never entered recovery or requested state")
		}
		time.Sleep(time.Millisecond)
	}

	fetchID := r.fetchSeq.Load()
	r.newStateCh <- &BusNewState{
		ViewId: 1, FromSlot: 0, ToSlot: 0, FetchId: fetchID, SenderIdx: 1,
		Entries: []StateEntry{{
			Slot: 0, ClientId: 1, ReqId: 1, IsBus: true,
			Payload: marshalRequests([]RequestMessage{{ClientId: 1, RequestId: 1, Op: []byte("x")}}),
		}},
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("complete state transfer did not finish StartView installation")
	}
	close(r.fetchQ)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != statusNormal || r.lastNormalView != 1 || r.nextExpected != 1 {
		t.Fatalf("status=%v lastNormalView=%d nextExpected=%d, want normal/1/1",
			r.status, r.lastNormalView, r.nextExpected)
	}
	got := repliesBySlot(t, r, 1)
	if !reflect.DeepEqual(got, map[uint64]int{0: 1}) {
		t.Fatalf("recovery replies by slot=%v, want map[0:1]", got)
	}
}

func TestStartViewRequiresCurrentViewLeader(t *testing.T) {
	r := testReplica(0) // replica 1 leads view 1
	base := BusStartView{ViewId: 1, SenderIdx: 0, SelectedReports: []uint32{0, 1}}
	r.handleStartView(&base)
	base.SenderIdx = 99
	r.handleStartView(&base)
	if n := len(r.startViewQ); n != 0 {
		t.Fatalf("queued %d invalid StartViews", n)
	}
	base.SenderIdx = 1
	r.handleStartView(&base)
	if n := len(r.startViewQ); n != 1 {
		t.Fatalf("queued %d valid StartViews, want 1", n)
	}
}

func TestLaterStartViewResponseRetainsSelectedReports(t *testing.T) {
	r := testReplica(1)
	r.mu.Lock()
	r.viewId.Store(1)
	r.lastNormalView = 1
	r.status = statusNormal
	r.startViewView = 1
	r.startViewUsed = []uint32{0, 1}
	r.mu.Unlock()

	msg := r.startViewMsg()
	if msg == nil {
		t.Fatal("normal leader did not produce StartView")
	}
	if !reflect.DeepEqual(msg.SelectedReports, []uint32{0, 1}) {
		t.Errorf("selected reports=%v, want [0 1]", msg.SelectedReports)
	}
}

// A fetch from a peer whose connection has broken must give up at once. Every
// fetch shares one goroutine, so a range left waiting out stateFetchTimeout on a
// dead peer also holds up the view change queued behind it.
func TestFetchAbandonsDisconnectedPeer(t *testing.T) {
	r := testReplica(0) // no peer ever dialed, so peerWriters[1] is nil

	done := make(chan struct{})
	start := time.Now()
	go func() {
		r.runFetch(fetchReq{peer: 1, from: 0, to: 100})
		close(done)
	}()

	select {
	case <-done:
		if el := time.Since(start); el > time.Second {
			t.Errorf("fetch took %v to give up on a dead peer, want well under %v",
				el, stateFetchTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("fetch from a disconnected peer still blocked after 2s "+
			"(it is waiting out the %v timeout)", stateFetchTimeout)
	}
}

// ── Memory reclamation ──────────────────────────────────────────────────────

func TestPruneReleasesDedup(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	const n = 400 // retainSlots is 64 here
	for s := uint64(0); s < n; s++ {
		busAt(r, s, s+1, s*2+1, s*2+2)
		r.advanceNextExpectedLocked()
		r.setStableLocked(s) // commit point keeps up, so the floor can move
	}
	if len(r.dedup) > 200 {
		t.Errorf("dedup holds %d entries after %d slots; pruning is not releasing them",
			len(r.dedup), n)
	}
	if len(r.globalLog) > 200 {
		t.Errorf("globalLog holds %d slots, want it bounded by the retain window",
			len(r.globalLog))
	}
}

// The cursor normally commits a run of slots at once — a hole fills and
// everything behind it sweeps forward together. Prune has to release those
// slots' dedup entries just the same, which it did not when the range was read
// from the log-index ring: by the time prune saw them they were already older
// than the ring reached, so they were skipped and the map grew without bound.
func TestPruneReleasesDedupAfterBurstAdvance(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	const (
		n     = 600
		burst = 50 // far longer than the two slots of ring margin
	)
	for s := uint64(0); s < n; s++ {
		busAt(r, s, s+1, s*2+1, s*2+2)
		if (s+1)%burst == 0 {
			r.advanceNextExpectedLocked()
			r.setStableLocked(s)
		}
	}
	r.advanceNextExpectedLocked()
	r.setStableLocked(n - 1)
	if r.nextExpected != n {
		t.Fatalf("executed %d slots, want %d", r.nextExpected, n)
	}
	// The retain window is 64 slots of two requests each, plus one burst of
	// slack before the floor catches up.
	if len(r.dedup) > 4*burst {
		t.Errorf("dedup holds %d entries after %d slots committed in bursts of %d",
			len(r.dedup), n, burst)
	}
}

func TestPruneFloorRespectsCommitPoint(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	// The commit point keeps up for a while...
	for s := uint64(0); s < 100; s++ {
		busAt(r, s, s+1, s+1)
		r.advanceNextExpectedLocked()
		r.setStableLocked(s)
	}
	// ...then the leader dies, so it stalls while the cursor runs on. Everything
	// from the commit point up has to stay resident: that is the range a view
	// change rewinds through and merges over.
	for s := uint64(100); s < 400; s++ {
		busAt(r, s, s+1, s+1)
		r.advanceNextExpectedLocked()
	}
	if r.prunedBelow > 99 {
		t.Errorf("pruned below %d, past the commit point at 99", r.prunedBelow)
	}
	if _, ok := r.globalLog[99]; !ok {
		t.Error("the commit point slot itself was reclaimed")
	}
}

// A slot is one bus, and a bus can carry a thousand requests — so the slot
// window alone is a weak memory bound. The byte budget has to close it, without
// ever reclaiming past the commit point.
func TestPruneRespectsByteBudget(t *testing.T) {
	r := testReplica(0)
	r.retainBytes = 8 << 10 // tiny, so it binds well before the 64-slot window
	r.mu.Lock()
	defer r.mu.Unlock()

	big := make([]RequestMessage, 20)
	for i := range big {
		big[i] = RequestMessage{ClientId: 1, RequestId: uint64(i), Op: make([]byte, 512)}
	}
	for s := uint64(0); s < 40; s++ {
		reqs := make([]RequestMessage, len(big))
		copy(reqs, big)
		for i := range reqs {
			reqs[i].RequestId = s*100 + uint64(i)
		}
		r.recordBusReceivedLocked(s, 1, s+1, reqs)
		r.advanceNextExpectedLocked()
		r.setStableLocked(s)
	}
	if r.residentBytes > 4*uint64(r.retainBytes) {
		t.Errorf("resident payload %d bytes against a %d-byte budget",
			r.residentBytes, r.retainBytes)
	}
	if len(r.globalLog) >= 40 {
		t.Errorf("byte budget reclaimed nothing: %d slots still resident", len(r.globalLog))
	}

	// With the commit point stalled the budget must yield, not breach it.
	before := len(r.globalLog)
	for s := uint64(40); s < 80; s++ {
		reqs := make([]RequestMessage, len(big))
		copy(reqs, big)
		for i := range reqs {
			reqs[i].RequestId = s*100 + uint64(i)
		}
		r.recordBusReceivedLocked(s, 1, s+1, reqs)
		r.advanceNextExpectedLocked()
	}
	if len(r.globalLog) <= before {
		t.Error("slots above a stalled commit point were reclaimed anyway")
	}
	if r.prunedBelow > 39 {
		t.Errorf("pruned below %d, past the commit point at 39", r.prunedBelow)
	}
}

// ── Reclaimed slots are not gaps ────────────────────────────────────────────

// runAhead executes n slots with the commit point following, so the retain
// window reclaims everything but its own tail.
func runAhead(r *Replica, n uint64) {
	r.mu.Lock()
	for s := uint64(0); s < n; s++ {
		busAt(r, s, s+1, s+1)
		r.advanceNextExpectedLocked()
		r.setStableLocked(s)
	}
	r.mu.Unlock()
}

// An absent entry reads as slotEmpty whether the slot was never received or
// received, executed and since reclaimed. A peer that falls further behind than
// the retain window reaches asks for the second kind, and answering it as a gap
// makes the leader agree a no-op over a slot it already committed and replied
// on — corruption with no failure anywhere, just a lagging reader.
func TestGapRequestForReclaimedSlotIsNotResolved(t *testing.T) {
	r := testReplica(0)
	runAhead(r, 400) // retainSlots is 64 here, so slot 10 is long gone
	if !r.AmLeader() {
		t.Fatal("replica 0 is not the leader of view 0")
	}

	r.mu.Lock()
	if _, resident := r.globalLog[10]; resident {
		t.Fatal("slot 10 is still resident; the test proves nothing")
	}
	if r.prunedBelow <= 10 {
		t.Fatalf("prune floor is %d, so slot 10 was never reclaimed", r.prunedBelow)
	}
	executedBefore := r.nextExpected
	r.mu.Unlock()

	r.handleGapRequest(&BusGapRequest{Slot: 10, SenderIdx: 1, ViewId: r.view()})

	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.gaps); n != 0 {
		t.Errorf("gap agreement started for a reclaimed slot (%d in flight)", n)
	}
	if e, resurrected := r.globalLog[10]; resurrected {
		t.Errorf("reclaimed slot 10 was resurrected below the prune floor as %v", e.state)
	}
	if r.nextExpected != executedBefore {
		t.Errorf("cursor moved from %d to %d", executedBefore, r.nextExpected)
	}
}

// The same slot arriving as an already-decided no-op, from a leader that
// resolved it while we were ahead of it. Applying it would overwrite settled
// history, and acking it would help it reach quorum, so neither may happen.
func TestGapCommitBelowCursorIsIgnoredAndUnacked(t *testing.T) {
	r := testReplica(1)
	runAhead(r, 400)

	r.mu.Lock()
	executedBefore, noopsBefore := r.nextExpected, r.winNoops
	r.mu.Unlock()

	r.handleGapCommit(&BusGapCommit{Slot: 10, SenderIdx: 0, ViewId: r.view()})

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, resurrected := r.globalLog[10]; resurrected {
		t.Error("a stale no-op commit wrote over a reclaimed, already-executed slot")
	}
	if r.winNoops != noopsBefore {
		t.Errorf("no-ops went %d -> %d; the commit was applied", noopsBefore, r.winNoops)
	}
	if r.nextExpected != executedBefore {
		t.Errorf("cursor moved from %d to %d", executedBefore, r.nextExpected)
	}
}

// The backstop under every caller: setNoOpLocked allocates through
// slotEntryLocked, so without the guard a no-op below the cursor both rewrites a
// committed slot and leaves a phantom entry beneath the prune floor that nothing
// will ever visit again.
func TestSetNoOpRefusesBelowCursor(t *testing.T) {
	r := testReplica(0)
	runAhead(r, 400)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.setNoOpLocked(10) {
		t.Error("setNoOpLocked accepted a slot below the executed frontier")
	}
	if _, resurrected := r.globalLog[10]; resurrected {
		t.Error("setNoOpLocked allocated a phantom entry below the prune floor")
	}
	// Still allowed above it, where a slot genuinely may be missing.
	if !r.setNoOpLocked(r.nextExpected + 5) {
		t.Error("setNoOpLocked refused a genuine gap above the cursor")
	}
}

// ── No-op agreement ─────────────────────────────────────────────────────────

func testReplicaN(idx, n, f int) *Replica {
	cfg := &Config{N: n, F: f}
	for i := 0; i < n; i++ {
		cfg.Replicas = append(cfg.Replicas, fmt.Sprintf("r%d:%d", i, 1000+i))
	}
	r := NewReplica(cfg, idx, "", "", dropNone, 0, 0, RecoveryOptions{RetainSlots: 64})
	r.clients[1] = &clientLine{baseNs: 0, intervalNs: ms}
	r.busMode = true
	return r
}

func setNormalViewForTest(r *Replica, view uint64) {
	r.mu.Lock()
	r.viewId.Store(view)
	r.lastNormalView = view
	r.status = statusNormal
	r.mu.Unlock()
}

func TestGapMessagesRoundTripView(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		want := &BusGapRequest{Slot: 9, SenderIdx: 2, ViewId: 7}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusGapRequest
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})

	t.Run("reply", func(t *testing.T) {
		want := &BusGapReply{Slot: 9, SenderIdx: 2, ViewId: 7, Found: true, Bus: true, Op: []byte("bus")}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusGapReply
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})

	t.Run("commit reply", func(t *testing.T) {
		want := &BusGapCommitReply{Slot: 9, SenderIdx: 2, ViewId: 7}
		var wire bytes.Buffer
		want.Marshal(&wire)
		var got BusGapCommitReply
		if err := got.Unmarshal(&wire); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, want) {
			t.Fatalf("round trip = %+v, want %+v", got, *want)
		}
	})
}

func TestGapCommitRequiresCurrentViewLeader(t *testing.T) {
	r := testReplica(2)
	setNormalViewForTest(r, 1) // replica 1 leads view 1

	r.handleGapCommit(&BusGapCommit{Slot: 0, SenderIdx: 0, ViewId: 0})
	r.handleGapCommit(&BusGapCommit{Slot: 0, SenderIdx: 0, ViewId: 1})
	r.handleGapCommit(&BusGapCommit{Slot: 0, SenderIdx: 99, ViewId: 1})
	r.mu.Lock()
	if st := r.slotStateLocked(0); st != slotEmpty {
		r.mu.Unlock()
		t.Fatalf("stale/non-leader commit changed slot to %v", st)
	}
	r.mu.Unlock()

	r.handleGapCommit(&BusGapCommit{Slot: 0, SenderIdx: 1, ViewId: 1})
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.slotStateLocked(0); st != slotNoOp {
		t.Fatalf("current leader commit left slot in state %v", st)
	}
}

func TestGapRequestAndReplyRequireMatchingView(t *testing.T) {
	r := testReplica(0)
	r.handleGapRequest(&BusGapRequest{Slot: 7, SenderIdx: 1, ViewId: 1})
	r.mu.Lock()
	if len(r.gaps) != 0 {
		r.mu.Unlock()
		t.Fatal("future-view request created current-view gap state")
	}
	key := gapKey{view: 0, slot: 7}
	gs := newGapState(nowNs(), key.view)
	r.gaps[key] = gs
	r.mu.Unlock()

	r.handleGapReply(&BusGapReply{Slot: 7, SenderIdx: 1, ViewId: 1})
	select {
	case reply := <-gs.probeReplies:
		t.Fatalf("mismatched reply from view %d reached view 0", reply.ViewId)
	default:
	}

	r.handleGapReply(&BusGapReply{Slot: 7, SenderIdx: 1, ViewId: 0})
	select {
	case reply := <-gs.probeReplies:
		if reply.ViewId != 0 {
			t.Fatalf("matching reply carried view %d", reply.ViewId)
		}
	default:
		t.Fatal("matching reply did not reach the active gap")
	}
}

func TestLeaderResolvesGapAfterDistinctNotFoundReplies(t *testing.T) {
	r := testReplicaN(0, 3, 1)
	key := gapKey{view: r.view(), slot: 0}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()
	t.Cleanup(gs.cancel)

	done := make(chan struct{})
	go func() {
		r.leaderResolve(key, gs)
		close(done)
	}()

	// A retransmitting replica still counts only once toward the all-missing
	// result. Queue the commit ACK now so an incorrectly early NO-OP would let
	// leaderResolve return and make the duplicate-counting bug observable.
	r.handleGapReply(&BusGapReply{Slot: key.slot, SenderIdx: 1, ViewId: key.view})
	r.handleGapReply(&BusGapReply{Slot: key.slot, SenderIdx: 1, ViewId: key.view})
	r.handleGapCommitReply(&BusGapCommitReply{Slot: key.slot, SenderIdx: 1, ViewId: key.view})
	select {
	case <-done:
		t.Fatal("duplicate not-found replies triggered a NO-OP")
	case <-time.After(50 * time.Millisecond):
	}

	// The other peer's distinct not-found reply completes n-1 immediately;
	// leaderResolve must not wait for the three-second recovery timeout.
	r.handleGapReply(&BusGapReply{Slot: key.slot, SenderIdx: 2, ViewId: key.view})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("all distinct not-found replies did not resolve the gap early")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.slotStateLocked(key.slot); st != slotNoOp {
		t.Fatalf("all peers reported not found, slot state = %v, want NO-OP", st)
	}
}

func TestGapCommitReplyRequiresActiveMatchingView(t *testing.T) {
	r := testReplica(1)
	setNormalViewForTest(r, 1) // this replica leads view 1
	key := gapKey{view: 1, slot: 7}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()

	r.handleGapCommitReply(&BusGapCommitReply{Slot: 7, SenderIdx: 2, ViewId: 0})
	r.handleGapCommitReply(&BusGapCommitReply{Slot: 8, SenderIdx: 2, ViewId: 1})
	select {
	case idx := <-gs.commitAcks:
		t.Fatalf("mismatched ACK from replica %d reached the active gap", idx)
	default:
	}

	r.handleGapCommitReply(&BusGapCommitReply{Slot: 7, SenderIdx: 2, ViewId: 1})
	select {
	case idx := <-gs.commitAcks:
		if idx != 2 {
			t.Fatalf("ACK sender = %d, want 2", idx)
		}
	default:
		t.Fatal("matching ACK did not reach the active gap")
	}

	r.mu.Lock()
	r.finishGapLocked(key, gs)
	r.mu.Unlock()
	r.handleGapCommitReply(&BusGapCommitReply{Slot: 7, SenderIdx: 2, ViewId: 1})
	select {
	case idx := <-gs.commitAcks:
		t.Fatalf("late ACK from replica %d reached a finished gap", idx)
	default:
	}
}

func TestViewChangeCancelsGapCollector(t *testing.T) {
	r := testReplica(0)
	key := gapKey{view: 0, slot: 7}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()

	done := make(chan bool, 1)
	go func() { done <- r.collectNoOpQuorum(key, gs) }()
	r.startViewChange(1)

	select {
	case ok := <-done:
		if ok {
			t.Fatal("old-view collector reported a quorum after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("old-view collector did not stop on view change")
	}
	if !gs.cancelled() {
		t.Fatal("view change did not cancel the old gap state")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.gaps) != 0 {
		t.Fatalf("view change retained %d old gap states", len(r.gaps))
	}
}

func TestViewChangeCancelsGapResolutionBeforeNoOp(t *testing.T) {
	r := testReplica(0)
	key := gapKey{view: 0, slot: 0}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.leaderResolve(key, gs)
		close(done)
	}()
	r.startViewChange(1)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old-view gap resolution did not stop on view change")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.slotStateLocked(0); st != slotEmpty {
		t.Fatalf("cancelled resolution installed %v in the new view", st)
	}
	if r.winNoops != 0 {
		t.Fatalf("cancelled resolution recorded %d no-ops", r.winNoops)
	}
}

func TestOldGapCannotDeleteReplacementState(t *testing.T) {
	r := testReplica(0)
	key := gapKey{view: 0, slot: 7}
	old := newGapState(nowNs(), key.view)
	replacement := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = replacement
	r.finishGapLocked(key, old)
	got := r.gaps[key]
	r.mu.Unlock()
	if got != replacement {
		t.Fatal("an old gap goroutine deleted replacement state")
	}
}

func TestSyncCommitRequiresCurrentLeader(t *testing.T) {
	r := testReplica(2)
	r.mu.Lock()
	busAt(r, 0, 1, 1)
	r.advanceNextExpectedLocked()
	r.mu.Unlock()

	r.handleSyncCommit(&BusSyncCommit{ViewId: 0, StableSlot: 0, SenderIdx: 1})
	r.mu.Lock()
	if r.haveStable {
		r.mu.Unlock()
		t.Fatal("non-leader advanced the stable frontier")
	}
	r.mu.Unlock()

	r.handleSyncCommit(&BusSyncCommit{ViewId: 0, StableSlot: 0, SenderIdx: 0})
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.haveStable || r.stableSlot != 0 {
		t.Fatalf("leader commit produced stable=%d/%v, want 0/true", r.stableSlot, r.haveStable)
	}
}

// One reachable follower acks once per round, so counting acks rather than
// distinct senders would let it supply a whole quorum by itself.
func TestNoOpQuorumCountsDistinctReplicas(t *testing.T) {
	r := testReplicaN(0, 5, 2) // quorum 3: the leader plus two others
	key := gapKey{view: r.view(), slot: 7}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()

	done := make(chan bool, 1)
	go func() { done <- r.collectNoOpQuorum(key, gs) }()

	gs.commitAcks <- 3
	gs.commitAcks <- 3 // same replica, next round
	select {
	case <-done:
		t.Fatal("one replica acking twice was counted as a quorum")
	case <-time.After(50 * time.Millisecond):
	}

	gs.commitAcks <- 4
	select {
	case ok := <-done:
		if !ok {
			t.Error("quorum of distinct replicas did not complete the round")
		}
	case <-time.After(time.Second):
		t.Fatal("two distinct acks did not satisfy a quorum of 3")
	}
}

type retrySignalWriter struct {
	writes chan struct{}
}

func (w *retrySignalWriter) Write(p []byte) (int, error) {
	w.writes <- struct{}{}
	return len(p), nil
}

func TestNoOpCommitUsesConfiguredRetryTimeout(t *testing.T) {
	r := testReplicaN(0, 3, 1)
	if r.gapRetryTimeout != 1500*time.Millisecond {
		t.Fatalf("default gap retry timeout = %v, want 1.5s", r.gapRetryTimeout)
	}
	r = NewReplica(r.config, 0, "", "", dropNone, 0, 0,
		RecoveryOptions{RetainSlots: 64, GapRetryTimeoutMs: 10})
	if r.gapRetryTimeout != 10*time.Millisecond {
		t.Fatalf("configured gap retry timeout = %v, want 10ms", r.gapRetryTimeout)
	}

	w := &retrySignalWriter{writes: make(chan struct{}, 8)}
	r.mu.Lock()
	r.peerWriters[1] = &lockedWriter{w: bufio.NewWriter(w)}
	r.mu.Unlock()

	key := gapKey{view: r.view(), slot: 7}
	gs := newGapState(nowNs(), key.view)
	r.mu.Lock()
	r.gaps[key] = gs
	r.mu.Unlock()
	done := make(chan bool, 1)
	go func() { done <- r.collectNoOpQuorum(key, gs) }()

	select {
	case <-w.writes: // initial broadcast
	case <-time.After(time.Second):
		t.Fatal("initial gap commit was not broadcast")
	}
	select {
	case <-w.writes: // timeout-driven rebroadcast
	case <-time.After(250 * time.Millisecond):
		t.Fatal("gap commit was not rebroadcast at the configured timeout")
	}

	gs.commitAcks <- 1
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("collector abandoned a valid quorum")
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not finish after the retry was acknowledged")
	}
}

func TestBusIntakeContinuesWhileNoOpQuorumPending(t *testing.T) {
	r := testReplica(0)
	key := gapKey{view: r.view(), slot: 0}
	gs := newGapState(nowNs(), key.view)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaps[key] = gs

	// Slot 1 arrives while slot 0 is still being resolved. Retain it in its
	// assigned slot instead of buffering it behind the agreement goroutine.
	r.applyDuringRecoveryLocked(&BusMessage{
		ClientId:  1,
		BusSeqNum: 2,
		Requests: []RequestMessage{{
			ClientId: 1, RequestId: 2, Op: []byte("later"),
		}},
	})
	if st := r.slotStateLocked(1); st != slotReceived {
		t.Fatalf("later bus state = %v, want received", st)
	}
	if r.nextExpected != 0 {
		t.Fatalf("cursor advanced through the unresolved hole to %d", r.nextExpected)
	}
	if len(r.pendingBuses) != 0 {
		t.Fatalf("buffered %d buses behind gap agreement", len(r.pendingBuses))
	}

	// The leader installs its speculative no-op before collecting follower
	// acks. That fills slot 0 and releases slot 1 even though the gap state—and
	// therefore the quorum collector—is still live.
	if !r.applyGapCommitLocked(0) {
		t.Fatal("leader's speculative no-op was not installed")
	}
	if r.nextExpected != 2 {
		t.Fatalf("cursor = %d after filling the hole, want 2", r.nextExpected)
	}
	if r.gaps[key] != gs {
		t.Fatal("gap agreement ended before its ack quorum completed")
	}

	// New traffic also keeps advancing while the same gap waits for its acks.
	r.applyDuringRecoveryLocked(&BusMessage{
		ClientId:  1,
		BusSeqNum: 3,
		Requests: []RequestMessage{{
			ClientId: 1, RequestId: 3, Op: []byte("newer"),
		}},
	})
	if r.nextExpected != 3 {
		t.Fatalf("new bus blocked by pending no-op quorum; cursor = %d, want 3", r.nextExpected)
	}
}

// Installing the no-op moves the cursor past the slot, so every retransmitted
// round arrives at a slot the follower has already executed. It must still ack:
// a follower that goes silent after the first round leaves a leader whose ack
// was lost retrying forever.
func TestGapCommitIsIdempotentAcrossRounds(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	busAt(r, 0, 1, 1)
	r.advanceNextExpectedLocked()

	if !r.applyGapCommitLocked(1) {
		t.Fatal("first round was not acked")
	}
	if r.nextExpected != 2 {
		t.Fatalf("cursor at %d after the no-op filled slot 1, want 2", r.nextExpected)
	}
	noops := r.winNoops

	for round := 2; round <= 4; round++ {
		if !r.applyGapCommitLocked(1) {
			t.Errorf("round %d was not acked; the leader would retry forever", round)
		}
	}
	if r.winNoops != noops {
		t.Errorf("no-ops counted %d -> %d; a repeat round was applied twice",
			noops, r.winNoops)
	}
	if r.nextExpected != 2 {
		t.Errorf("cursor moved to %d on a repeat round", r.nextExpected)
	}

	// A slot that executed as a real bus is still refused, and still silently.
	if r.applyGapCommitLocked(0) {
		t.Error("acked a no-op over a slot that executed a real bus")
	}
}

// ── Durable log round trip ──────────────────────────────────────────────────

func TestDurableLogReadBack(t *testing.T) {
	dir := t.TempDir()
	r := testReplica(0)
	busLog, err := openDurableLog(dir, "replica.log")
	if err != nil {
		t.Fatal(err)
	}
	reqLog, err := openDurableLog(dir, "requestlist.log")
	if err != nil {
		t.Fatal(err)
	}
	r.durable, r.reqListLog = busLog, reqLog

	r.mu.Lock()
	for s := uint64(0); s < 40; s++ {
		busAt(r, s, s+1, s*3+1, s*3+2, s*3+3)
		r.advanceNextExpectedLocked()
	}
	r.setStableLocked(39)
	r.mu.Unlock()

	for _, slot := range []uint64{0, 7, 39} {
		ent, ok := r.readSlotFromDisk(slot)
		if !ok {
			t.Fatalf("slot %d not readable from disk", slot)
		}
		if !ent.IsBus || ent.IsNoOp {
			t.Fatalf("slot %d came back as bus=%v noop=%v", slot, ent.IsBus, ent.IsNoOp)
		}
		if ent.ClientId != 1 || ent.ReqId != slot+1 {
			t.Errorf("slot %d owner = c%d#%d, want c1#%d", slot, ent.ClientId, ent.ReqId, slot+1)
		}
		reqs, err := unmarshalRequests(ent.Payload)
		if err != nil {
			t.Fatalf("slot %d payload: %v", slot, err)
		}
		if len(reqs) != 3 {
			t.Fatalf("slot %d has %d passengers, want 3", slot, len(reqs))
		}
		for i, req := range reqs {
			wantId := slot*3 + uint64(i) + 1
			if req.ClientId != 1 || req.RequestId != wantId {
				t.Errorf("slot %d passenger %d = c%d#%d, want c1#%d",
					slot, i, req.ClientId, req.RequestId, wantId)
			}
			if string(req.Op) != "x" {
				t.Errorf("slot %d passenger %d op = %q, want \"x\"", slot, i, req.Op)
			}
		}
	}

	// A slot reclaimed from memory is still served, now from disk.
	r.mu.Lock()
	delete(r.globalLog, 7)
	r.prunedBelow = 8
	r.mu.Unlock()
	if _, ok := r.readSlot(7); !ok {
		t.Error("readSlot did not fall back to the durable log for a reclaimed slot")
	}

	busLog.close()
	reqLog.close()
	if _, err := os.Stat(dir + "/replica.log"); err != nil {
		t.Fatal(err)
	}
}

// ── The view-change request quorum ──────────────────────────────────────────
//
// A replica must not send its BusViewChange on its own suspicion: it waits
// until a quorum of replicas has asked for the view. testReplica has no peer
// connections, and sendToPeer is a no-op without one, so these drive the gate
// directly and read the decision off vcState.

func vcRequestsSent(r *Replica) (n int, sent bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.vc == nil {
		return 0, false
	}
	return len(r.vc.requests), r.vc.reportSent
}

func cleanupViewChangeTest(t *testing.T, r *Replica) {
	t.Helper()
	t.Cleanup(func() {
		r.mu.Lock()
		if r.vc != nil {
			close(r.vc.abort)
			r.vc = nil
		}
		r.cancelRecoveryLocked()
		r.cancelViewChangeWatchdogLocked()
		r.mu.Unlock()
	})
}

func currentViewChangeWatchdog(t *testing.T, r *Replica) *viewChangeWatchdog {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.viewChangeWatchdog == nil {
		t.Fatal("view-change fallback is not armed")
	}
	return r.viewChangeWatchdog
}

func TestViewChangeFallbackDefaultsAndConfiguration(t *testing.T) {
	r := testReplica(0)
	if r.viewChangeTimeout != 15*time.Second {
		t.Fatalf("default leader report timeout = %v, want 15s", r.viewChangeTimeout)
	}
	if r.viewChangeFallbackTimeout != 20*time.Second {
		t.Fatalf("default view-change fallback = %v, want 20s", r.viewChangeFallbackTimeout)
	}

	r = NewReplica(r.config, 0, "", "", dropNone, 0, 0, RecoveryOptions{
		RetainSlots:                 64,
		ViewChangeTimeoutMs:         7,
		ViewChangeFallbackTimeoutMs: 11,
	})
	if r.viewChangeTimeout != 7*time.Millisecond ||
		r.viewChangeFallbackTimeout != 11*time.Millisecond {
		t.Fatalf("configured timeouts = leader %v fallback %v, want 7ms/11ms",
			r.viewChangeTimeout, r.viewChangeFallbackTimeout)
	}
}

func TestViewChangeFallbackStartsOnEveryReplica(t *testing.T) {
	for idx := 0; idx < 3; idx++ {
		t.Run(fmt.Sprintf("replica-%d", idx), func(t *testing.T) {
			r := testReplica(idx)
			cleanupViewChangeTest(t, r)
			r.startViewChange(1)

			watchdog := currentViewChangeWatchdog(t, r)
			r.mu.Lock()
			view, status := r.view(), r.status
			r.mu.Unlock()
			if view != 1 || status != statusViewChange || watchdog.view != 1 {
				t.Fatalf("view=%d status=%v watchdog.view=%d, want view-change/1 with watchdog/1",
					view, status, watchdog.view)
			}
		})
	}
}

func TestAcceptedStartViewResetsFallback(t *testing.T) {
	r := testReplica(0) // replica 1 leads view 1
	cleanupViewChangeTest(t, r)
	r.startViewChange(1)
	old := currentViewChangeWatchdog(t, r)

	// Receipt only queues the valid message. Acceptance in installStartView is
	// the point that replaces the watchdog with a fresh full interval.
	r.handleStartView(&BusStartView{ViewId: 1, SenderIdx: 1, MaxSlot: 0, HasMax: true})
	if got := currentViewChangeWatchdog(t, r); got != old {
		t.Fatal("StartView receipt reset the fallback before install acceptance")
	}
	msg := <-r.startViewQ
	done := make(chan struct{})
	go func() {
		r.installStartView(msg)
		close(done)
	}()

	// The missing canonical slot makes the installer publish recovery and block
	// on its fetch, giving the test a deterministic acceptance signal.
	<-r.fetchQ
	fresh := currentViewChangeWatchdog(t, r)
	if fresh == old || fresh.view != 1 || fresh.generation <= old.generation {
		t.Fatalf("watchdog was not freshly rearmed: old=%p/%d fresh=%p/%d view=%d",
			old, old.generation, fresh, fresh.generation, fresh.view)
	}

	// Even if the replaced timer's callback was already queued, its identity is
	// stale and cannot consume the fresh interval.
	r.expireViewChangeWatchdog(old)
	if got := currentViewChangeWatchdog(t, r); got != fresh || r.view() != 1 {
		t.Fatal("replaced fallback advanced or disturbed the accepted StartView")
	}

	// Expiring the active fallback cancels the blocked install through rec.abort.
	r.expireViewChangeWatchdog(fresh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fallback did not cancel the active StartView install")
	}
}

func TestRejectedStartViewsDoNotResetFallback(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	r.startViewChange(1)
	want := currentViewChangeWatchdog(t, r)

	// Invalid sender for current view.
	r.handleStartView(&BusStartView{ViewId: 1, SenderIdx: 0})
	// Valid leader for an already stale view.
	r.handleStartView(&BusStartView{ViewId: 0, SenderIdx: 0})

	// A current-view StartView is a duplicate while that view's installation is
	// already active.
	r.mu.Lock()
	r.recovery = &viewRecovery{
		view: 1, generation: 1, leader: 1, abort: make(chan struct{}),
	}
	r.mu.Unlock()
	r.handleStartView(&BusStartView{ViewId: 1, SenderIdx: 1})

	if got := currentViewChangeWatchdog(t, r); got != want {
		t.Fatalf("rejected StartView replaced watchdog %p with %p", want, got)
	}
	if n := len(r.startViewQ); n != 0 {
		t.Fatalf("queued %d invalid/stale/duplicate StartViews", n)
	}
}

func TestViewChangeHeartbeatDoesNotResetFallback(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	r.startViewChange(1)
	want := currentViewChangeWatchdog(t, r)

	// The current leader's sync traffic may update heartbeat bookkeeping, but
	// ViewChange liveness belongs exclusively to the fallback watchdog.
	r.handleSyncPrepare(&BusSyncPrepare{ViewId: 1, SenderIdx: 1})
	r.handleSyncCommit(&BusSyncCommit{ViewId: 1, SenderIdx: 1})
	if got := currentViewChangeWatchdog(t, r); got != want {
		t.Fatalf("heartbeat traffic replaced watchdog %p with %p", want, got)
	}
}

func TestRecoveryCompletionCancelsFallbackAndFencesStaleExpiry(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	r.startViewChange(1)
	watchdog := currentViewChangeWatchdog(t, r)

	r.mu.Lock()
	if r.vc != nil {
		close(r.vc.abort)
		r.vc = nil
	}
	r.recoveryGen++
	rec := &viewRecovery{
		view: 1, generation: r.recoveryGen, leader: 1, abort: make(chan struct{}),
	}
	r.recovery = rec
	r.mu.Unlock()

	if !r.finishRecoveryIfComplete(rec) {
		t.Fatal("empty canonical range did not complete recovery")
	}
	r.mu.Lock()
	status, current := r.status, r.viewChangeWatchdog
	r.mu.Unlock()
	if status != statusNormal || current != nil {
		t.Fatalf("completion published status=%v watchdog=%p, want normal/nil", status, current)
	}

	// A callback that raced Timer.Stop must observe the cleared identity and may
	// not move a now-Normal replica.
	r.expireViewChangeWatchdog(watchdog)
	if view := r.view(); view != 1 {
		t.Fatalf("stale fallback advanced Normal replica to view %d", view)
	}
}

func TestNewerViewReplacesAndFencesOldFallback(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	r.startViewChange(1)
	old := currentViewChangeWatchdog(t, r)
	r.startViewChange(3)
	fresh := currentViewChangeWatchdog(t, r)
	if fresh == old || fresh.view != 3 {
		t.Fatalf("newer view retained old watchdog: old=%p fresh=%p fresh.view=%d",
			old, fresh, fresh.view)
	}

	r.expireViewChangeWatchdog(old)
	if view := r.view(); view != 3 {
		t.Fatalf("old fallback advanced newer view to %d", view)
	}
	if got := currentViewChangeWatchdog(t, r); got != fresh {
		t.Fatal("old fallback replaced the newer view's watchdog")
	}
}

func TestFallbackExpiryAdvancesAndBroadcastsViewChangeRequest(t *testing.T) {
	r := testReplica(0)
	cleanupViewChangeTest(t, r)
	var peers [3]bytes.Buffer
	r.mu.Lock()
	for idx := 1; idx < 3; idx++ {
		r.peerWriters[idx] = &lockedWriter{w: bufio.NewWriter(&peers[idx])}
	}
	r.mu.Unlock()

	r.startViewChange(1)
	oldVC := r.vc
	for idx := 1; idx < 3; idx++ {
		peers[idx].Reset() // discard view 1's ordinary request
	}
	r.expireViewChangeWatchdog(currentViewChangeWatchdog(t, r))

	if view := r.view(); view != 2 {
		t.Fatalf("fallback advanced to view %d, want 2", view)
	}
	select {
	case <-oldVC.abort:
	default:
		t.Fatal("fallback did not cancel the old view-change work")
	}
	for idx := 1; idx < 3; idx++ {
		code, err := peers[idx].ReadByte()
		if err != nil {
			t.Fatalf("peer %d received no fallback broadcast: %v", idx, err)
		}
		if code != MsgBusViewChangeRequest {
			t.Fatalf("peer %d message code=%d, want view-change request %d",
				idx, code, MsgBusViewChangeRequest)
		}
		var msg BusViewChangeRequest
		if err := msg.Unmarshal(&peers[idx]); err != nil {
			t.Fatalf("peer %d bad request: %v", idx, err)
		}
		if msg.ViewId != 2 || msg.SenderIdx != 0 {
			t.Fatalf("peer %d request=%+v, want view=2 sender=0", idx, msg)
		}
	}
}

func TestConfiguredFallbackTimerExpires(t *testing.T) {
	cfg := &Config{N: 3, F: 1, Replicas: []string{"a:1", "b:2", "c:3"}}
	r := NewReplica(cfg, 0, "", "", dropNone, 0, 0, RecoveryOptions{
		RetainSlots:                 64,
		ViewChangeFallbackTimeoutMs: 10,
	})
	cleanupViewChangeTest(t, r)
	writes := make(chan struct{}, 4)
	r.mu.Lock()
	r.peerWriters[1] = &lockedWriter{w: bufio.NewWriter(&retrySignalWriter{writes: writes})}
	r.mu.Unlock()

	r.startViewChange(1)
	<-writes // view 1 request
	select {
	case <-writes: // fallback's view 2 request
	case <-time.After(time.Second):
		t.Fatal("configured fallback timer did not expire")
	}
	if view := r.view(); view != 2 {
		t.Fatalf("configured fallback ended in view %d, want 2", view)
	}
}

func TestLeaderReportDeadlineRemainsSeparateFromFallback(t *testing.T) {
	cfg := &Config{N: 3, F: 1, Replicas: []string{"a:1", "b:2", "c:3"}}
	r := NewReplica(cfg, 1, "", "", dropNone, 0, 0, RecoveryOptions{
		RetainSlots:                 64,
		ViewChangeTimeoutMs:         10,
		ViewChangeFallbackTimeoutMs: 60_000,
	})
	cleanupViewChangeTest(t, r)
	r.startViewChange(1) // replica 1 is the designated leader for view 1

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for r.view() != 2 {
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("leader report deadline did not advance view 1")
		}
	}
	watchdog := currentViewChangeWatchdog(t, r)
	if watchdog.view != 2 {
		t.Fatalf("leader deadline did not replace fallback for view 2: %+v", watchdog)
	}
}

func TestViewChangeReportWaitsForRequestQuorum(t *testing.T) {
	r := testReplica(0) // view 1's leader is replica 1, so this one is a follower
	r.startViewChange(1)

	if n, sent := vcRequestsSent(r); n != 1 || sent {
		t.Fatalf("after own suspicion: requests=%d sent=%v, want 1 and not sent", n, sent)
	}
	if r.status != statusViewChange || r.view() != 1 {
		t.Fatalf("status=%v view=%d, want viewchange in view 1", r.status, r.view())
	}

	// The second request completes the quorum (f+1 = 2), and only now may the
	// suffix report go out.
	r.handleViewChangeRequest(&BusViewChangeRequest{ViewId: 1, SenderIdx: 2})
	if n, sent := vcRequestsSent(r); n != 2 || !sent {
		t.Fatalf("after peer request: requests=%d sent=%v, want 2 and sent", n, sent)
	}
}

func TestViewChangeRequestFromPeerReachesQuorumImmediately(t *testing.T) {
	// A replica woken by someone else's request already has two: its own and the
	// waker's. It must not wait another half round trip for a third.
	r := testReplica(0)
	r.handleViewChangeRequest(&BusViewChangeRequest{ViewId: 1, SenderIdx: 2})

	if n, sent := vcRequestsSent(r); n != 2 || !sent {
		t.Fatalf("woken by peer: requests=%d sent=%v, want 2 and sent", n, sent)
	}
}

func TestViewChangeReportSentOnlyOnce(t *testing.T) {
	r := testReplica(0)
	r.startViewChange(1)
	r.handleViewChangeRequest(&BusViewChangeRequest{ViewId: 1, SenderIdx: 2})

	// Re-delivery of the same request must neither inflate the quorum count nor
	// clear the latch that stops a second report going out.
	r.handleViewChangeRequest(&BusViewChangeRequest{ViewId: 1, SenderIdx: 2})
	if n, sent := vcRequestsSent(r); n != 2 || !sent {
		t.Fatalf("duplicate request: requests=%d sent=%v, want 2 and still sent", n, sent)
	}
}

func TestStaleViewChangeRequestIgnored(t *testing.T) {
	r := testReplica(0)
	r.startViewChange(2)
	r.handleViewChangeRequest(&BusViewChangeRequest{ViewId: 1, SenderIdx: 2})
	if n, sent := vcRequestsSent(r); n != 1 || sent {
		t.Fatalf("stale request counted: requests=%d sent=%v, want 1 and not sent", n, sent)
	}
}

// ── The failure detector ────────────────────────────────────────────────────
//
// Suspicion must come from missing heartbeats and nothing else. A broken socket
// to the leader is a local, one-sided observation — a partition raises it
// without a crash, and a crash that kills the machine or its power never raises
// it at all — so acting on it would make detection fast only for the failures
// that happen to close their sockets politely.

func suspicionFires(r *Replica, within time.Duration) bool {
	go r.suspicionLoop()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if r.view() > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestClosedLeaderSocketAloneDoesNotSuspect(t *testing.T) {
	r := testReplica(1) // view 0's leader is replica 0, so this one is a follower
	r.suspectTimeout = time.Hour
	r.status = statusNormal
	r.mu.Lock()
	r.lastHeartbeatNs = nowNs()
	r.mu.Unlock()

	// The leader's connection breaks. On its own this must change nothing.
	r.retirePeer(0, nil)
	r.mu.Lock()
	lost := r.leaderLost
	r.mu.Unlock()
	if !lost {
		t.Fatal("retirePeer did not record the lost leader connection")
	}
	if suspicionFires(r, 50*time.Millisecond) {
		t.Fatal("a closed leader socket started a view change; only the heartbeat timeout may")
	}
}

func TestSilentLeaderSuspectedOnTimeout(t *testing.T) {
	r := testReplica(1)
	r.suspectTimeout = 20 * time.Millisecond
	r.status = statusNormal
	r.mu.Lock()
	r.lastHeartbeatNs = nowNs()
	r.mu.Unlock()

	// No socket close at all — the leader has simply gone quiet, which is what a
	// frozen process or a partition looks like from here.
	if !suspicionFires(r, 2*time.Second) {
		t.Fatal("a silent leader was never suspected")
	}
	r.mu.Lock()
	view, status := r.view(), r.status
	r.mu.Unlock()
	if view != 1 || status != statusViewChange {
		t.Fatalf("view=%d status=%v, want view 1 in viewchange", view, status)
	}
}

func TestPreStartViewChangeDoesNotResuspectSilentLeader(t *testing.T) {
	r := testReplica(0) // replica 1 leads view 1
	r.suspectTimeout = 20 * time.Millisecond
	r.startViewChange(1)

	go r.suspicionLoop()
	time.Sleep(50 * time.Millisecond)
	if view := r.view(); view != 1 {
		t.Fatalf("pre-StartView view change advanced to view %d, want view 1", view)
	}
}

func TestReplicaInstallingStartViewIgnoresSuspectTimeout(t *testing.T) {
	r := testReplica(1) // view 0's leader is replica 0
	r.suspectTimeout = 20 * time.Millisecond
	abort := make(chan struct{})
	r.mu.Lock()
	r.status = statusViewChange
	r.recovery = &viewRecovery{
		view: 0, generation: 1, leader: 0, abort: abort,
	}
	r.lastHeartbeatNs = nowNs()
	r.mu.Unlock()

	// Make the heartbeat old enough to trigger immediately if the old special
	// recovery suspicion path still exists. The atomic check is deterministic;
	// no suspicion-loop sleep is needed.
	r.mu.Lock()
	r.lastHeartbeatNs = nowNs() - int64(time.Second)
	r.mu.Unlock()
	if r.suspectLeaderIfTimedOut() {
		t.Fatal("suspect timeout advanced an active StartView install")
	}
	r.mu.Lock()
	view, status := r.view(), r.status
	r.mu.Unlock()
	if view != 0 || status != statusViewChange {
		t.Fatalf("view=%d status=%v, want view 0 still in viewchange", view, status)
	}
	select {
	case <-abort:
		t.Fatal("suspect timeout canceled the active StartView recovery")
	default:
	}
}
