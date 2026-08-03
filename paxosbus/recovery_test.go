package paxosbus

import (
	"os"
	"reflect"
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
	rewound, did := r.installViewLocked(1, 1, true, []uint64{2}, false)
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

	rewound, did := r.installViewLocked(2, 6, true, nil, true)
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

// A replica keeps executing for as long as it takes to notice the leader died,
// and those replies name a leader that will never answer — the client's quorum
// rule discards them. Installing the new view has to say them again, or the
// requests sit stranded until the client's own request timeout re-boards them.
func TestInstallViewReplaysRepliesAboveCommitPoint(t *testing.T) {
	r := testReplica(0)
	r.mu.Lock()
	defer r.mu.Unlock()

	for s := uint64(0); s < 6; s++ {
		busAt(r, s, s+1, s*10+1, s*10+2)
	}
	r.advanceNextExpectedLocked() // executed under the old leader, replies in view 0
	r.setStableLocked(2)
	r.pendingReplies = nil // the view-0 replies; the client could never count them

	// A bus that arrived while the cursor was frozen, so this replica has
	// recorded it but never executed or replied for it.
	r.status = statusViewChange
	busAt(r, 6, 7, 61, 62)

	r.installViewLocked(1, 2, true, nil, false)

	got := repliesBySlot(t, r, 1)
	want := map[uint64]int{3: 2, 4: 2, 5: 2, 6: 2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replies by slot = %v, want %v", got, want)
	}
	if r.nextExpected != 7 {
		t.Errorf("cursor at %d, want 7", r.nextExpected)
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
	r.installViewLocked(1, 2, true, []uint64{4}, false)

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

	r.installViewLocked(1, 3, true, nil, false)

	if n := len(r.pendingReplies); n != 0 {
		t.Errorf("%d replies queued for a fully committed log, want none", n)
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
