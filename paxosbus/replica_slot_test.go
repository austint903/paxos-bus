package paxosbus

import "testing"

// ms is a millisecond expressed in the lines' nanosecond units.
const ms = int64(1e6)

// worked example from GLOBAL_LOG.md: c1 (k=2ms, b=0), c2 (k=3ms, b=1ms).
//
//	time(ms) 0     1     2     4        4        6     7
//	msg      c1#1  c2#1  c1#2  c1#3     c2#2     c1#4  c2#3
//	slot     0     1     2     3(id1)   4(id2)   5     6
func workedLines() map[uint64]*clientLine {
	return map[uint64]*clientLine{
		1: {baseNs: 0, intervalNs: 2 * ms},
		2: {baseNs: 1 * ms, intervalNs: 3 * ms},
	}
}

func TestComputeGlobalSlotWorkedExample(t *testing.T) {
	lines := workedLines()
	cases := []struct {
		clientId, reqId uint64
		want            uint64
	}{
		{1, 1, 0},
		{2, 1, 1},
		{1, 2, 2},
		{1, 3, 3}, // ties c2#2 at t=4, smaller clientId wins
		{2, 2, 4},
		{1, 4, 5},
		{2, 3, 6},
	}
	for _, c := range cases {
		if got := computeGlobalSlot(lines, c.clientId, c.reqId); got != c.want {
			t.Errorf("computeGlobalSlot(c%d#%d) = %d, want %d", c.clientId, c.reqId, got, c.want)
		}
	}
}

// TestCursorInvertsClosedForm checks the merge cursor (slot -> owner) is the
// exact inverse of computeGlobalSlot (owner -> slot) over the worked example.
func TestCursorInvertsClosedForm(t *testing.T) {
	r := &Replica{
		clients:     workedLines(),
		cursorNextN: make(map[uint64]uint64),
		slotMeta:    make(map[uint64]slotMetaEntry),
	}
	r.genCursorUpToLocked(6)

	wantOwner := []struct{ clientId, reqId uint64 }{
		{1, 1}, {2, 1}, {1, 2}, {1, 3}, {2, 2}, {1, 4}, {2, 3},
	}
	for slot, w := range wantOwner {
		m, ok := r.slotMeta[uint64(slot)]
		if !ok {
			t.Fatalf("slot %d not generated", slot)
		}
		if m.clientId != w.clientId || m.reqId != w.reqId {
			t.Errorf("slot %d owner = c%d#%d, want c%d#%d",
				slot, m.clientId, m.reqId, w.clientId, w.reqId)
		}
		// round-trip: owner's closed-form slot must equal this slot.
		if got := computeGlobalSlot(r.clients, m.clientId, m.reqId); got != uint64(slot) {
			t.Errorf("round-trip: computeGlobalSlot(c%d#%d) = %d, want %d",
				m.clientId, m.reqId, got, slot)
		}
	}
}

// TestGlobalSlotBijection checks the mapping over a denser pair of lines is a
// bijection onto 0..N-1 (no slot collisions, no gaps), which the global log and
// nextExpected advance both rely on.
func TestGlobalSlotBijection(t *testing.T) {
	lines := map[uint64]*clientLine{
		1: {baseNs: 0, intervalNs: 7 * ms},
		2: {baseNs: 3 * ms, intervalNs: 5 * ms},
		3: {baseNs: 1 * ms, intervalNs: 11 * ms},
	}
	const perClient = 40
	seen := make(map[uint64]string)
	for cid := uint64(1); cid <= 3; cid++ {
		for n := uint64(1); n <= perClient; n++ {
			slot := computeGlobalSlot(lines, cid, n)
			key := seen[slot]
			if key != "" {
				t.Fatalf("slot %d assigned twice: %s and c%d#%d", slot, key, cid, n)
			}
			seen[slot] = string(rune('0'+cid)) + "#" + itoa(n)
		}
	}
	// The lowest 3*perClient - maxInterval messages must form a contiguous prefix
	// 0..k-1 (the tail is ragged because the three lines extend past each other).
	// Check no gaps below the densest guaranteed-complete prefix.
	complete := uint64(2 * perClient) // conservative contiguous prefix bound
	for s := uint64(0); s < complete; s++ {
		if seen[s] == "" {
			t.Fatalf("gap in slot space at %d (expected contiguous prefix)", s)
		}
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
