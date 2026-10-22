package paxosbus

import (
	"sort"
	"testing"
)

func TestIndexedScheduleMatchesEnumeratedSlots(t *testing.T) {
	r := scheduleReplica()
	var history []BusSyncMessage
	var buses []struct{ id, arrival uint64 }
	for window := uint64(1); window <= 100; window++ {
		for id := uint64(0); id < 3; id++ {
			interval := (window + id) % 7
			start, end := window*10e6, (window+1)*10e6
			history = append(history, BusSyncMessage{ClientId: id, FirstMsgNs: start, EndTime: end, IntervalMs: interval})
			if interval > 0 {
				for arrival := start; arrival < end; arrival += interval * 1e6 {
					buses = append(buses, struct{ id, arrival uint64 }{id, arrival})
				}
			}
		}
	}
	sort.Slice(buses, func(i, j int) bool {
		return buses[i].arrival < buses[j].arrival || buses[i].arrival == buses[j].arrival && buses[i].id < buses[j].id
	})
	r.installScheduleLocked(history, 0, true)
	for slot, bus := range buses {
		if got := computeGlobalSlot(r.clients, bus.id, bus.arrival); got != uint64(slot) {
			t.Fatalf("slot %d got %d", slot, got)
		}
		owner := r.slotOwnerLocked(uint64(slot))
		if owner.clientId != bus.id || owner.reqId != bus.arrival {
			t.Fatalf("owner at %d: %+v", slot, owner)
		}
		if !r.clients[bus.id].validSequence(bus.arrival, r.scheduleCommittedEnd) {
			t.Fatalf("rejected bus %+v", bus)
		}
	}
}
