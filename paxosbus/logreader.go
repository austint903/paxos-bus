package paxosbus

// Reading the durable logs back.
//
// State transfer prefers memory, but the resident window is a few seconds wide
// and a replica can be further behind than that — so anything already reclaimed
// is rebuilt from disk instead. The bus log stores only which log indexes a slot
// carries, not the requests themselves, so rebuilding a bus is a join against
// the request log list. That is exact: the list is contiguous and in index
// order, and re-boarded passengers reference the index they were first given.
// The one field that does not survive the round trip is SendTimeNs, which is
// never persisted — it drives the client's own latency clock and is meaningless
// on a replica receiving the entry.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
)

// diskRecord covers bus records and hole placeholders.
type diskRecord struct {
	Slot       uint64   `json:"slot"`
	Client     uint64   `json:"client"`
	ReqId      uint64   `json:"req_id"`
	Bus        *uint64  `json:"bus"`
	LogIndexes []uint64 `json:"log_indexes"`
	NoOp       bool     `json:"noop"`
	Pending    bool     `json:"pending"`
}

// reqDiskRecord is one line of the request log list.
type reqDiskRecord struct {
	LogIndex uint64 `json:"log_index"`
	Client   uint64 `json:"client"`
	ReqId    uint64 `json:"req_id"`
	Op       string `json:"op"`
}

// readRecords returns the raw bodies for keys in [lo, hi] in a single forward
// pass. Keys the log does not (yet) hold are simply absent.
func (cl *durableLog) readRecords(lo, hi uint64) map[uint64][]byte {
	if cl == nil || lo > hi {
		return nil
	}
	cl.flushForRead()

	cl.readMu.Lock()
	defer cl.readMu.Unlock()

	if !cl.seekToLocked(lo) {
		return nil
	}
	out := make(map[uint64][]byte)
	for cl.rKey <= hi {
		line, err := cl.rbr.ReadBytes('\n')
		if err != nil {
			// Short read at the tail: the rest simply is not on disk yet.
			cl.rValid = false
			if len(line) == 0 || err != io.EOF {
				return out
			}
			return out
		}
		key := cl.rKey
		cl.rOff += int64(len(line))
		cl.rKey++
		if key >= lo {
			body := bytes.TrimRight(line[:len(line)-1], " ")
			if len(body) > 0 {
				out[key] = append([]byte(nil), body...)
			}
		}
	}
	return out
}

// seekToLocked positions the read cursor at key. Consecutive reads walk forward
// in key order — which is how state transfer scans a range — so the common case
// costs nothing; otherwise it jumps to the nearest sampled offset.
func (cl *durableLog) seekToLocked(key uint64) bool {
	if cl.rValid && cl.rKey <= key {
		// Already positioned at or before key: skip forward line by line.
		for cl.rKey < key {
			line, err := cl.rbr.ReadBytes('\n')
			if err != nil {
				cl.rValid = false
				return false
			}
			cl.rOff += int64(len(line))
			cl.rKey++
		}
		return true
	}

	cl.idxMu.Lock()
	haveFirst, firstKey := cl.haveFirst, cl.firstKey
	var off int64
	var atKey uint64
	ok := false
	if haveFirst && key >= firstKey && len(cl.offIdx) > 0 {
		i := (key - firstKey) / offStride
		if int(i) >= len(cl.offIdx) {
			i = uint64(len(cl.offIdx)) - 1
		}
		off, atKey, ok = cl.offIdx[i], firstKey+i*offStride, true
	}
	cl.idxMu.Unlock()
	if !ok {
		return false
	}

	if _, err := cl.rf.Seek(off, io.SeekStart); err != nil {
		return false
	}
	cl.rbr.Reset(cl.rf)
	cl.rKey, cl.rOff, cl.rValid = atKey, off, true
	for cl.rKey < key {
		line, err := cl.rbr.ReadBytes('\n')
		if err != nil {
			cl.rValid = false
			return false
		}
		cl.rOff += int64(len(line))
		cl.rKey++
	}
	return true
}

// readSlotFromDisk rebuilds one slot's content from the durable logs.
func (r *Replica) readSlotFromDisk(slot uint64) (StateEntry, bool) {
	if r.durable == nil {
		return StateEntry{}, false
	}
	bodies := r.durable.readRecords(slot, slot)
	body, ok := bodies[slot]
	if !ok {
		return StateEntry{}, false
	}
	var rec diskRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		Warning("[%s] unreadable bus-log record for slot %d: %v", r.self, slot, err)
		return StateEntry{}, false
	}
	if rec.Pending {
		return StateEntry{}, false // a hole that was never filled in
	}

	ent := StateEntry{Slot: slot, ClientId: rec.Client, ReqId: rec.ReqId, IsNoOp: rec.NoOp}
	if rec.Bus != nil {
		ent.ReqId = *rec.Bus
	}
	if rec.NoOp {
		return ent, true
	}

	if rec.Bus == nil {
		Warning("[%s] bus-log record for slot %d has no bus identity", r.self, slot)
		return StateEntry{}, false
	}

	reqs, ok := r.readRequestsFromDisk(rec.LogIndexes)
	if !ok {
		return StateEntry{}, false
	}
	ent.IsBus = true
	ent.Payload = marshalRequests(reqs)
	return ent, true
}

// readRequestsFromDisk rebuilds a bus's passenger list from the request log
// list. A bus's new passengers get consecutive indexes, so the whole set is
// covered by one scan of [min, max] rather than a lookup each.
func (r *Replica) readRequestsFromDisk(idxs []uint64) ([]RequestMessage, bool) {
	if r.reqListLog == nil {
		return nil, false
	}
	if len(idxs) == 0 {
		return nil, true
	}
	lo, hi := idxs[0], idxs[0]
	for _, li := range idxs {
		if li < lo {
			lo = li
		}
		if li > hi {
			hi = li
		}
	}
	bodies := r.reqListLog.readRecords(lo, hi)

	reqs := make([]RequestMessage, 0, len(idxs))
	for _, li := range idxs {
		body, ok := bodies[li]
		if !ok {
			return nil, false
		}
		var rec reqDiskRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return nil, false
		}
		op, err := hex.DecodeString(rec.Op)
		if err != nil {
			return nil, false
		}
		reqs = append(reqs, RequestMessage{
			ClientId:  rec.Client,
			RequestId: rec.ReqId,
			Op:        op,
		})
	}
	return reqs, true
}
