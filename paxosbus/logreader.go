package paxosbus

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
)

type diskRecord struct {
	Slot       uint64   `json:"slot"`
	Client     uint64   `json:"client"`
	ReqId      uint64   `json:"req_id"`
	Bus        *uint64  `json:"bus"`
	LogIndexes []uint64 `json:"log_indexes"`
	NoOp       bool     `json:"noop"`
	Pending    bool     `json:"pending"`
}

type reqDiskRecord struct {
	LogIndex uint64 `json:"log_index"`
	Client   uint64 `json:"client"`
	ReqId    uint64 `json:"req_id"`
	Op       string `json:"op"`
}

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

func (cl *durableLog) seekToLocked(key uint64) bool {
	if cl.rValid && cl.rKey <= key {
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
