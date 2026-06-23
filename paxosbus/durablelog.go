package paxosbus

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// clientLog is a replica's durable, append-only operation log for ONE client,
// kept separate from the existing stderr summary/event logs. Slot number ==
// the client's BusRequestMessage.RequestId: slot N holds that client's request
// N, so a dropped message leaves a hole at its slot and the slots line up across
// replicas (gap agreement can copy slot N verbatim from a peer).
//
// The file is kept STRICTLY CONTIGUOUS in slot order. Normally requests arrive
// in slot order and are appended compactly at the tail. When a slot is dropped,
// later slots still arrive and overtake it; rather than writing them ahead of
// the hole (which would leave the recovered record dangling at end-of-file when
// gap agreement finishes much later), the log reserves the missing slot's
// position with a fixed-width PLACEHOLDER record and remembers its byte offset.
// When gap agreement resolves that slot (recovered op or committed NoOp), the
// placeholder is overwritten in place via WriteAt, so the record lands at its
// natural position between its neighbours instead of being appended out of order.
//
// Appends only touch the in-memory bufio buffer; the buffer is pushed to disk
// in batches by the replica's flush loop (see Replica.flushLoop), so the hot
// path never blocks on I/O. At most one flush interval of appends is at risk on
// a hard crash.
type clientLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	// tailOff is the byte offset of the next append. The file is opened WITHOUT
	// O_APPEND (which on Linux forces every write to end-of-file and ignores
	// WriteAt offsets), so we track the tail ourselves: bufio appends advance the
	// OS file offset sequentially, and placeholder patches use WriteAt, which is
	// positional and leaves the file offset at the tail.
	tailOff int64
	// nextSlot is the next slot expected to be written contiguously at the tail.
	// Zero means "unset"; it is initialised to the first slot recorded.
	nextSlot uint64
	haveNext bool
	// holes maps a reserved-but-unresolved slot to the byte offset of its
	// fixed-width placeholder record, so a later resolution can patch it in place.
	holes map[uint64]int64
}

// clientLogBufBytes sizes the append-batching buffer. The bufio writer flushes
// to the OS on its own when full, so high message rates still batch to disk
// between flush ticks instead of stalling on every append.
const clientLogBufBytes = 1 << 16 // 64 KiB

// holeRecordWidth is the fixed byte width (including the trailing newline) of a
// reserved gap placeholder and of the record that later overwrites it in place.
// It must be at least as wide as the widest resolved record so the patched line
// always fits inside the bytes the placeholder reserved. Records in this system
// are tiny (a few bytes of op), so this is generously sized; gaps are rare, so
// the padding cost is negligible.
const holeRecordWidth = 256

// openClientLog opens (creating, or reopening) the durable log file for one
// client under dir, e.g. <dir>/client-<id>.log. The file is opened without
// O_APPEND so placeholder records can be patched in place with WriteAt.
func openClientLog(dir string, clientId uint64) (*clientLog, error) {
	path := filepath.Join(dir, fmt.Sprintf("client-%d.log", clientId))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	// Resume writing at the current end of file (normally 0 for a fresh run dir).
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &clientLog{
		f:       f,
		w:       bufio.NewWriterSize(f, clientLogBufBytes),
		tailOff: off,
		holes:   make(map[uint64]int64),
	}, nil
}

// recordBody renders the JSON body of one slot record (no newline, no padding).
// slot == req id for a normally-received request; gap agreement makes them
// diverge (a recovered op keeps its original req_id, a NoOp fill sets noop=true
// with req_id 0). The op bytes are stored hex-encoded so a peer can serve the
// exact request during recovery and the record stays valid JSON for arbitrary
// payloads.
func recordBody(slot, reqId uint64, op []byte, noop bool) string {
	return fmt.Sprintf(
		"{\"slot\":%d,\"req_id\":%d,\"len\":%d,\"op\":\"%s\",\"noop\":%t}",
		slot, reqId, len(op), hex.EncodeToString(op), noop)
}

// placeholderBody renders a reserved-but-unresolved slot. The pending flag marks
// it so a reader (or a crash mid-gap) can tell the slot is awaiting resolution.
func placeholderBody(slot uint64) string {
	return fmt.Sprintf(
		"{\"slot\":%d,\"req_id\":0,\"len\":0,\"op\":\"\",\"noop\":false,\"pending\":true}",
		slot)
}

// padLine pads body to exactly holeRecordWidth bytes including the trailing
// newline (spaces before the newline are valid JSON whitespace). Used for any
// record that occupies a reservable, in-place-patchable position.
func padLine(body string) []byte {
	line := make([]byte, holeRecordWidth)
	n := copy(line, body)
	for i := n; i < holeRecordWidth-1; i++ {
		line[i] = ' '
	}
	line[holeRecordWidth-1] = '\n'
	return line
}

// record writes one slot's record while keeping the file strictly contiguous in
// slot order:
//   - slot == nextSlot: append the compact record at the tail and advance.
//   - slot >  nextSlot: reserve a fixed-width placeholder for every intervening
//     slot (remembering each offset), then append this slot, then advance.
//   - slot <  nextSlot: this is a gap resolution; overwrite the slot's reserved
//     placeholder in place (WriteAt). If there is no placeholder (already
//     resolved or a duplicate), ignore it.
func (cl *clientLog) record(slot, reqId uint64, op []byte, noop bool) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if !cl.haveNext {
		cl.nextSlot, cl.haveNext = slot, true
	}

	if slot < cl.nextSlot {
		cl.patchHoleLocked(slot, reqId, op, noop)
		return
	}

	// Reserve placeholders for any slots skipped between the tail and this one.
	for s := cl.nextSlot; s < slot; s++ {
		off := cl.tailOff
		cl.writeAtTailLocked(padLine(placeholderBody(s)))
		cl.holes[s] = off
	}
	// Append this slot compactly at the tail.
	body := recordBody(slot, reqId, op, noop)
	cl.writeAtTailLocked([]byte(body + "\n"))
	cl.nextSlot = slot + 1
}

// patchHoleLocked overwrites a reserved placeholder with its resolved record.
// The bufio buffer is flushed first so the (now stale) placeholder bytes are on
// disk before WriteAt overwrites them — otherwise a later flush of the buffered
// placeholder would clobber the patch. WriteAt is positional and does not move
// the file offset, so subsequent appends continue at the tail. Callers hold
// cl.mu.
func (cl *clientLog) patchHoleLocked(slot, reqId uint64, op []byte, noop bool) {
	off, ok := cl.holes[slot]
	if !ok {
		return // already resolved, or a duplicate fill
	}
	body := recordBody(slot, reqId, op, noop)
	cl.w.Flush()
	cl.f.WriteAt(padLine(body), off)
	delete(cl.holes, slot)
}

// writeAtTailLocked appends bytes through the bufio buffer and advances the
// tracked tail offset. Callers hold cl.mu.
func (cl *clientLog) writeAtTailLocked(b []byte) {
	cl.w.Write(b)
	cl.tailOff += int64(len(b))
}

// flush pushes the buffer to the OS and fsyncs it to disk. The replica calls it
// on a timer so appends are persisted in batches.
func (cl *clientLog) flush() {
	cl.mu.Lock()
	cl.w.Flush()
	cl.f.Sync()
	cl.mu.Unlock()
}

// close flushes and closes the file (best effort).
func (cl *clientLog) close() {
	cl.mu.Lock()
	cl.w.Flush()
	cl.f.Sync()
	cl.f.Close()
	cl.mu.Unlock()
}
