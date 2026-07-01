package paxosbus

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type durableLog struct {
	f *os.File
	w *bufio.Writer

	// pending is an unbounded in-memory hand-off to writeLoop. The hot path
	// (record*) only appends under mu and never blocks on the disk goroutine, so
	// a disk latency spike can't stall the caller's r.mu — it just grows this
	// buffer transiently and drains once the disk catches up. maxDepth is the
	// high-water mark since the last backlogMax() read, exposed in the stats line.
	mu       sync.Mutex
	pending  []logRecord
	maxDepth int
	closing  bool
	wake     chan struct{} // cap-1 signal that pending is non-empty (or closing)

	closed chan struct{}

	tailOff  int64
	nextSlot uint64
	haveNext bool
	holes    map[uint64]int64
}

type logRecord struct {
	slot uint64
	body string
}

const durableLogBufBytes = 1 << 16

const durableSyncInterval = time.Second

const holeRecordWidth = 256

func openDurableLog(dir, name string) (*durableLog, error) {
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	cl := &durableLog{
		f:       f,
		w:       bufio.NewWriterSize(f, durableLogBufBytes),
		wake:    make(chan struct{}, 1),
		closed:  make(chan struct{}),
		tailOff: off,
		holes:   make(map[uint64]int64),
	}
	go cl.writeLoop()
	return cl, nil
}

func recordBody(slot, clientId, reqId uint64, op []byte, noop bool) string {
	return fmt.Sprintf(
		"{\"slot\":%d,\"client\":%d,\"req_id\":%d,\"len\":%d,\"op\":\"%s\",\"noop\":%t}",
		slot, clientId, reqId, len(op), hex.EncodeToString(op), noop)
}

// busRecordBody is one line of the BusMessage Log: which bus occupies this
// global slot and which request-log-list indexes its passengers map to. The
// request payloads themselves live in the separate request log list (see
// reqListRecordBody); the bus log only references them by index, so a request
// carried by several buses (re-boarded after missing quorum) is stored once.
func busRecordBody(slot, clientId, busSeq uint64, logIdxs []uint64, noop bool) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "{\"slot\":%d,\"client\":%d,\"bus\":%d,\"log_indexes\":[", slot, clientId, busSeq)
	for i, li := range logIdxs {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%d", li)
	}
	fmt.Fprintf(&sb, "],\"noop\":%t}", noop)
	return sb.String()
}

// reqListRecordBody is one line of the Request Log List: the deduplicated
// request stored at log_index. Appended in strict log-index order, so the file
// is contiguous (no holes) unlike the slot-indexed bus/global logs.
func reqListRecordBody(logIndex, clientId, reqId uint64, op []byte) string {
	return fmt.Sprintf(
		"{\"log_index\":%d,\"client\":%d,\"req_id\":%d,\"len\":%d,\"op\":\"%s\"}",
		logIndex, clientId, reqId, len(op), hex.EncodeToString(op))
}

func placeholderBody(slot uint64) string {
	return fmt.Sprintf(
		"{\"slot\":%d,\"req_id\":0,\"len\":0,\"op\":\"\",\"noop\":false,\"pending\":true}",
		slot)
}

func padLine(body string) []byte {
	line := make([]byte, holeRecordWidth)
	n := copy(line, body)
	for i := n; i < holeRecordWidth-1; i++ {
		line[i] = ' '
	}
	line[holeRecordWidth-1] = '\n'
	return line
}

func (cl *durableLog) record(slot, clientId, reqId uint64, op []byte, noop bool) {
	cl.push(logRecord{slot, recordBody(slot, clientId, reqId, op, noop)})
}

func (cl *durableLog) recordBus(slot, clientId, busSeq uint64, logIdxs []uint64, noop bool) {
	cl.push(logRecord{slot, busRecordBody(slot, clientId, busSeq, logIdxs, noop)})
}

func (cl *durableLog) recordReq(logIndex, clientId, reqId uint64, op []byte) {
	cl.push(logRecord{logIndex, reqListRecordBody(logIndex, clientId, reqId, op)})
}

// push is the hot-path enqueue. It only appends under mu and wakes writeLoop —
// it never waits on disk — so callers holding r.mu are never blocked by an fsync
// stall. The disk write itself happens off-lock in writeLoop.
func (cl *durableLog) push(rec logRecord) {
	cl.mu.Lock()
	cl.pending = append(cl.pending, rec)
	if n := len(cl.pending); n > cl.maxDepth {
		cl.maxDepth = n
	}
	cl.mu.Unlock()
	select {
	case cl.wake <- struct{}{}:
	default:
	}
}

// backlogMax returns and resets the high-water mark of the pending buffer since
// the last call — the durable analogue of the reply backlog stat.
func (cl *durableLog) backlogMax() int {
	cl.mu.Lock()
	m := cl.maxDepth
	cl.maxDepth = 0
	cl.mu.Unlock()
	return m
}

func (cl *durableLog) writeLoop() {
	ticker := time.NewTicker(durableSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cl.wake:
		case <-ticker.C:
			cl.w.Flush()
			cl.f.Sync()
			continue
		}
		// Drain the whole pending buffer (swapped out under mu so the hot path
		// keeps appending while we do disk I/O off-lock), then, if we were asked
		// to close, flush+sync+close once it's empty.
		for {
			cl.mu.Lock()
			batch := cl.pending
			cl.pending = nil
			closing := cl.closing
			cl.mu.Unlock()
			for i := range batch {
				cl.apply(batch[i].slot, batch[i].body)
			}
			if len(batch) == 0 {
				if closing {
					cl.w.Flush()
					cl.f.Sync()
					cl.f.Close()
					close(cl.closed)
					return
				}
				break
			}
		}
	}
}

func (cl *durableLog) apply(slot uint64, body string) {
	if !cl.haveNext {
		cl.nextSlot, cl.haveNext = slot, true
	}

	if slot < cl.nextSlot {
		cl.patchHole(slot, body)
		return
	}

	for s := cl.nextSlot; s < slot; s++ {
		off := cl.tailOff
		cl.writeAtTail(padLine(placeholderBody(s)))
		cl.holes[s] = off
	}
	cl.writeAtTail([]byte(body + "\n"))
	cl.nextSlot = slot + 1
}

func (cl *durableLog) patchHole(slot uint64, body string) {
	off, ok := cl.holes[slot]
	if !ok {
		return
	}
	cl.w.Flush()
	cl.f.WriteAt(padLine(body), off)
	delete(cl.holes, slot)
}

func (cl *durableLog) writeAtTail(b []byte) {
	cl.w.Write(b)
	cl.tailOff += int64(len(b))
}

func (cl *durableLog) close() {
	cl.mu.Lock()
	cl.closing = true
	cl.mu.Unlock()
	select {
	case cl.wake <- struct{}{}:
	default:
	}
	<-cl.closed
}
