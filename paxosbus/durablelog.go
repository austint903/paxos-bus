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

	flushReq chan chan struct{}

	// Readers use a separate descriptor so state transfer cannot disturb the writer.
	idxMu     sync.Mutex
	offIdx    []int64
	firstKey  uint64
	haveFirst bool

	readMu sync.Mutex
	rf     *os.File
	rbr    *bufio.Reader
	rKey   uint64
	rOff   int64
	rValid bool

	// pending lets the hot path enqueue records without waiting for disk.
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

// offStride controls the sparse disk index spacing.
const offStride = 1024

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
	rf, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	cl := &durableLog{
		f:        f,
		w:        bufio.NewWriterSize(f, durableLogBufBytes),
		wake:     make(chan struct{}, 1),
		flushReq: make(chan chan struct{}, 4),
		closed:   make(chan struct{}),
		tailOff:  off,
		holes:    make(map[uint64]int64),
		rf:       rf,
		rbr:      bufio.NewReaderSize(rf, 1<<15),
	}
	go cl.writeLoop()
	return cl, nil
}

func recordBody(slot, clientId, reqId uint64, op []byte, noop bool) string {
	return fmt.Sprintf(
		"{\"slot\":%d,\"client\":%d,\"req_id\":%d,\"len\":%d,\"op\":\"%s\",\"noop\":%t}",
		slot, clientId, reqId, len(op), hex.EncodeToString(op), noop)
}

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
		case ack := <-cl.flushReq:
			cl.drain()
			cl.w.Flush()
			close(ack)
			continue
		case <-ticker.C:
			cl.w.Flush()
			cl.f.Sync()
			continue
		}
		if cl.drain() {
			cl.w.Flush()
			cl.f.Sync()
			cl.f.Close()
			cl.rf.Close()
			close(cl.closed)
			return
		}
	}
}

func (cl *durableLog) drain() (closing bool) {
	for {
		cl.mu.Lock()
		batch := cl.pending
		cl.pending = nil
		closing = cl.closing
		cl.mu.Unlock()
		for i := range batch {
			cl.apply(batch[i].slot, batch[i].body)
		}
		if len(batch) == 0 {
			return closing
		}
	}
}

func (cl *durableLog) flushForRead() {
	ack := make(chan struct{})
	select {
	case cl.flushReq <- ack:
	default:
		return // a flush is already queued; its result is good enough
	}
	select {
	case <-ack:
	case <-time.After(2 * time.Second):
		Warning("durable log flush for read timed out")
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
		cl.noteOffset(s)
		cl.writeAtTail(padLine(placeholderBody(s)))
		cl.holes[s] = off
	}
	cl.noteOffset(slot)
	cl.writeAtTail([]byte(body + "\n"))
	cl.nextSlot = slot + 1
}

func (cl *durableLog) noteOffset(key uint64) {
	cl.idxMu.Lock()
	if !cl.haveFirst {
		cl.firstKey, cl.haveFirst = key, true
	}
	if key >= cl.firstKey && (key-cl.firstKey)%offStride == 0 {
		cl.offIdx = append(cl.offIdx, cl.tailOff)
	}
	cl.idxMu.Unlock()
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
