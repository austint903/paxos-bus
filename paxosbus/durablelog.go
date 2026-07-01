package paxosbus

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type durableLog struct {
	f  *os.File
	w  *bufio.Writer
	ch chan logRecord

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

const durableQueueDepth = 1 << 16

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
		ch:      make(chan logRecord, durableQueueDepth),
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
	cl.ch <- logRecord{slot, recordBody(slot, clientId, reqId, op, noop)}
}

func (cl *durableLog) recordBus(slot, clientId, busSeq uint64, logIdxs []uint64, noop bool) {
	cl.ch <- logRecord{slot, busRecordBody(slot, clientId, busSeq, logIdxs, noop)}
}

func (cl *durableLog) recordReq(logIndex, clientId, reqId uint64, op []byte) {
	cl.ch <- logRecord{logIndex, reqListRecordBody(logIndex, clientId, reqId, op)}
}

func (cl *durableLog) writeLoop() {
	ticker := time.NewTicker(durableSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case rec, ok := <-cl.ch:
			if !ok {
				cl.w.Flush()
				cl.f.Sync()
				cl.f.Close()
				close(cl.closed)
				return
			}
			cl.apply(rec.slot, rec.body)
		case <-ticker.C:
			cl.w.Flush()
			cl.f.Sync()
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
	close(cl.ch)
	<-cl.closed
}
