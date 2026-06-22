package paxosbus

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// clientLog is a replica's durable, append-only operation log for ONE client,
// kept separate from the existing stderr summary/event logs. Slot number ==
// the client's BusRequestMessage.RequestId: slot N holds that client's request
// N, so a dropped message leaves a hole at its slot and the slots line up across
// replicas (a future gap agreement can copy slot N verbatim from a peer).
//
// Appends only touch the in-memory bufio buffer; the buffer is pushed to disk
// in batches by the replica's flush loop (see Replica.flushLoop), so the hot
// path never blocks on I/O. At most one flush interval of appends is at risk on
// a hard crash.
type clientLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// clientLogBufBytes sizes the append-batching buffer. The bufio writer flushes
// to the OS on its own when full, so high message rates still batch to disk
// between flush ticks instead of stalling on every append.
const clientLogBufBytes = 1 << 16 // 64 KiB

// openClientLog opens (creating, or reopening for append) the durable log file
// for one client under dir, e.g. <dir>/client-<id>.log.
func openClientLog(dir string, clientId uint64) (*clientLog, error) {
	path := filepath.Join(dir, fmt.Sprintf("client-%d.log", clientId))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &clientLog{f: f, w: bufio.NewWriterSize(f, clientLogBufBytes)}, nil
}

// append writes one slot record as a single JSON line. slot == req id for a
// normally-received request; gap agreement makes them diverge (a recovered op
// keeps its original req_id, a NoOp fill sets noop=true with req_id 0). The op
// bytes are stored hex-encoded so a peer can serve the exact request during
// recovery, and so the record stays valid JSON for arbitrary payloads. No
// timestamps (client send time and replica recv time live on different clocks,
// so they can't yield an RTT).
func (cl *clientLog) append(slot, reqId uint64, op []byte, noop bool) {
	cl.mu.Lock()
	fmt.Fprintf(cl.w,
		"{\"slot\":%d,\"req_id\":%d,\"len\":%d,\"op\":\"%s\",\"noop\":%t}\n",
		slot, reqId, len(op), hex.EncodeToString(op), noop)
	cl.mu.Unlock()
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
