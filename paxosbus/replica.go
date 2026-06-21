package paxosbus

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// nowNs is a monotonic nanosecond clock (Go's time.Since uses the monotonic
// reading, matching the C++ CLOCK_MONOTONIC usage).
var startTime = time.Now()

func nowNs() int64 {
	return time.Since(startTime).Nanoseconds()
}

// clientStream is the replica's per-client arrival model. Expected arrival of
// seq N is base_recv_ns + (N-1)*interval_ns (the T = m*x + b line: m=interval,
// b=base). base is provisionally anchored from the sync schedule, then pinned
// ONCE through the first observed data arrival. The line stays fixed after
// that, so delta measures cumulative drift from a stable baseline. (Valid
// locally where there are no packet drops to break the seq->time mapping.)
type clientStream struct {
	baseRecvNs   int64
	intervalNs   int64
	anchored     bool
	nextExpected uint64
	maxSeqSeen   uint64
}

type Replica struct {
	config *Config
	idx    int
	viewId uint64
	self   string // log-line identity, e.g. "Replica 1 europe-north1"

	mu      sync.Mutex
	clients map[uint64]*clientStream

	// Durable per-client logs (slot == client seq). Separate from the stderr
	// logs above; empty logDir disables them. Guarded by mu.
	logDir     string
	clientLogs map[uint64]*clientLog

	// 1-second window counters for the periodic summary line (all clients).
	winRecv       uint64
	winDeltaSumUs int64
	winDeltaMinUs int64
	winDeltaMaxUs int64
}

func NewReplica(config *Config, idx int, label, logDir string) *Replica {
	self := "Replica " + strconv.Itoa(idx)
	if label != "" {
		self += " " + label
	}
	r := &Replica{
		config:     config,
		idx:        idx,
		viewId:     0,
		self:       self,
		clients:    make(map[uint64]*clientStream),
		logDir:     logDir,
		clientLogs: make(map[uint64]*clientLog),
	}
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			Warning("[%s] cannot create durable log dir %s: %v", self, logDir, err)
			r.logDir = ""
		} else {
			Notice("[%s] durable per-client logs in %s", self, logDir)
		}
	}
	leader := "no"
	if r.AmLeader() {
		leader = "yes"
	}
	Notice("[%s] started (view=0, f=%d, quorum=%d, leader=%s)",
		r.self, config.F, config.QuorumSize(), leader)
	return r
}

func (r *Replica) AmLeader() bool {
	return r.config.LeaderIndex(r.viewId) == r.idx
}

// Run binds the replica port, then serves one reader goroutine per client
// connection. Blocks forever.
func (r *Replica) Run() error {
	l, err := net.Listen("tcp", "0.0.0.0:"+r.config.Port(r.idx))
	if err != nil {
		return err
	}
	go r.statsLoop()
	if r.logDir != "" {
		go r.flushLoop()
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			Warning("[%s] accept error: %v", r.self, err)
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go r.clientListener(conn)
	}
}

// clientListener handles one client connection: a sync message followed by
// the data stream. Replies go back on the same connection, so the writer is
// owned by this goroutine and needs no lock.
func (r *Replica) clientListener(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var (
		syncMsg SyncMessage
		dataMsg DataMessage
		reply   DataReplyMessage
	)

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			Notice("[%s] client connection %s closed: %v",
				r.self, conn.RemoteAddr(), err)
			return
		}
		switch msgType {
		case MsgSync:
			if err := syncMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad sync message: %v", r.self, err)
				return
			}
			r.handleSync(&syncMsg)

		case MsgData:
			if err := dataMsg.Unmarshal(reader); err != nil {
				Warning("[%s] bad data message: %v", r.self, err)
				return
			}
			if !r.handleData(&dataMsg) {
				continue
			}
			reply = DataReplyMessage{
				ClientId:   dataMsg.ClientId,
				SeqNum:     dataMsg.SeqNum,
				ViewId:     r.viewId,
				LogSlotNum: dataMsg.SeqNum,
				ReplicaIdx: uint32(r.idx),
			}
			writer.WriteByte(MsgDataReply)
			reply.Marshal(writer)
			if err := writer.Flush(); err != nil {
				Warning("[%s] failed to send reply for seq=%d: %v",
					r.self, dataMsg.SeqNum, err)
				return
			}

		default:
			Warning("[%s] unknown message type %d", r.self, msgType)
			return
		}
	}
}

func (r *Replica) handleSync(msg *SyncMessage) {
	recvNs := nowNs()
	r.mu.Lock()
	// Anchor baseline to when the first data message is expected to arrive:
	// base = sync_recv + start_delay, so expected(N) = base + (N-1)*interval.
	// Provisional; re-anchored to observed arrivals in handleData.
	r.clients[msg.ClientId] = &clientStream{
		baseRecvNs:   recvNs + int64(msg.StartDelayMs)*1e6,
		intervalNs:   int64(msg.IntervalMs) * 1e6,
		anchored:     false,
		nextExpected: 1,
		maxSeqSeen:   0,
	}
	r.mu.Unlock()
	Notice("[%s] sync from client %d: interval=%dms",
		r.self, msg.ClientId, msg.IntervalMs)
}

// handleData updates the arrival model and stats; returns false if the client
// never synced (no reply is sent then, as in the C++ implementation).
func (r *Replica) handleData(msg *DataMessage) bool {
	actualNs := nowNs()
	r.mu.Lock()
	s, ok := r.clients[msg.ClientId]
	if !ok {
		r.mu.Unlock()
		Warning("[%s] data from unsynced client %d, ignoring", r.self, msg.ClientId)
		return false
	}
	if msg.SeqNum > s.maxSeqSeen {
		s.maxSeqSeen = msg.SeqNum
	}

	// Anchor once: pin b through the first observed arrival so expected(this
	// seq) == actual, then leave the line fixed. Subsequent deltas are measured
	// against that fixed line, so they capture cumulative drift, not per-message
	// jitter off a sliding anchor.
	var deltaUs int64
	if !s.anchored {
		s.baseRecvNs = actualNs - int64(msg.SeqNum-1)*s.intervalNs
		s.anchored = true
	} else {
		expectedNs := s.baseRecvNs + int64(msg.SeqNum-1)*s.intervalNs
		deltaUs = (actualNs - expectedNs) / 1000
	}
	if msg.SeqNum == s.nextExpected {
		s.nextExpected++
	}

	if r.winRecv == 0 {
		r.winDeltaMinUs, r.winDeltaMaxUs = deltaUs, deltaUs
	} else {
		if deltaUs < r.winDeltaMinUs {
			r.winDeltaMinUs = deltaUs
		}
		if deltaUs > r.winDeltaMaxUs {
			r.winDeltaMaxUs = deltaUs
		}
	}
	r.winDeltaSumUs += deltaUs
	r.winRecv++
	// Lazily open the durable log on the first data op (the sync message itself
	// is never logged — only client->replica operations get a slot).
	cl := r.clientLogs[msg.ClientId]
	if cl == nil && r.logDir != "" {
		var err error
		if cl, err = openClientLog(r.logDir, msg.ClientId); err != nil {
			Warning("[%s] cannot open durable log for client %d: %v",
				r.self, msg.ClientId, err)
		} else {
			r.clientLogs[msg.ClientId] = cl
		}
	}
	r.mu.Unlock()

	// Durably record the op in this client's per-slot log (slot == seq). The
	// append only buffers; flushLoop persists it in batches. clientLog has its
	// own lock, so this stays outside r.mu.
	if cl != nil {
		cl.append(msg.SeqNum, msg.SeqNum, len(msg.Payload))
	}
	return true
}

func (r *Replica) statsLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		r.mu.Lock()
		recv := r.winRecv
		sum, min, max := r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs
		r.winRecv, r.winDeltaSumUs, r.winDeltaMinUs, r.winDeltaMaxUs = 0, 0, 0, 0
		r.mu.Unlock()
		if recv == 0 {
			continue
		}
		avg := sum / int64(recv)
		// Same shape as the C++ replica summary; gap fields stay 0 in normal
		// processing so analyze-logs.py parses both implementations alike.
		Notice("[%s] 1s: received=%d dropped=0 delta_avg=%+dus delta_min=%+dus delta_max=%+dus gaps=0 recovered=0 noops=0",
			r.self, recv, avg, min, max)
	}
}

// flushLoop batches durable-log writes to disk. Every tick it snapshots the set
// of client logs under mu, then flushes each (the per-log lock serializes with
// concurrent appends), so at most one tick of appends is at risk on a crash.
func (r *Replica) flushLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		r.mu.Lock()
		logs := make([]*clientLog, 0, len(r.clientLogs))
		for _, cl := range r.clientLogs {
			logs = append(logs, cl)
		}
		r.mu.Unlock()
		for _, cl := range logs {
			cl.flush()
		}
	}
}
