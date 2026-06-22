package paxosbus

import (
	"encoding/binary"
	"io"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
)

// Message type codes: one byte on the wire, followed by the marshaled body.
// Same framing as the swiftpaxos RPC tables, so larger messages ride a TCP
// stream instead of UDP datagrams.
const (
	MsgBusSync uint8 = iota + 1
	MsgBusRequest
	MsgBusReply
	// Gap-agreement messages (inter-replica). They ride the same accept loop as
	// client traffic; the type byte distinguishes a peer message from a client
	// message. Every gap message carries SenderIdx so the receiver replies over
	// its own outbound connection to that peer (peerWriters[SenderIdx]).
	MsgBusGapRequest
	MsgBusGapReply
	MsgBusGapCommit
	MsgBusGapCommitReply
)

// wireMsg is anything with a little-endian Marshal, so lockedWriter can send any
// message type over a connection (see lockedWriter.sendMsg).
type wireMsg interface {
	Marshal(io.Writer)
}

// Phase 1: clock sync - sent once per client at startup.
type BusSyncMessage struct {
	ClientId     uint64
	SendTimeNs   uint64
	IntervalMs   uint64
	StartDelayMs uint64 // ms from sync send until first request send
}

// Phase 2: a client bus request. RequestId is the client-assigned bus message
// number (monotonic, one per request). Without gap agreement the replica's log
// slot equals RequestId; once gap agreement lands (NoOp fill / recovery) the
// two diverge, which is why the reply carries both.
type BusRequestMessage struct {
	ClientId   uint64
	RequestId  uint64
	SendTimeNs uint64
	Op         []byte // the operation to apply (placeholder until an exec layer)
}

// Phase 2: a replica's reply to a BusRequest (sent back on the same connection
// the request arrived on). RequestId echoes the request for matching; LogSlotNum
// is where the replica placed it (== RequestId pre-gap-agreement); Result is the
// operation's result (nil for now — there is no execution layer yet).
type BusReplyMessage struct {
	ClientId   uint64
	RequestId  uint64
	LogSlotNum uint64
	ViewId     uint64
	ReplicaIdx uint32
	Result     []byte
}

func (m *BusSyncMessage) New() fastrpc.Serializable {
	return new(BusSyncMessage)
}

func (m *BusSyncMessage) Marshal(wire io.Writer) {
	var b [32]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.SendTimeNs)
	binary.LittleEndian.PutUint64(b[16:24], m.IntervalMs)
	binary.LittleEndian.PutUint64(b[24:32], m.StartDelayMs)
	wire.Write(b[:])
}

func (m *BusSyncMessage) Unmarshal(wire io.Reader) error {
	var b [32]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.SendTimeNs = binary.LittleEndian.Uint64(b[8:16])
	m.IntervalMs = binary.LittleEndian.Uint64(b[16:24])
	m.StartDelayMs = binary.LittleEndian.Uint64(b[24:32])
	return nil
}

func (m *BusRequestMessage) New() fastrpc.Serializable {
	return new(BusRequestMessage)
}

func (m *BusRequestMessage) Marshal(wire io.Writer) {
	var b [28]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.RequestId)
	binary.LittleEndian.PutUint64(b[16:24], m.SendTimeNs)
	binary.LittleEndian.PutUint32(b[24:28], uint32(len(m.Op)))
	wire.Write(b[:])
	if len(m.Op) > 0 {
		wire.Write(m.Op)
	}
}

func (m *BusRequestMessage) Unmarshal(wire io.Reader) error {
	var b [28]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.RequestId = binary.LittleEndian.Uint64(b[8:16])
	m.SendTimeNs = binary.LittleEndian.Uint64(b[16:24])
	opLen := binary.LittleEndian.Uint32(b[24:28])
	m.Op = make([]byte, opLen)
	if _, err := io.ReadFull(wire, m.Op); err != nil {
		return err
	}
	return nil
}

func (m *BusReplyMessage) New() fastrpc.Serializable {
	return new(BusReplyMessage)
}

func (m *BusReplyMessage) Marshal(wire io.Writer) {
	var b [40]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.RequestId)
	binary.LittleEndian.PutUint64(b[16:24], m.LogSlotNum)
	binary.LittleEndian.PutUint64(b[24:32], m.ViewId)
	binary.LittleEndian.PutUint32(b[32:36], m.ReplicaIdx)
	binary.LittleEndian.PutUint32(b[36:40], uint32(len(m.Result)))
	wire.Write(b[:])
	if len(m.Result) > 0 {
		wire.Write(m.Result)
	}
}

func (m *BusReplyMessage) Unmarshal(wire io.Reader) error {
	var b [40]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.RequestId = binary.LittleEndian.Uint64(b[8:16])
	m.LogSlotNum = binary.LittleEndian.Uint64(b[16:24])
	m.ViewId = binary.LittleEndian.Uint64(b[24:32])
	m.ReplicaIdx = binary.LittleEndian.Uint32(b[32:36])
	resLen := binary.LittleEndian.Uint32(b[36:40])
	m.Result = make([]byte, resLen)
	if _, err := io.ReadFull(wire, m.Result); err != nil {
		return err
	}
	return nil
}

// BusGapRequest asks a peer "do you have client ClientId's slot Slot?". A
// follower sends it to the leader; the leader also broadcasts it to all peers
// when it lacks the slot itself.
type BusGapRequest struct {
	ClientId  uint64
	Slot      uint64
	SenderIdx uint32
}

// BusGapReply answers a BusGapRequest. It is the ONLY carrier of recovered real
// data (pull-based, point-to-point): Found + the op bytes, or Found=false.
type BusGapReply struct {
	ClientId  uint64
	Slot      uint64
	SenderIdx uint32
	Found     bool
	Op        []byte
}

// BusGapCommit is the leader's authoritative NoOp commit for a slot (Scenario 3
// only; it never carries real data). Followers apply it unconditionally.
type BusGapCommit struct {
	ClientId  uint64
	Slot      uint64
	SenderIdx uint32
	ViewId    uint64
}

// BusGapCommitReply acks a BusGapCommit back to the leader.
type BusGapCommitReply struct {
	ClientId  uint64
	Slot      uint64
	SenderIdx uint32
}

func (m *BusGapRequest) New() fastrpc.Serializable { return new(BusGapRequest) }

func (m *BusGapRequest) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.Slot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusGapRequest) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.Slot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	return nil
}

func (m *BusGapReply) New() fastrpc.Serializable { return new(BusGapReply) }

func (m *BusGapReply) Marshal(wire io.Writer) {
	var b [25]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.Slot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	if m.Found {
		b[20] = 1
	}
	binary.LittleEndian.PutUint32(b[21:25], uint32(len(m.Op)))
	wire.Write(b[:])
	if len(m.Op) > 0 {
		wire.Write(m.Op)
	}
}

func (m *BusGapReply) Unmarshal(wire io.Reader) error {
	var b [25]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.Slot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	m.Found = b[20] != 0
	opLen := binary.LittleEndian.Uint32(b[21:25])
	m.Op = make([]byte, opLen)
	if _, err := io.ReadFull(wire, m.Op); err != nil {
		return err
	}
	return nil
}

func (m *BusGapCommit) New() fastrpc.Serializable { return new(BusGapCommit) }

func (m *BusGapCommit) Marshal(wire io.Writer) {
	var b [28]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.Slot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[20:28], m.ViewId)
	wire.Write(b[:])
}

func (m *BusGapCommit) Unmarshal(wire io.Reader) error {
	var b [28]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.Slot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	m.ViewId = binary.LittleEndian.Uint64(b[20:28])
	return nil
}

func (m *BusGapCommitReply) New() fastrpc.Serializable { return new(BusGapCommitReply) }

func (m *BusGapCommitReply) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.Slot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusGapCommitReply) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.Slot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	return nil
}
