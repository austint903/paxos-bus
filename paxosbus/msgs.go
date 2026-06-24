package paxosbus

import (
	"encoding/binary"
	"io"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
)

const (
	MsgBusSync uint8 = iota + 1
	MsgBusRequest
	MsgBusReply
	MsgBusGapRequest
	MsgBusGapReply
	MsgBusGapCommit
	MsgBusGapCommitReply
)

type wireMsg interface {
	Marshal(io.Writer)
}

type BusSyncMessage struct {
	ClientId     uint64
	SendTimeNs   uint64
	IntervalMs   uint64
	StartDelayMs uint64
}

type BusRequestMessage struct {
	ClientId   uint64
	RequestId  uint64
	SendTimeNs uint64
	Op         []byte
}

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

type BusGapRequest struct {
	Slot      uint64
	SenderIdx uint32
}

type BusGapReply struct {
	Slot      uint64
	SenderIdx uint32
	Found     bool
	Op        []byte
}

type BusGapCommit struct {
	Slot      uint64
	SenderIdx uint32
	ViewId    uint64
}

type BusGapCommitReply struct {
	Slot      uint64
	SenderIdx uint32
}

func (m *BusGapRequest) New() fastrpc.Serializable { return new(BusGapRequest) }

func (m *BusGapRequest) Marshal(wire io.Writer) {
	var b [12]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusGapRequest) Unmarshal(wire io.Reader) error {
	var b [12]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	return nil
}

func (m *BusGapReply) New() fastrpc.Serializable { return new(BusGapReply) }

func (m *BusGapReply) Marshal(wire io.Writer) {
	var b [17]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	if m.Found {
		b[12] = 1
	}
	binary.LittleEndian.PutUint32(b[13:17], uint32(len(m.Op)))
	wire.Write(b[:])
	if len(m.Op) > 0 {
		wire.Write(m.Op)
	}
}

func (m *BusGapReply) Unmarshal(wire io.Reader) error {
	var b [17]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	m.Found = b[12] != 0
	opLen := binary.LittleEndian.Uint32(b[13:17])
	m.Op = make([]byte, opLen)
	if _, err := io.ReadFull(wire, m.Op); err != nil {
		return err
	}
	return nil
}

func (m *BusGapCommit) New() fastrpc.Serializable { return new(BusGapCommit) }

func (m *BusGapCommit) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[12:20], m.ViewId)
	wire.Write(b[:])
}

func (m *BusGapCommit) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	m.ViewId = binary.LittleEndian.Uint64(b[12:20])
	return nil
}

func (m *BusGapCommitReply) New() fastrpc.Serializable { return new(BusGapCommitReply) }

func (m *BusGapCommitReply) Marshal(wire io.Writer) {
	var b [12]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusGapCommitReply) Unmarshal(wire io.Reader) error {
	var b [12]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	return nil
}
