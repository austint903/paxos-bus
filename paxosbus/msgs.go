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
	MsgSync uint8 = iota + 1
	MsgData
	MsgDataReply
)

// Phase 1: clock sync - sent once per client at startup.
type SyncMessage struct {
	ClientId     uint64
	SendTimeNs   uint64
	IntervalMs   uint64
	StartDelayMs uint64 // ms from sync send until first data send
}

// Phase 2: periodic data delivery.
type DataMessage struct {
	ClientId   uint64
	SeqNum     uint64
	SendTimeNs uint64
	AppReqId   uint64 // stable id across resends (resend lands at new seq)
	Payload    []byte
}

// Phase 2: replica acknowledgement of a DataMessage (sent back to the client
// on the same connection the data arrived on).
type DataReplyMessage struct {
	ClientId   uint64
	SeqNum     uint64
	ViewId     uint64
	LogSlotNum uint64
	ReplicaIdx uint32
}

func (m *SyncMessage) New() fastrpc.Serializable {
	return new(SyncMessage)
}

func (m *SyncMessage) Marshal(wire io.Writer) {
	var b [32]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.SendTimeNs)
	binary.LittleEndian.PutUint64(b[16:24], m.IntervalMs)
	binary.LittleEndian.PutUint64(b[24:32], m.StartDelayMs)
	wire.Write(b[:])
}

func (m *SyncMessage) Unmarshal(wire io.Reader) error {
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

func (m *DataMessage) New() fastrpc.Serializable {
	return new(DataMessage)
}

func (m *DataMessage) Marshal(wire io.Writer) {
	var b [36]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.SeqNum)
	binary.LittleEndian.PutUint64(b[16:24], m.SendTimeNs)
	binary.LittleEndian.PutUint64(b[24:32], m.AppReqId)
	binary.LittleEndian.PutUint32(b[32:36], uint32(len(m.Payload)))
	wire.Write(b[:])
	if len(m.Payload) > 0 {
		wire.Write(m.Payload)
	}
}

func (m *DataMessage) Unmarshal(wire io.Reader) error {
	var b [36]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.SeqNum = binary.LittleEndian.Uint64(b[8:16])
	m.SendTimeNs = binary.LittleEndian.Uint64(b[16:24])
	m.AppReqId = binary.LittleEndian.Uint64(b[24:32])
	plen := binary.LittleEndian.Uint32(b[32:36])
	m.Payload = make([]byte, plen)
	if _, err := io.ReadFull(wire, m.Payload); err != nil {
		return err
	}
	return nil
}

func (m *DataReplyMessage) New() fastrpc.Serializable {
	return new(DataReplyMessage)
}

func (m *DataReplyMessage) Marshal(wire io.Writer) {
	var b [36]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.SeqNum)
	binary.LittleEndian.PutUint64(b[16:24], m.ViewId)
	binary.LittleEndian.PutUint64(b[24:32], m.LogSlotNum)
	binary.LittleEndian.PutUint32(b[32:36], m.ReplicaIdx)
	wire.Write(b[:])
}

func (m *DataReplyMessage) Unmarshal(wire io.Reader) error {
	var b [36]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.SeqNum = binary.LittleEndian.Uint64(b[8:16])
	m.ViewId = binary.LittleEndian.Uint64(b[16:24])
	m.LogSlotNum = binary.LittleEndian.Uint64(b[24:32])
	m.ReplicaIdx = binary.LittleEndian.Uint32(b[32:36])
	return nil
}
