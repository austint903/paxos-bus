package paxosbus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
)

func marshalRequests(reqs []RequestMessage) []byte {
	var buf bytes.Buffer
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(len(reqs)))
	buf.Write(b[:])
	for i := range reqs {
		reqs[i].Marshal(&buf)
	}
	return buf.Bytes()
}

func unmarshalRequests(p []byte) ([]RequestMessage, error) {
	rd := bytes.NewReader(p)
	var b [4]byte
	if _, err := io.ReadFull(rd, b[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(b[:])
	reqs := make([]RequestMessage, n)
	for i := range reqs {
		if err := reqs[i].Unmarshal(rd); err != nil {
			return nil, err
		}
	}
	return reqs, nil
}

const (
	MsgBusSync uint8 = iota + 1
	_                // retired: direct, unbatched request
	_                // retired: direct, unbatched reply
	MsgBusGapRequest
	MsgBusGapReply
	MsgBusGapCommit
	MsgBusGapCommitReply
	MsgBus
	MsgRequestReply

	// Failure recovery. The first three are the periodic leader heartbeat that
	// doubles as the commit-point protocol; the rest drive view change and the
	// state transfer that replaces shipping the log in a message.
	MsgBusSyncPrepare
	MsgBusSyncReply
	MsgBusSyncCommit
	MsgBusViewChangeRequest
	MsgBusViewChange
	MsgBusStartView
	MsgBusStateQuery
	MsgBusGetState
	MsgBusNewState
)

// Sanity caps on the variable-length parts of the recovery messages. A view
// change carries only metadata, so anything beyond these is a corrupt or
// hostile frame rather than a big log.
const (
	maxBitmapBytes  = 1 << 20
	maxSlotListLen  = 1 << 20
	maxStateEntries = 1 << 16
)

type wireMsg interface {
	Marshal(io.Writer)
}

type BusSyncMessage struct {
	ClientId   uint64
	FirstMsgNs uint64 // wall-clock ns when this client's first bus ARRIVES (it departs maxOWD earlier)
	IntervalMs uint64 // bus interval; expect msg n at FirstMsgNs + (n-1)*IntervalMs
}

func (m *BusSyncMessage) New() fastrpc.Serializable {
	return new(BusSyncMessage)
}

func (m *BusSyncMessage) Marshal(wire io.Writer) {
	var b [24]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.FirstMsgNs)
	binary.LittleEndian.PutUint64(b[16:24], m.IntervalMs)
	wire.Write(b[:])
}

func (m *BusSyncMessage) Unmarshal(wire io.Reader) error {
	var b [24]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.FirstMsgNs = binary.LittleEndian.Uint64(b[8:16])
	m.IntervalMs = binary.LittleEndian.Uint64(b[16:24])
	return nil
}

type RequestMessage struct {
	ClientId   uint64
	RequestId  uint64
	SendTimeNs uint64
	Op         []byte
}

type BusMessage struct {
	ClientId   uint64
	BusSeqNum  uint64
	SendTimeNs uint64
	Requests   []RequestMessage
}

type RequestReplyMessage struct {
	ClientId   uint64
	RequestId  uint64
	BusSlotNum uint64
	LogIndex   uint64
	ViewId     uint64
	ReplicaIdx uint32
	Result     []byte
}

func (m *RequestMessage) New() fastrpc.Serializable {
	return new(RequestMessage)
}

func (m *RequestMessage) Marshal(wire io.Writer) {
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

func (m *RequestMessage) Unmarshal(wire io.Reader) error {
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

func (m *BusMessage) New() fastrpc.Serializable {
	return new(BusMessage)
}

func (m *BusMessage) Marshal(wire io.Writer) {
	var b [28]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.BusSeqNum)
	binary.LittleEndian.PutUint64(b[16:24], m.SendTimeNs)
	binary.LittleEndian.PutUint32(b[24:28], uint32(len(m.Requests)))
	wire.Write(b[:])
	for i := range m.Requests {
		m.Requests[i].Marshal(wire)
	}
}

func (m *BusMessage) Unmarshal(wire io.Reader) error {
	var b [28]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.BusSeqNum = binary.LittleEndian.Uint64(b[8:16])
	m.SendTimeNs = binary.LittleEndian.Uint64(b[16:24])
	count := binary.LittleEndian.Uint32(b[24:28])
	m.Requests = make([]RequestMessage, count)
	for i := uint32(0); i < count; i++ {
		if err := m.Requests[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	return nil
}

func (m *RequestReplyMessage) New() fastrpc.Serializable {
	return new(RequestReplyMessage)
}

func (m *RequestReplyMessage) Marshal(wire io.Writer) {
	var b [48]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ClientId)
	binary.LittleEndian.PutUint64(b[8:16], m.RequestId)
	binary.LittleEndian.PutUint64(b[16:24], m.BusSlotNum)
	binary.LittleEndian.PutUint64(b[24:32], m.LogIndex)
	binary.LittleEndian.PutUint64(b[32:40], m.ViewId)
	binary.LittleEndian.PutUint32(b[40:44], m.ReplicaIdx)
	binary.LittleEndian.PutUint32(b[44:48], uint32(len(m.Result)))
	wire.Write(b[:])
	if len(m.Result) > 0 {
		wire.Write(m.Result)
	}
}

func (m *RequestReplyMessage) Unmarshal(wire io.Reader) error {
	var b [48]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[0:8])
	m.RequestId = binary.LittleEndian.Uint64(b[8:16])
	m.BusSlotNum = binary.LittleEndian.Uint64(b[16:24])
	m.LogIndex = binary.LittleEndian.Uint64(b[24:32])
	m.ViewId = binary.LittleEndian.Uint64(b[32:40])
	m.ReplicaIdx = binary.LittleEndian.Uint32(b[40:44])
	resLen := binary.LittleEndian.Uint32(b[44:48])
	m.Result = make([]byte, resLen)
	if _, err := io.ReadFull(wire, m.Result); err != nil {
		return err
	}
	return nil
}

// Gap messages use (ViewId, Slot) as the identity of one logical agreement.
// Retransmissions keep both fields unchanged, and a view change invalidates all
// messages from the previous identity.
type BusGapRequest struct {
	Slot      uint64
	SenderIdx uint32
	ViewId    uint64
}

type BusGapReply struct {
	Slot      uint64
	SenderIdx uint32
	ViewId    uint64
	Found     bool
	Bus       bool
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
	ViewId    uint64
}

func (m *BusGapRequest) New() fastrpc.Serializable { return new(BusGapRequest) }

func (m *BusGapRequest) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[12:20], m.ViewId)
	wire.Write(b[:])
}

func (m *BusGapRequest) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	m.ViewId = binary.LittleEndian.Uint64(b[12:20])
	return nil
}

func (m *BusGapReply) New() fastrpc.Serializable { return new(BusGapReply) }

func (m *BusGapReply) Marshal(wire io.Writer) {
	var b [26]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[12:20], m.ViewId)
	if m.Found {
		b[20] = 1
	}
	if m.Bus {
		b[21] = 1
	}
	binary.LittleEndian.PutUint32(b[22:26], uint32(len(m.Op)))
	wire.Write(b[:])
	if len(m.Op) > 0 {
		wire.Write(m.Op)
	}
}

func (m *BusGapReply) Unmarshal(wire io.Reader) error {
	var b [26]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	m.ViewId = binary.LittleEndian.Uint64(b[12:20])
	m.Found = b[20] != 0
	m.Bus = b[21] != 0
	opLen := binary.LittleEndian.Uint32(b[22:26])
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
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.Slot)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[12:20], m.ViewId)
	wire.Write(b[:])
}

func (m *BusGapCommitReply) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.Slot = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	m.ViewId = binary.LittleEndian.Uint64(b[12:20])
	return nil
}

// ── Failure recovery ────────────────────────────────────────────────────────

func putSlotList(wire io.Writer, slots []uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(slots)))
	wire.Write(b[:4])
	for _, s := range slots {
		binary.LittleEndian.PutUint64(b[:], s)
		wire.Write(b[:])
	}
}

func readSlotList(wire io.Reader) ([]uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(wire, b[:4]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(b[0:4])
	if n > maxSlotListLen {
		return nil, fmt.Errorf("slot list too long: %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]uint64, n)
	for i := range out {
		if _, err := io.ReadFull(wire, b[:]); err != nil {
			return nil, err
		}
		out[i] = binary.LittleEndian.Uint64(b[:])
	}
	return out, nil
}

func putReplicaList(wire io.Writer, replicas []uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(len(replicas)))
	wire.Write(b[:])
	for _, replica := range replicas {
		binary.LittleEndian.PutUint32(b[:], replica)
		wire.Write(b[:])
	}
}

func readReplicaList(wire io.Reader) ([]uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(b[:])
	if n > maxStateEntries {
		return nil, fmt.Errorf("replica list too long: %d", n)
	}
	out := make([]uint32, n)
	for i := range out {
		if _, err := io.ReadFull(wire, b[:]); err != nil {
			return nil, err
		}
		out[i] = binary.LittleEndian.Uint32(b[:])
	}
	return out, nil
}

func putBool(b []byte, v bool) {
	if v {
		b[0] = 1
	} else {
		b[0] = 0
	}
}

// BusSyncPrepare is the leader's periodic heartbeat and the first phase of the
// commit point: it names the slot the leader wants made stable and the prefix
// hash a follower must match to agree. HasSlot is false before any client
// traffic, when the message is a bare liveness beat.
type BusSyncPrepare struct {
	ViewId     uint64
	SlotToSync uint64
	HasSlot    bool
	PrefixHash uint64
	SenderIdx  uint32
}

// BusSyncReply agrees that this replica's log matches the leader's through Slot.
type BusSyncReply struct {
	ViewId    uint64
	Slot      uint64
	SenderIdx uint32
}

// BusSyncCommit advances the stable slot once the leader has f+1 agreements
// including its own. On and below StableSlot the log is durable at a quorum.
type BusSyncCommit struct {
	ViewId     uint64
	StableSlot uint64
	SenderIdx  uint32
}

func (m *BusSyncPrepare) New() fastrpc.Serializable { return new(BusSyncPrepare) }

func (m *BusSyncPrepare) Marshal(wire io.Writer) {
	var b [29]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.SlotToSync)
	putBool(b[16:17], m.HasSlot)
	binary.LittleEndian.PutUint64(b[17:25], m.PrefixHash)
	binary.LittleEndian.PutUint32(b[25:29], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusSyncPrepare) Unmarshal(wire io.Reader) error {
	var b [29]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.SlotToSync = binary.LittleEndian.Uint64(b[8:16])
	m.HasSlot = b[16] != 0
	m.PrefixHash = binary.LittleEndian.Uint64(b[17:25])
	m.SenderIdx = binary.LittleEndian.Uint32(b[25:29])
	return nil
}

func (m *BusSyncReply) New() fastrpc.Serializable { return new(BusSyncReply) }

func (m *BusSyncReply) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.Slot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusSyncReply) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.Slot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	return nil
}

func (m *BusSyncCommit) New() fastrpc.Serializable { return new(BusSyncCommit) }

func (m *BusSyncCommit) Marshal(wire io.Writer) {
	var b [20]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.StableSlot)
	binary.LittleEndian.PutUint32(b[16:20], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusSyncCommit) Unmarshal(wire io.Reader) error {
	var b [20]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.StableSlot = binary.LittleEndian.Uint64(b[8:16])
	m.SenderIdx = binary.LittleEndian.Uint32(b[16:20])
	return nil
}

// BusViewChangeRequest announces that this replica suspects the leader and has
// moved to ViewId. Every replica that hears it joins immediately rather than
// waiting for its own timer, so the new leader collects its quorum in ~0.5 RTT.
type BusViewChangeRequest struct {
	ViewId    uint64
	SenderIdx uint32
}

// BusViewChange reports one replica's state to the new leader. It carries no
// log entries: FilledBitmap says which suffix slots this replica holds and
// NoOpSlots which of them are agreed no-ops, so the leader can merge from
// metadata alone and pull only the entries it actually lacks over BusGetState.
type BusViewChange struct {
	SenderIdx      uint32
	ViewId         uint64
	LastNormalView uint64
	StableSlot     uint64
	HasStable      bool
	PrefixHash     uint64 // at StableSlot
	NextExpected   uint64 // executed prefix length; slot NextExpected-1 is the last executed
	MaxSlotFilled  uint64
	HasMax         bool
	BitmapBase     uint64 // slot of bit 0 of FilledBitmap
	FilledBitmap   []byte // bit i set = slot BitmapBase+i is non-empty
	NoOpSlots      []uint64
}

// BusStartView installs the new view. Like BusViewChange it is metadata only:
// followers learn the committed prefix (StableSlot), how far the merged suffix
// runs (MaxSlot), which reports selected it, and which slots became no-ops.
// They remain in recovery while fetching every missing entry.
type BusStartView struct {
	ViewId          uint64
	StableSlot      uint64
	HasStable       bool
	MaxSlot         uint64
	HasMax          bool
	PrefixHash      uint64 // at StableSlot
	SenderIdx       uint32
	NoOpSlots       []uint64
	SelectedReports []uint32 // reports retained after filtering to the highest LastNormalView
}

// BusStateQuery is how a replica that finds itself in a stale view asks to be
// caught up. It does not start a view change — the current leader answers with
// its BusStartView and the querier installs it like any other.
type BusStateQuery struct {
	ViewId    uint64
	SenderIdx uint32
}

func (m *BusViewChangeRequest) New() fastrpc.Serializable { return new(BusViewChangeRequest) }

func (m *BusViewChangeRequest) Marshal(wire io.Writer) {
	var b [12]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusViewChangeRequest) Unmarshal(wire io.Reader) error {
	var b [12]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	return nil
}

func (m *BusStateQuery) New() fastrpc.Serializable { return new(BusStateQuery) }

func (m *BusStateQuery) Marshal(wire io.Writer) {
	var b [12]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint32(b[8:12], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusStateQuery) Unmarshal(wire io.Reader) error {
	var b [12]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.SenderIdx = binary.LittleEndian.Uint32(b[8:12])
	return nil
}

func (m *BusViewChange) New() fastrpc.Serializable { return new(BusViewChange) }

func (m *BusViewChange) Marshal(wire io.Writer) {
	var b [58]byte
	binary.LittleEndian.PutUint32(b[0:4], m.SenderIdx)
	binary.LittleEndian.PutUint64(b[4:12], m.ViewId)
	binary.LittleEndian.PutUint64(b[12:20], m.LastNormalView)
	binary.LittleEndian.PutUint64(b[20:28], m.StableSlot)
	putBool(b[28:29], m.HasStable)
	binary.LittleEndian.PutUint64(b[29:37], m.PrefixHash)
	binary.LittleEndian.PutUint64(b[37:45], m.NextExpected)
	binary.LittleEndian.PutUint64(b[45:53], m.MaxSlotFilled)
	putBool(b[53:54], m.HasMax)
	binary.LittleEndian.PutUint32(b[54:58], uint32(len(m.FilledBitmap)))
	wire.Write(b[:])
	if len(m.FilledBitmap) > 0 {
		wire.Write(m.FilledBitmap)
	}
	var base [8]byte
	binary.LittleEndian.PutUint64(base[:], m.BitmapBase)
	wire.Write(base[:])
	putSlotList(wire, m.NoOpSlots)
}

func (m *BusViewChange) Unmarshal(wire io.Reader) error {
	var b [58]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.SenderIdx = binary.LittleEndian.Uint32(b[0:4])
	m.ViewId = binary.LittleEndian.Uint64(b[4:12])
	m.LastNormalView = binary.LittleEndian.Uint64(b[12:20])
	m.StableSlot = binary.LittleEndian.Uint64(b[20:28])
	m.HasStable = b[28] != 0
	m.PrefixHash = binary.LittleEndian.Uint64(b[29:37])
	m.NextExpected = binary.LittleEndian.Uint64(b[37:45])
	m.MaxSlotFilled = binary.LittleEndian.Uint64(b[45:53])
	m.HasMax = b[53] != 0
	bmLen := binary.LittleEndian.Uint32(b[54:58])
	if bmLen > maxBitmapBytes {
		return fmt.Errorf("view-change bitmap too long: %d", bmLen)
	}
	m.FilledBitmap = make([]byte, bmLen)
	if _, err := io.ReadFull(wire, m.FilledBitmap); err != nil {
		return err
	}
	var base [8]byte
	if _, err := io.ReadFull(wire, base[:]); err != nil {
		return err
	}
	m.BitmapBase = binary.LittleEndian.Uint64(base[:])
	slots, err := readSlotList(wire)
	if err != nil {
		return err
	}
	m.NoOpSlots = slots
	return nil
}

func (m *BusStartView) New() fastrpc.Serializable { return new(BusStartView) }

func (m *BusStartView) Marshal(wire io.Writer) {
	var b [38]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.StableSlot)
	putBool(b[16:17], m.HasStable)
	binary.LittleEndian.PutUint64(b[17:25], m.MaxSlot)
	putBool(b[25:26], m.HasMax)
	binary.LittleEndian.PutUint64(b[26:34], m.PrefixHash)
	binary.LittleEndian.PutUint32(b[34:38], m.SenderIdx)
	wire.Write(b[:])
	putSlotList(wire, m.NoOpSlots)
	putReplicaList(wire, m.SelectedReports)
}

func (m *BusStartView) Unmarshal(wire io.Reader) error {
	var b [38]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.StableSlot = binary.LittleEndian.Uint64(b[8:16])
	m.HasStable = b[16] != 0
	m.MaxSlot = binary.LittleEndian.Uint64(b[17:25])
	m.HasMax = b[25] != 0
	m.PrefixHash = binary.LittleEndian.Uint64(b[26:34])
	m.SenderIdx = binary.LittleEndian.Uint32(b[34:38])
	slots, err := readSlotList(wire)
	if err != nil {
		return err
	}
	m.NoOpSlots = slots
	replicas, err := readReplicaList(wire)
	if err != nil {
		return err
	}
	m.SelectedReports = replicas
	return nil
}

// BusGetState asks a peer for the log content of [FromSlot, ToSlot]. This is
// the only message that ever carries entries, and the responder is free to
// answer a shorter range than asked (see BusNewState.ToSlot).
type BusGetState struct {
	ViewId    uint64
	FromSlot  uint64
	ToSlot    uint64
	FetchId   uint64
	SenderIdx uint32
}

// StateEntry is one slot's content. Payload is the marshaled request list for a
// bus (see marshalRequests); a no-op carries none.
type StateEntry struct {
	Slot     uint64
	ClientId uint64
	ReqId    uint64
	IsBus    bool
	IsNoOp   bool
	Payload  []byte
}

// BusNewState answers a BusGetState. ToSlot is how far the responder actually
// got: replies are capped by bytes, not slot count, because one bus can carry a
// thousand requests. The requester loops from ToSlot+1 until it is caught up.
// Slots the responder does not have are simply absent from Entries.
type BusNewState struct {
	ViewId    uint64
	FromSlot  uint64
	ToSlot    uint64
	FetchId   uint64
	SenderIdx uint32
	Entries   []StateEntry
}

func (m *BusGetState) New() fastrpc.Serializable { return new(BusGetState) }

func (m *BusGetState) Marshal(wire io.Writer) {
	var b [36]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.FromSlot)
	binary.LittleEndian.PutUint64(b[16:24], m.ToSlot)
	binary.LittleEndian.PutUint64(b[24:32], m.FetchId)
	binary.LittleEndian.PutUint32(b[32:36], m.SenderIdx)
	wire.Write(b[:])
}

func (m *BusGetState) Unmarshal(wire io.Reader) error {
	var b [36]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.FromSlot = binary.LittleEndian.Uint64(b[8:16])
	m.ToSlot = binary.LittleEndian.Uint64(b[16:24])
	m.FetchId = binary.LittleEndian.Uint64(b[24:32])
	m.SenderIdx = binary.LittleEndian.Uint32(b[32:36])
	return nil
}

func (e *StateEntry) Marshal(wire io.Writer) {
	var b [30]byte
	binary.LittleEndian.PutUint64(b[0:8], e.Slot)
	binary.LittleEndian.PutUint64(b[8:16], e.ClientId)
	binary.LittleEndian.PutUint64(b[16:24], e.ReqId)
	putBool(b[24:25], e.IsBus)
	putBool(b[25:26], e.IsNoOp)
	binary.LittleEndian.PutUint32(b[26:30], uint32(len(e.Payload)))
	wire.Write(b[:])
	if len(e.Payload) > 0 {
		wire.Write(e.Payload)
	}
}

func (e *StateEntry) Unmarshal(wire io.Reader) error {
	var b [30]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	e.Slot = binary.LittleEndian.Uint64(b[0:8])
	e.ClientId = binary.LittleEndian.Uint64(b[8:16])
	e.ReqId = binary.LittleEndian.Uint64(b[16:24])
	e.IsBus = b[24] != 0
	e.IsNoOp = b[25] != 0
	n := binary.LittleEndian.Uint32(b[26:30])
	e.Payload = make([]byte, n)
	if _, err := io.ReadFull(wire, e.Payload); err != nil {
		return err
	}
	return nil
}

func (m *BusNewState) New() fastrpc.Serializable { return new(BusNewState) }

func (m *BusNewState) Marshal(wire io.Writer) {
	var b [40]byte
	binary.LittleEndian.PutUint64(b[0:8], m.ViewId)
	binary.LittleEndian.PutUint64(b[8:16], m.FromSlot)
	binary.LittleEndian.PutUint64(b[16:24], m.ToSlot)
	binary.LittleEndian.PutUint64(b[24:32], m.FetchId)
	binary.LittleEndian.PutUint32(b[32:36], m.SenderIdx)
	binary.LittleEndian.PutUint32(b[36:40], uint32(len(m.Entries)))
	wire.Write(b[:])
	for i := range m.Entries {
		m.Entries[i].Marshal(wire)
	}
}

func (m *BusNewState) Unmarshal(wire io.Reader) error {
	var b [40]byte
	if _, err := io.ReadFull(wire, b[:]); err != nil {
		return err
	}
	m.ViewId = binary.LittleEndian.Uint64(b[0:8])
	m.FromSlot = binary.LittleEndian.Uint64(b[8:16])
	m.ToSlot = binary.LittleEndian.Uint64(b[16:24])
	m.FetchId = binary.LittleEndian.Uint64(b[24:32])
	m.SenderIdx = binary.LittleEndian.Uint32(b[32:36])
	n := binary.LittleEndian.Uint32(b[36:40])
	if n > maxStateEntries {
		return fmt.Errorf("state transfer too many entries: %d", n)
	}
	m.Entries = make([]StateEntry, n)
	for i := range m.Entries {
		if err := m.Entries[i].Unmarshal(wire); err != nil {
			return err
		}
	}
	return nil
}
