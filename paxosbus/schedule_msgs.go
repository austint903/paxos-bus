package paxosbus

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	MsgHsRequest uint8 = MsgBusScheduleInstall + 3 + iota
	MsgSchdPrepare
	MsgSchdPrepareReply
	MsgSchdCommit
	MsgHsReply
)

type HsRequest struct{ ClientId, StartTime, IntervalMs uint64 }
type SchdPrepare struct {
	ReplicaId          uint32
	ViewId             uint64
	HsReqList          []HsRequest
	StartTime, EndTime uint64
}
type SchdPrepareReply struct {
	ReplicaId                  uint32
	ViewId, StartTime, EndTime uint64
}
type SchdCommit SchdPrepareReply
type HsReply struct {
	ReplicaId                              uint32
	ViewId, IntervalMs, StartTime, EndTime uint64
}

func (m *HsRequest) Marshal(w io.Writer) {
	var b [24]byte
	for i, v := range [...]uint64{m.ClientId, m.StartTime, m.IntervalMs} {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	w.Write(b[:])
}
func (m *HsRequest) Unmarshal(r io.Reader) error {
	var b [24]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.ClientId = binary.LittleEndian.Uint64(b[:])
	m.StartTime = binary.LittleEndian.Uint64(b[8:])
	m.IntervalMs = binary.LittleEndian.Uint64(b[16:])
	return nil
}
func (m *SchdPrepareReply) Marshal(w io.Writer) {
	var b [28]byte
	binary.LittleEndian.PutUint32(b[:], m.ReplicaId)
	for i, v := range [...]uint64{m.ViewId, m.StartTime, m.EndTime} {
		binary.LittleEndian.PutUint64(b[4+i*8:], v)
	}
	w.Write(b[:])
}
func (m *SchdPrepareReply) Unmarshal(r io.Reader) error {
	var b [28]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.ReplicaId = binary.LittleEndian.Uint32(b[:])
	m.ViewId = binary.LittleEndian.Uint64(b[4:])
	m.StartTime = binary.LittleEndian.Uint64(b[12:])
	m.EndTime = binary.LittleEndian.Uint64(b[20:])
	return nil
}
func (m *SchdCommit) Marshal(w io.Writer)         { (*SchdPrepareReply)(m).Marshal(w) }
func (m *SchdCommit) Unmarshal(r io.Reader) error { return (*SchdPrepareReply)(m).Unmarshal(r) }
func (m *SchdPrepare) Marshal(w io.Writer) {
	h := SchdPrepareReply{m.ReplicaId, m.ViewId, m.StartTime, m.EndTime}
	h.Marshal(w)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(len(m.HsReqList)))
	w.Write(b[:])
	for i := range m.HsReqList {
		m.HsReqList[i].Marshal(w)
	}
}
func (m *SchdPrepare) Unmarshal(r io.Reader) error {
	var h SchdPrepareReply
	if err := h.Unmarshal(r); err != nil {
		return err
	}
	m.ReplicaId, m.ViewId, m.StartTime, m.EndTime = h.ReplicaId, h.ViewId, h.StartTime, h.EndTime
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint32(b[:])
	if n > maxScheduleLen {
		return fmt.Errorf("schedule too long: %d", n)
	}
	m.HsReqList = make([]HsRequest, n)
	for i := range m.HsReqList {
		if err := m.HsReqList[i].Unmarshal(r); err != nil {
			return err
		}
	}
	return nil
}
func (m *HsReply) Marshal(w io.Writer) {
	h := SchdPrepareReply{m.ReplicaId, m.ViewId, m.StartTime, m.EndTime}
	h.Marshal(w)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], m.IntervalMs)
	w.Write(b[:])
}
func (m *HsReply) Unmarshal(r io.Reader) error {
	var h SchdPrepareReply
	if err := h.Unmarshal(r); err != nil {
		return err
	}
	m.ReplicaId, m.ViewId, m.StartTime, m.EndTime = h.ReplicaId, h.ViewId, h.StartTime, h.EndTime
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	m.IntervalMs = binary.LittleEndian.Uint64(b[:])
	return nil
}
