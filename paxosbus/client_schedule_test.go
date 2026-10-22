package paxosbus

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestScheduleMessages(t *testing.T) {
	for _, m := range []interface {
		wireMsg
		Unmarshal(io.Reader) error
	}{
		&HsRequest{7, 100, 2}, &SchdPrepare{ReplicaId: 1, ViewId: 4, StartTime: 100, EndTime: 200, HsReqList: []HsRequest{{7, 100, 0}, {9, 100, 2}}},
		&SchdPrepareReply{1, 4, 100, 200}, &SchdCommit{1, 4, 100, 200}, &HsReply{1, 4, 2, 100, 200},
		&BusSyncMessage{ClientId: 7, FirstMsgNs: 100, IntervalMs: 2, EndTime: 200},
	} {
		var b bytes.Buffer
		m.Marshal(&b)
		data := append([]byte(nil), b.Bytes()...)
		out := reflect.New(reflect.TypeOf(m).Elem()).Interface().(interface{ Unmarshal(io.Reader) error })
		if err := out.Unmarshal(&b); err != nil || !reflect.DeepEqual(m, out) {
			t.Fatalf("%T roundtrip: %v %v", m, out, err)
		}
		for n := 0; n < len(data); n++ {
			if err := out.Unmarshal(bytes.NewReader(data[:n])); err == nil {
				t.Fatalf("%T accepted truncated message at %d", m, n)
			}
		}
	}
	var b bytes.Buffer
	(&SchdPrepareReply{}).Marshal(&b)
	binary.Write(&b, binary.LittleEndian, uint32(maxScheduleLen+1))
	if new(SchdPrepare).Unmarshal(&b) == nil {
		t.Fatal("unbounded schedule accepted")
	}
}

func TestHandshakeValidationAndReconfigure(t *testing.T) {
	c := NewClient(&Config{N: 3, F: 1, ScheduleWindowMs: 100}, 7, 1, 1, "", 1000, false, 1, 1, 1)
	start := uint64(1_000_000_000)
	requests := map[uint64]HsRequest{start: {ClientId: 7, StartTime: start, IntervalMs: 1}}
	m := HsReply{ReplicaId: 0, ViewId: 0, IntervalMs: 1, StartTime: start, EndTime: start + 100_000_000}
	if !c.acceptHandshake(m, requests, 0) {
		t.Fatal("valid schedule rejected")
	}
	for _, bad := range []HsReply{{0, 0, 2, start, m.EndTime}, {1, 0, 1, start, m.EndTime}, {0, 0, 1, start, m.EndTime + 1}, {0, 0, 1, start + 1, m.EndTime}} {
		if c.acceptHandshake(bad, requests, 0) {
			t.Fatalf("invalid reply accepted: %+v", bad)
		}
	}
	if err := c.SetBusInterval(0); err != nil || c.desiredInterval != 0 {
		t.Fatal("disable failed")
	}
	if c.windows[start].IntervalMs != 1 {
		t.Fatal("reconfiguration changed committed schedule")
	}
	if err := c.SetBusInterval(2); err != nil || c.desiredInterval != 2 {
		t.Fatal("resume failed")
	}
	if c.SetBusInterval(math.MaxUint64) == nil {
		t.Fatal("overflow interval accepted")
	}
	c.pauseForView(1)
	if c.scheduleViewAllowed(0) {
		t.Fatal("old view remains active")
	}
	requests[start+100_000_000] = HsRequest{ClientId: 7, StartTime: start + 100_000_000, IntervalMs: 2}
	m = HsReply{ReplicaId: 1, ViewId: 1, IntervalMs: 2, StartTime: start + 100_000_000, EndTime: start + 200_000_000}
	if !c.acceptHandshake(m, requests, 1) || !c.scheduleViewAllowed(1) || c.scheduleViewAllowed(0) {
		t.Fatal("new-view window did not fence old traffic")
	}
}

func TestScheduleTarget(t *testing.T) {
	for _, tt := range []struct {
		now  int64
		want uint64
	}{{0, 20}, {9, 20}, {10, 30}, {19, 30}, {20, 40}} {
		if got := scheduleTarget(tt.now, 10*time.Nanosecond); got != tt.want {
			t.Fatalf("at %d got %d want %d", tt.now, got, tt.want)
		}
	}
	if (&Config{}).ScheduleWindow() != 10*time.Second {
		t.Fatal("default window")
	}
}

func TestHandshakeAcceptsEarlierSubmittedInterval(t *testing.T) {
	c := NewClient(&Config{N: 1, ScheduleWindowMs: 100}, 7, 1, 1, "", 1000, false, 1, 1, 1)
	start := uint64(1_000_000_000)
	c.submitted[start] = map[uint64]bool{1: true, 0: true}
	requests := map[uint64]HsRequest{start: {ClientId: 7, StartTime: start, IntervalMs: 0}}
	reply := HsReply{StartTime: start, EndTime: start + 100_000_000, IntervalMs: 1}
	if !c.acceptHandshake(reply, requests, 0) {
		t.Fatal("leader's earlier frozen interval was rejected")
	}
	if c.windows[start].IntervalMs != 1 {
		t.Fatal("committed interval changed")
	}
	reply.IntervalMs = 0
	if c.acceptHandshake(reply, requests, 0) {
		t.Fatal("same window changed after commit")
	}
}

func TestScheduleWriteDeadline(t *testing.T) {
	writer, reader := net.Pipe()
	defer writer.Close()
	defer reader.Close()
	lw := &lockedWriter{w: bufio.NewWriter(writer), conn: writer}
	done := make(chan error, 1)
	go func() { done <- lw.sendMsg(MsgHsRequest, &HsRequest{ClientId: 1}) }()
	select {
	case err := <-done:
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			t.Fatalf("expected bounded write timeout, got %v", err)
		}
	case <-time.After(clientControlWriteTimeout + time.Second):
		t.Fatal("schedule write blocked past deadline")
	}
}
func TestScheduleHistoryBeyondSingleWindowLimit(t *testing.T) {
	history := make([]BusSyncMessage, maxScheduleLen+1)
	for i := range history {
		history[i] = BusSyncMessage{ClientId: uint64(i % 8), FirstMsgNs: uint64(i/8 + 1), EndTime: uint64(i/8 + 2), IntervalMs: 1}
	}
	var b bytes.Buffer
	putSchedule(&b, history)
	got, err := readSchedule(&b)
	if err != nil || !reflect.DeepEqual(got, history) {
		t.Fatalf("history roundtrip: len=%d err=%v", len(got), err)
	}
	b.Reset()
	binary.Write(&b, binary.LittleEndian, uint32(maxScheduleHistoryLen+1))
	if _, err := readSchedule(&b); err == nil {
		t.Fatal("oversized history accepted")
	}
}
