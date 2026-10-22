package paxosbus

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReconfigurationProcess(t *testing.T) {
	role := os.Getenv("PAXOSBUS_TEST_ROLE")
	if role == "" {
		return
	}
	cfg, err := ReadConfig(os.Getenv("PAXOSBUS_TEST_CONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := strconv.Atoi(os.Getenv("PAXOSBUS_TEST_ID"))
	dir := os.Getenv("PAXOSBUS_TEST_DIR")
	if role == "replica" {
		r := NewReplica(cfg, id, "", dir, dropNone, 0, 50, RecoveryOptions{SyncIntervalMs: 50, SuspectTimeoutMs: 2000})
		t.Fatal(r.Run())
	}
	c := NewClient(cfg, uint64(id), 10, 500, "", 1000, false, 1, 1, 8)
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if b, err := os.ReadFile(filepath.Join(dir, "interval")); err == nil {
				v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
				if err == nil {
					c.SetBusInterval(v)
				}
			}
			now := uint64(wallNs() + c.maxOwdNs)
			start := now / uint64(cfg.ScheduleWindow()) * uint64(cfg.ScheduleWindow())
			c.scheduleMu.Lock()
			m, ok := c.windows[start]
			c.scheduleMu.Unlock()
			if ok {
				os.WriteFile(filepath.Join(dir, "active"), []byte(strconv.FormatUint(m.IntervalMs, 10)), 0600)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	c.Run()
}

func TestReconfigurationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess integration")
	}
	dir := t.TempDir()
	var listeners []net.Listener
	var addresses []string
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		addresses = append(addresses, l.Addr().String())
	}
	cfg := filepath.Join(dir, "cluster.conf")
	data := "f 1\nclients 2\nschedule-window-ms 500\n"
	for _, a := range addresses {
		data += "replica " + a + "\n"
	}
	if err := os.WriteFile(cfg, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, l := range listeners {
		l.Close()
	}
	logs := make(map[string]string)
	processes := make(map[string]*exec.Cmd)
	start := func(role string, id int) string {
		name := fmt.Sprintf("%s-%d", role, id)
		work := filepath.Join(dir, name)
		if err := os.MkdirAll(work, 0700); err != nil {
			t.Fatal(err)
		}
		log := filepath.Join(work, "process.log")
		out, err := os.Create(log)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestReconfigurationProcess$")
		cmd.Env = append(os.Environ(), "PAXOSBUS_TEST_ROLE="+role, "PAXOSBUS_TEST_CONFIG="+cfg, "PAXOSBUS_TEST_ID="+strconv.Itoa(id), "PAXOSBUS_TEST_DIR="+work)
		cmd.Stdout, cmd.Stderr = out, out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait(); out.Close() })
		logs[name] = log
		processes[name] = cmd
		return work
	}
	t.Cleanup(func() {
		if t.Failed() {
			for name, path := range logs {
				b, _ := os.ReadFile(path)
				if len(b) > 12000 {
					b = b[len(b)-12000:]
				}
				t.Logf("%s:\n%s", name, b)
			}
		}
	})
	for i := 0; i < 3; i++ {
		start("replica", i)
	}
	await := func(what string, fn func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if fn() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("timed out: %s", what)
	}
	await("listeners", func() bool {
		for _, a := range addresses {
			c, e := net.DialTimeout("tcp", a, 20*time.Millisecond)
			if e != nil {
				return false
			}
			c.Close()
		}
		return true
	})
	client := start("client", 1)
	start("client", 2)
	commits := func(id int) int {
		b, _ := os.ReadFile(logs[fmt.Sprintf("client-%d", id)])
		return bytes.Count(b, []byte("COMMITTED req="))
	}
	await("initial commits", func() bool { return commits(1) > 100 && commits(2) > 100 })
	set := func(interval string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(client, "interval"), []byte(interval), 0600); err != nil {
			t.Fatal(err)
		}
		await("interval "+interval, func() bool { b, _ := os.ReadFile(filepath.Join(client, "active")); return string(b) == interval })
	}
	set("0")
	time.Sleep(200 * time.Millisecond)
	disabled, other := commits(1), commits(2)
	time.Sleep(600 * time.Millisecond)
	if got := commits(1); got != disabled {
		t.Fatalf("disabled client committed new requests: %d -> %d", disabled, got)
	}
	if commits(2) <= other+100 {
		t.Fatal("active client stalled while peer disabled")
	}
	set("20")
	await("resumed commits", func() bool { return commits(1) > disabled+100 })
	beforeFailure := commits(1)
	if err := processes["replica-0"].Process.Kill(); err != nil {
		t.Fatal(err)
	}
	await("commits after leader failure", func() bool { return commits(1) > beforeFailure+100 })
	time.Sleep(1100 * time.Millisecond)
	var reference []string
	for i := 0; i < 3; i++ {
		path := filepath.Join(dir, fmt.Sprintf("replica-%d", i), "requestlist.log")
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(f)
		var entries []string
		for scanner.Scan() {
			entries = append(entries, strings.TrimSpace(scanner.Text()))
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if len(entries) < 100 {
			t.Fatalf("replica %d executed only %d requests", i, len(entries))
		}
		if i == 0 {
			reference = entries
			continue
		}
		n := len(entries)
		if len(reference) < n {
			n = len(reference)
		}
		for j := 0; j < n; j++ {
			if entries[j] != reference[j] {
				t.Fatalf("replicas disagree at request log index %d", j)
			}
		}
	}
}
