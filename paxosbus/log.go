package paxosbus

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Notice/Warning mimic the C++ lib/message.cc output with TIMESTAMP_NUMERIC:
//
//	20260511-170203-1234 12345 * SendTick        (client.go:85):     [Client 1] ...
//
// The timestamp keeps message.cc's 0-based month quirk so analyze-logs.py's
// NUMERIC_TS parser (which does month+1) reads both C++ and Go logs the same
// way. The trailing -1234 field is tenths of milliseconds.

var logMu sync.Mutex
var pid = os.Getpid()

func logLine(prefix, format string, args ...any) {
	now := time.Now()
	funcName := "???"
	filePos := "(???:0):"
	if pc, file, line, ok := runtime.Caller(2); ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			name := fn.Name()
			funcName = name[strings.LastIndex(name, ".")+1:]
		}
		if i := strings.LastIndex(file, "/"); i >= 0 {
			file = file[i+1:]
		}
		filePos = fmt.Sprintf("(%s:%d):", file, line)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%04d%02d%02d-%02d%02d%02d-%04d %05d %s %-15s %-19s ",
		now.Year(), int(now.Month())-1, now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		now.Nanosecond()/100000, pid, prefix, funcName, filePos)
	fmt.Fprintf(&sb, format, args...)
	sb.WriteByte('\n')

	logMu.Lock()
	os.Stderr.WriteString(sb.String())
	logMu.Unlock()
}

func Notice(format string, args ...any) {
	logLine("*", format, args...)
}

func Warning(format string, args ...any) {
	logLine("!", format, args...)
}

func Panic(format string, args ...any) {
	logLine("PANIC", format, args...)
	os.Exit(1)
}
