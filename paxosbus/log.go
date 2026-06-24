package paxosbus

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

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
