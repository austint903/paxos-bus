package paxosbus

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestScheduleWindowConfig(t *testing.T) {
	for _, tc := range []struct {
		directive string
		want      time.Duration
		invalid   bool
	}{
		{"", 10 * time.Second, false},
		{"schedule-window-ms 250\n", 250 * time.Millisecond, false},
		{"schedule-window-ms 0\n", 0, true},
		{"schedule-window-ms -1\n", 0, true},
		{"schedule-window-ms 18446744073709551615\n", 0, true},
	} {
		t.Run(tc.directive, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cluster.conf")
			if err := os.WriteFile(path, []byte("f 0\nclients 1\nreplica 127.0.0.1:7000\n"+tc.directive), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if tc.invalid {
				if err == nil {
					t.Fatal("accepted invalid window")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ScheduleWindow() != tc.want {
				t.Fatalf("window %v want %v", cfg.ScheduleWindow(), tc.want)
			}
		})
	}
}
