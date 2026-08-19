package paxosbus

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func configText(f, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "f %d\n", f)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "replica 127.0.0.1:%d\n", 7000+i)
	}
	return b.String()
}

func TestReadConfigRequiresExactQuorumTopology(t *testing.T) {
	tests := []struct {
		name    string
		f, n    int
		wantErr string
	}{
		{name: "single replica", f: 0, n: 1},
		{name: "three replicas", f: 1, n: 3},
		{name: "too few replicas", f: 1, n: 2, wantErr: "need n=2f+1"},
		{name: "even replica count", f: 1, n: 4, wantErr: "need n=2f+1"},
		{name: "fault count does not match replica count", f: 0, n: 3, wantErr: "need n=2f+1"},
		{name: "reply mask limit", f: 16, n: 33, wantErr: "reply-mask limit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir() + "/paxosbus.conf"
			if err := os.WriteFile(path, []byte(configText(tt.f, tt.n)), 0o600); err != nil {
				t.Fatal(err)
			}

			config, err := ReadConfig(path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ReadConfig() error = %v", err)
				}
				if config.N != tt.n || config.F != tt.f || config.QuorumSize() != tt.f+1 {
					t.Fatalf("config = N=%d F=%d quorum=%d, want N=%d F=%d quorum=%d", config.N, config.F, config.QuorumSize(), tt.n, tt.f, tt.f+1)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ReadConfig() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}
