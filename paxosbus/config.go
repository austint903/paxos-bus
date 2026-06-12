package paxosbus

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config mirrors the C++ specpaxos configuration file format:
//
//	f 1
//	replica 172.29.0.10:7000
//	replica 172.29.0.11:7000
//	replica 172.29.0.12:7000
type Config struct {
	N        int
	F        int
	Replicas []string
}

func ReadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	c := &Config{F: -1}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		switch fields[0] {
		case "f":
			if len(fields) != 2 {
				return nil, fmt.Errorf("config: malformed f line: %q", line)
			}
			v, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, fmt.Errorf("config: malformed f line: %q", line)
			}
			c.F = v
		case "replica":
			if len(fields) != 2 || !strings.Contains(fields[1], ":") {
				return nil, fmt.Errorf("config: malformed replica line: %q", line)
			}
			c.Replicas = append(c.Replicas, fields[1])
		default:
			return nil, fmt.Errorf("config: unknown directive: %q", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if c.F < 0 {
		return nil, fmt.Errorf("config: missing f line in %s", path)
	}
	if len(c.Replicas) == 0 {
		return nil, fmt.Errorf("config: no replica lines in %s", path)
	}
	c.N = len(c.Replicas)
	if c.N < 2*c.F+1 {
		return nil, fmt.Errorf("config: n=%d too small for f=%d (need 2f+1)", c.N, c.F)
	}
	return c, nil
}

// QuorumSize is f+1: a majority that, combined with the leader-inclusion rule,
// makes a commit durable across gap agreement.
func (c *Config) QuorumSize() int {
	return c.F + 1
}

func (c *Config) LeaderIndex(viewId uint64) int {
	return int(viewId % uint64(c.N))
}

// Port returns the port part of replica i's address (replicas bind 0.0.0.0).
func (c *Config) Port(i int) string {
	addr := c.Replicas[i]
	return addr[strings.LastIndex(addr, ":")+1:]
}
