package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/imdea-software/swiftpaxos/paxosbus"
)

func main() {
	configPath := flag.String("c", "", "path to replica config file")
	index := flag.Int("i", -1, "replica index")
	label := flag.String("l", "", "location label shown in every log line, e.g. us-east1")
	logDir := flag.String("d", "", "directory for the durable logs (replica.log bus-message log + requestlist.log request log list; empty = disabled)")
	dropMode := flag.String("drop-mode", "none",
		"artificial drop scenario: none|leader|followers|all (gap-agreement testing)")
	dropEvery := flag.Uint64("drop-every", 0,
		"drop a slot when requestId %% drop-every == 0 (0 = disabled)")
	gapDeltaMs := flag.Uint64("gap-delta-ms", 5000,
		"how long past a slot's expected arrival before it is treated as a gap; must exceed max one-way delay + prediction error")
	syncIntervalMs := flag.Uint64("sync-interval-ms", 100,
		"leader heartbeat / commit-point interval in ms; must exceed the round trip to the nearest follower")
	suspectTimeoutMs := flag.Uint64("suspect-timeout-ms", 1000,
		"how long without a leader heartbeat before suspecting it and starting a view change; sized against TCP retransmission and node stalls, not as a multiple of the heartbeat")
	viewChangeTimeoutMs := flag.Uint64("view-change-timeout-ms", 15000,
		"how long the new leader waits for a view-change quorum before moving to the next view")
	retainSlots := flag.Uint64("retain-slots", 1<<14,
		"how many committed slots stay in memory; older ones are served from the durable log instead")
	retainMB := flag.Uint64("retain-mb", 256,
		"cap on retained request payloads in MB; reclaims past -retain-slots when a bus carries many requests")
	flag.Parse()

	if *configPath == "" || *index < 0 {
		fmt.Fprintf(os.Stderr,
			"usage: %s -c <config-file> -i <replica-index> [-l <label>] [-drop-mode <mode>] [-drop-every <n>]\n", os.Args[0])
		os.Exit(1)
	}

	mode, err := paxosbus.ParseDropMode(*dropMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	config, err := paxosbus.ReadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read config: %v\n", err)
		os.Exit(1)
	}
	if *index >= config.N {
		fmt.Fprintf(os.Stderr, "replica index %d out of range (n=%d)\n", *index, config.N)
		os.Exit(1)
	}

	replica := paxosbus.NewReplica(config, *index, *label, *logDir, mode, *dropEvery, *gapDeltaMs,
		paxosbus.RecoveryOptions{
			SyncIntervalMs:      *syncIntervalMs,
			SuspectTimeoutMs:    *suspectTimeoutMs,
			ViewChangeTimeoutMs: *viewChangeTimeoutMs,
			RetainSlots:         *retainSlots,
			RetainBytes:         *retainMB << 20,
		})
	if err := replica.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "replica failed: %v\n", err)
		os.Exit(1)
	}
}
