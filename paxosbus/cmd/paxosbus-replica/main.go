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

	replica := paxosbus.NewReplica(config, *index, *label, *logDir, mode, *dropEvery, *gapDeltaMs)
	if err := replica.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "replica failed: %v\n", err)
		os.Exit(1)
	}
}
