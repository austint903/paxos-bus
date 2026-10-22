package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/imdea-software/swiftpaxos/paxosbus"
)

func main() {
	configPath := flag.String("c", "", "path to replica config file")
	clientId := flag.Uint64("I", 0, "client ID (unique per client; 0-indexed)")
	intervalMs := flag.Uint64("p", 1, "bus interval in milliseconds")
	resendMs := flag.Uint64("t", 5000, "per-request no-quorum re-board timeout in ms (0 uses the 5000 ms default)")
	label := flag.String("l", "", "location label shown in every log line, e.g. asia-east1")
	genIntervalUs := flag.Uint64("g", 500, "request generation interval in microseconds")
	commandSize := flag.Int("command-size", paxosbus.DefaultCommandSize, "write value size in bytes (all requests are PUTs)")
	verbose := flag.Bool("v", false, "log every per-replica REPLY line (3 log writes per request at high rates; COMMITTED lines are always logged)")
	startDelayMs := flag.Uint64("w", 5000, "delay in ms between schedule announcement and the data phase; the leader schedule barrier should complete within this window")
	maxOwdMs := flag.Float64("owd", 0, "max one-way delay to any replica in ms; buses depart this early so they arrive on the announced schedule (0 = auto-measure as max TCP dial RTT / 2)")
	reconfigure := flag.String("reconfigure", "", "comma-separated duration:bus-interval-ms changes after startup, e.g. 35s:0,55s:1")
	flag.Parse()
	if *commandSize <= 0 || uint64(*commandSize) > uint64(^uint32(0)) {
		fmt.Fprintln(os.Stderr, "command-size must be between 1 and 4294967295 bytes")
		os.Exit(1)
	}

	idSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "I" {
			idSet = true
		}
	})

	if *configPath == "" || !idSet {
		fmt.Fprintf(os.Stderr,
			"usage: %s -c <config-file> -I <client-id> [-p <bus-interval-ms>] [-t <resend-ms>] [-l <label>] [-g <gen-us>] [-command-size <bytes>]\n",
			os.Args[0])
		os.Exit(1)
	}

	config, err := paxosbus.ReadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read config: %v\n", err)
		os.Exit(1)
	}

	client := paxosbus.NewClient(config, *clientId, *intervalMs, *resendMs, *label,
		*genIntervalUs, *verbose, *startDelayMs, *maxOwdMs, *commandSize)
	if err := client.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect: %v\n", err)
		os.Exit(1)
	}
	if *reconfigure != "" {
		type change struct {
			after    time.Duration
			interval uint64
		}
		var changes []change
		for _, entry := range strings.Split(*reconfigure, ",") {
			fields := strings.Split(entry, ":")
			if len(fields) != 2 {
				fmt.Fprintln(os.Stderr, "invalid reconfigure entry:", entry)
				os.Exit(1)
			}
			after, err := time.ParseDuration(fields[0])
			interval, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || parseErr != nil || after < 0 || len(changes) > 0 && after < changes[len(changes)-1].after {
				fmt.Fprintln(os.Stderr, "invalid reconfigure entry:", entry)
				os.Exit(1)
			}
			changes = append(changes, change{after, interval})
		}
		go func() {
			start := time.Now()
			for _, change := range changes {
				time.Sleep(time.Until(start.Add(change.after)))
				if err := client.SetBusInterval(change.interval); err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}
		}()
	}
	client.Run()
}
