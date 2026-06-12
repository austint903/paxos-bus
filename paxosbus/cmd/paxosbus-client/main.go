package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/imdea-software/swiftpaxos/paxosbus"
)

func main() {
	configPath := flag.String("c", "", "path to replica config file")
	clientId := flag.Uint64("I", 0, "client ID (positive integer, unique per client)")
	intervalMs := flag.Uint64("p", 1, "message interval in milliseconds")
	resendMs := flag.Uint64("t", 0, "resend-on-no-quorum timeout in ms (0 = disabled)")
	label := flag.String("l", "", "location label shown in every log line, e.g. asia-east1")
	flag.Parse()

	if *configPath == "" || *clientId == 0 || *intervalMs == 0 {
		fmt.Fprintf(os.Stderr,
			"usage: %s -c <config-file> -I <client-id> [-p <interval-ms>] [-t <resend-ms>] [-l <label>]\n",
			os.Args[0])
		os.Exit(1)
	}

	config, err := paxosbus.ReadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read config: %v\n", err)
		os.Exit(1)
	}

	client := paxosbus.NewClient(config, *clientId, *intervalMs, *resendMs, *label)
	if err := client.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect: %v\n", err)
		os.Exit(1)
	}
	client.Run()
}
