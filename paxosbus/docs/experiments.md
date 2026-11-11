This guide provides step-by-step instructions for running PaxosBus experiments using the scripts in [`paxosbus/scripts`](../scripts/).

## Prerequisites

These scripts require prior authentication to Google Cloud Platform (GCP), a configured GCP project, and existing VMs in that project. Both the single-machine and multi-machine scripts run experiments on GCP VMs.

- VMs do not need to be started manually; the scripts start them automatically and stop when runs are complete (it will not automatically shut down if there is an error)

Create a `.env` file in the **repository root** and fill it out using [`.env.example`](../../.env.example) as a reference. Replace the placeholder project ID, VM names, and zones with your own values. The order of entries in `GCP_REPLICAS` and `GCP_CLIENTS` determines their IDs; both start at 0. Replica 0 is the initial leader.

For the majority of our PaxosBus experiments, we used the three-replica, eight-client regional setup described in [`.env.example.paxosbus`](../../.env.example.paxosbus).

Adjust `PAXOSBUS_PORT` (default: `7000`) and `GCP_GOARCH` (default: `amd64`; use `arm64` for ARM VMs) if needed.

<details>
<summary>Single-machine experiments</summary>

For single-VM smoke tests using `run-paxosbus-single-machine.sh`, configure your `.env` as follows:

- **Required:** `GCP_PROJECT_ID`, `GCP_SINGLE_VM`, and `GCP_SINGLE_ZONE`
- **Optional:** `PAXOSBUS_REPLICA_COUNT`, `PAXOSBUS_CLIENT_COUNT`, `PAXOSBUS_PORT`,  `GCP_GOARCH`
- **Not needed:** `GCP_REPLICAS`, `GCP_CLIENTS`

<details>
<summary>Default values if omitted</summary>

| Setting | Default |
| --- | --- |
| `PAXOSBUS_REPLICA_COUNT` | `3` replica processes |
| `PAXOSBUS_CLIENT_COUNT` | `8` client processes |
| `PAXOSBUS_PORT` | `7000` |
| `GCP_GOARCH` | `amd64` |

All replica and client processes run on the single configured VM.

</details>

<br>

Replace any placeholders in settings you keep with actual values. Omit optional settings to use their defaults

</details>

## Single and Multi-Machine Tests

Single-machine and multi-machine tests use the same underlying script; only the entry script and `.env` settings differ, as described in [Prerequisites](#prerequisites). (The script parameters used are the defaults; feel free to override with your own.)

Single-machine tests:

```bash
paxosbus/scripts/run-paxosbus-single-machine.sh \
  --normal \
  --request-interval-us 500 \
  --bus-interval-ms 1 \
  --command-size 512 \
  --schedule-window-ms 2000 \
  --duration-s 60
```

Multi-machine tests:

```bash
paxosbus/scripts/run-paxosbus.sh \
  --normal \
  --request-interval-us 500 \
  --bus-interval-ms 1 \
  --command-size 512 \
  --schedule-window-ms 2000 \
  --duration-s 60
```

Each run saves its logs in the main checkout under `paxosbus/logs/gcp-paxosbus/gcp-run-YYYYMMDD-HHMMSS/`. The script prints the absolute directory path after `Done. VMs stopped. Logs:`.
```bash
RUN_DIR="paxosbus/logs/gcp-paxosbus/gcp-run-YYYYMMDD-HHMMSS"
ls "$RUN_DIR"
```

Run the log analyzer.

```bash
python3 paxosbus/scripts/analyzer.py "$RUN_DIR"
```
The analyzer will print committed request counts, committed requests per second, and mean, p50, p95, and p99 latency in milliseconds for each client and all clients combined. It will first print metrics for each client then all clients at the end

<details>
<summary>Sample use</summary>

Experiment command, logs, and completion:

```bash
$ paxosbus/scripts/run-paxosbus.sh --normal
...
[r<id>] <timestamp> <thread-id> * statsLoop (replica.go:<line>): [Replica <id> <region>] 1s: bus_log_len=<length> bus_log_high=<high> entry_log_len=<entries> pruned_below=<slot>
[r<id>] <timestamp> <thread-id> * statsLoop (replica.go:<line>): [Replica <id> <region>] 1s: bus_log_len=<length> bus_log_high=<high> entry_log_len=<entries> pruned_below=<slot>
...
  all experiment VMs stopped
==> Done. VMs stopped. Logs: /path/to/paxos-bus/paxosbus/logs/gcp-paxosbus/gcp-run-<YYYYMMDD-HHMMSS>
```

Analyzer command and output:

```bash
$ python3 paxosbus/scripts/analyzer.py "$RUN_DIR"
...
paxosbus-client-<id>: committed=<count> committed/s=<throughput> mean=<mean>ms p50=<p50>ms p95=<p95>ms p99=<p99>ms
paxosbus-client-<id>: committed=<count> committed/s=<throughput> mean=<mean>ms p50=<p50>ms p95=<p95>ms p99=<p99>ms
...
all-clients: committed=<total-count> committed/s=<total-throughput> mean=<mean>ms p50=<p50>ms p95=<p95>ms p99=<p99>ms
```

</details>
