#!/usr/bin/env bash
set -euo pipefail

# Local run of the classic protocols (paxos, epaxos, ...) on Docker.
#
# Unlike paxosbus, these run through the single root `swiftpaxos` binary as a
# 3-role cluster: one master + N servers/replicas + M clients, all reading the
# structured aws.conf-style config. This script generates that config with
# local Docker IPs, launches every role in its own container, archives all
# logs, and prints overall throughput + latency at the end.

# ── Defaults (override via flags) ────────────────────────────────────────────
PROTOCOL=paxos          # -P  paxos|epaxos
NUM_REPLICAS=3          # -n
NUM_CLIENTS=2           # -m
REQS=1000               # -r  requests per client (closed-loop)
CMD_SIZE=16             # -s  command payload size (bytes)
CLONES=0                # -c  extra closed-loop clients per client process (Clones+1 total)
WRITES=100              # ratio of writes (%)
CONFLICTS=2             # conflict ratio
DURATION_S=60           # -d  max seconds to wait for clients to finish
FORCE_BUILD=0           # -b
# ─────────────────────────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 [-P paxos|epaxos] [-n replicas] [-m clients] [-r reqs] [-s cmd_size] [-c clones] [-d seconds] [-b]"
    echo "  -P <proto>    protocol: paxos|epaxos (default: $PROTOCOL)"
    echo "  -n <n>        number of replicas (default: $NUM_REPLICAS)"
    echo "  -m <m>        number of clients (default: $NUM_CLIENTS)"
    echo "  -r <reqs>     requests per client (default: $REQS)"
    echo "  -s <bytes>    command payload size (default: $CMD_SIZE)"
    echo "  -c <clones>   extra closed-loop clients per client process, Clones+1 total (default: $CLONES)"
    echo "  -d <seconds>  max wait for clients to finish, then teardown (default: $DURATION_S)"
    echo "  -b            force rebuild of Docker image"
    exit 1
}

while getopts "P:n:m:r:s:c:d:bh" opt; do
    case $opt in
        P) PROTOCOL=$OPTARG ;;
        n) NUM_REPLICAS=$OPTARG ;;
        m) NUM_CLIENTS=$OPTARG ;;
        r) REQS=$OPTARG ;;
        s) CMD_SIZE=$OPTARG ;;
        c) CLONES=$OPTARG ;;
        d) DURATION_S=$OPTARG ;;
        b) FORCE_BUILD=1 ;;
        h) usage ;;
        *) usage ;;
    esac
done

case "$PROTOCOL" in
    paxos|epaxos) ;;
    *) echo "ERROR: unsupported protocol '$PROTOCOL' (use paxos or epaxos)"; exit 1 ;;
esac

SUBNET="172.30.0.0/24"
NETWORK="swiftpaxos-net"
IMAGE="swiftpaxos-go"
MASTER_OCTET=5            # master:   172.30.0.5
BASE_REPLICA_OCTET=10     # replicas: 172.30.0.10 ..
BASE_CLIENT_OCTET=100     # clients:  172.30.0.100 ..
REPLICA_PORT=7070
MASTER_PORT=7087

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/config-swiftpaxos"

CONTAINERS=()
LOG_PIDS=()

cleanup() {
    echo ""
    echo "Cleaning up..."
    [[ ${#LOG_PIDS[@]} -gt 0 ]] && kill "${LOG_PIDS[@]}" 2>/dev/null || true
    for c in "${CONTAINERS[@]}"; do
        docker rm -f "$c" &>/dev/null || true
    done
    docker network rm "$NETWORK" &>/dev/null || true
}
trap cleanup EXIT

# ── Build ────────────────────────────────────────────────────────────────────
if [[ $FORCE_BUILD -eq 1 ]] || ! docker image inspect "$IMAGE" &>/dev/null; then
    echo "Building Docker image '$IMAGE'..."
    docker build -t "$IMAGE" -f "$SCRIPT_DIR/Dockerfile.swiftpaxos" "$SCRIPT_DIR"
else
    echo "Using existing Docker image '$IMAGE' (run with -b to rebuild)"
fi

docker run --rm "$IMAGE" test -x /swiftpaxos/swiftpaxos || {
    echo "ERROR: /swiftpaxos/swiftpaxos not found in image. Rebuild with: $0 -b"
    exit 1
}

# ── Cleanup stale state ───────────────────────────────────────────────────────
docker ps -aq --filter "name=swiftpaxos-" | xargs -r docker rm -f &>/dev/null || true
docker network rm "$NETWORK" &>/dev/null || true

# ── Config (structured aws.conf format, all-lowercase aliases) ───────────────
mkdir -p "$CONFIG_DIR"
CONF="$CONFIG_DIR/swiftpaxos.conf"
MASTER_IP="172.30.0.$MASTER_OCTET"
REPLICA0_IP="172.30.0.$BASE_REPLICA_OCTET"
{
    echo "-- Replicas --"
    for i in $(seq 0 $((NUM_REPLICAS - 1))); do
        echo "r$i 172.30.0.$((BASE_REPLICA_OCTET + i))"
    done
    echo ""
    echo "-- Clients --"
    for i in $(seq 1 $NUM_CLIENTS); do
        echo "c$i 172.30.0.$((BASE_CLIENT_OCTET + i - 1))"
    done
    echo ""
    echo "-- Master --"
    echo "m $MASTER_IP"
    echo ""
    echo "masterport: $MASTER_PORT"
    echo ""
    echo "protocol: $PROTOCOL"
    echo "noop: false"
    # paxos is leader-based; pin replica 0 as leader. epaxos is leaderless.
    if [[ "$PROTOCOL" == "paxos" ]]; then
        echo "leader: $REPLICA0_IP"
    fi
    echo ""
    echo "reqs: $REQS"
    echo "writes: $WRITES"
    echo "conflicts: $CONFLICTS"
    echo "commandsize: $CMD_SIZE"
    echo "clones: $CLONES"
    echo "key: 42"
    echo "pipeline: false"
    echo ""
    # Proxy: map every client to replica 0 (initial connection point). Must come
    # after the Clients section and use lowercase aliases.
    echo "-- Proxy --"
    echo "server_alias r0"
    for i in $(seq 1 $NUM_CLIENTS); do
        if [[ $i -eq 1 ]]; then
            echo "c$i (local)"
        else
            echo "c$i"
        fi
    done
    echo "---"
} > "$CONF"

echo "Config ($CONF):"
sed 's/^/  /' "$CONF"
echo ""
echo "Protocol: $PROTOCOL   replicas=$NUM_REPLICAS  clients=$NUM_CLIENTS  clones=$CLONES  reqs/client=$REQS"
echo ""

# ── Per-run log directory ────────────────────────────────────────────────────
RUN_LOG_DIR="$SCRIPT_DIR/paxosbus/logs/swiftpaxos/$PROTOCOL-run-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_LOG_DIR"
{
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "protocol=$PROTOCOL"
    echo "num_replicas=$NUM_REPLICAS"
    echo "num_clients=$NUM_CLIENTS"
    echo "reqs_per_client=$REQS"
    echo "command_size=$CMD_SIZE"
    echo "clones=$CLONES"
    echo "writes=$WRITES"
    echo "conflicts=$CONFLICTS"
} > "$RUN_LOG_DIR/run-meta.txt"
echo "Logs: $RUN_LOG_DIR"
echo ""

# ── Network ──────────────────────────────────────────────────────────────────
docker network create --subnet="$SUBNET" "$NETWORK" > /dev/null

# ── Master ───────────────────────────────────────────────────────────────────
echo "+ master  ($MASTER_IP:$MASTER_PORT)"
docker run -d \
    --name "swiftpaxos-master" \
    --network "$NETWORK" \
    --ip "$MASTER_IP" \
    -v "$CONFIG_DIR:/config:ro" \
    "$IMAGE" \
    /swiftpaxos/swiftpaxos -run master -config /config/swiftpaxos.conf -protocol "$PROTOCOL" \
    > /dev/null
CONTAINERS+=("swiftpaxos-master")
sleep 2

# ── Replicas ─────────────────────────────────────────────────────────────────
for i in $(seq 0 $((NUM_REPLICAS - 1))); do
    NAME="swiftpaxos-replica-$i"
    IP="172.30.0.$((BASE_REPLICA_OCTET + i))"
    echo "+ replica $NAME  ($IP:$REPLICA_PORT  alias=r$i)"
    docker run -d \
        --name "$NAME" \
        --network "$NETWORK" \
        --ip "$IP" \
        -v "$CONFIG_DIR:/config:ro" \
        "$IMAGE" \
        /swiftpaxos/swiftpaxos -run server -config /config/swiftpaxos.conf -protocol "$PROTOCOL" -alias "r$i" \
        > /dev/null
    CONTAINERS+=("$NAME")
done

echo "Waiting 3s for replicas to register with master..."
sleep 3

# ── Clients ──────────────────────────────────────────────────────────────────
for i in $(seq 1 $NUM_CLIENTS); do
    NAME="swiftpaxos-client-$i"
    IP="172.30.0.$((BASE_CLIENT_OCTET + i - 1))"
    echo "+ client  $NAME  ($IP  alias=c$i)"
    docker run -d \
        --name "$NAME" \
        --network "$NETWORK" \
        --ip "$IP" \
        -v "$CONFIG_DIR:/config:ro" \
        "$IMAGE" \
        /swiftpaxos/swiftpaxos -run client -config /config/swiftpaxos.conf -protocol "$PROTOCOL" -alias "c$i" \
        > /dev/null
    CONTAINERS+=("$NAME")
done

echo ""
echo "All containers running. Clients are closed-loop ($REQS reqs each, $((CLONES + 1)) per container)."
echo "Per-request 'latency <ms>' lines stream live below; summary prints at the end."
echo "──────────────────────────────────────────────────────────────"

# ── Follow logs (tee a durable copy of each node's stream) ───────────────────
# disown each follower so the shell stays quiet (no "Terminated"/"Done" job
# notifications) when the EXIT trap / teardown kills them by PID.
# --timestamps prepends an RFC3339Nano wall-clock to every line; the durable
# copy (tee target) keeps it so the metrics parser can build a global throughput
# window, while the live display strips it back off for readability.
STRIP_TS='s/^[0-9T:.+-]+Z //'
docker logs -f --timestamps "swiftpaxos-master" 2>&1 \
    | tee "$RUN_LOG_DIR/master.log" | sed -E "$STRIP_TS" | sed "s/^/[master]   /" &
LOG_PIDS+=($!); disown
for i in $(seq 0 $((NUM_REPLICAS - 1))); do
    docker logs -f --timestamps "swiftpaxos-replica-$i" 2>&1 \
        | tee "$RUN_LOG_DIR/replica-$i.log" | sed -E "$STRIP_TS" | sed "s/^/[replica-$i] /" &
    LOG_PIDS+=($!); disown
done
for i in $(seq 1 $NUM_CLIENTS); do
    docker logs -f --timestamps "swiftpaxos-client-$i" 2>&1 \
        | tee "$RUN_LOG_DIR/client-$i.log" | sed -E "$STRIP_TS" | sed "s/^/[client-$i]  /" &
    LOG_PIDS+=($!); disown
done

# ── Wait for clients to finish (up to DURATION_S), then stop ─────────────────
deadline=$((SECONDS + DURATION_S))
while (( SECONDS < deadline )); do
    running=$(docker ps -q --filter "name=swiftpaxos-client-" | wc -l | tr -d ' ')
    [[ "$running" -eq 0 ]] && break
    sleep 1
done

echo ""
echo "──────────────────────────────────────────────────────────────"
if [[ "$running" -eq 0 ]]; then
    echo "All clients finished."
else
    echo "Duration cap (${DURATION_S}s) hit — stopping with clients still running."
fi

# Give log followers a moment to flush the final lines.
sleep 1
[[ ${#LOG_PIDS[@]} -gt 0 ]] && kill "${LOG_PIDS[@]}" 2>/dev/null || true
LOG_PIDS=()

# ── Collect per-clone client logs (clones > 0) ───────────────────────────────
# Only clone 0 logs to stdout (captured above via docker logs). Clones 1..CLONES
# each write to /client_<j> inside their container, invisible to docker logs, so
# pull them out and count them too — otherwise aggregate throughput undercounts
# by a factor of (clones+1). docker cp works on running or exited containers.
if [[ "$CLONES" -gt 0 ]]; then
    for i in $(seq 1 $NUM_CLIENTS); do
        for j in $(seq 1 $CLONES); do
            docker cp "swiftpaxos-client-$i:/client_$j" \
                "$RUN_LOG_DIR/client-$i-clone-$j.log" &>/dev/null || true
        done
    done
fi

# ── Throughput (committed) + latency (RTT) summary, pooled over all threads ───
# Throughput = committed requests / wall-clock window. Each per-request `latency`
# line is emitted only on reply (i.e. a committed request), so in-flight requests
# are never counted. The window is the global first->last committed reply across
# all timestamped (stdout) client logs; clone files add to the committed count.
python3 - "$RUN_LOG_DIR" <<'PY' | tee "$RUN_LOG_DIR/metrics.txt"
import sys, glob, os, re
from datetime import datetime, timezone

run_dir = sys.argv[1]

# Docker --timestamps prefix: 2026-06-28T21:00:00.123456789Z
DOCKER_TS = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z')
LAT = re.compile(r'latency\s+([0-9.]+)')

def ts_of(line):
    m = DOCKER_TS.match(line)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1)[:26]).replace(
            tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None

# stdout logs (client-N.log) carry docker timestamps; clone files
# (client-N-clone-J.log) do not, so they add to the committed count but not the
# window. The "client-*.log" glob also matches clone files, so filter them out.
stdout_logs = [p for p in sorted(glob.glob(os.path.join(run_dir, "client-*.log")))
               if "-clone-" not in os.path.basename(p)]
clone_logs  = sorted(glob.glob(os.path.join(run_dir, "client-*-clone-*.log")))

all_lat = []          # every latency/RTT sample (ms), all threads
committed = 0         # total committed requests (= latency lines), all threads
g_first = g_last = None
per_thread = []       # (name, count)

for path in stdout_logs + clone_logs:
    n = 0
    for line in open(path, errors="replace"):
        m = LAT.search(line)
        if not m:
            continue
        all_lat.append(float(m.group(1)))
        n += 1
        t = ts_of(line)
        if t is not None:
            g_first = t if (g_first is None or t < g_first) else g_first
            g_last  = t if (g_last  is None or t > g_last)  else g_last
    committed += n
    per_thread.append((os.path.basename(path), n))

def pct(vals, p):
    if not vals:
        return 0.0
    return vals[int(round((p / 100.0) * (len(vals) - 1)))]

print("================ RESULTS ================")
for name, n in per_thread:
    print(f"  {name}: {n} committed")
print("-----------------------------------------")
print(f"COMMITTED (aggregate): {committed} requests")
if g_first is not None and g_last is not None and g_last > g_first:
    window = g_last - g_first
    print(f"WINDOW: {window:.2f}s (global first->last committed reply)")
    print(f"THROUGHPUT (aggregate): {committed / window:.1f} req/s committed")
else:
    print("THROUGHPUT: no timestamped samples (need 'docker logs --timestamps')")

if all_lat:
    s = sorted(all_lat)
    print(f"LATENCY/RTT (ms): n={len(s)}  avg={sum(s)/len(s):.3f}  "
          f"p50={pct(s,50):.3f}  p99={pct(s,99):.3f}  min={s[0]:.3f}  max={s[-1]:.3f}")
else:
    print("LATENCY: no samples collected")
print("=========================================")
PY

echo ""
echo "Saved logs + metrics in: $RUN_LOG_DIR"
exit 0
