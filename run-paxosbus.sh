#!/usr/bin/env bash
set -euo pipefail

# Local PaxosBus run (Go implementation, normal processing only).
# Mirrors prevImplementation/run-paxosbus.sh minus the gap-agreement knobs.

# ── Message rate and topology ───────────────────────────────────────────────
MSG_INTERVAL_MS=1      # change this: 1000=1s  100=100ms  10=10ms  2=2ms  1=1ms (bus interval)
NUM_REPLICAS=3
NUM_CLIENTS=2
RESEND_MS=2000         # client resend-on-no-quorum timeout (ms; 0 uses the client default)
DURATION_S="${DURATION_S:-60}"  # seconds of data phase, then auto-stop; 0 = run until Ctrl+C
SYNC_WARMUP_S=5        # client sync wait before data starts (matches syncStartDelayMs=5000)
DROP_MODE=none         # artificial drop scenario: none|leader|followers|all
DROP_EVERY=0           # drop a slot when reqId % DROP_EVERY == 0 (0 = disabled)
GEN_INTERVAL_US=500    # request generation interval in µs (-g 500 -p 1 ≈ 2 reqs/bus)
KILL_AT_S=0            # kill the current leader this many seconds into the data phase (0 = never)
KILL_COUNT=1           # how many successive leaders to kill (view 0 -> 1 -> 2 ...)
RETAIN_SLOTS=16384     # committed slots kept in memory; smaller forces state transfer to read the durable log
GAP_RETRY_TIMEOUT_MS="${GAP_RETRY_TIMEOUT_MS:-1500}" # gap-commit rebroadcast interval
# ────────────────────────────────────────────────────────────────────────────

FORCE_BUILD=0

usage() {
    echo "Usage: $0 [-b] [-g <gen_us>] [-p <interval_ms>] [-t <resend_ms>] [-d <seconds>] [-D <drop_mode>] [-F <drop_every>] [-K <seconds>] [-N <kills>]"
    echo "  -b            force rebuild of Docker image"
	 echo "  -g <us>       request generation interval in µs (default: $GEN_INTERVAL_US)"
	 echo "  -p <ms>       bus interval in ms (default: $MSG_INTERVAL_MS)"
    echo "  -t <ms>       client no-quorum re-board timeout (default: $RESEND_MS)"
    echo "  -d <seconds>  auto-stop after this many seconds of data phase (default: run until Ctrl+C)"
    echo "  -D <mode>     artificial drop scenario: none|leader|followers|all (default: $DROP_MODE)"
    echo "  -F <n>        drop a slot when reqId % n == 0 (default: $DROP_EVERY, 0=off)"
    echo "  -K <seconds>  kill the leader this far into the data phase, to exercise view change (0=off)"
    echo "  -N <kills>    how many successive leaders to kill (default: $KILL_COUNT; needs f spare replicas, so >1 wants -n 5)"
    echo "  -n <count>    odd number of replicas, 3 through 31 (default: $NUM_REPLICAS)"
    echo "  -R <slots>    committed slots kept in memory (default: $RETAIN_SLOTS; small values force disk-backed state transfer)"
    exit 1
}

while getopts "bg:p:t:d:D:F:K:N:n:R:h" opt; do
    case $opt in
        b) FORCE_BUILD=1 ;;
        g) GEN_INTERVAL_US=$OPTARG ;;
        p) MSG_INTERVAL_MS=$OPTARG ;;
        t) RESEND_MS=$OPTARG ;;
        d) DURATION_S=$OPTARG ;;
        D) DROP_MODE=$OPTARG ;;
        F) DROP_EVERY=$OPTARG ;;
        K) KILL_AT_S=$OPTARG ;;
        N) KILL_COUNT=$OPTARG ;;
        n) NUM_REPLICAS=$OPTARG ;;
        R) RETAIN_SLOTS=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

if ! [[ "$NUM_REPLICAS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: -n must be a positive integer (got: $NUM_REPLICAS)" >&2
    exit 1
fi
NUM_REPLICAS=$((10#$NUM_REPLICAS))
if (( NUM_REPLICAS < 3 || NUM_REPLICAS > 31 || NUM_REPLICAS % 2 == 0 )); then
    echo "ERROR: -n must be odd and between 3 and 31 (got: $NUM_REPLICAS)" >&2
    exit 1
fi

SUBNET="172.29.0.0/24"
NETWORK="paxosbus-net"
IMAGE="paxosbus-go"
BASE_REPLICA_OCTET=10     # replicas: 172.29.0.10 .. .12
BASE_CLIENT_OCTET=100     # clients:  172.29.0.100, .101
REPLICA_PORT=7000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/config-paxosbus"

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
    docker build -t "$IMAGE" -f "$SCRIPT_DIR/Dockerfile.paxosbus" "$SCRIPT_DIR"
else
    echo "Using existing Docker image '$IMAGE' (run with -b to rebuild)"
fi

# Verify the PaxosBus binaries exist inside the image
docker run --rm "$IMAGE" test -x /paxosbus/paxosbus-replica || {
    echo "ERROR: /paxosbus/paxosbus-replica not found in image."
    echo "Rebuild the image with: $0 -b"
    exit 1
}

# ── Cleanup stale state ───────────────────────────────────────────────────────
docker ps -aq --filter "name=paxosbus-" | xargs -r docker rm -f &>/dev/null || true
docker network rm "$NETWORK" &>/dev/null || true

# ── Config ───────────────────────────────────────────────────────────────────
mkdir -p "$CONFIG_DIR"
CONF="$CONFIG_DIR/paxosbus.conf"
F=$(( (NUM_REPLICAS - 1) / 2 ))
{
    echo "f $F"
    for i in $(seq 0 $((NUM_REPLICAS - 1))); do
        echo "replica 172.29.0.$((BASE_REPLICA_OCTET + i)):$REPLICA_PORT"
    done
} > "$CONF"

echo "Config ($CONF):"
sed 's/^/  /' "$CONF"
if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" -gt 0 ]]; then
    echo "Mode: ARTIFICIAL DROP (scenario=$DROP_MODE, every reqId%$DROP_EVERY==0)"
else
    echo "Mode: NORMAL (no artificial drops)"
fi
echo "Path: request generation (gen=${GEN_INTERVAL_US}us, bus=${MSG_INTERVAL_MS}ms)"
echo ""

# ── Per-run log directory (durable copy of every node's stream) ──────────────
RUN_LOG_DIR="$SCRIPT_DIR/paxosbus/logs/local/local-run-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_LOG_DIR"
{
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "implementation=go"
    echo "interval_ms=$MSG_INTERVAL_MS"
    echo "num_replicas=$NUM_REPLICAS"
    echo "num_clients=$NUM_CLIENTS"
    echo "resend_ms=$RESEND_MS"
    echo "drop_mode=$DROP_MODE"
    echo "drop_every=$DROP_EVERY"
    echo "gen_interval_us=$GEN_INTERVAL_US"
    echo "gap_retry_timeout_ms=$GAP_RETRY_TIMEOUT_MS"
    echo "kill_at_s=$KILL_AT_S"
    echo "kill_count=$KILL_COUNT"
} > "$RUN_LOG_DIR/run-meta.txt"
echo "Logs: $RUN_LOG_DIR"

# ── Durable global log (separate from the stderr archive above) ──────────────
# One subdir per replica; each replica writes a single global replica.log of
# slot records into its mounted /durable. Bind-mounted so the files survive the
# container.
DURABLE_DIR="$SCRIPT_DIR/paxosbus/logs/durable/local/$(basename "$RUN_LOG_DIR")"
echo "Durable logs: $DURABLE_DIR"
echo ""

# ── Network ──────────────────────────────────────────────────────────────────
docker network create --subnet="$SUBNET" "$NETWORK" > /dev/null

# ── Replicas ─────────────────────────────────────────────────────────────────
for i in $(seq 0 $((NUM_REPLICAS - 1))); do
    NAME="paxosbus-replica-$i"
    IP="172.29.0.$((BASE_REPLICA_OCTET + i))"
    echo "+ replica $NAME  ($IP:$REPLICA_PORT)"
    mkdir -p "$DURABLE_DIR/replica-$i"
    docker run -d \
        --name "$NAME" \
        --network "$NETWORK" \
        --ip "$IP" \
        -v "$CONFIG_DIR:/config:ro" \
        -v "$DURABLE_DIR/replica-$i:/durable" \
        "$IMAGE" \
        /paxosbus/paxosbus-replica -c /config/paxosbus.conf -i "$i" -d /durable \
            -drop-mode "$DROP_MODE" -drop-every "$DROP_EVERY" \
            -gap-retry-timeout-ms "$GAP_RETRY_TIMEOUT_MS" \
            -retain-slots "$RETAIN_SLOTS" \
        > /dev/null
    CONTAINERS+=("$NAME")
done

echo "Waiting 2s for replicas to bind..."
sleep 2

# ── Clients ───────────────────────────────────────────────────────────────────
CLIENT_FLAGS=(-t "$RESEND_MS" -g "$GEN_INTERVAL_US")
for i in $(seq 0 $((NUM_CLIENTS - 1))); do
    NAME="paxosbus-client-$i"
    IP="172.29.0.$((BASE_CLIENT_OCTET + i))"
    echo "+ client  $NAME  ($IP  id=$i  interval=${MSG_INTERVAL_MS}ms)"
    docker run -d \
        --name "$NAME" \
        --network "$NETWORK" \
        --ip "$IP" \
        -v "$CONFIG_DIR:/config:ro" \
        "$IMAGE" \
        /paxosbus/paxosbus-client \
            -c /config/paxosbus.conf \
            -I "$i" \
            -p "$MSG_INTERVAL_MS" \
            ${CLIENT_FLAGS[@]+"${CLIENT_FLAGS[@]}"} \
        > /dev/null
    CONTAINERS+=("$NAME")
done

echo ""
echo "All containers running."
echo "Clients will sync (${SYNC_WARMUP_S}s wait), then stream every ${MSG_INTERVAL_MS}ms."
if [[ "$DURATION_S" -gt 0 ]]; then
    echo "Auto-stopping after ${DURATION_S}s of data phase (+${SYNC_WARMUP_S}s sync warmup)."
else
    echo "Press Ctrl+C to stop."
fi
echo "──────────────────────────────────────────────────────────────"

# ── Follow replica logs (tee a durable copy per node into $RUN_LOG_DIR) ──────
for i in $(seq 0 $((NUM_REPLICAS - 1))); do
    docker logs -f --timestamps "paxosbus-replica-$i" 2>&1 \
        | tee "$RUN_LOG_DIR/replica-$i.log" \
        | sed "s/^/[replica-$i] /" &
    LOG_PIDS+=($!)
done

# Also follow client logs so sync/send messages are visible
for i in $(seq 0 $((NUM_CLIENTS - 1))); do
    docker logs -f --timestamps "paxosbus-client-$i" 2>&1 \
        | tee "$RUN_LOG_DIR/client-$i.log" \
        | sed "s/^/[client-$i]  /" &
    LOG_PIDS+=($!)
done

# ── Leader kill schedule (exercises view change) ────────────────────────────
# The leader of view v is replica v % N, so killing replicas in index order
# takes out each successive leader in turn.
if [[ "$KILL_AT_S" -gt 0 ]]; then
    KILL_GAP_S=25   # comfortably over suspect timeout (5s) + a view change
    (
        sleep $((SYNC_WARMUP_S + KILL_AT_S))
        for k in $(seq 0 $((KILL_COUNT - 1))); do
            VICTIM="paxosbus-replica-$((k % NUM_REPLICAS))"
            echo ""
            echo "══════ killing $VICTIM (leader of view $k) ══════"
            docker kill "$VICTIM" &>/dev/null || true
            if [[ $((k + 1)) -lt "$KILL_COUNT" ]]; then
                sleep "$KILL_GAP_S"
            fi
        done
    ) &
    LOG_PIDS+=($!)
fi

if [[ "$DURATION_S" -gt 0 ]]; then
    # Bounded run: wait out the sync warmup + requested data-phase seconds, then
    # exit so the EXIT trap tears down containers + network. Logs are already
    # archived live under $RUN_LOG_DIR.
    sleep $((SYNC_WARMUP_S + DURATION_S))
    echo ""
    echo "──────────────────────────────────────────────────────────────"
    echo "Ran ${DURATION_S}s of data phase — stopping."
    exit 0
else
    wait
fi
