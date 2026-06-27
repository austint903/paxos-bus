#!/usr/bin/env bash
set -euo pipefail

# Local PaxosBus run (Go implementation, normal processing only).
# Mirrors prevImplementation/run-paxosbus.sh minus the gap-agreement knobs.

# ── Message rate and topology ───────────────────────────────────────────────
MSG_INTERVAL_MS=1      # change this: 1000=1s  100=100ms  10=10ms  2=2ms  1=1ms (bus interval under -r)
NUM_REPLICAS=3
NUM_CLIENTS=2
RESEND_MS=0            # client resend-on-no-quorum timeout (ms); 0 = disabled (bus timeout under -r)
DURATION_S="${DURATION_S:-60}"  # seconds of data phase, then auto-stop; 0 = run until Ctrl+C
SYNC_WARMUP_S=5        # client sync wait before data starts (matches syncStartDelayMs=5000)
DROP_MODE=none         # artificial drop scenario: none|leader|followers|all
DROP_EVERY=0           # drop a slot when reqId % DROP_EVERY == 0 (0 = disabled)
REQUEST_GEN=0          # 1 = request-generator mode (-r): batch requests onto buses
GEN_INTERVAL_US=1      # request generation interval in µs (-r only; -g 1 -p 1 ≈ 1000 reqs/bus)
# ────────────────────────────────────────────────────────────────────────────

FORCE_BUILD=0

usage() {
    echo "Usage: $0 [-b] [-r] [-g <gen_us>] [-p <interval_ms>] [-t <resend_ms>] [-d <seconds>] [-D <drop_mode>] [-F <drop_every>]"
    echo "  -b            force rebuild of Docker image"
    echo "  -r            request-generator mode: batch requests onto buses (two-layer log)"
    echo "  -g <us>       request generation interval in µs (-r only; default: $GEN_INTERVAL_US)"
    echo "  -p <ms>       message interval in ms (bus interval under -r) (default: $MSG_INTERVAL_MS)"
    echo "  -t <ms>       client resend/bus timeout (default: $RESEND_MS, 0=off)"
    echo "  -d <seconds>  auto-stop after this many seconds of data phase (default: run until Ctrl+C)"
    echo "  -D <mode>     artificial drop scenario: none|leader|followers|all (default: $DROP_MODE)"
    echo "  -F <n>        drop a slot when reqId % n == 0 (default: $DROP_EVERY, 0=off)"
    exit 1
}

while getopts "brg:p:t:d:D:F:h" opt; do
    case $opt in
        b) FORCE_BUILD=1 ;;
        r) REQUEST_GEN=1 ;;
        g) GEN_INTERVAL_US=$OPTARG ;;
        p) MSG_INTERVAL_MS=$OPTARG ;;
        t) RESEND_MS=$OPTARG ;;
        d) DURATION_S=$OPTARG ;;
        D) DROP_MODE=$OPTARG ;;
        F) DROP_EVERY=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

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
if [[ $REQUEST_GEN -eq 1 ]]; then
    echo "Path: REQUEST-GEN (-r, gen=${GEN_INTERVAL_US}us, bus=${MSG_INTERVAL_MS}ms)"
else
    echo "Path: DEFAULT (one request per bus)"
fi
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
    echo "request_gen=$REQUEST_GEN"
    echo "gen_interval_us=$GEN_INTERVAL_US"
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
        > /dev/null
    CONTAINERS+=("$NAME")
done

echo "Waiting 2s for replicas to bind..."
sleep 2

# ── Clients ───────────────────────────────────────────────────────────────────
CLIENT_FLAGS=()
if [[ $RESEND_MS -gt 0 ]]; then
    CLIENT_FLAGS+=(-t "$RESEND_MS")
fi
if [[ $REQUEST_GEN -eq 1 ]]; then
    CLIENT_FLAGS+=(-r -g "$GEN_INTERVAL_US")
fi
for i in $(seq 1 $NUM_CLIENTS); do
    NAME="paxosbus-client-$i"
    IP="172.29.0.$((BASE_CLIENT_OCTET + i - 1))"
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
for i in $(seq 1 $NUM_CLIENTS); do
    docker logs -f --timestamps "paxosbus-client-$i" 2>&1 \
        | tee "$RUN_LOG_DIR/client-$i.log" \
        | sed "s/^/[client-$i]  /" &
    LOG_PIDS+=($!)
done

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
