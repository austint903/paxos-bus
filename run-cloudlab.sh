#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# CloudLab WAN run of the Go PaxosBus implementation.
#
# Same shape as run-gcp.sh, but for FOUR separate single-node CloudLab
# experiments (one per cluster) instead of one GCP project. This script runs on
# your LAPTOP and SSHes into each node's public control hostname (every CloudLab
# node is reachable on :22), so there is no jump-host / controller VM.
#
# Topology (edit cloudlab/nodes.env):
#   client    Utah        (closed-loop clients)
#   replica 0 Wisconsin   (initial leader)
#   replica 1 Clemson
#   replica 2 Mass
#
# Cross-experiment transport (priority #1): nodes dial each other's PUBLIC
# control IPs on :7000. CloudLab's control-network border firewall may block
# that across clusters, so we PROBE it first; if blocked we fall back to an
# SSH-tunnel mesh over :22 (always open). Tunnel mode is correct but adds SSH
# overhead that perturbs the latency numbers — prefer direct.
#
# Prereqs on each node are provided by cloudlab/setup.sh (run automatically by
# the profile on boot, or re-run here if missing).
#
# Usage:
#   ./run-cloudlab.sh                 # full run (auto transport)
#   ./run-cloudlab.sh probe           # just the firewall reachability probe
#   ./run-cloudlab.sh setup           # (re)run setup.sh on all nodes, then exit
#
# Env knobs (mirror run-gcp.sh):
#   INTERVAL_MS DURATION_S DROP_MODE DROP_EVERY REQUEST_GEN GEN_INTERVAL_US
#   NUM_CLIENTS                 number of clients on the Utah node (default 2)
#   TRANSPORT=auto|direct|tunnel  (default auto: probe, then pick)
#   NODES_FILE                 path to nodes.env (default cloudlab/nodes.env)
# ─────────────────────────────────────────────────────────────────────────────

INTERVAL_MS="${INTERVAL_MS:-1}"
DURATION_S="${DURATION_S:-60}"
DROP_MODE="${DROP_MODE:-none}"
DROP_EVERY="${DROP_EVERY:-0}"
REQUEST_GEN="${REQUEST_GEN:-0}"
GEN_INTERVAL_US="${GEN_INTERVAL_US:-1}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
TRANSPORT="${TRANSPORT:-auto}"
REPO_URL="${REPO_URL:-https://github.com/austint903/paxos-bus.git}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES_FILE="${NODES_FILE:-$SCRIPT_DIR/cloudlab/nodes.env}"

BIN=/local/paxosbus-cloudlab/bin     # built by cloudlab/setup.sh
CONF=/tmp/paxosbus.conf              # pushed to each node
PORT=7000
TUN_BASE=17000                       # tunnel: local fwd port for replica j = TUN_BASE+j

SUBCMD="${1:-run}"

[[ -f "$NODES_FILE" ]] || { echo "ERROR: $NODES_FILE not found (copy cloudlab/nodes.env.example)"; exit 1; }
# shellcheck disable=SC1090
source "$NODES_FILE"
: "${SSH_USER:?set SSH_USER in nodes.env}"
: "${CLIENT_HOST:?}" "${REPLICA0_HOST:?}" "${REPLICA1_HOST:?}" "${REPLICA2_HOST:?}"

RHOST=("$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST")
RLABEL=("${REPLICA0_LABEL:-wisc}" "${REPLICA1_LABEL:-clemson}" "${REPLICA2_LABEL:-mass}")
CLIENT_LABEL="${CLIENT_LABEL:-utah}"
RIP=("" "" "")

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o BatchMode=yes)
ssh_to()   { ssh "${SSH_OPTS[@]}" "$SSH_USER@$1" "$2"; }
scp_from() { scp "${SSH_OPTS[@]}" "$SSH_USER@$1:$2" "$3"; }
scp_to()   { scp "${SSH_OPTS[@]}" "$3" "$SSH_USER@$1:$2"; }

# Control IP straight from Emulab's record (with routable_control_ip this is the
# public IP). Falls back to DNS resolution of the hostname.
host_ip() {
    local ip
    ip=$(ssh_to "$1" "cat /var/emulab/boot/myip 2>/dev/null" | tr -d '[:space:]' || true)
    [[ -z "$ip" ]] && ip=$(dig +short "$1" 2>/dev/null | grep -Eo '^[0-9.]+$' | tail -n1 || true)
    echo "$ip"
}

ensure_ready() {
    local host="$1"
    if ssh_to "$host" "test -x $BIN/paxosbus-replica && test -f /local/paxosbus-cloudlab/setup.done"; then
        return 0
    fi
    echo "  [$host] not ready — running setup.sh (first time can take a few minutes)"
    ssh_to "$host" "sudo bash -c '
        export DEBIAN_FRONTEND=noninteractive
        if [ ! -d /local/paxos-bus/.git ]; then git clone $REPO_URL /local/paxos-bus
        else git -C /local/paxos-bus pull --ff-only || true; fi
        REPO_URL=$REPO_URL bash /local/paxos-bus/cloudlab/setup.sh'"
}

# ── Preflight: SSH reachable, binaries built, resolve control IPs ─────────────
preflight() {
    echo "==> Preflight (SSH + setup) on all 4 nodes"
    local all=("$CLIENT_HOST" "$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST")
    for h in "${all[@]}"; do
        ssh_to "$h" "true" || { echo "  CANNOT SSH to $h — check nodes.env / your SSH key"; exit 1; }
        ensure_ready "$h"
    done
    echo "==> Resolving public control IPs"
    for i in 0 1 2; do
        RIP[$i]=$(host_ip "${RHOST[$i]}")
        [[ -n "${RIP[$i]}" ]] || { echo "  could not resolve control IP for ${RHOST[$i]}"; exit 1; }
        printf "  R%d  %-32s %s  (%s)\n" "$i" "${RHOST[$i]}" "${RIP[$i]}" "${RLABEL[$i]}"
    done
    printf "  CL  %-32s %s\n" "$CLIENT_HOST" "$CLIENT_LABEL"
}

# ── Reachability probe: can the client reach every replica's :7000 directly? ──
# Returns 0 (direct works) or 1 (blocked → use tunnels).
do_probe() {
    echo "==> Probing control-network reachability to replica :$PORT"
    local ok=0
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "pkill -f 'nc -l' 2>/dev/null; (nohup timeout 25 nc -l $PORT </dev/null >/dev/null 2>&1 &) ; sleep 0.3" || true
        if ssh_to "$CLIENT_HOST" "nc -z -w5 ${RIP[$i]} $PORT"; then
            echo "  R$i ${RIP[$i]}:$PORT  OPEN"
        else
            echo "  R$i ${RIP[$i]}:$PORT  BLOCKED"
            ok=1
        fi
        ssh_to "${RHOST[$i]}" "pkill -f 'nc -l' 2>/dev/null" || true
    done
    return $ok
}

# ── Tunnel mesh (fallback) ───────────────────────────────────────────────────
# Distribute a dedicated keypair so nodes can SSH to each other, then bring up
# autossh -L forwards: "reach replica j" == connect 127.0.0.1:(TUN_BASE+j).
ensure_tunnel_key() {
    local key="$SCRIPT_DIR/cloudlab/.tunnel_key"
    [[ -f "$key" ]] || ssh-keygen -t ed25519 -N '' -q -f "$key" -C paxosbus-tunnel
    local pub; pub=$(cat "$key.pub")
    for h in "$CLIENT_HOST" "${RHOST[@]}"; do
        # home-relative paths so this works regardless of /users vs /home
        ssh_to "$h" "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
        scp_to "$h" ".ssh/paxosbus_tunnel" "$key"
        ssh_to "$h" "chmod 600 ~/.ssh/paxosbus_tunnel; grep -qF '$pub' ~/.ssh/authorized_keys 2>/dev/null || echo '$pub' >> ~/.ssh/authorized_keys"
    done
}

start_tunnels_on() {   # $1=host  rest="j:Hj" pairs to forward to
    local host="$1"; shift
    ssh_to "$host" "pkill -f 'autossh' 2>/dev/null; pkill -f 'paxosbus_tunnel' 2>/dev/null; true" || true
    local pair j Hj lport
    for pair in "$@"; do
        j="${pair%%:*}"; Hj="${pair##*:}"; lport=$((TUN_BASE + j))
        ssh_to "$host" "autossh -M 0 -f -N \
            -o StrictHostKeyChecking=accept-new -o ExitOnForwardFailure=yes \
            -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
            -i ~/.ssh/paxosbus_tunnel \
            -L $lport:127.0.0.1:$PORT $SSH_USER@$Hj"
    done
}

setup_tunnels() {
    echo "==> Bringing up SSH-tunnel mesh (transport=tunnel)"
    ensure_tunnel_key
    # each replica reaches the other two; client reaches all three
    start_tunnels_on "${RHOST[0]}" "1:${RHOST[1]}" "2:${RHOST[2]}"
    start_tunnels_on "${RHOST[1]}" "0:${RHOST[0]}" "2:${RHOST[2]}"
    start_tunnels_on "${RHOST[2]}" "0:${RHOST[0]}" "1:${RHOST[1]}"
    start_tunnels_on "$CLIENT_HOST" "0:${RHOST[0]}" "1:${RHOST[1]}" "2:${RHOST[2]}"
    sleep 2
}

# ── Config generation + push ─────────────────────────────────────────────────
# self=-1 for the client node; otherwise the node's own replica index.
write_conf() {   # $1=outfile  $2=self_index
    local out="$1" self="$2" j
    echo "f 1" > "$out"
    for j in 0 1 2; do
        if [[ "$MODE" == "direct" ]]; then
            echo "replica ${RIP[$j]}:$PORT" >> "$out"
        elif [[ "$j" == "$self" ]]; then
            echo "replica 127.0.0.1:$PORT" >> "$out"
        else
            echo "replica 127.0.0.1:$((TUN_BASE + j))" >> "$out"
        fi
    done
}

push_confs() {
    echo "==> Generating + distributing paxosbus.conf (mode=$MODE)"
    mkdir -p "$SCRIPT_DIR/config-cloudlab"
    local tmp="$SCRIPT_DIR/config-cloudlab/paxosbus.conf"
    for i in 0 1 2; do
        write_conf "$tmp" "$i"
        scp_to "${RHOST[$i]}" "$CONF" "$tmp"
    done
    write_conf "$tmp" "-1"
    scp_to "$CLIENT_HOST" "$CONF" "$tmp"
    echo "  conf (client view):"; sed 's/^/    /' "$tmp"
}

# ── Launch / tail / collect ──────────────────────────────────────────────────
launch() {
    echo "==> Killing stale processes"
    for i in 0 1 2; do ssh_to "${RHOST[$i]}" "pkill -f '[p]axosbus-replica' || true"; done
    ssh_to "$CLIENT_HOST" "pkill -f '[p]axosbus-client' || true"
    sleep 2

    echo "==> Launching replicas"
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "
            rm -f /tmp/paxosbus.log
            rm -rf /tmp/paxosbus-durable && mkdir -p /tmp/paxosbus-durable
            nohup $BIN/paxosbus-replica \
              -c $CONF -i $i -l ${RLABEL[$i]} -d /tmp/paxosbus-durable \
              -drop-mode $DROP_MODE -drop-every $DROP_EVERY \
              </dev/null >/tmp/paxosbus.log 2>&1 &
            sleep 1
            if pgrep -f '[p]axosbus-replica' >/dev/null; then
              echo '  [replica $i] running'
            else
              echo '  [replica $i] NOT RUNNING — startup log:'; cat /tmp/paxosbus.log 2>/dev/null || true
            fi"
    done
    sleep 3

    echo "==> Launching $NUM_CLIENTS client(s) on $CLIENT_HOST ($CLIENT_LABEL)"
    local extra=""
    [[ "$REQUEST_GEN" == "1" ]] && extra="-r -g $GEN_INTERVAL_US"
    for id in $(seq 1 "$NUM_CLIENTS"); do
        ssh_to "$CLIENT_HOST" "
            rm -f /tmp/paxosbus-client-$id.log
            nohup $BIN/paxosbus-client \
              -c $CONF -I $id -p $INTERVAL_MS -l $CLIENT_LABEL $extra \
              </dev/null >/tmp/paxosbus-client-$id.log 2>&1 &
            sleep 1
            if pgrep -f '[p]axosbus-client.*-I $id' >/dev/null; then echo '  [client $id] running'
            else echo '  [client $id] NOT RUNNING — startup log:'; cat /tmp/paxosbus-client-$id.log 2>/dev/null || true; fi"
    done
}

tail_logs() {
    echo ""
    echo "==> Live tail (running for $((DURATION_S + 6))s)"
    echo "----------------------------------------------------------------"
    local pids=()
    for i in 0 1 2; do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$i]}" "tail -f /tmp/paxosbus.log | sed -u 's/^/[r$i] /'" & pids+=($!)
    done
    for id in $(seq 1 "$NUM_CLIENTS"); do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_HOST" "tail -f /tmp/paxosbus-client-$id.log | sed -u 's/^/[c$id] /'" & pids+=($!)
    done
    sleep $((DURATION_S + 6))
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo "----------------------------------------------------------------"
}

collect() {
    echo "==> Stopping replicas + clients"
    for i in 0 1 2; do ssh_to "${RHOST[$i]}" "pkill -f '[p]axosbus-replica' || true"; done
    ssh_to "$CLIENT_HOST" "pkill -f '[p]axosbus-client' || true"

    local ts run_dir durable_dir
    ts=$(date +%Y%m%d-%H%M%S)
    run_dir="$SCRIPT_DIR/paxosbus/logs/cloudlab/cloudlab-run-$ts"
    durable_dir="$SCRIPT_DIR/paxosbus/logs/durable/cloudlab/cloudlab-run-$ts"
    mkdir -p "$run_dir" "$durable_dir"

    echo "==> Collecting logs -> $run_dir"
    for i in 0 1 2; do
        scp_from "${RHOST[$i]}" "/tmp/paxosbus.log" "$run_dir/replica-$i.log" \
            || echo "  WARN: no /tmp/paxosbus.log on ${RHOST[$i]}"
        scp "${SSH_OPTS[@]}" -r "$SSH_USER@${RHOST[$i]}:/tmp/paxosbus-durable" "$durable_dir/replica-$i" \
            || echo "  WARN: no durable logs on ${RHOST[$i]}"
    done
    for id in $(seq 1 "$NUM_CLIENTS"); do
        scp_from "$CLIENT_HOST" "/tmp/paxosbus-client-$id.log" "$run_dir/paxosbus-client-$id.log" \
            || echo "  WARN: no client-$id log"
    done

    {
        echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
        echo "platform=cloudlab"
        echo "transport=$MODE"
        echo "interval_ms=$INTERVAL_MS"
        echo "duration_s=$DURATION_S"
        echo "num_clients=$NUM_CLIENTS"
        echo "drop_mode=$DROP_MODE"
        echo "drop_every=$DROP_EVERY"
        echo "request_gen=$REQUEST_GEN"
        echo "gen_interval_us=$GEN_INTERVAL_US"
        echo "client=$CLIENT_LABEL replicas=${RLABEL[*]}"
        if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" -gt 0 ]]; then echo "mode=drop-$DROP_MODE"; else echo "mode=normal"; fi
    } > "$run_dir/run-meta.txt"

    echo ""
    echo "==> Per-replica RTT summary (from client logs)"
    for id in $(seq 1 "$NUM_CLIENTS"); do
        echo "=== client $id ==="
        for r in 0 1 2; do
            # extract ONLY the rtt number (not the replica index) and sort -n so the
            # awk below stays portable (macOS awk has no asort)
            grep -oE "REPLY from replica=$r  rtt=[0-9]+us" "$run_dir/paxosbus-client-$id.log" 2>/dev/null \
                | sed -E 's/.*rtt=([0-9]+)us.*/\1/' | sort -n \
                | awk -v r=$r '
                    NF { a[++n]=$1; s+=$1 }
                    END {
                        if (!n) { printf "  replica=%d  no data\n", r; exit }
                        i50=int((n+1)*0.50); if (i50<1) i50=1; if (i50>n) i50=n
                        i99=int((n+1)*0.99); if (i99<1) i99=1; if (i99>n) i99=n
                        printf "  replica=%d  n=%d  avg=%.0fus  p50=%dus  p99=%dus\n",
                               r, n, s/n, a[i50], a[i99] }'
        done
    done
    echo ""
    echo "==> Done. Logs: $run_dir  (durable: $durable_dir)"
}

# ── Main ─────────────────────────────────────────────────────────────────────
MODE=direct
case "$SUBCMD" in
    setup)
        echo "==> setup-only: (re)running setup.sh on all nodes"
        for h in "$CLIENT_HOST" "$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST"; do
            ssh_to "$h" "true" || { echo "  CANNOT SSH to $h"; exit 1; }
            ensure_ready "$h"
        done
        echo "==> all nodes ready"; exit 0 ;;
    probe)
        preflight
        if do_probe; then echo "==> DIRECT OK — control IPs are reachable on :$PORT"; exit 0
        else echo "==> BLOCKED — direct :$PORT is firewalled; a real run would use TRANSPORT=tunnel"; exit 1; fi ;;
    run) ;;
    *) echo "usage: $0 [run|probe|setup]"; exit 1 ;;
esac

preflight

case "$TRANSPORT" in
    direct) MODE=direct ;;
    tunnel) MODE=tunnel ;;
    auto)
        if do_probe; then MODE=direct; echo "==> probe: DIRECT works"
        else MODE=tunnel; echo "==> probe: BLOCKED — falling back to SSH tunnels (latency numbers include SSH overhead)"; fi ;;
    *) echo "bad TRANSPORT=$TRANSPORT"; exit 1 ;;
esac

[[ "$MODE" == "tunnel" ]] && setup_tunnels
push_confs
launch
tail_logs
collect
