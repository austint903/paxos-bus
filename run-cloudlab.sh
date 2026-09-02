#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# CloudLab WAN run of the Go PaxosBus implementation.
#
# Same shape as run-gcp.sh, but for THREE separate single-node CloudLab
# experiments (one per cluster) instead of one GCP project. This script runs on
# your LAPTOP and SSHes into each node's public control hostname (every CloudLab
# node is reachable on :22), so there is no jump-host / controller VM.
#
# Topology (edit cloudlab/nodes.env): one replica per cluster, and — since there
# is no dedicated client node — the closed-loop clients run ON a NON-leader
# replica's cluster (CLIENT_REPLICA, default 1).
#   replica 0 Utah        (initial leader)
#   replica 1 Wisconsin   (also runs the clients, default)
#   replica 2 Clemson
#
# Cross-experiment transport (priority #1): nodes dial each other's PUBLIC
# control IPs on :$PORT directly. Empirically (Utah/Wisconsin/Clemson) the
# control network is fully open between clusters on arbitrary ports including
# 7000 — there is NO border firewall here. We still PROBE the full replica mesh
# first as a sanity check; only if a link is genuinely down do we fall back to
# an SSH-tunnel mesh over :22. Tunnel mode adds SSH overhead that perturbs the
# latency numbers, so we use direct whenever the probe passes (it should).
#
# The probe runs ONE ssh process per node, in parallel. Two foot-guns it avoids,
# both of which previously forced every run onto tunnels:
#   * never `pkill -f 'nc -l …'` / 'portserver' — the launching shell's own argv
#     contains that string, so pkill kills its own session and the listener
#     never starts (=> spurious BLOCKED);
#   * start the listener in the foreground on the remote (background the ssh
#     locally) and let it settle before probing, or you get spurious REFUSEDs.
#
# Prereqs on each node are provided by cloudlab/setup.sh (run automatically by
# the profile on boot, or re-run here if missing).
#
# Usage:
#   ./run-cloudlab.sh                        # small-scale run (auto transport)
#   ./run-cloudlab.sh --scale small          # 1 client on a NON-leader replica's
#                                            #   cluster (CLIENT_REPLICA), the
#                                            #   original benchmark topology
#   ./run-cloudlab.sh --scale large          # 9 clients / 3 replicas: 3 clients
#                                            #   on EVERY replica's cluster,
#                                            #   including the leader's
#   ./run-cloudlab.sh probe                  # just the firewall reachability probe
#   ./run-cloudlab.sh setup                  # (re)run setup.sh on all nodes, then exit
#
# Env knobs (mirror run-gcp.sh):
#   INTERVAL_MS DURATION_S DROP_MODE DROP_EVERY GEN_INTERVAL_US
#   GAP_RETRY_TIMEOUT_MS             gap-commit rebroadcast interval (default 1500)
#   SCALE=small|large           same as --scale (flag wins)
#   NUM_CLIENTS                 [small] clients on the client cluster (default 1)
#   CLIENTS_PER_HOST            [large] clients on EACH replica cluster (default 3)
#   CLIENT_REPLICA              [small] which replica's cluster runs the clients;
#                               must be a non-leader (1 or 2, default 1)
#   RESEND_MS                   per-request no-quorum re-board timeout, ms
#                               (default 5000; 0 uses the client default)
#   START_DELAY_MS              client sync->data-phase delay (default 5000 small /
#                               10000 large). EVERY client must sync inside this
#                               window at ALL replicas or the slot mapping diverges,
#                               so large keeps a bigger cushion for 9 ssh launches.
#   COLLECT_DURABLE=0|1         scp back /tmp/paxosbus-durable (default 1 small /
#                               0 large — ~0.5GB at 18k req/s x 60s)
#   TRANSPORT=direct|auto|tunnel  (default direct: public IP:$PORT, never SSH
#                               tunnels; use 'auto' to probe-then-pick, or
#                               'tunnel' to force the SSH-tunnel mesh)
#   NODES_FILE                 path to nodes.env (default cloudlab/nodes.env)
# ─────────────────────────────────────────────────────────────────────────────

INTERVAL_MS="${INTERVAL_MS:-1}"
DURATION_S="${DURATION_S:-60}"
DROP_MODE="${DROP_MODE:-none}"
DROP_EVERY="${DROP_EVERY:-0}"
GEN_INTERVAL_US="${GEN_INTERVAL_US:-500}"
GAP_RETRY_TIMEOUT_MS="${GAP_RETRY_TIMEOUT_MS:-1500}"
NUM_CLIENTS="${NUM_CLIENTS:-1}"
CLIENTS_PER_HOST="${CLIENTS_PER_HOST:-3}"
RESEND_MS="${RESEND_MS:-5000}"
SCALE="${SCALE:-small}"
TRANSPORT="${TRANSPORT:-direct}"
# Failure-recovery experiment: SIGKILL the leader replica this many seconds into
# the DATA phase (0 = never). Clients keep running, so the recovery cost shows up
# as a gap in their commit stream — see cloudlab/aggregate-viewchange-stats.py.
KILL_LEADER_AT_S="${KILL_LEADER_AT_S:-0}"
# Subdirectory under paxosbus/logs/cloudlab/ to collect into, so a view-change
# campaign does not intermix with the throughput-latency runs.
# Extra flags appended verbatim to every paxosbus-replica command line, e.g.
# REPLICA_FLAGS="-sync-interval-ms 5000 -retain-slots 262144". Recorded in
# run-meta.txt so a run's tuning is recoverable from its own log directory.
REPLICA_FLAGS="${REPLICA_FLAGS:-}"
RUN_SUBDIR="${RUN_SUBDIR:-}"
RUN_TAG="${RUN_TAG:-}"
REPO_URL="${REPO_URL:-https://github.com/austint903/paxos-bus.git}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES_FILE="${NODES_FILE:-$SCRIPT_DIR/cloudlab/nodes.env}"

KILL_INFO_FILE="$(mktemp -t paxosbus-kill)"   # written by kill_leader_after, collected into the run dir

BIN=/local/paxosbus-cloudlab/bin     # built by cloudlab/setup.sh
CONF=/tmp/paxosbus.conf              # pushed to each node
PORT="${PORT:-7000}"                 # direct-transport port (override with PORT=NNNN)
TUN_BASE=17000                       # tunnel: local fwd port for replica j = TUN_BASE+j

SUBCMD=run
while [[ $# -gt 0 ]]; do
    case "$1" in
        run|probe|setup) SUBCMD="$1" ;;
        small|large)     SCALE="$1" ;;
        --scale)         SCALE="${2:?--scale needs small|large}"; shift ;;
        --scale=*)       SCALE="${1#--scale=}" ;;
        *) echo "usage: $0 [run|probe|setup] [--scale small|large]"; exit 1 ;;
    esac
    shift
done
[[ "$SCALE" =~ ^(small|large)$ ]] || { echo "ERROR: bad SCALE '$SCALE' (want small|large)"; exit 1; }

# START_DELAY_MS / COLLECT_DURABLE default differently per scale (see header).
if [[ "$SCALE" == "large" ]]; then
    START_DELAY_MS="${START_DELAY_MS:-10000}"
    COLLECT_DURABLE="${COLLECT_DURABLE:-0}"
else
    START_DELAY_MS="${START_DELAY_MS:-5000}"
    COLLECT_DURABLE="${COLLECT_DURABLE:-1}"
fi

[[ -f "$NODES_FILE" ]] || { echo "ERROR: $NODES_FILE not found (copy cloudlab/nodes.env.example)"; exit 1; }
# shellcheck disable=SC1090
source "$NODES_FILE"
: "${SSH_USER:?set SSH_USER in nodes.env}"
: "${REPLICA0_HOST:?}" "${REPLICA1_HOST:?}" "${REPLICA2_HOST:?}"

RHOST=("$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST")
RLABEL=("${REPLICA0_LABEL:-utah}" "${REPLICA1_LABEL:-wisc}" "${REPLICA2_LABEL:-clemson}")
RIP=("" "" "")

# Replica 0 is the initial Paxos leader. There is no dedicated client node, so
# clients run ON replica clusters:
#   small: all clients on ONE non-leader cluster (CLIENT_REPLICA, default 1) —
#          the original topology where the client must not share the leader's
#          cluster;
#   large: CLIENTS_PER_HOST clients on EVERY cluster, leader's included (the
#          non-co-location requirement is explicitly waived at this scale).
# Client IDs must be GLOBALLY unique — the slot mapping keys on them — so in
# large mode host i owns ids i*CLIENTS_PER_HOST+1 .. (i+1)*CLIENTS_PER_HOST.
LEADER_IDX=0
CLIENT_REPLICA="${CLIENT_REPLICA:-1}"
[[ "$CLIENT_REPLICA" =~ ^[12]$ ]] || { echo "ERROR: CLIENT_REPLICA must be 1 or 2 (a non-leader cluster), got '$CLIENT_REPLICA'"; exit 1; }

if [[ "$SCALE" == "large" ]]; then
    CLIENT_HOST_IDXS=(0 1 2)
    TOTAL_CLIENTS=$((3 * CLIENTS_PER_HOST))
else
    CLIENT_HOST_IDXS=("$CLIENT_REPLICA")
    TOTAL_CLIENTS="$NUM_CLIENTS"
fi

# ids_on_host <position-in-CLIENT_HOST_IDXS> -> that host's client ids
ids_on_host() {
    local pos="$1"
    if [[ "$SCALE" == "large" ]]; then
        seq $((pos * CLIENTS_PER_HOST + 1)) $(((pos + 1) * CLIENTS_PER_HOST))
    else
        seq 1 "$NUM_CLIENTS"
    fi
}

# clients_on_replica <replica-idx> -> how many clients that replica's host runs
clients_on_replica() {
    local ridx="$1" pos
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        if [[ "${CLIENT_HOST_IDXS[$pos]}" == "$ridx" ]]; then
            ids_on_host "$pos" | wc -l | tr -d ' '
            return
        fi
    done
    echo 0
}

# Keepalives matter here: a long setup/tail ssh that dies mid-flight (the
# laptop's socket layer hiccuping is enough) otherwise leaves that node holding
# a STALE BINARY while the run proceeds and reports success. verify_binaries()
# is the backstop; these just make the drop far less likely.
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o BatchMode=yes
          -o ServerAliveInterval=15 -o ServerAliveCountMax=8)
ssh_to()   { ssh "${SSH_OPTS[@]}" "$SSH_USER@$1" "$2"; }
scp_from() { scp "${SSH_OPTS[@]}" "$SSH_USER@$1:$2" "$3"; }
scp_to()   { scp "${SSH_OPTS[@]}" "$3" "$SSH_USER@$1:$2"; }

# Every long-lived LOCAL background ssh we spawn (probe listeners, log tails)
# registers its PID here so cleanup() can guarantee none survive the script.
LOCAL_BG_PIDS=()
# Set to 1 once we've launched replicas/clients or brought up tunnels, so the
# teardown only pays for remote pkills when there's actually something to kill.
STARTED_REMOTE=0

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
    
    echo "  [$host] syncing origin/master + rebuilding (first time can take a few minutes)"
    ssh_to "$host" "sudo bash -c '
        export DEBIAN_FRONTEND=noninteractive
        if [ ! -d /local/paxos-bus/.git ]; then git clone $REPO_URL /local/paxos-bus; fi
        git -C /local/paxos-bus fetch --prune origin
        git -C /local/paxos-bus reset --hard origin/master
        git -C /local/paxos-bus clean -fd
        REPO_URL=$REPO_URL bash /local/paxos-bus/cloudlab/setup.sh'"
}

# ── Binary verification ──────────────────────────────────────────────────────
# ensure_ready succeeding is NOT evidence the node rebuilt: an ssh that dies
# mid-setup (a laptop socket hiccup will do it) leaves that node running the
# PREVIOUS binary while the run proceeds and reports a clean exit. A whole
# 60s x 9-client run then silently measures the wrong code. So check the
# artefact, not the exit code: every node must report the same commit as
# origin/master and a binary newer than that commit's checkout.
verify_binaries() {
    echo "==> Verifying every node built the expected commit"
    local want ok=1 i sha built
    want="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo '')"
    for i in 0 1 2; do
        read -r sha built <<<"$(ssh_to "${RHOST[$i]}" '
            echo "$(sudo git -C /local/paxos-bus rev-parse --short HEAD 2>/dev/null || echo UNKNOWN) \
                  $(( $(date +%s) - $(stat -c %Y /local/paxosbus-cloudlab/bin/paxosbus-replica 2>/dev/null || echo 0) ))"' 2>/dev/null)"
        printf "  R%d %-10s sha=%s binary_age=%ss\n" "$i" "${RLABEL[$i]}" "${sha:-ERR}" "${built:-ERR}"
        [[ -n "$want" && "$sha" != "$want" ]] && { echo "     ^ expected sha=$want"; ok=0; }
        [[ -z "$sha" || "$sha" == UNKNOWN ]] && ok=0
    done
    [[ "$ok" == 1 ]] || {
        echo "  ABORT: a node is not on the expected commit — re-run './run-cloudlab.sh setup' and retry."
        echo "  (running anyway would produce numbers for the wrong code)"
        exit 1
    }
}

# ── Preflight: SSH reachable, binaries built, resolve control IPs ─────────────
preflight() {
    echo "==> Preflight (SSH + setup) on all 3 nodes — one ssh process per node, in parallel"
    local pids=() p
    for h in "$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST"; do
        {
            ssh_to "$h" "true" || { echo "  CANNOT SSH to $h — check nodes.env / your SSH key"; exit 1; }
            ensure_ready "$h"
        } &
        pids+=($!)
    done
    local rc=0
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    [[ "$rc" == 0 ]] || { echo "  preflight failed on at least one node (see above)"; exit 1; }

    verify_binaries

    echo "==> Resolving public control IPs (parallel)"
    local ipd; ipd="$(mktemp -d)"
    pids=()
    for i in 0 1 2; do host_ip "${RHOST[$i]}" > "$ipd/$i" & pids+=($!); done
    for p in "${pids[@]}"; do wait "$p" || true; done
    for i in 0 1 2; do
        RIP[$i]="$(tr -d '[:space:]' < "$ipd/$i")"
        [[ -n "${RIP[$i]}" ]] || { echo "  could not resolve control IP for ${RHOST[$i]}"; exit 1; }
        local tag="${RLABEL[$i]}" nc
        [[ "$i" == "$LEADER_IDX" ]] && tag="$tag, leader"
        nc="$(clients_on_replica "$i")"
        [[ "$nc" != 0 ]] && tag="$tag, +${nc} client(s)"
        printf "  R%d  %-32s %s  (%s)\n" "$i" "${RHOST[$i]}" "${RIP[$i]}" "$tag"
    done
    rm -rf "$ipd"
}

# ── Reachability probe: is the full replica mesh reachable on :$PORT directly? ─
# Returns 0 (direct works) or 1 (some link blocked → use tunnels). Uses python3
# (present on every CloudLab node) instead of nc so there's no -k/variant
# ambiguity. See the transport notes at the top for the foot-guns this avoids.
PY_LISTEN='import socket,sys; p=int(sys.argv[1]); s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(("0.0.0.0",p)); s.listen(16); [c.close() for c in iter(lambda: s.accept()[0], None)]'
PY_DIAL='import socket,sys; socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=5).close()'

do_probe() {
    echo "==> Probing direct reachability of the full replica mesh on :$PORT (one ssh per node, parallel)"
    local i j p res; res="$(mktemp -d)"

    # 1) one foreground listener per node, each behind a locally-backgrounded ssh
    local lpids=()
    for i in 0 1 2; do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$i]}" \
            "exec timeout 15 python3 -c '$PY_LISTEN' $PORT" >/dev/null 2>&1 &
        lpids+=($!)
    done
    LOCAL_BG_PIDS+=("${lpids[@]}")   # so cleanup() kills listeners on Ctrl-C mid-probe
    sleep 2   # let every listener bind before we test (skipping this => false BLOCKED)

    # 2) every node dials the other two — independent ssh processes, in parallel
    local cpids=()
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            { ssh_to "${RHOST[$i]}" "python3 -c '$PY_DIAL' ${RIP[$j]} $PORT" >/dev/null 2>&1 \
                && echo OPEN > "$res/$i-$j" || echo BLOCKED > "$res/$i-$j"; } &
            cpids+=($!)
        done
    done
    for p in "${cpids[@]}"; do wait "$p" || true; done

    # 3) tear the listeners down (kill the local ssh -> remote python dies)
    for p in "${lpids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait "${lpids[@]}" 2>/dev/null || true

    # 4) verdict — every directed link must be OPEN
    local ok=0 r
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            r="$(cat "$res/$i-$j" 2>/dev/null || echo BLOCKED)"
            printf "  %-7s -> %-7s :%s  %s\n" "${RLABEL[$i]}" "${RLABEL[$j]}" "$PORT" "$r"
            [[ "$r" == OPEN ]] || ok=1
        done
    done
    rm -rf "$res"
    return $ok
}

# ── Tunnel mesh (fallback) ───────────────────────────────────────────────────
# Distribute a dedicated keypair so nodes can SSH to each other, then bring up
# autossh -L forwards: "reach replica j" == connect 127.0.0.1:(TUN_BASE+j).
ensure_tunnel_key() {
    local key="$SCRIPT_DIR/cloudlab/.tunnel_key"
    [[ -f "$key" ]] || ssh-keygen -t ed25519 -N '' -q -f "$key" -C paxosbus-tunnel
    local pub; pub=$(cat "$key.pub")
    for h in "${RHOST[@]}"; do
        # home-relative paths so this works regardless of /users vs /home
        ssh_to "$h" "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
        scp_to "$h" ".ssh/paxosbus_tunnel" "$key"
        ssh_to "$h" "chmod 600 ~/.ssh/paxosbus_tunnel; grep -qF '$pub' ~/.ssh/authorized_keys 2>/dev/null || echo '$pub' >> ~/.ssh/authorized_keys"
    done
}

start_tunnels_on() {   # $1=host  rest="j:Hj" pairs to forward to
    local host="$1"; shift
    # exact-name + bracket-trick patterns so this pkill can't match (and kill)
    # its own ssh session — the plain 'autossh'/'paxosbus_tunnel' forms did.
    ssh_to "$host" "pkill -x autossh 2>/dev/null; pkill -f 'paxosbus[_]tunnel' 2>/dev/null; true" || true
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
    STARTED_REMOTE=1     # tunnels now exist -> teardown must run
    echo "==> Bringing up SSH-tunnel mesh (transport=tunnel)"
    ensure_tunnel_key
    # Each replica reaches the other two. The clients run on replica
    # $CLIENT_REPLICA's host, so they reuse that replica's tunnels (to the other
    # two) and reach the co-located replica directly on 127.0.0.1:$PORT.
    start_tunnels_on "${RHOST[0]}" "1:${RHOST[1]}" "2:${RHOST[2]}"
    start_tunnels_on "${RHOST[1]}" "0:${RHOST[0]}" "2:${RHOST[2]}"
    start_tunnels_on "${RHOST[2]}" "0:${RHOST[0]}" "1:${RHOST[1]}"
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
    # No separate client conf: the clients run on replica $CLIENT_REPLICA's host
    # and read the same /tmp/paxosbus.conf as that replica (co-located replica
    # on 127.0.0.1:$PORT in tunnel mode; all-public in direct mode).
    write_conf "$tmp" "$CLIENT_REPLICA"
    echo "  conf (replica $CLIENT_REPLICA / client view):"; sed 's/^/    /' "$tmp"
}

# ── Launch / tail / collect ──────────────────────────────────────────────────
launch() {
    STARTED_REMOTE=1     # replicas/clients about to run -> teardown must run
    echo "==> Killing stale processes (parallel)"
    local p kpids=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "pkill -f '[p]axosbus-replica' || true; pkill -f '[p]axosbus-client' || true" & kpids+=($!)
    done
    for p in "${kpids[@]}"; do wait "$p" || true; done
    sleep 2

    echo "==> Launching replicas — one ssh process per node, in parallel"
    local lpids=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "
            rm -f /tmp/paxosbus.log
            rm -rf /tmp/paxosbus-durable && mkdir -p /tmp/paxosbus-durable
            nohup $BIN/paxosbus-replica \
              -c $CONF -i $i -l ${RLABEL[$i]} -d /tmp/paxosbus-durable \
              -drop-mode $DROP_MODE -drop-every $DROP_EVERY \
              -gap-retry-timeout-ms $GAP_RETRY_TIMEOUT_MS $REPLICA_FLAGS \
              </dev/null >/tmp/paxosbus.log 2>&1 &
            sleep 1
            if pgrep -f '[p]axosbus-replica' >/dev/null; then
              echo '  [replica $i] running'
            else
              echo '  [replica $i] NOT RUNNING — startup log:'; cat /tmp/paxosbus.log 2>/dev/null || true
            fi" & lpids+=($!)
    done
    for p in "${lpids[@]}"; do wait "$p" || true; done
    sleep 3

    echo "==> Launching $TOTAL_CLIENTS client(s), scale=$SCALE (parallel, one ssh per host)"
    # All of a host's clients go up in ONE ssh, and the hosts launch in
    # parallel: every client must send its sync inside every other client's
    # START_DELAY_MS window (slot mapping diverges otherwise), so the launch
    # spread has to stay well under it — sequential ssh per client did not.
    local extra="-g $GEN_INTERVAL_US -t $RESEND_MS"
    local pos cpids=()
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        local ridx="${CLIENT_HOST_IDXS[$pos]}"
        local chost="${RHOST[$ridx]}" clabel="${RLABEL[$ridx]}"
        local ids; ids="$(ids_on_host "$pos" | tr '\n' ' ')"
        ssh_to "$chost" "
            for id in $ids; do
              rm -f /tmp/paxosbus-client-\$id.log
              nohup $BIN/paxosbus-client \
                -c $CONF -I \$id -p $INTERVAL_MS -l $clabel -w $START_DELAY_MS $extra \
                </dev/null >/tmp/paxosbus-client-\$id.log 2>&1 &
            done
            sleep 1
            for id in $ids; do
              if pgrep -f \"[p]axosbus-client.*-I \$id -p\" >/dev/null; then
                echo \"  [client \$id @ $clabel] running\"
              else
                echo \"  [client \$id @ $clabel] NOT RUNNING — startup log:\"
                cat /tmp/paxosbus-client-\$id.log 2>/dev/null || true
              fi
            done" & cpids+=($!)
    done
    for p in "${cpids[@]}"; do wait "$p" || true; done
}

# kill_leader_after backgrounds the failure injection: wait out the sync delay
# plus KILL_LEADER_AT_S of data phase, then SIGKILL the leader replica.
#
# SIGKILL, not a graceful stop, because the point is to measure what a crash
# costs — and it also exercises the fast detection path, where the peers notice
# the socket close rather than waiting out the heartbeat timeout.
#
# The kill instant is recorded twice: once on the leader's own clock and once on
# the laptop's. Neither is used to measure the recovery gap (that comes from a
# single client's own commit stream, which needs no cross-node clock alignment)
# — they are there to confirm the gap found is the one the kill caused.
kill_leader_after() {
    local wait_s=$((START_DELAY_MS / 1000 + KILL_LEADER_AT_S))
    local lh="${RHOST[$LEADER_IDX]}"
    (
        sleep "$wait_s"
        local remote_ts
        remote_ts="$(ssh_to "$lh" "date +%s.%N; pkill -9 -f '[p]axosbus-replica' || true" 2>/dev/null | head -1)"
        {
            echo "kill_leader_idx=$LEADER_IDX"
            echo "kill_leader_label=${RLABEL[$LEADER_IDX]}"
            echo "kill_leader_host=$lh"
            echo "kill_at_data_phase_s=$KILL_LEADER_AT_S"
            echo "kill_wall_local=$(date +%s.%N)"
            echo "kill_wall_leader=$remote_ts"
        } > "$KILL_INFO_FILE"
        echo ""
        echo "══════ KILLED leader replica $LEADER_IDX (${RLABEL[$LEADER_IDX]}) at +${KILL_LEADER_AT_S}s of data phase ══════"
    ) &
    LOCAL_BG_PIDS+=($!)
}

tail_logs() {
    # Data phase starts START_DELAY_MS after the syncs, so keep the run alive
    # for delay + duration + slack.
    local run_for=$((DURATION_S + START_DELAY_MS / 1000 + 6))
    if [[ "$KILL_LEADER_AT_S" != "0" ]]; then
        echo "==> Leader kill armed: replica $LEADER_IDX (${RLABEL[$LEADER_IDX]}) at +${KILL_LEADER_AT_S}s into the data phase"
        kill_leader_after
    fi
    echo ""
    echo "==> Live tail (running for ${run_for}s; tailing replicas + first client per host)"
    echo "----------------------------------------------------------------"
    local pos pids=()
    for i in 0 1 2; do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$i]}" "tail -f /tmp/paxosbus.log | sed -u 's/^/[r$i] /'" & pids+=($!)
    done
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        local ridx="${CLIENT_HOST_IDXS[$pos]}"
        local first; first="$(ids_on_host "$pos" | head -n1)"
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$ridx]}" \
            "tail -f /tmp/paxosbus-client-$first.log | sed -u 's/^/[c$first] /'" & pids+=($!)
    done
    LOCAL_BG_PIDS+=("${pids[@]}")     # so cleanup() kills these tails on Ctrl-C
    sleep "$run_for"
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo "----------------------------------------------------------------"
}

collect() {
    echo "==> Stopping replicas + clients (parallel)"
    local p sp=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "pkill -f '[p]axosbus-replica' || true; pkill -f '[p]axosbus-client' || true" & sp+=($!)
    done
    for p in "${sp[@]}"; do wait "$p" || true; done

    local ts run_dir durable_dir base name
    ts=$(date +%Y%m%d-%H%M%S)
    name="cloudlab-run-$ts"
    [[ -n "$RUN_TAG" ]] && name="$name-$RUN_TAG"
    base="$SCRIPT_DIR/paxosbus/logs/cloudlab"
    [[ -n "$RUN_SUBDIR" ]] && base="$base/$RUN_SUBDIR"
    run_dir="$base/$name"
    # Durable logs land WITH the run that produced them. The old parallel tree
    # (logs/durable/cloudlab/) ignored RUN_SUBDIR, so a campaign run scattered its
    # evidence across two directories; the durable log is only meaningful next to
    # the replica log that says what the replica was doing when it wrote it.
    durable_dir="$run_dir/durable"
    mkdir -p "$run_dir"
    RUN_DIR_OUT="$run_dir"     # so a driver can find what this run produced

    echo "==> Collecting logs -> $run_dir"
    for i in 0 1 2; do
        scp_from "${RHOST[$i]}" "/tmp/paxosbus.log" "$run_dir/replica-$i.log" \
            || echo "  WARN: no /tmp/paxosbus.log on ${RHOST[$i]}"
        if [[ "$COLLECT_DURABLE" == "1" ]]; then
            mkdir -p "$durable_dir"
            # -C: these are tens of MB of repetitive text per replica, and the
            # next run deletes them on the node, so the copy has to succeed here.
            if scp "${SSH_OPTS[@]}" -C -r "$SSH_USER@${RHOST[$i]}:/tmp/paxosbus-durable" \
                   "$durable_dir/replica-$i"; then
                echo "  durable r$i: $(du -sh "$durable_dir/replica-$i" 2>/dev/null | cut -f1) -> $durable_dir/replica-$i"
            else
                echo "  WARN: durable logs NOT collected from ${RHOST[$i]} — they will be erased by the next run"
            fi
        fi
    done
    local pos
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        local ridx="${CLIENT_HOST_IDXS[$pos]}"
        for id in $(ids_on_host "$pos"); do
            scp_from "${RHOST[$ridx]}" "/tmp/paxosbus-client-$id.log" "$run_dir/paxosbus-client-$id.log" \
                || echo "  WARN: no client-$id log on ${RHOST[$ridx]}"
        done
    done

    {
        echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
        echo "platform=cloudlab"
        echo "transport=$MODE"
        echo "scale=$SCALE"
        echo "interval_ms=$INTERVAL_MS"
        echo "duration_s=$DURATION_S"
        echo "total_clients=$TOTAL_CLIENTS"
        echo "clients_per_host=$([[ "$SCALE" == "large" ]] && echo "$CLIENTS_PER_HOST" || echo "-")"
        echo "start_delay_ms=$START_DELAY_MS"
        echo "resend_ms=$RESEND_MS"
        echo "drop_mode=$DROP_MODE"
        echo "drop_every=$DROP_EVERY"
		echo "gen_interval_us=$GEN_INTERVAL_US"
        echo "gap_retry_timeout_ms=$GAP_RETRY_TIMEOUT_MS"
        echo "client_hosts=$(for pos in "${!CLIENT_HOST_IDXS[@]}"; do printf '%s ' "${RLABEL[${CLIENT_HOST_IDXS[$pos]}]}"; done)"
        echo "replicas=${RLABEL[*]}"
        echo "kill_leader_at_s=$KILL_LEADER_AT_S"
        echo "replica_flags=$REPLICA_FLAGS"
        echo "leader_idx=$LEADER_IDX"
        echo "leader_label=${RLABEL[$LEADER_IDX]}"
        echo "commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
        if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" -gt 0 ]]; then echo "mode=drop-$DROP_MODE"; else echo "mode=normal"; fi
    } > "$run_dir/run-meta.txt"

    [[ -s "$KILL_INFO_FILE" ]] && cp "$KILL_INFO_FILE" "$run_dir/kill-info.txt"
    echo "$run_dir" > "${RUN_DIR_POINTER:-/dev/null}"

    echo ""
    python3 "$SCRIPT_DIR/cloudlab/aggregate-stats.py" "$run_dir" \
        || echo "  WARN: aggregate-stats.py failed"
    echo ""
    if [[ "$COLLECT_DURABLE" == "1" ]]; then
        echo "==> Done. Logs: $run_dir  (durable: $durable_dir)"
    else
        echo "==> Done. Logs: $run_dir  (durable logs left on nodes; COLLECT_DURABLE=0)"
    fi
}

# ── Teardown (runs on EVERY exit, including Ctrl-C / errors) ──────────────────
# Guarantees we never leak: local background ssh (probe listeners, log tails),
# remote replicas/clients, or the autossh SSH-tunnel mesh. It ONLY kills
# processes — logs are left on disk untouched (collected runs under
# paxosbus/logs/... AND the remote /tmp/*.log). Patterns are self-kill-safe
# (bracket trick / exact-name) so the teardown can't kill its own ssh session.
cleanup() {
    local ec=$?
    trap - EXIT INT TERM            # disable re-entry
    if ((${#LOCAL_BG_PIDS[@]})); then
        kill "${LOCAL_BG_PIDS[@]}" 2>/dev/null || true
    fi
    if [[ "$STARTED_REMOTE" == 1 ]] && ((${#RHOST[@]})); then
        echo "==> Teardown: stopping replicas/clients + tunnels on all nodes (logs kept)"
        local h p pids=()
        for h in "${RHOST[@]}"; do
            ssh "${SSH_OPTS[@]}" "$SSH_USER@$h" \
                'pkill -x autossh 2>/dev/null; pkill -f "paxosbus[_]tunnel" 2>/dev/null; pkill -f "[p]axosbus-replica" 2>/dev/null; pkill -f "[p]axosbus-client" 2>/dev/null; true' \
                >/dev/null 2>&1 &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
    fi
    exit "$ec"
}

# ── Main ─────────────────────────────────────────────────────────────────────
trap cleanup EXIT INT TERM
MODE=direct
case "$SUBCMD" in
    setup)
        echo "==> setup-only: (re)running setup.sh on all nodes"
        for h in "$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST"; do
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
