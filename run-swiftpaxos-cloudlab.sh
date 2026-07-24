#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# CloudLab WAN run of the swiftpaxos consensus protocols (paxos | epaxos).
#
# Companion to run-cloudlab.sh (which runs the standalone PaxosBus impl): same
# three single-node CloudLab experiments, same cloudlab/nodes.env, same
# SSH-from-laptop orchestration — but this drives the ROOT swiftpaxos binary,
# which plays three roles rendezvous'd by a master:
#   master    on the leader replica's host  (:$MASTER_PORT, default 7087)
#   replica   r0..r2, one per host          (:$PORT data, :$PORT+1000 master RPC)
#   client    CLOSED-LOOP. Each client PROCESS runs CLONES+1 independent
#             closed-loop clients (goroutines), so total concurrency =
#             (#client processes) x (CLONES+1). 1000 concurrent clients =
#             e.g. NUM_CLIENTS=10 CLONES=99 — NOT 1000 processes.
#
# Launch order matters: master -> replicas -> clients. Replicas hot-spin while
# the master is unreachable (run.go registerWithMaster sleeps 4ns between
# retries), so the master always goes up first. Every closed-loop client exits
# on its own after REQS requests; the script polls for that (DURATION_S cap),
# then tears down and collects logs + metrics.
#
# Routing (from the generated conf's -- Proxy -- block: every client is
# (local) to its co-located replica):
#   paxos   clients send to the LEADER (r$LEADER_REPLICA) over the WAN
#   epaxos  leaderless; clients send to their co-located (closest) replica
#
# Transport is DIRECT only — public control IPs, empirically open on arbitrary
# ports between Utah/Wisconsin/Clemson (see run-cloudlab.sh). `probe` verifies
# the port mesh ($PORT, $PORT+1000, $MASTER_PORT) AND ICMP: the master
# Fatal()s if it cannot `ping` a replica (master/master.go:217) and clients
# ping replicas to find the closest (client/client.go:360), so ICMP is a hard
# requirement, not a nicety.
#
# Usage:
#   ./run-swiftpaxos-cloudlab.sh                    # paxos, small scale
#   ./run-swiftpaxos-cloudlab.sh -P epaxos          # epaxos instead
#   ./run-swiftpaxos-cloudlab.sh --scale large      # clients on EVERY host
#   ./run-swiftpaxos-cloudlab.sh probe              # port + ICMP reachability
#   ./run-swiftpaxos-cloudlab.sh setup              # (re)run setup.sh, exit
#
# Env knobs (flags win over env):
#   PROTOCOL=paxos|epaxos   same as -P
#   SCALE=small|large       same as --scale
#   NUM_CLIENTS             [small] client processes on the client host (1)
#   CLIENTS_PER_HOST        [large] client processes on EACH host (1)
#   CLONES                  extra closed-loop clients per process (0)
#   REQS                    timed requests per closed-loop client (1000);
#                           the binary sends one extra untimed warm-up request
#   WRITES / CONFLICTS      % writes (100) / % ops on the conflict key (2)
#   COMMAND_SIZE / KEY      payload bytes (16) / conflict key (42)
#   DURATION_S              hard cap on the client phase (300); closed-loop
#                           clients normally finish well before this
#   LEADER_REPLICA          paxos leader; its host also runs the master (0)
#   CLIENT_REPLICA          [small] which host runs the clients; must differ
#                           from LEADER_REPLICA (1)
#   PORT / MASTER_PORT      replica / master ports (7070 / 7087)
#   SKIP_PING_CHECK=1       skip the preflight ICMP check
#   NODES_FILE              path to nodes.env (default cloudlab/nodes.env)
# ─────────────────────────────────────────────────────────────────────────────

PROTOCOL="${PROTOCOL:-paxos}"
SCALE="${SCALE:-small}"
NUM_CLIENTS="${NUM_CLIENTS:-1}"
CLIENTS_PER_HOST="${CLIENTS_PER_HOST:-1}"
CLONES="${CLONES:-0}"
REQS="${REQS:-1000}"
WRITES="${WRITES:-100}"
CONFLICTS="${CONFLICTS:-2}"
COMMAND_SIZE="${COMMAND_SIZE:-16}"
KEY="${KEY:-42}"
DURATION_S="${DURATION_S:-300}"
LEADER_REPLICA="${LEADER_REPLICA:-0}"
CLIENT_REPLICA="${CLIENT_REPLICA:-1}"
PORT="${PORT:-7070}"
MASTER_PORT="${MASTER_PORT:-7087}"
SKIP_PING_CHECK="${SKIP_PING_CHECK:-0}"
REPO_URL="${REPO_URL:-https://github.com/austint903/paxos-bus.git}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES_FILE="${NODES_FILE:-$SCRIPT_DIR/cloudlab/nodes.env}"

BIN=/local/paxosbus-cloudlab/bin     # built by cloudlab/setup.sh
CONF=/tmp/swiftpaxos.conf            # pushed to each node
RPC_PORT=$((PORT + 1000))            # replica<->master RPC (run.go:65)

SUBCMD=run
while [[ $# -gt 0 ]]; do
    case "$1" in
        run|probe|setup) SUBCMD="$1" ;;
        small|large)     SCALE="$1" ;;
        paxos|epaxos)    PROTOCOL="$1" ;;
        -P|--protocol)   PROTOCOL="${2:?-P needs paxos|epaxos}"; shift ;;
        --protocol=*)    PROTOCOL="${1#--protocol=}" ;;
        --scale)         SCALE="${2:?--scale needs small|large}"; shift ;;
        --scale=*)       SCALE="${1#--scale=}" ;;
        *) echo "usage: $0 [run|probe|setup] [-P paxos|epaxos] [--scale small|large]"; exit 1 ;;
    esac
    shift
done
[[ "$PROTOCOL" =~ ^(paxos|epaxos)$ ]] || { echo "ERROR: bad PROTOCOL '$PROTOCOL' (want paxos|epaxos)"; exit 1; }
[[ "$SCALE" =~ ^(small|large)$ ]] || { echo "ERROR: bad SCALE '$SCALE' (want small|large)"; exit 1; }
[[ "$LEADER_REPLICA" =~ ^[012]$ ]] || { echo "ERROR: LEADER_REPLICA must be 0, 1 or 2"; exit 1; }
[[ "$CLIENT_REPLICA" =~ ^[012]$ ]] || { echo "ERROR: CLIENT_REPLICA must be 0, 1 or 2"; exit 1; }
if [[ "$SCALE" == "small" && "$CLIENT_REPLICA" == "$LEADER_REPLICA" ]]; then
    echo "ERROR: CLIENT_REPLICA ($CLIENT_REPLICA) must differ from LEADER_REPLICA ($LEADER_REPLICA) in small scale"
    exit 1
fi

[[ -f "$NODES_FILE" ]] || { echo "ERROR: $NODES_FILE not found (copy cloudlab/nodes.env.example)"; exit 1; }
# shellcheck disable=SC1090
source "$NODES_FILE"
: "${SSH_USER:?set SSH_USER in nodes.env}"
: "${REPLICA0_HOST:?}" "${REPLICA1_HOST:?}" "${REPLICA2_HOST:?}"

RHOST=("$REPLICA0_HOST" "$REPLICA1_HOST" "$REPLICA2_HOST")
RLABEL=("${REPLICA0_LABEL:-utah}" "${REPLICA1_LABEL:-wisc}" "${REPLICA2_LABEL:-clemson}")
RIP=("" "" "")

# Client processes get GLOBALLY unique ids (aliases c1..cN in the conf):
#   small: ids 1..NUM_CLIENTS, all on CLIENT_REPLICA's host;
#   large: host at position i owns ids i*CLIENTS_PER_HOST+1 .. (i+1)*CLIENTS_PER_HOST.
if [[ "$SCALE" == "large" ]]; then
    CLIENT_HOST_IDXS=(0 1 2)
    TOTAL_PROCS=$((3 * CLIENTS_PER_HOST))
else
    CLIENT_HOST_IDXS=("$CLIENT_REPLICA")
    TOTAL_PROCS="$NUM_CLIENTS"
fi
TOTAL_CLIENTS=$((TOTAL_PROCS * (CLONES + 1)))

# ids_on_host <position-in-CLIENT_HOST_IDXS> -> that host's client ids
ids_on_host() {
    local pos="$1"
    if [[ "$SCALE" == "large" ]]; then
        seq $((pos * CLIENTS_PER_HOST + 1)) $(((pos + 1) * CLIENTS_PER_HOST))
    else
        seq 1 "$NUM_CLIENTS"
    fi
}

# clients_on_replica <replica-idx> -> how many client processes on that host
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

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o BatchMode=yes)
ssh_to()   { ssh "${SSH_OPTS[@]}" "$SSH_USER@$1" "$2"; }
scp_from() { scp "${SSH_OPTS[@]}" "$SSH_USER@$1:$2" "$3"; }
scp_to()   { scp "${SSH_OPTS[@]}" "$3" "$SSH_USER@$1:$2"; }

LOCAL_BG_PIDS=()      # local background ssh (probe listeners, log tails)
STARTED_REMOTE=0      # 1 once remote processes exist -> teardown must pkill

host_ip() {
    local ip
    ip=$(ssh_to "$1" "cat /var/emulab/boot/myip 2>/dev/null" | tr -d '[:space:]' || true)
    [[ -z "$ip" ]] && ip=$(dig +short "$1" 2>/dev/null | grep -Eo '^[0-9.]+$' | tail -n1 || true)
    echo "$ip"
}

# Push the LOCAL cloudlab/setup.sh (not origin/master's copy) so a setup.sh
# change — e.g. the swiftpaxos build step — works without pushing first. The
# Go sources themselves still come from origin/master.
ensure_ready() {
    local host="$1"
    echo "  [$host] syncing origin/master + rebuilding (first time can take a few minutes)"
    scp_to "$host" "/tmp/swiftpaxos-setup.sh" "$SCRIPT_DIR/cloudlab/setup.sh"
    ssh_to "$host" "sudo bash -c '
        export DEBIAN_FRONTEND=noninteractive
        if [ ! -d /local/paxos-bus/.git ]; then git clone $REPO_URL /local/paxos-bus; fi
        git -C /local/paxos-bus fetch --prune origin
        git -C /local/paxos-bus reset --hard origin/master
        git -C /local/paxos-bus clean -fd
        REPO_URL=$REPO_URL bash /tmp/swiftpaxos-setup.sh'
        test -x $BIN/swiftpaxos || { echo \"  [$host] $BIN/swiftpaxos missing after setup\"; exit 1; }"
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

    echo "==> Resolving public control IPs (parallel)"
    local ipd; ipd="$(mktemp -d)"
    pids=()
    for i in 0 1 2; do host_ip "${RHOST[$i]}" > "$ipd/$i" & pids+=($!); done
    for p in "${pids[@]}"; do wait "$p" || true; done
    for i in 0 1 2; do
        RIP[$i]="$(tr -d '[:space:]' < "$ipd/$i")"
        [[ -n "${RIP[$i]}" ]] || { echo "  could not resolve control IP for ${RHOST[$i]}"; exit 1; }
        local tag="${RLABEL[$i]}" nc
        [[ "$i" == "$LEADER_REPLICA" ]] && tag="$tag, leader+master"
        nc="$(clients_on_replica "$i")"
        [[ "$nc" != 0 ]] && tag="$tag, +${nc} client proc(s)"
        printf "  r%d  %-32s %s  (%s)\n" "$i" "${RHOST[$i]}" "${RIP[$i]}" "$tag"
    done
    rm -rf "$ipd"
}

# ── ICMP check: master Fatal()s if it can't ping a replica ───────────────────
ping_check() {
    echo "==> ICMP mesh check (master/clients shell out to ping)"
    local i j p res; res="$(mktemp -d)"
    local pids=()
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            { ssh_to "${RHOST[$i]}" "ping -c 2 -W 2 -q ${RIP[$j]} >/dev/null 2>&1" \
                && echo OK > "$res/$i-$j" || echo FAIL > "$res/$i-$j"; } &
            pids+=($!)
        done
    done
    for p in "${pids[@]}"; do wait "$p" || true; done
    local bad=0 r
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            r="$(cat "$res/$i-$j" 2>/dev/null || echo FAIL)"
            printf "  %-7s -> %-7s ping  %s\n" "${RLABEL[$i]}" "${RLABEL[$j]}" "$r"
            [[ "$r" == OK ]] || bad=1
        done
    done
    rm -rf "$res"
    return $bad
}

# ── Port probe: full replica mesh on $PORT, $RPC_PORT and $MASTER_PORT ────────
# Same python listener/dialer approach as run-cloudlab.sh (no nc ambiguity, no
# self-killing pkill), extended to listen on all three ports per node.
PY_LISTEN='import socket,sys,threading,time
def serve(p):
    s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    s.bind(("0.0.0.0",p)); s.listen(16)
    while True:
        c,_=s.accept(); c.close()
for p in sys.argv[1:]:
    threading.Thread(target=serve,args=(int(p),),daemon=True).start()
time.sleep(30)'
PY_DIAL='import socket,sys; socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=5).close()'

do_probe() {
    echo "==> Probing direct reachability on :$PORT :$RPC_PORT :$MASTER_PORT (one listener ssh per node)"
    local i j prt p res; res="$(mktemp -d)"

    local lpids=()
    for i in 0 1 2; do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$i]}" \
            "exec timeout 30 python3 -c '$PY_LISTEN' $PORT $RPC_PORT $MASTER_PORT" >/dev/null 2>&1 &
        lpids+=($!)
    done
    LOCAL_BG_PIDS+=("${lpids[@]}")
    sleep 2   # let the listeners bind before dialing (else false BLOCKED)

    local cpids=()
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            for prt in "$PORT" "$RPC_PORT" "$MASTER_PORT"; do
                { ssh_to "${RHOST[$i]}" "python3 -c '$PY_DIAL' ${RIP[$j]} $prt" >/dev/null 2>&1 \
                    && echo OPEN > "$res/$i-$j-$prt" || echo BLOCKED > "$res/$i-$j-$prt"; } &
                cpids+=($!)
            done
        done
    done
    for p in "${cpids[@]}"; do wait "$p" || true; done

    for p in "${lpids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait "${lpids[@]}" 2>/dev/null || true

    local ok=0 r
    for i in 0 1 2; do
        for j in 0 1 2; do
            [[ "$i" == "$j" ]] && continue
            for prt in "$PORT" "$RPC_PORT" "$MASTER_PORT"; do
                r="$(cat "$res/$i-$j-$prt" 2>/dev/null || echo BLOCKED)"
                printf "  %-7s -> %-7s :%-5s %s\n" "${RLABEL[$i]}" "${RLABEL[$j]}" "$prt" "$r"
                [[ "$r" == OPEN ]] || ok=1
            done
        done
    done
    rm -rf "$res"
    return $ok
}

# ── Config generation + push ─────────────────────────────────────────────────
# ONE conf, identical on every node (all public IPs — no per-self variants).
# Everything stays lowercase: config.Read lowercases each line, but the
# -- Proxy -- block is parsed from the RAW text, so mixed case there would
# break the ClientAddrs lookups. Clients section must precede Proxy (the
# proxy parser resolves client aliases as it reads).
write_conf() {
    local out="$1" j pos ridx id
    {
        echo "// generated by run-swiftpaxos-cloudlab.sh $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo ""
        echo "-- Replicas --"
        for j in 0 1 2; do echo "r$j ${RIP[$j]}"; done
        echo ""
        echo "-- Clients --"
        for pos in "${!CLIENT_HOST_IDXS[@]}"; do
            ridx="${CLIENT_HOST_IDXS[$pos]}"
            for id in $(ids_on_host "$pos"); do echo "c$id ${RIP[$ridx]}"; done
        done
        echo ""
        echo "-- Master --"
        echo "m ${RIP[$LEADER_REPLICA]}"
        echo ""
        echo "masterport: $MASTER_PORT"
        echo "port: $PORT"
        echo ""
        echo "protocol: $PROTOCOL"
        echo "noop: false"
        if [[ "$PROTOCOL" == "paxos" ]]; then
            # The master string-compares this against the registering replica's
            # addr:port; the parser's default-port fallback is hardwired to
            # 7070, so append :$PORT explicitly.
            echo "leader: ${RIP[$LEADER_REPLICA]}:$PORT"
        fi
        echo ""
        echo "reqs: $REQS"
        echo "writes: $WRITES"
        echo "conflicts: $CONFLICTS"
        echo "commandsize: $COMMAND_SIZE"
        echo "clones: $CLONES"
        echo "key: $KEY"
        echo "pipeline: false"
        echo ""
        echo "-- Proxy --"
        for j in 0 1 2; do
            echo "server_alias r$j"
            for pos in "${!CLIENT_HOST_IDXS[@]}"; do
                [[ "${CLIENT_HOST_IDXS[$pos]}" == "$j" ]] || continue
                for id in $(ids_on_host "$pos"); do echo "c$id (local)"; done
            done
        done
        echo "---"
    } > "$out"
}

push_confs() {
    echo "==> Generating + distributing swiftpaxos.conf"
    mkdir -p "$SCRIPT_DIR/config-swiftpaxos"
    local tmp="$SCRIPT_DIR/config-swiftpaxos/swiftpaxos-cloudlab.conf"
    write_conf "$tmp"
    local i pids=() p
    for i in 0 1 2; do
        scp_to "${RHOST[$i]}" "$CONF" "$tmp" & pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || { echo "  conf push failed"; exit 1; }; done
    echo "  conf:"; sed 's/^/    /' "$tmp"
}

# ── Launch: master -> replicas -> clients ────────────────────────────────────
kill_stale() {
    echo "==> Killing stale swiftpaxos processes (parallel)"
    local i p kpids=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "pkill -f 'bin/[s]wiftpaxos' || true; rm -rf /tmp/swiftpaxos-client-c* /tmp/swiftpaxos-master.log /tmp/swiftpaxos-replica.log" & kpids+=($!)
    done
    for p in "${kpids[@]}"; do wait "$p" || true; done
    sleep 1
}

launch_master() {
    STARTED_REMOTE=1
    local mhost="${RHOST[$LEADER_REPLICA]}"
    echo "==> Launching master on ${RLABEL[$LEADER_REPLICA]} :$MASTER_PORT"
    # Every client keeps an RPC conn to the master open, so raise the soft fd
    # limit (default 1024) before launching — same on replicas and clients.
    ssh_to "$mhost" "
        ulimit -n \$(ulimit -Hn) 2>/dev/null || true
        nohup $BIN/swiftpaxos -run master -config $CONF -protocol $PROTOCOL \
          </dev/null >/tmp/swiftpaxos-master.log 2>&1 &
        sleep 1
        if pgrep -f '^$BIN/swiftpaxos -run master' >/dev/null; then
          echo '  [master @ ${RLABEL[$LEADER_REPLICA]}] running'
        else
          echo '  [master @ ${RLABEL[$LEADER_REPLICA]}] NOT RUNNING — startup log:'
          cat /tmp/swiftpaxos-master.log 2>/dev/null || true
        fi"
    sleep 2
}

launch_replicas() {
    echo "==> Launching replicas — one ssh process per node, in parallel"
    local i p lpids=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "
            ulimit -n \$(ulimit -Hn) 2>/dev/null || true
            nohup $BIN/swiftpaxos -run server -config $CONF -protocol $PROTOCOL -alias r$i \
              </dev/null >/tmp/swiftpaxos-replica.log 2>&1 &
            sleep 1
            if pgrep -f '^$BIN/swiftpaxos -run server' >/dev/null; then
              echo '  [replica $i @ ${RLABEL[$i]}] running'
            else
              echo '  [replica $i @ ${RLABEL[$i]}] NOT RUNNING — startup log:'
              cat /tmp/swiftpaxos-replica.log 2>/dev/null || true
            fi" & lpids+=($!)
    done
    for p in "${lpids[@]}"; do wait "$p" || true; done
    sleep 3   # let all replicas register with the master
}

launch_clients() {
    echo "==> Launching $TOTAL_PROCS client proc(s) x $((CLONES + 1)) closed-loop clients = $TOTAL_CLIENTS clients, scale=$SCALE"
    # A generated launcher script (scp'd per host) sidesteps ssh quoting: it
    # starts every client process in its own /tmp/swiftpaxos-client-c<id>/
    # workdir — clone 0 logs to client.log (stdout), clones 1..N write
    # client_<j> into the cwd — and brackets the run with RUN_START/RUN_END
    # epoch markers for a clock-skew-free wall window.
    local launcher; launcher="$(mktemp)"
    cat > "$launcher" <<EOF
#!/bin/sh
# generated by run-swiftpaxos-cloudlab.sh — usage: <label> <id>...
# Each clone holds ~4 conns (master + 3 replicas) plus a log file, so a
# process with hundreds of clones blows the default 1024 soft fd limit.
ulimit -n \$(ulimit -Hn) 2>/dev/null || true
label="\$1"; shift
for id in "\$@"; do
    d=/tmp/swiftpaxos-client-c\$id
    rm -rf "\$d" && mkdir -p "\$d" && cd "\$d"
    echo "\$label" > host
    echo "RUN_START \$(date +%s.%N)" > client.log
    nohup sh -c "$BIN/swiftpaxos -run client -config $CONF -protocol $PROTOCOL -alias c\$id; echo RUN_END \\\$(date +%s.%N)" \\
        </dev/null >> client.log 2>&1 &
done
sleep 1
for id in "\$@"; do
    if pgrep -f "^$BIN/swiftpaxos -run client .*-alias c\$id\\\$" >/dev/null; then
        echo "  [client c\$id @ \$label] running"
    else
        echo "  [client c\$id @ \$label] NOT RUNNING — startup log:"
        cat /tmp/swiftpaxos-client-c\$id/client.log 2>/dev/null || true
    fi
done
EOF
    local pos p cpids=()
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        local ridx="${CLIENT_HOST_IDXS[$pos]}"
        local chost="${RHOST[$ridx]}" clabel="${RLABEL[$ridx]}"
        local ids; ids="$(ids_on_host "$pos" | tr '\n' ' ')"
        {
            scp_to "$chost" "/tmp/swiftpaxos-launch-clients.sh" "$launcher"
            ssh_to "$chost" "sh /tmp/swiftpaxos-launch-clients.sh $clabel $ids"
        } & cpids+=($!)
    done
    for p in "${cpids[@]}"; do wait "$p" || true; done
    rm -f "$launcher"
}

# ── Wait for the closed-loop clients to finish ───────────────────────────────
tail_logs() {
    echo "==> Live tail of master + replicas (clients tracked by the poll below)"
    echo "----------------------------------------------------------------"
    local i pids=()
    ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$LEADER_REPLICA]}" \
        "tail -f /tmp/swiftpaxos-master.log | sed -u 's/^/[m ] /'" & pids+=($!)
    for i in 0 1 2; do
        ssh "${SSH_OPTS[@]}" "$SSH_USER@${RHOST[$i]}" \
            "tail -f /tmp/swiftpaxos-replica.log | sed -u 's/^/[r$i] /'" & pids+=($!)
    done
    LOCAL_BG_PIDS+=("${pids[@]}")
    TAIL_PIDS=("${pids[@]}")
}

wait_clients() {
    local start=$SECONDS deadline=$((SECONDS + DURATION_S))
    echo "==> Waiting for clients to finish their $REQS reqs each (cap ${DURATION_S}s)"
    while :; do
        local left=0 pos ridx n
        for pos in "${!CLIENT_HOST_IDXS[@]}"; do
            ridx="${CLIENT_HOST_IDXS[$pos]}"
            n=$(ssh_to "${RHOST[$ridx]}" "pgrep -fc '^$BIN/swiftpaxos -run client' 2>/dev/null || true")
            left=$((left + ${n:-0}))
        done
        if [[ "$left" -eq 0 ]]; then
            echo "==> All clients finished after $((SECONDS - start))s"
            break
        fi
        if [[ $SECONDS -ge $deadline ]]; then
            echo "==> WARN: ${DURATION_S}s cap hit with $left client proc(s) still up — killing them"
            break
        fi
        echo "  $left client proc(s) still running ($((SECONDS - start))s elapsed)"
        sleep 5
    done
    local p
    for p in "${TAIL_PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo "----------------------------------------------------------------"
}

# ── Collect ──────────────────────────────────────────────────────────────────
collect() {
    echo "==> Stopping master/replicas/clients (parallel)"
    local i p sp=()
    for i in 0 1 2; do
        ssh_to "${RHOST[$i]}" "pkill -f 'bin/[s]wiftpaxos' || true" & sp+=($!)
    done
    for p in "${sp[@]}"; do wait "$p" || true; done

    local ts run_dir
    ts=$(date +%Y%m%d-%H%M%S)
    run_dir="$SCRIPT_DIR/paxosbus/logs/swiftpaxos/cloudlab-$PROTOCOL-run-$ts"
    mkdir -p "$run_dir"

    echo "==> Collecting logs -> $run_dir"
    scp_from "${RHOST[$LEADER_REPLICA]}" "/tmp/swiftpaxos-master.log" "$run_dir/master.log" \
        || echo "  WARN: no master log on ${RHOST[$LEADER_REPLICA]}"
    for i in 0 1 2; do
        scp_from "${RHOST[$i]}" "/tmp/swiftpaxos-replica.log" "$run_dir/replica-$i.log" \
            || echo "  WARN: no replica log on ${RHOST[$i]}"
    done
    # tar-over-ssh, not scp -r: a big run has thousands of small clone logs per
    # host and scp round-trips per file (the 5400-client collect took ~1 h);
    # one compressed tar stream brings that back to ~1 min.
    local pos
    for pos in "${!CLIENT_HOST_IDXS[@]}"; do
        local ridx="${CLIENT_HOST_IDXS[$pos]}"
        ssh_to "${RHOST[$ridx]}" "cd /tmp && tar czf - swiftpaxos-client-c*" \
            | tar xzf - -C "$run_dir" \
            || echo "  WARN: no client dirs on ${RHOST[$ridx]}"
    done

    {
        echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
        echo "platform=cloudlab"
        echo "system=swiftpaxos"
        echo "protocol=$PROTOCOL"
        echo "scale=$SCALE"
        echo "client_procs=$TOTAL_PROCS"
        echo "clones=$CLONES"
        echo "total_clients=$TOTAL_CLIENTS"
        echo "reqs_per_client=$REQS"
        echo "writes=$WRITES"
        echo "conflicts=$CONFLICTS"
        echo "command_size=$COMMAND_SIZE"
        echo "key=$KEY"
        echo "port=$PORT"
        echo "master_port=$MASTER_PORT"
        echo "duration_cap_s=$DURATION_S"
        echo "leader_replica=$LEADER_REPLICA (${RLABEL[$LEADER_REPLICA]})"
        echo "client_hosts=$(for pos in "${!CLIENT_HOST_IDXS[@]}"; do printf '%s ' "${RLABEL[${CLIENT_HOST_IDXS[$pos]}]}"; done)"
        echo "replicas=${RLABEL[*]}"
    } > "$run_dir/run-meta.txt"

    echo ""
    python3 "$SCRIPT_DIR/cloudlab/aggregate-swiftpaxos-stats.py" "$run_dir" \
        | tee "$run_dir/metrics.txt" \
        || echo "  WARN: aggregate-swiftpaxos-stats.py failed"
    echo ""
    echo "==> Done. Logs: $run_dir"
}

# ── Teardown (every exit incl. Ctrl-C): kill processes, keep logs ─────────────
cleanup() {
    local ec=$?
    trap - EXIT INT TERM
    if ((${#LOCAL_BG_PIDS[@]})); then
        kill "${LOCAL_BG_PIDS[@]}" 2>/dev/null || true
    fi
    if [[ "$STARTED_REMOTE" == 1 ]] && ((${#RHOST[@]})); then
        echo "==> Teardown: stopping swiftpaxos on all nodes (logs kept)"
        local h p pids=()
        for h in "${RHOST[@]}"; do
            ssh "${SSH_OPTS[@]}" "$SSH_USER@$h" "pkill -f 'bin/[s]wiftpaxos' 2>/dev/null; true" \
                >/dev/null 2>&1 & pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
    fi
    exit "$ec"
}

# ── Main ─────────────────────────────────────────────────────────────────────
trap cleanup EXIT INT TERM
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
        rc=0
        ping_check || rc=1
        do_probe || rc=1
        if [[ "$rc" == 0 ]]; then echo "==> ALL OK — ports and ICMP reachable across the mesh"; exit 0
        else echo "==> BLOCKED — see FAIL/BLOCKED lines above; a run would break (master Fatal()s without ping)"; exit 1; fi ;;
    run) ;;
    *) echo "usage: $0 [run|probe|setup]"; exit 1 ;;
esac

preflight
if [[ "$SKIP_PING_CHECK" != 1 ]]; then
    ping_check || { echo "ERROR: ICMP mesh incomplete — the master would Fatal(). SKIP_PING_CHECK=1 to override."; exit 1; }
fi
push_confs
kill_stale
launch_master
launch_replicas
launch_clients
tail_logs
wait_clients
collect
