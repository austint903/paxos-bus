#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SINGLE_MACHINE="${PAXOSBUS_SINGLE_MACHINE:-0}"
ENV_FILE="${PAXOSBUS_GCP_ENV_FILE:-$REPO_ROOT/.env}"

if [[ " $* " == *" --help "* || " $* " == *" -h "* ]]; then
    echo "Usage: $0 [options]. See $REPO_ROOT/README.md for configuration and examples."
    exit 0
fi

[[ -f "$ENV_FILE" ]] || { echo "ERROR: environment file not found: $ENV_FILE" >&2; exit 1; }
set -a
source "$ENV_FILE"
set +a

PROJECT_ID="${GCP_PROJECT_ID:-}"
PORT="${PAXOSBUS_PORT:-7000}"
GCP_GOARCH="${GCP_GOARCH:-amd64}"
GEN_INTERVAL_US=500
INTERVAL_MS=1
COMMAND_SIZE=512
SCHEDULE_WINDOW_MS=2000
DURATION_S=60
SCENARIO=normal
DROP_EVERY=0
DROP_MODE=none
START_DELAY_MS=10000
RESEND_MS=2000
FAILURE_AT_S=20
SUSPECT_TIMEOUT_MS=2000
SYNC_INTERVAL_MS=500
VIEW_CHANGE_TIMEOUT_MS=15000
VIEW_CHANGE_FALLBACK_TIMEOUT_MS=20000
GAP_DELTA_MS=5000
GAP_RETRY_TIMEOUT_MS=1500
RETAIN_SLOTS=16384
RETAIN_MB=256
SCHEDULE_START_NS=0
REMOTE_DIR="paxosbus-gcp"
REMOTE_CONF="/tmp/paxosbus.conf"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    option="${1%%=*}"
    case "$1" in
        --normal|--failure) SCENARIO="${1#--}"; shift; continue ;;
    esac
    case "$option" in
        -g|--request-interval-us) variable=GEN_INTERVAL_US ;;
        -p|--bus-interval-ms) variable=INTERVAL_MS ;;
        --command-size) variable=COMMAND_SIZE ;;
        --schedule-window-ms) variable=SCHEDULE_WINDOW_MS ;;
        -d|--duration-s) variable=DURATION_S ;;
        --drop-every) variable=DROP_EVERY ;;
        *) die "unknown argument '$1'; see $REPO_ROOT/README.md" ;;
    esac
    if [[ "$1" == *=* ]]; then
        value="${1#*=}"
    else
        [[ $# -ge 2 ]] || die "$option needs a value"
        value="$2"
        shift
    fi
    [[ -n "$value" ]] || die "$option needs a value"
    printf -v "$variable" '%s' "$value"
    shift
done

require_uint() {
    local name="$1" value="$2" allow_zero="${3:-0}"
    [[ "$value" =~ ^(0|[1-9][0-9]*)$ ]] || die "$name must be an integer (got '$value')"
    if [[ "$allow_zero" == "0" ]] && ((10#$value == 0)); then
        die "$name must be greater than zero"
    fi
}

[[ -n "$PROJECT_ID" ]] || die "GCP_PROJECT_ID must be set in $ENV_FILE"
[[ "$SINGLE_MACHINE" =~ ^[01]$ ]] || die "PAXOSBUS_SINGLE_MACHINE must be 0 or 1"

require_uint request-interval-us "$GEN_INTERVAL_US"
require_uint bus-interval-ms "$INTERVAL_MS"
require_uint command-size "$COMMAND_SIZE"
((COMMAND_SIZE <= 4294967295)) || die "command-size exceeds uint32"
require_uint schedule-window-ms "$SCHEDULE_WINDOW_MS"
require_uint duration-s "$DURATION_S"
require_uint drop-every "$DROP_EVERY" 1
require_uint port "$PORT"
((PORT <= 65535)) || die "invalid replica port"
if [[ "$SCENARIO" == failure ]] && ((DURATION_S <= FAILURE_AT_S)); then
    die "failure runs must last longer than $FAILURE_AT_S seconds"
fi
if ((DROP_EVERY > 0)); then DROP_MODE=leader; fi
[[ "$GCP_GOARCH" =~ ^(amd64|arm64)$ ]] || die "GCP_GOARCH must be amd64 or arm64"
for dependency in gcloud python3; do
    command -v "$dependency" >/dev/null 2>&1 || die "$dependency is not installed or not on PATH"
done

NODE_KEYS=()
NODE_VMS=()
NODE_ZONES=()
NODE_LABELS=()
NODE_STATUSES=()
NODE_IPS=()
HOST_INDICES=()
REPLICA_INDICES=()
CLIENT_INDICES=()

validate_counts() {
    require_uint replicas "$REPLICA_COUNT"
    require_uint clients "$CLIENT_COUNT"
    [[ ${#REPLICA_COUNT} -le 2 ]] && ((REPLICA_COUNT <= 31 && REPLICA_COUNT % 2 == 1)) \
        || die "replica count must be odd and between 1 and 31"
    [[ ${#CLIENT_COUNT} -le 5 ]] && ((CLIENT_COUNT <= 65536)) \
        || die "client count must be between 1 and 65536"
}

append_nodes() {
    local role="$1" nodes="$2" entry name zone label
    local entries=()
    IFS=$' \t\n' read -r -a entries <<< "${nodes//$'\n'/ }"
    ((${#entries[@]} > 0)) || die "$role must list at least one VM"
    for entry in "${entries[@]}"; do
        [[ "$entry" =~ ^[^:]+:[^:]+(:[^:]+)?$ ]] || die "invalid $role entry '$entry'; use VM:zone[:label]"
        IFS=: read -r name zone label <<< "$entry"
        NODE_VMS+=("$name")
        NODE_ZONES+=("$zone")
        NODE_LABELS+=("${label:-${zone%-*}}")
    done
}

if [[ "$SINGLE_MACHINE" == 1 ]]; then
    : "${GCP_SINGLE_VM:?Set GCP_SINGLE_VM in the environment file}"
    : "${GCP_SINGLE_ZONE:?Set GCP_SINGLE_ZONE in the environment file}"
    REPLICA_COUNT="${PAXOSBUS_REPLICA_COUNT:-3}"
    CLIENT_COUNT="${PAXOSBUS_CLIENT_COUNT:-8}"
    validate_counts
    ((PORT + REPLICA_COUNT - 1 <= 65535)) || die "single-machine replica ports exceed 65535"
    for ((idx=0; idx<REPLICA_COUNT+CLIENT_COUNT; idx++)); do
        NODE_VMS+=("$GCP_SINGLE_VM")
        NODE_ZONES+=("$GCP_SINGLE_ZONE")
        NODE_LABELS+=("${GCP_SINGLE_ZONE%-*}")
    done
else
    append_nodes GCP_REPLICAS "${GCP_REPLICAS:-}"
    REPLICA_COUNT=${#NODE_VMS[@]}
    append_nodes GCP_CLIENTS "${GCP_CLIENTS:-}"
    CLIENT_COUNT=$((${#NODE_VMS[@]} - REPLICA_COUNT))
    validate_counts
fi
for idx in "${!NODE_VMS[@]}"; do
    [[ "${NODE_VMS[$idx]}" =~ ^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "invalid VM name '${NODE_VMS[$idx]}'"
    [[ "${NODE_ZONES[$idx]}" =~ ^[a-z][a-z0-9-]+$ ]] || die "invalid zone '${NODE_ZONES[$idx]}'"
    [[ "${NODE_LABELS[$idx]}" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die "invalid label '${NODE_LABELS[$idx]}'"
    if ((idx < REPLICA_COUNT)); then
        NODE_KEYS+=("replica$idx")
        REPLICA_INDICES+=("$idx")
    else
        NODE_KEYS+=("client$((idx - REPLICA_COUNT))")
        CLIENT_INDICES+=("$idx")
    fi
    if [[ "$SINGLE_MACHINE" == 0 ]]; then
        for ((previous=0; previous<idx; previous++)); do
            [[ "${NODE_VMS[$idx]}:${NODE_ZONES[$idx]}" != "${NODE_VMS[$previous]}:${NODE_ZONES[$previous]}" ]] \
                || die "each process requires a separate VM"
        done
    fi
    if [[ "$SINGLE_MACHINE" == 0 || "$idx" == 0 ]]; then HOST_INDICES+=("$idx"); fi
done

discover_instances() {
    echo "==> Loading configured GCP instances from $ENV_FILE"
    local idx details status ip
    for idx in "${HOST_INDICES[@]}"; do
        details="$(gcloud compute instances describe "${NODE_VMS[$idx]}" \
            --zone="${NODE_ZONES[$idx]}" \
            --project="$PROJECT_ID" \
            --format="value(status,networkInterfaces[0].networkIP)")" \
            || die "could not describe ${NODE_VMS[$idx]} in ${NODE_ZONES[$idx]}"
        read -r status ip <<< "$details"
        [[ -n "$status" ]] || die "${NODE_VMS[$idx]} returned no status"
        [[ -n "$ip" ]] || die "${NODE_VMS[$idx]} has no internal IP"
        NODE_STATUSES[$idx]="$status"
        NODE_IPS[$idx]="$ip"
    done
    if [[ "$SINGLE_MACHINE" == 1 ]]; then
        for idx in "${!NODE_VMS[@]}"; do
            NODE_STATUSES[$idx]="${NODE_STATUSES[0]}"
            NODE_IPS[$idx]="${NODE_IPS[0]}"
        done
    fi
}

client_ids_for_node() {
    if (($1 >= REPLICA_COUNT)); then echo $(($1 - REPLICA_COUNT)); fi
}

replica_index_for_node() {
    if (($1 < REPLICA_COUNT)); then echo "$1"; else return 1; fi
}

print_topology() {
    local idx ridx role ids
    echo "==> GCP topology"
    for idx in "${!NODE_VMS[@]}"; do
        ids="$(client_ids_for_node "$idx")"
        if ridx="$(replica_index_for_node "$idx")"; then
            role="replica $ridx"
            [[ "$ridx" == 0 ]] && role="replica 0 (leader)"
        else
            role="client $ids"
        fi
        printf "  %-14s %-24s %-22s %-10s %s  %s\n" \
            "${NODE_KEYS[$idx]}" "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" \
            "${NODE_STATUSES[$idx]}" "${NODE_IPS[$idx]}" "$role"
    done
    echo "  total: ${#HOST_INDICES[@]} VMs, $CLIENT_COUNT clients, $REPLICA_COUNT replicas"
    echo "  load: request=${GEN_INTERVAL_US}us bus=${INTERVAL_MS}ms duration=${DURATION_S}s"
    if [[ "$SCENARIO" == "failure" ]]; then
        echo "  scenario: kill leader at +${FAILURE_AT_S}s; detection timeout=${SUSPECT_TIMEOUT_MS}ms"
    else
        echo "  scenario: normal processing; detection timeout=${SUSPECT_TIMEOUT_MS}ms"
    fi
}

ssh_to() {
    local instance="$1" zone="$2" command="$3"
    gcloud compute ssh "$instance" \
        --tunnel-through-iap \
        --zone="$zone" \
        --project="$PROJECT_ID" \
        --quiet \
        --command="$command"
}

scp_to_dir() {
    local instance="$1" zone="$2" local_replica="$3" local_client="$4" local_conf="$5"
    gcloud compute scp \
        "$local_replica" "$local_client" "$local_conf" "${instance}:${REMOTE_DIR}/" \
        --tunnel-through-iap \
        --zone="$zone" \
        --project="$PROJECT_ID" \
        --quiet
}

scp_from() {
    local instance="$1" zone="$2" remote_path="$3" local_path="$4"
    local attempt
    for attempt in 1 2 3; do
        if gcloud compute scp "${instance}:${remote_path}" "${local_path}.partial" \
            --tunnel-through-iap --zone="$zone" --project="$PROJECT_ID" --compress --scp-flag=-oServerAliveInterval=15 --scp-flag=-oServerAliveCountMax=3 --quiet; then
            mv "${local_path}.partial" "$local_path"
            return 0
        fi
        rm -f "${local_path}.partial"
    done
    return 1
}

ensure_running() {
    local idx pid rc=0
    local pids=()
    for idx in "${HOST_INDICES[@]}"; do
        [[ "${NODE_STATUSES[$idx]}" == "RUNNING" ]] && continue
        echo "  starting ${NODE_VMS[$idx]} (${NODE_ZONES[$idx]})"
        gcloud compute instances start "${NODE_VMS[$idx]}" \
            --zone="${NODE_ZONES[$idx]}" --project="$PROJECT_ID" --quiet &
        pids+=($!)
    done
    if ((${#pids[@]})); then
        for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    fi
    [[ "$rc" == 0 ]] || die "at least one VM failed to start"

    echo "==> Waiting for IAP SSH on configured VMs"
    for idx in "${HOST_INDICES[@]}"; do
        local attempt ready=0
        for attempt in $(seq 1 24); do
            if ssh_to "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" "true" >/dev/null 2>&1; then
                ready=1
                break
            fi
            sleep 5
        done
        [[ "$ready" == 1 ]] || die "IAP SSH did not become ready on ${NODE_VMS[$idx]}"
        echo "  ${NODE_VMS[$idx]} ready"
    done
}

WORK_DIR=""
LOCAL_REPLICA=""
LOCAL_CLIENT=""
LOCAL_CONF=""

prepare_artifacts() {
    command -v go >/dev/null 2>&1 || die "Go is required locally to build the Linux binaries"
    WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/paxosbus-gcp.XXXXXX")"
    LOCAL_REPLICA="$WORK_DIR/paxosbus-replica"
    LOCAL_CLIENT="$WORK_DIR/paxosbus-client"
    LOCAL_CONF="$WORK_DIR/paxosbus.conf"

    echo "==> Building static linux/$GCP_GOARCH binaries from the current checkout"
    (
        cd "$REPO_ROOT"
        CGO_ENABLED=0 GOOS=linux GOARCH="$GCP_GOARCH" go build -trimpath \
            -o "$LOCAL_REPLICA" ./paxosbus/cmd/paxosbus-replica
        CGO_ENABLED=0 GOOS=linux GOARCH="$GCP_GOARCH" go build -trimpath \
            -o "$LOCAL_CLIENT" ./paxosbus/cmd/paxosbus-client
    )

    local idx
    {
        echo "f $(((REPLICA_COUNT - 1) / 2))"
        echo "clients $CLIENT_COUNT"
        echo "schedule-window-ms $SCHEDULE_WINDOW_MS"
        for idx in "${REPLICA_INDICES[@]}"; do
            echo "replica ${NODE_IPS[$idx]}:$((PORT + idx * SINGLE_MACHINE))"
        done
    } > "$LOCAL_CONF"
    echo "==> paxosbus.conf (internal VPC transport)"
    sed 's/^/  /' "$LOCAL_CONF"
}

deploy_artifacts() {
    echo "==> Deploying binaries + config over IAP"
    local idx pid rc=0
    local pids=()
    for idx in "${HOST_INDICES[@]}"; do
        ssh_to "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" \
            "mkdir -p ~/$REMOTE_DIR" >/dev/null
    done
    for idx in "${HOST_INDICES[@]}"; do
        {
            scp_to_dir "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" \
                "$LOCAL_REPLICA" "$LOCAL_CLIENT" "$LOCAL_CONF"
            ssh_to "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" \
                "set -e; chmod +x ~/$REMOTE_DIR/paxosbus-replica ~/$REMOTE_DIR/paxosbus-client; cp ~/$REMOTE_DIR/paxosbus.conf $REMOTE_CONF"
            echo "  deployed ${NODE_VMS[$idx]}"
        } &
        pids+=($!)
    done
    if ((${#pids[@]})); then
        for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    fi
    [[ "$rc" == 0 ]] || die "binary/config deployment failed on at least one VM"
}

LOCAL_BG_PIDS=()
STARTED_REMOTE=0
RUN_ACTIVE=0
LOGS_COLLECTED=0
RUN_LOG_DIR=""
KILL_INFO_FILE=""
FAILURE_PID=""
STOP_VMS_ON_EXIT=0

stop_remote() {
    echo "==> Stopping replicas and clients (logs kept on the VMs)"
    local idx pid
    local pids=()
    for idx in "${HOST_INDICES[@]}"; do
        ssh_to "${NODE_VMS[$idx]}" "${NODE_ZONES[$idx]}" \
            "pkill -f '[p]axosbus-replica' 2>/dev/null || true; pkill -f '[p]axosbus-client' 2>/dev/null || true" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    STARTED_REMOTE=0
}

stop_configured_vms() {
    echo "==> Stopping configured GCP VMs"
    local idx pid rc=0
    local pids=()
    for idx in "${HOST_INDICES[@]}"; do
        gcloud compute instances stop "${NODE_VMS[$idx]}" \
            --zone="${NODE_ZONES[$idx]}" \
            --project="$PROJECT_ID" \
            --quiet &
        pids+=($!)
    done
    if ((${#pids[@]})); then
        for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    fi
    if [[ "$rc" != 0 ]]; then
        echo "  WARN: at least one VM failed to stop" >&2
        return 1
    fi
    STOP_VMS_ON_EXIT=0
    echo "  all experiment VMs stopped"
}

initialize_run_dir() {
    local name ts git_dirty git_commit main_dir
    ts="$(date +%Y%m%d-%H%M%S)"
    name="gcp-run-$ts"
    main_dir="$(dirname "$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir)")"
    [[ "$main_dir/" != */.codex/* ]] || die "experiment logs must be outside .codex"
    RUN_LOG_DIR="$main_dir/paxosbus/logs/gcp-paxosbus/$name"
    mkdir -p "$(dirname "$RUN_LOG_DIR")"
    mkdir "$RUN_LOG_DIR"
    KILL_INFO_FILE="$RUN_LOG_DIR/kill-info.txt"
    RUN_ACTIVE=1
    git_dirty=0
    git_commit="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]] && git_dirty=1

    {
        echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "git_commit=$git_commit"
        echo "commit=$git_commit"
        echo "git_dirty=$git_dirty"
        echo "implementation=go"
        echo "platform=gcp"
        echo "single_machine=$SINGLE_MACHINE"
        echo "project=$PROJECT_ID"
        echo "gcp_env_file=$ENV_FILE"
        echo "gcp_goarch=$GCP_GOARCH"
        echo "transport=gcp-internal-vpc"
        echo "ssh_transport=iap"
        echo "scenario=$SCENARIO"
        echo "interval_ms=$INTERVAL_MS"
        echo "command_size=$COMMAND_SIZE"
        echo "gen_interval_us=$GEN_INTERVAL_US"
        echo "duration_s=$DURATION_S"
        echo "total_clients=$CLIENT_COUNT"
        echo "num_replicas=$REPLICA_COUNT"
        echo "start_delay_ms=$START_DELAY_MS"
        echo "schedule_window_ms=$SCHEDULE_WINDOW_MS"
        echo "resend_ms=$RESEND_MS"
        echo "failure_at_s=$([[ "$SCENARIO" == failure ]] && echo "$FAILURE_AT_S" || echo 0)"
        echo "kill_leader_at_s=$([[ "$SCENARIO" == failure ]] && echo "$FAILURE_AT_S" || echo 0)"
        echo "suspect_timeout_ms=$SUSPECT_TIMEOUT_MS"
        echo "sync_interval_ms=$SYNC_INTERVAL_MS"
        echo "view_change_timeout_ms=$VIEW_CHANGE_TIMEOUT_MS"
        echo "view_change_fallback_timeout_ms=$VIEW_CHANGE_FALLBACK_TIMEOUT_MS"
        echo "gap_delta_ms=$GAP_DELTA_MS"
        echo "gap_retry_timeout_ms=$GAP_RETRY_TIMEOUT_MS"
        echo "drop_mode=$DROP_MODE"
        echo "drop_every=$DROP_EVERY"
        echo "retain_slots=$RETAIN_SLOTS"
        echo "retain_mb=$RETAIN_MB"
        echo "collect_durable=1"
        echo "stop_vms_after_run=1"
        echo "leader_idx=0"
        echo "leader_label=${NODE_LABELS[0]}"
        local idx
        for idx in "${!NODE_VMS[@]}"; do
            echo "region_${NODE_KEYS[$idx]}_vm=${NODE_VMS[$idx]}"
            echo "region_${NODE_KEYS[$idx]}_zone=${NODE_ZONES[$idx]}"
            echo "region_${NODE_KEYS[$idx]}_ip=${NODE_IPS[$idx]}"
            echo "region_${NODE_KEYS[$idx]}_client_ids=$(client_ids_for_node "$idx")"
        done
        echo "replicas=${NODE_LABELS[*]:0:$REPLICA_COUNT}"
        if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" != "0" ]]; then
            echo "mode=drop-$DROP_MODE"
        else
            echo "mode=$SCENARIO"
        fi
    } > "$RUN_LOG_DIR/run-meta.txt"
    cp "$LOCAL_CONF" "$RUN_LOG_DIR/paxosbus.conf"
    echo "==> Run logs will be collected into $RUN_LOG_DIR"
}

launch_replicas() {
    echo "==> Launching $REPLICA_COUNT replicas (replica 0 is the initial leader)"
    local node_idx ids pid rc=0
    local pids=() launch_indices=("${REPLICA_INDICES[@]}")
    if [[ "$SINGLE_MACHINE" == 1 ]]; then launch_indices=(0); fi
    STARTED_REMOTE=1
    for node_idx in "${launch_indices[@]}"; do
        ids="$node_idx"
        if [[ "$SINGLE_MACHINE" == 1 ]]; then ids="${REPLICA_INDICES[*]}"; fi
        ssh_to "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" "
            for ridx in $ids; do
                rm -f /tmp/paxosbus-replica-\$ridx.log
                rm -rf /tmp/paxosbus-durable-\$ridx && mkdir -p /tmp/paxosbus-durable-\$ridx
                nohup ~/$REMOTE_DIR/paxosbus-replica \\
                  -c $REMOTE_CONF -i \$ridx -l ${NODE_LABELS[$node_idx]} -d /tmp/paxosbus-durable-\$ridx \\
                  -drop-mode $DROP_MODE -drop-every $DROP_EVERY \\
                  -sync-interval-ms $SYNC_INTERVAL_MS -suspect-timeout-ms $SUSPECT_TIMEOUT_MS \\
                  -view-change-timeout-ms $VIEW_CHANGE_TIMEOUT_MS \\
                  -view-change-fallback-timeout-ms $VIEW_CHANGE_FALLBACK_TIMEOUT_MS \\
                  -gap-delta-ms $GAP_DELTA_MS -gap-retry-timeout-ms $GAP_RETRY_TIMEOUT_MS \\
                  -retain-slots $RETAIN_SLOTS -retain-mb $RETAIN_MB \\
                  </dev/null >/tmp/paxosbus-replica-\$ridx.log 2>&1 &
                echo \$! >/tmp/paxosbus-replica-\$ridx.pid
            done
            sleep 1
            failed=0
            for ridx in $ids; do
                if kill -0 \$(cat /tmp/paxosbus-replica-\$ridx.pid) 2>/dev/null; then
                    echo \"  [replica \$ridx @ ${NODE_LABELS[$node_idx]}] running\"
                else
                    echo \"  [replica \$ridx] NOT RUNNING\"
                    cat /tmp/paxosbus-replica-\$ridx.log 2>/dev/null || true
                    failed=1
                fi
            done
            exit \$failed" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    [[ "$rc" == 0 ]] || die "at least one replica failed to launch"
    sleep 3
}

launch_clients() {
    echo "==> Launching $CLIENT_COUNT clients (single-machine=$SINGLE_MACHINE)"
    local node_idx ids pid rc=0
    local pids=() launch_indices=("${CLIENT_INDICES[@]}")
    if [[ "$SINGLE_MACHINE" == 1 ]]; then launch_indices=("${CLIENT_INDICES[0]}"); fi
    SCHEDULE_START_NS="$(python3 -c 'import sys,time; w=int(sys.argv[1])*1000000; print((time.time_ns()//w+max(4,(30000000000+w-1)//w+1))*w)' "$SCHEDULE_WINDOW_MS")"
    echo "schedule_start_ns=$SCHEDULE_START_NS" >> "$RUN_LOG_DIR/run-meta.txt"
    for node_idx in "${launch_indices[@]}"; do
        ids="$(client_ids_for_node "$node_idx")"
        if [[ "$SINGLE_MACHINE" == 1 ]]; then ids="\$(seq 0 $((CLIENT_COUNT - 1)))"; fi
        ssh_to "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" "
            for id in $ids; do
                rm -f /tmp/paxosbus-client-\$id.log
                nohup ~/$REMOTE_DIR/paxosbus-client \\
                  -c $REMOTE_CONF -I \$id -p $INTERVAL_MS -g $GEN_INTERVAL_US -command-size $COMMAND_SIZE \\
                  -t $RESEND_MS -w $START_DELAY_MS -l ${NODE_LABELS[$node_idx]} -schedule-start-ns $SCHEDULE_START_NS \\
                  </dev/null >/tmp/paxosbus-client-\$id.log 2>&1 &
                echo \$! >/tmp/paxosbus-client-\$id.pid
            done
            sleep 1
            failed=0
            for id in $ids; do
                if pgrep -f \"[p]axosbus-client.*-I \$id -p\" >/dev/null; then
                    echo \"  [client \$id @ ${NODE_LABELS[$node_idx]}] running\"
                else
                    echo \"  [client \$id @ ${NODE_LABELS[$node_idx]}] NOT RUNNING\"
                    cat /tmp/paxosbus-client-\$id.log 2>/dev/null || true
                    failed=1
                fi
            done
            exit \$failed" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    [[ "$rc" == 0 ]] || die "at least one client failed to launch"
}

schedule_leader_failure() {
    local target_ns=$((SCHEDULE_START_NS + FAILURE_AT_S * 1000000000))
    (
        local output
        output="$(ssh_to "${NODE_VMS[0]}" "${NODE_ZONES[0]}" "
            set -e
            delay=\$(awk -v target=$target_ns -v now=\$(date +%s%N) 'BEGIN { seconds = (target - now) / 1000000000; printf \"%.6f\", (seconds > 0 ? seconds : 0) }')
            sleep \"\$delay\"
            kill -9 \"\$(cat /tmp/paxosbus-replica-0.pid)\"
            date +%s.%N")" || exit 1
        {
            echo "kill_leader_idx=0"
            echo "kill_leader_label=${NODE_LABELS[0]}"
            echo "kill_leader_host=${NODE_VMS[0]}"
            echo "kill_at_data_phase_s=$FAILURE_AT_S"
            echo "detection_time_ms=$SUSPECT_TIMEOUT_MS"
            echo "kill_wall_local=$(date +%s.%N)"
            echo "kill_wall_leader=$output"
        } > "$KILL_INFO_FILE"
        echo ""
        echo "══════ KILLED leader replica 0 (${NODE_LABELS[0]}) at +${FAILURE_AT_S}s of data phase ══════"
    ) &
    FAILURE_PID=$!
    LOCAL_BG_PIDS+=("$FAILURE_PID")
}

tail_for_run() {
    local warmup_s
    warmup_s="$(python3 -c 'import sys,time; print(max(0,(int(sys.argv[1])-time.time_ns()+999999999)//1000000000))' "$SCHEDULE_START_NS")"
    local run_for=$((warmup_s + DURATION_S + 6))
    local node_idx ridx first_id pid
    local tail_pids=()

    if [[ "$SCENARIO" == "failure" ]]; then
        echo "==> Failure armed: kill leader at +${FAILURE_AT_S}s; detection=${SUSPECT_TIMEOUT_MS}ms"
        schedule_leader_failure
    fi
    echo "==> Live tail for ${run_for}s (${warmup_s}s sync + ${DURATION_S}s data + slack)"
    echo "----------------------------------------------------------------"
    if [[ "$SINGLE_MACHINE" == 1 ]]; then
        local log_files=""
        for node_idx in "${REPLICA_INDICES[@]}"; do
            log_files+=" /tmp/paxosbus-replica-$node_idx.log"
        done
        for node_idx in "${CLIENT_INDICES[@]}"; do
            log_files+=" /tmp/paxosbus-client-$(client_ids_for_node "$node_idx").log"
        done
        ssh_to "${NODE_VMS[0]}" "${NODE_ZONES[0]}" \
            "tail -q -n +1 -F $log_files | grep --line-buffered -E '1s:|data phase|SUSPECT|VIEW|PANIC'" &
        tail_pids+=($!)
    else
        for node_idx in "${REPLICA_INDICES[@]}"; do
            ridx="$(replica_index_for_node "$node_idx")"
            ssh_to "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" \
                "tail -n +1 -F /tmp/paxosbus-replica-$ridx.log | grep --line-buffered -E '1s:|SUSPECT|VIEW|PANIC' | sed -u 's/^/[r$ridx] /'" &
            tail_pids+=($!)
        done
        for node_idx in "${CLIENT_INDICES[@]}"; do
            first_id="$(client_ids_for_node "$node_idx")"
            ssh_to "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" \
                "tail -n +1 -F /tmp/paxosbus-client-$first_id.log | grep --line-buffered -E '1s:|data phase|PANIC' | sed -u 's/^/[c$first_id] /'" &
            tail_pids+=($!)
        done
    fi
    LOCAL_BG_PIDS+=("${tail_pids[@]}")

    sleep "$run_for"
    for pid in "${tail_pids[@]}"; do kill_tree "$pid"; done
    for pid in "${tail_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    if [[ -n "$FAILURE_PID" ]]; then
        wait "$FAILURE_PID" || die "leader failure injection failed"
        FAILURE_PID=""
    fi
    LOCAL_BG_PIDS=()
    echo "----------------------------------------------------------------"
}

collect_logs() {
    [[ -n "$RUN_LOG_DIR" ]] || return 0
    echo "==> Copying replica + client logs to $RUN_LOG_DIR"
    local node_idx ridx id pid file
    local pids=()
    for node_idx in "${!NODE_VMS[@]}"; do
        (
            if ridx="$(replica_index_for_node "$node_idx")"; then
                scp_from "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" \
                    "/tmp/paxosbus-replica-$ridx.log" "$RUN_LOG_DIR/replica-$ridx.log" \
                    || echo "  WARN: missing replica-$ridx log on ${NODE_VMS[$node_idx]}"
            fi
            for id in $(client_ids_for_node "$node_idx"); do
                scp_from "${NODE_VMS[$node_idx]}" "${NODE_ZONES[$node_idx]}" \
                    "/tmp/paxosbus-client-$id.log" "$RUN_LOG_DIR/paxosbus-client-$id.log" \
                    || echo "  WARN: missing client-$id log on ${NODE_VMS[$node_idx]}"
            done
        ) &
        pids+=($!)
        if [[ "$SINGLE_MACHINE" == 1 ]]; then wait "$!" || true; fi
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done

    echo "==> Copying durable replica logs"
    mkdir -p "$RUN_LOG_DIR/durable"
    pids=()
    local durable_rc=0
    for node_idx in "${REPLICA_INDICES[@]}"; do
        gcloud compute scp "${NODE_VMS[$node_idx]}:/tmp/paxosbus-durable-$node_idx" \
            "$RUN_LOG_DIR/durable/replica-$node_idx" \
            --tunnel-through-iap --zone="${NODE_ZONES[$node_idx]}" \
            --project="$PROJECT_ID" --recurse --compress --quiet &
        pids+=($!)
        if [[ "$SINGLE_MACHINE" == 1 ]]; then wait "$!" || durable_rc=1; fi
    done
    for pid in "${pids[@]}"; do wait "$pid" || durable_rc=1; done
    for node_idx in "${REPLICA_INDICES[@]}"; do
        for file in replica.log requestlist.log; do
            [[ -f "$RUN_LOG_DIR/durable/replica-$node_idx/$file" ]] || durable_rc=1
        done
    done
    local want=$((REPLICA_COUNT + CLIENT_COUNT)) got
    got=0
    for file in "$RUN_LOG_DIR"/replica-*.log "$RUN_LOG_DIR"/paxosbus-client-*.log; do
        if [[ -f "$file" ]]; then got=$((got + 1)); fi
    done
    if [[ "$got" == "$want" ]]; then
        echo "  collected $got/$want log files into $RUN_LOG_DIR"
    else
        LOGS_COLLECTED=1
        echo "ERROR: collected only $got/$want log files into $RUN_LOG_DIR" >&2
        return 1
    fi

    LOGS_COLLECTED=1
    if [[ "$durable_rc" != 0 ]]; then
        echo "ERROR: durable replica log collection is incomplete" >&2
        return 1
    fi
}

kill_tree() {
    local pid="$1" child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        kill_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
}

reap_local_children() {
    local joblist pid
    joblist="$(jobs -p 2>/dev/null || true)"
    [[ -n "$joblist" ]] || return 0
    while read -r pid; do
        [[ -n "$pid" ]] || continue
        kill_tree "$pid"
    done <<< "$joblist"
}

cleanup() {
    local ec=$? pid
    trap - EXIT INT TERM

    if ((${#LOCAL_BG_PIDS[@]})); then
        for pid in "${LOCAL_BG_PIDS[@]}"; do kill_tree "$pid"; done
        LOCAL_BG_PIDS=()
    fi

    if [[ "$STARTED_REMOTE" == 1 ]]; then
        stop_remote || true
    fi

    if [[ "$RUN_ACTIVE" == 1 && "$LOGS_COLLECTED" == 0 ]]; then
        echo "==> Attempting to preserve partial logs after an interrupted/failed run"
        collect_logs || true
    fi

    if [[ "$STOP_VMS_ON_EXIT" == 1 && "$ec" == 0 ]]; then
        stop_configured_vms || true
    fi

    reap_local_children

    if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
        rm -rf -- "$WORK_DIR"
    fi
    exit "$ec"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

discover_instances
print_topology

STOP_VMS_ON_EXIT=1
ensure_running
prepare_artifacts
stop_remote
deploy_artifacts
initialize_run_dir
launch_replicas
launch_clients
tail_for_run
stop_remote
collect_logs
stop_configured_vms

echo "==> Done. VMs stopped. Logs: $RUN_LOG_DIR"
