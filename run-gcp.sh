#!/usr/bin/env bash
set -euo pipefail

# PaxosBus WAN runner for the four GCP region VMs.
#
# This script runs on the operator's laptop. Every management connection uses:
#
#   gcloud compute ssh <INSTANCE_NAME> --tunnel-through-iap \
#     --zone=<ZONE> --project=<PROJECT_ID>
#
# Project ID, VM names/zones, protocol port, and VM architecture are loaded
# from .env next to this script (override with PAXOSBUS_GCP_ENV_FILE). PaxosBus
# traffic itself uses the VMs' internal VPC addresses. There is no controller/
# jump VM. The configured regional topology is:
#
#   US             replica 0 (initial leader) + clients
#   Europe         replica 1                  + clients
#   Asia                                      + clients
#   South America  replica 2                  + clients
#
# By default, two clients run in each region (8 total). Client IDs are globally
# unique and assigned in the region order above. Only the four VMs named in
# .env are used, so unrelated VMs such as pb-test and pb-test2 are never touched.
# Binaries are built from the current local checkout and copied to all four VMs,
# so a run measures the code that invoked this script.
#
# Examples:
#   ./run-gcp.sh
#   ./run-gcp.sh --clients-per-region 4 --request-interval-us 250
#   ./run-gcp.sh --bus-interval-ms 2 --duration-s 120
#   ./run-gcp.sh --failure --failure-at-s 20 --detection-time-ms 2000
#   ./run-gcp.sh list                    # inventory only; never starts VMs
#   ./run-gcp.sh --dry-run               # print topology/options; never starts VMs
#   ./run-gcp.sh setup                   # start, build, and deploy; do not run
#
# Existing environment names used by run-cloudlab.sh remain supported:
#   GEN_INTERVAL_US INTERVAL_MS DURATION_S CLIENTS_PER_REGION START_DELAY_MS
#   RESEND_MS DROP_MODE DROP_EVERY GAP_RETRY_TIMEOUT_MS KILL_LEADER_AT_S
#   COLLECT_DURABLE RUN_TAG

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${PAXOSBUS_GCP_ENV_FILE:-$SCRIPT_DIR/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: GCP environment file not found: $ENV_FILE" >&2
    echo "Copy $SCRIPT_DIR/.env.example to $SCRIPT_DIR/.env and fill in the GCP values." >&2
    exit 1
fi
# .env is a trusted shell-format configuration file. Export its values as well
# so subprocesses invoked by this runner see the same configuration.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PROJECT_ID="${GCP_PROJECT_ID:-}"
PORT="${PAXOSBUS_PORT:-}"
GCP_GOARCH="${GCP_GOARCH:-}"
GEN_INTERVAL_US="${GEN_INTERVAL_US:-${REQUEST_INTERVAL_US:-500}}"
INTERVAL_MS="${INTERVAL_MS:-${BUS_INTERVAL_MS:-1}}"
DURATION_S="${DURATION_S:-60}"
CLIENTS_PER_REGION="${CLIENTS_PER_REGION:-2}"
START_DELAY_MS="${START_DELAY_MS:-10000}"
RESEND_MS="${RESEND_MS:-2000}"

SCENARIO="${SCENARIO:-normal}"
FAILURE_AT_S="${FAILURE_AT_S:-${KILL_LEADER_AT_S:-20}}"
SUSPECT_TIMEOUT_MS="${SUSPECT_TIMEOUT_MS:-${DETECTION_TIME_MS:-2000}}"
SYNC_INTERVAL_MS="${SYNC_INTERVAL_MS:-100}"
VIEW_CHANGE_TIMEOUT_MS="${VIEW_CHANGE_TIMEOUT_MS:-15000}"
VIEW_CHANGE_FALLBACK_TIMEOUT_MS="${VIEW_CHANGE_FALLBACK_TIMEOUT_MS:-20000}"
GAP_RETRY_TIMEOUT_MS="${GAP_RETRY_TIMEOUT_MS:-1500}"

DROP_MODE="${DROP_MODE:-none}"
DROP_EVERY="${DROP_EVERY:-0}"
RETAIN_SLOTS="${RETAIN_SLOTS:-16384}"
RETAIN_MB="${RETAIN_MB:-256}"
COLLECT_DURABLE="${COLLECT_DURABLE:-0}"
STOP_VMS_AFTER_RUN="${STOP_VMS_AFTER_RUN:-1}"
RUN_TAG="${RUN_TAG:-}"
AUTO_START=1
DRY_RUN=0
SUBCMD=run

# Compatibility: the CloudLab failure knob implies the failure scenario when
# it is explicitly set to a non-zero value.
if [[ -n "${KILL_LEADER_AT_S+x}" && "${KILL_LEADER_AT_S:-0}" != "0" ]]; then
    SCENARIO=failure
fi

REMOTE_DIR="paxosbus-gcp"
REMOTE_CONF="/tmp/paxosbus.conf"

usage() {
    cat <<EOF
Usage: $0 [run|setup|list] [options]

Workload:
  -g, --request-interval-us US    Request generation interval (default: 500)
  -p, --bus-interval-ms MS        Bus generation interval (default: 1)
  -d, --duration-s SECONDS        Data-phase duration (default: 60)
  -c, --clients-per-region N      Clients on each of 4 region VMs (default: 2)
      --start-delay-ms MS         Client sync-to-data delay (default: 10000)
  -t, --resend-ms MS              Client no-quorum resend timeout (default: 2000)

Failure/recovery:
      --normal                    Do not kill a replica (default)
      --failure                   Kill replica 0 during the data phase
      --scenario normal|failure   Explicit scenario selection
  -K, --failure-at-s SECONDS      Kill time within data phase (default: 20)
      --detection-time-ms MS      Missing-heartbeat detection time (default: 2000)
      --sync-interval-ms MS       Leader heartbeat interval (default: 100)
      --view-change-timeout-ms MS New-leader quorum timeout (default: 15000)
      --view-change-fallback-timeout-ms MS (default: 20000)

Gap/recovery and output:
      --drop-mode MODE            none|leader|followers|all (default: none)
      --drop-every N              Artificially drop reqId % N == 0 (default: 0)
      --gap-retry-timeout-ms MS   Gap-commit rebroadcast interval (default: 1500)
      --retain-slots N            In-memory committed-slot window (default: 16384)
      --retain-mb MB              In-memory request-payload cap (default: 256)
      --collect-durable           Copy durable replica logs (can be very large)
      --no-collect-durable        Do not copy durable logs (default)
      --stop-vms                  Stop all 4 VMs after a run (default)
      --keep-vms-running          Leave VMs running after a run
      --run-tag TAG               Append a safe tag to the run directory

GCP/control:
      --project PROJECT           Override GCP_PROJECT_ID from .env
      --port PORT                 Override PAXOSBUS_PORT from .env
      --no-start                  Require VMs to already be RUNNING
      --dry-run                   Discover and print the plan only; start nothing
  -h, --help                      Show this help

Subcommands:
  run    Start/deploy/run/collect (default)
  setup  Start and deploy binaries/config only
  list   Show the four discovered experiment VMs; start nothing

Project configuration:
  $ENV_FILE
  Set PAXOSBUS_GCP_ENV_FILE to load a different file.

Run logs are copied to:
  paxosbus/logs/gcp/gcp-run-YYYYMMDD-HHMMSS[-TAG]/
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

need_arg() {
    [[ $# -ge 2 && -n "$2" ]] || die "$1 needs a value"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        run|setup|list) SUBCMD="$1" ;;
        -g|--request-interval-us)
            need_arg "$1" "${2:-}"; GEN_INTERVAL_US="$2"; shift ;;
        --request-interval-us=*) GEN_INTERVAL_US="${1#*=}" ;;
        -p|--bus-interval-ms)
            need_arg "$1" "${2:-}"; INTERVAL_MS="$2"; shift ;;
        --bus-interval-ms=*) INTERVAL_MS="${1#*=}" ;;
        -d|--duration-s)
            need_arg "$1" "${2:-}"; DURATION_S="$2"; shift ;;
        --duration-s=*) DURATION_S="${1#*=}" ;;
        -c|--clients-per-region)
            need_arg "$1" "${2:-}"; CLIENTS_PER_REGION="$2"; shift ;;
        --clients-per-region=*) CLIENTS_PER_REGION="${1#*=}" ;;
        --start-delay-ms)
            need_arg "$1" "${2:-}"; START_DELAY_MS="$2"; shift ;;
        --start-delay-ms=*) START_DELAY_MS="${1#*=}" ;;
        -t|--resend-ms)
            need_arg "$1" "${2:-}"; RESEND_MS="$2"; shift ;;
        --resend-ms=*) RESEND_MS="${1#*=}" ;;
        --normal) SCENARIO=normal ;;
        --failure) SCENARIO=failure ;;
        --scenario)
            need_arg "$1" "${2:-}"; SCENARIO="$2"; shift ;;
        --scenario=*) SCENARIO="${1#*=}" ;;
        -K|--failure-at-s)
            need_arg "$1" "${2:-}"; FAILURE_AT_S="$2"; SCENARIO=failure; shift ;;
        --failure-at-s=*) FAILURE_AT_S="${1#*=}"; SCENARIO=failure ;;
        --detection-time-ms|--suspect-timeout-ms)
            need_arg "$1" "${2:-}"; SUSPECT_TIMEOUT_MS="$2"; shift ;;
        --detection-time-ms=*|--suspect-timeout-ms=*) SUSPECT_TIMEOUT_MS="${1#*=}" ;;
        --sync-interval-ms)
            need_arg "$1" "${2:-}"; SYNC_INTERVAL_MS="$2"; shift ;;
        --sync-interval-ms=*) SYNC_INTERVAL_MS="${1#*=}" ;;
        --view-change-timeout-ms)
            need_arg "$1" "${2:-}"; VIEW_CHANGE_TIMEOUT_MS="$2"; shift ;;
        --view-change-timeout-ms=*) VIEW_CHANGE_TIMEOUT_MS="${1#*=}" ;;
        --view-change-fallback-timeout-ms)
            need_arg "$1" "${2:-}"; VIEW_CHANGE_FALLBACK_TIMEOUT_MS="$2"; shift ;;
        --view-change-fallback-timeout-ms=*) VIEW_CHANGE_FALLBACK_TIMEOUT_MS="${1#*=}" ;;
        --drop-mode)
            need_arg "$1" "${2:-}"; DROP_MODE="$2"; shift ;;
        --drop-mode=*) DROP_MODE="${1#*=}" ;;
        --drop-every)
            need_arg "$1" "${2:-}"; DROP_EVERY="$2"; shift ;;
        --drop-every=*) DROP_EVERY="${1#*=}" ;;
        --gap-retry-timeout-ms)
            need_arg "$1" "${2:-}"; GAP_RETRY_TIMEOUT_MS="$2"; shift ;;
        --gap-retry-timeout-ms=*) GAP_RETRY_TIMEOUT_MS="${1#*=}" ;;
        --retain-slots)
            need_arg "$1" "${2:-}"; RETAIN_SLOTS="$2"; shift ;;
        --retain-slots=*) RETAIN_SLOTS="${1#*=}" ;;
        --retain-mb)
            need_arg "$1" "${2:-}"; RETAIN_MB="$2"; shift ;;
        --retain-mb=*) RETAIN_MB="${1#*=}" ;;
        --collect-durable) COLLECT_DURABLE=1 ;;
        --no-collect-durable) COLLECT_DURABLE=0 ;;
        --stop-vms) STOP_VMS_AFTER_RUN=1 ;;
        --keep-vms-running) STOP_VMS_AFTER_RUN=0 ;;
        --run-tag)
            need_arg "$1" "${2:-}"; RUN_TAG="$2"; shift ;;
        --run-tag=*) RUN_TAG="${1#*=}" ;;
        --project)
            need_arg "$1" "${2:-}"; PROJECT_ID="$2"; shift ;;
        --project=*) PROJECT_ID="${1#*=}" ;;
        --port)
            need_arg "$1" "${2:-}"; PORT="$2"; shift ;;
        --port=*) PORT="${1#*=}" ;;
        --no-start) AUTO_START=0 ;;
        --dry-run) DRY_RUN=1 ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown argument '$1' (use --help)" ;;
    esac
    shift
done

require_uint() {
    local name="$1" value="$2" allow_zero="${3:-0}"
    [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be an integer (got '$value')"
    if [[ "$allow_zero" == "0" ]] && ((10#$value == 0)); then
        die "$name must be greater than zero"
    fi
}

for config_var in PROJECT_ID PORT GCP_US_VM GCP_US_ZONE \
                  GCP_EUROPE_VM GCP_EUROPE_ZONE GCP_ASIA_VM GCP_ASIA_ZONE \
                  GCP_SOUTHAMERICA_VM GCP_SOUTHAMERICA_ZONE GCP_GOARCH; do
    [[ -n "${!config_var:-}" ]] || \
        die "$config_var must be set in $ENV_FILE"
done

require_uint request-interval-us "$GEN_INTERVAL_US"
require_uint bus-interval-ms "$INTERVAL_MS"
require_uint duration-s "$DURATION_S"
require_uint clients-per-region "$CLIENTS_PER_REGION"
require_uint start-delay-ms "$START_DELAY_MS"
require_uint resend-ms "$RESEND_MS" 1
require_uint failure-at-s "$FAILURE_AT_S" 1
require_uint detection-time-ms "$SUSPECT_TIMEOUT_MS"
require_uint sync-interval-ms "$SYNC_INTERVAL_MS"
require_uint view-change-timeout-ms "$VIEW_CHANGE_TIMEOUT_MS"
require_uint view-change-fallback-timeout-ms "$VIEW_CHANGE_FALLBACK_TIMEOUT_MS"
require_uint drop-every "$DROP_EVERY" 1
require_uint gap-retry-timeout-ms "$GAP_RETRY_TIMEOUT_MS"
require_uint retain-slots "$RETAIN_SLOTS"
require_uint retain-mb "$RETAIN_MB"
require_uint port "$PORT"

((10#$PORT <= 65535)) || die "port must be at most 65535"
[[ "$SCENARIO" =~ ^(normal|failure)$ ]] || die "scenario must be normal or failure"
[[ "$DROP_MODE" =~ ^(none|leader|followers|all)$ ]] || \
    die "drop-mode must be none, leader, followers, or all"
[[ "$COLLECT_DURABLE" =~ ^[01]$ ]] || die "COLLECT_DURABLE must be 0 or 1"
[[ "$STOP_VMS_AFTER_RUN" =~ ^[01]$ ]] || die "STOP_VMS_AFTER_RUN must be 0 or 1"
if [[ "$SCENARIO" == "failure" ]] && ((10#$FAILURE_AT_S >= 10#$DURATION_S)); then
    die "failure-at-s must be earlier than duration-s"
fi
if [[ "$SCENARIO" == "failure" ]] && ((10#$FAILURE_AT_S == 0)); then
    die "failure-at-s must be greater than zero in the failure scenario"
fi
if ((10#$SUSPECT_TIMEOUT_MS <= 10#$SYNC_INTERVAL_MS)); then
    die "detection-time-ms must be greater than sync-interval-ms"
fi
if [[ -n "$RUN_TAG" && ! "$RUN_TAG" =~ ^[A-Za-z0-9._-]+$ ]]; then
    die "run-tag may contain only letters, digits, dot, underscore, and hyphen"
fi
[[ "$GCP_GOARCH" =~ ^(amd64|arm64)$ ]] || die "GCP_GOARCH must be amd64 or arm64"
[[ "$GCP_US_ZONE" == us-* ]] || die "GCP_US_ZONE must be a US zone"
[[ "$GCP_EUROPE_ZONE" == europe-* ]] || die "GCP_EUROPE_ZONE must be a Europe zone"
[[ "$GCP_ASIA_ZONE" == asia-* ]] || die "GCP_ASIA_ZONE must be an Asia zone"
[[ "$GCP_SOUTHAMERICA_ZONE" == southamerica-* ]] || \
    die "GCP_SOUTHAMERICA_ZONE must be a South America zone"

command -v gcloud >/dev/null 2>&1 || die "gcloud is not installed or not on PATH"

# Region order controls the globally unique client ID ranges.
REGION_KEYS=(us europe asia southamerica)
REGION_VMS=("$GCP_US_VM" "$GCP_EUROPE_VM" "$GCP_ASIA_VM" "$GCP_SOUTHAMERICA_VM")
REGION_ZONES=("$GCP_US_ZONE" "$GCP_EUROPE_ZONE" "$GCP_ASIA_ZONE" "$GCP_SOUTHAMERICA_ZONE")
REGION_LABELS=("${GCP_US_ZONE%-*}" "${GCP_EUROPE_ZONE%-*}" \
               "${GCP_ASIA_ZONE%-*}" "${GCP_SOUTHAMERICA_ZONE%-*}")
REGION_STATUSES=("" "" "" "")
REGION_IPS=("" "" "" "")

discover_instances() {
    echo "==> Loading configured GCP instances from $ENV_FILE"
    local idx details status ip
    for idx in 0 1 2 3; do
        details="$(gcloud compute instances describe "${REGION_VMS[$idx]}" \
            --zone="${REGION_ZONES[$idx]}" \
            --project="$PROJECT_ID" \
            --format="value(status,networkInterfaces[0].networkIP)")" \
            || die "could not describe ${REGION_VMS[$idx]} in ${REGION_ZONES[$idx]}"
        read -r status ip <<< "$details"
        [[ -n "$status" ]] || die "${REGION_VMS[$idx]} returned no status"
        [[ -n "$ip" ]] || die "${REGION_VMS[$idx]} has no internal IP"
        REGION_STATUSES[$idx]="$status"
        REGION_IPS[$idx]="$ip"
    done
}

client_ids_for_region() {
    local region_idx="$1" first last id
    first=$((region_idx * CLIENTS_PER_REGION + 1))
    last=$(((region_idx + 1) * CLIENTS_PER_REGION))
    for ((id = first; id <= last; id++)); do
        printf '%s ' "$id"
    done
}

replica_index_for_region() {
    case "$1" in
        0) echo 0 ;;
        1) echo 1 ;;
        3) echo 2 ;;
        *) return 1 ;;
    esac
}

print_topology() {
    local idx ridx role ids
    echo "==> GCP topology"
    for idx in 0 1 2 3; do
        ids="$(client_ids_for_region "$idx")"
        if ridx="$(replica_index_for_region "$idx")"; then
            role="replica $ridx + clients $ids"
            [[ "$ridx" == 0 ]] && role="replica 0 (leader) + clients $ids"
        else
            role="clients $ids(no replica)"
        fi
        printf "  %-14s %-24s %-22s %-10s %s  %s\n" \
            "${REGION_KEYS[$idx]}" "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
            "${REGION_STATUSES[$idx]}" "${REGION_IPS[$idx]}" "$role"
    done
    echo "  total: $((4 * CLIENTS_PER_REGION)) clients, 3 replicas"
    echo "  load: request=${GEN_INTERVAL_US}us bus=${INTERVAL_MS}ms duration=${DURATION_S}s"
    if [[ "$SCENARIO" == "failure" ]]; then
        echo "  scenario: kill leader at +${FAILURE_AT_S}s; detection timeout=${SUSPECT_TIMEOUT_MS}ms"
    else
        echo "  scenario: normal processing; detection timeout=${SUSPECT_TIMEOUT_MS}ms"
    fi
}

# Keep this command shape in one place. All normal management SSH, launch,
# teardown, and live-tail calls go through it.
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
    gcloud compute scp "${instance}:${remote_path}" "$local_path" \
        --tunnel-through-iap \
        --zone="$zone" \
        --project="$PROJECT_ID" \
        --quiet
}

ensure_running() {
    local idx pid rc=0
    local pids=()
    for idx in 0 1 2 3; do
        [[ "${REGION_STATUSES[$idx]}" == "RUNNING" ]] && continue
        if [[ "$AUTO_START" == 0 ]]; then
            die "${REGION_VMS[$idx]} is ${REGION_STATUSES[$idx]}; start it or omit --no-start"
        fi
        echo "  starting ${REGION_VMS[$idx]} (${REGION_ZONES[$idx]})"
        gcloud compute instances start "${REGION_VMS[$idx]}" \
            --zone="${REGION_ZONES[$idx]}" --project="$PROJECT_ID" --quiet &
        pids+=($!)
    done
    if ((${#pids[@]})); then
        for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    fi
    [[ "$rc" == 0 ]] || die "at least one VM failed to start"

    echo "==> Waiting for IAP SSH on all four VMs"
    for idx in 0 1 2 3; do
        local attempt ready=0
        for attempt in $(seq 1 24); do
            if ssh_to "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" "true" >/dev/null 2>&1; then
                ready=1
                break
            fi
            sleep 5
        done
        [[ "$ready" == 1 ]] || die "IAP SSH did not become ready on ${REGION_VMS[$idx]}"
        echo "  ${REGION_VMS[$idx]} ready"
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
        cd "$SCRIPT_DIR"
        CGO_ENABLED=0 GOOS=linux GOARCH="$GCP_GOARCH" go build -trimpath \
            -o "$LOCAL_REPLICA" ./paxosbus/cmd/paxosbus-replica
        CGO_ENABLED=0 GOOS=linux GOARCH="$GCP_GOARCH" go build -trimpath \
            -o "$LOCAL_CLIENT" ./paxosbus/cmd/paxosbus-client
    )

    {
        echo "f 1"
        echo "replica ${REGION_IPS[0]}:$PORT"
        echo "replica ${REGION_IPS[1]}:$PORT"
        echo "replica ${REGION_IPS[3]}:$PORT"
    } > "$LOCAL_CONF"
    echo "==> paxosbus.conf (internal VPC transport)"
    sed 's/^/  /' "$LOCAL_CONF"
}

deploy_artifacts() {
    echo "==> Deploying binaries + config over IAP"
    local idx pid rc=0
    local pids=()
    for idx in 0 1 2 3; do
        ssh_to "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
            "mkdir -p ~/$REMOTE_DIR" >/dev/null
    done
    for idx in 0 1 2 3; do
        {
            scp_to_dir "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
                "$LOCAL_REPLICA" "$LOCAL_CLIENT" "$LOCAL_CONF"
            ssh_to "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
                "chmod +x ~/$REMOTE_DIR/paxosbus-replica ~/$REMOTE_DIR/paxosbus-client; cp ~/$REMOTE_DIR/paxosbus.conf $REMOTE_CONF"
            echo "  deployed ${REGION_VMS[$idx]}"
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
STOP_VMS_ON_EXIT=0

stop_remote() {
    echo "==> Stopping replicas and clients (logs kept on the VMs)"
    local idx pid
    local pids=()
    for idx in 0 1 2 3; do
        ssh_to "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
            "pkill -f '[p]axosbus-replica' 2>/dev/null || true; pkill -f '[p]axosbus-client' 2>/dev/null || true" \
            >/dev/null 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    STARTED_REMOTE=0
}

stop_configured_vms() {
    echo "==> Stopping all four configured GCP VMs"
    local idx pid rc=0
    local pids=()
    for idx in 0 1 2 3; do
        gcloud compute instances stop "${REGION_VMS[$idx]}" \
            --zone="${REGION_ZONES[$idx]}" \
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
    local name ts git_dirty git_commit
    ts="$(date +%Y%m%d-%H%M%S)"
    name="gcp-run-$ts"
    [[ -n "$RUN_TAG" ]] && name="$name-$RUN_TAG"
    RUN_LOG_DIR="$SCRIPT_DIR/paxosbus/logs/gcp/$name"
    mkdir -p "$RUN_LOG_DIR"
    KILL_INFO_FILE="$RUN_LOG_DIR/kill-info.txt"
    RUN_ACTIVE=1
    git_dirty=0
    git_commit="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    [[ -n "$(git -C "$SCRIPT_DIR" status --porcelain 2>/dev/null || true)" ]] && git_dirty=1

    {
        echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "git_commit=$git_commit"
        echo "commit=$git_commit"
        echo "git_dirty=$git_dirty"
        echo "implementation=go"
        echo "platform=gcp"
        echo "project=$PROJECT_ID"
        echo "gcp_env_file=$ENV_FILE"
        echo "gcp_goarch=$GCP_GOARCH"
        echo "transport=gcp-internal-vpc"
        echo "ssh_transport=iap"
        echo "scenario=$SCENARIO"
        echo "interval_ms=$INTERVAL_MS"
        echo "gen_interval_us=$GEN_INTERVAL_US"
        echo "duration_s=$DURATION_S"
        echo "total_clients=$((4 * CLIENTS_PER_REGION))"
        echo "clients_per_region=$CLIENTS_PER_REGION"
        echo "start_delay_ms=$START_DELAY_MS"
        echo "resend_ms=$RESEND_MS"
        echo "failure_at_s=$([[ "$SCENARIO" == failure ]] && echo "$FAILURE_AT_S" || echo 0)"
        echo "kill_leader_at_s=$([[ "$SCENARIO" == failure ]] && echo "$FAILURE_AT_S" || echo 0)"
        echo "suspect_timeout_ms=$SUSPECT_TIMEOUT_MS"
        echo "sync_interval_ms=$SYNC_INTERVAL_MS"
        echo "view_change_timeout_ms=$VIEW_CHANGE_TIMEOUT_MS"
        echo "view_change_fallback_timeout_ms=$VIEW_CHANGE_FALLBACK_TIMEOUT_MS"
        echo "gap_retry_timeout_ms=$GAP_RETRY_TIMEOUT_MS"
        echo "drop_mode=$DROP_MODE"
        echo "drop_every=$DROP_EVERY"
        echo "retain_slots=$RETAIN_SLOTS"
        echo "retain_mb=$RETAIN_MB"
        echo "collect_durable=$COLLECT_DURABLE"
        echo "stop_vms_after_run=$STOP_VMS_AFTER_RUN"
        echo "leader_idx=0"
        echo "leader_label=${REGION_LABELS[0]}"
        local idx
        for idx in 0 1 2 3; do
            echo "region_${REGION_KEYS[$idx]}_vm=${REGION_VMS[$idx]}"
            echo "region_${REGION_KEYS[$idx]}_zone=${REGION_ZONES[$idx]}"
            echo "region_${REGION_KEYS[$idx]}_ip=${REGION_IPS[$idx]}"
            echo "region_${REGION_KEYS[$idx]}_client_ids=$(client_ids_for_region "$idx")"
        done
        echo "replicas=${REGION_LABELS[0]} ${REGION_LABELS[1]} ${REGION_LABELS[3]}"
        if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" != "0" ]]; then
            echo "mode=drop-$DROP_MODE"
        else
            echo "mode=$SCENARIO"
        fi
    } > "$RUN_LOG_DIR/run-meta.txt"
    echo "==> Run logs will be collected into $RUN_LOG_DIR"
}

kill_stale_processes() {
    echo "==> Killing stale PaxosBus processes on all VMs"
    local idx pid
    local pids=()
    for idx in 0 1 2 3; do
        ssh_to "${REGION_VMS[$idx]}" "${REGION_ZONES[$idx]}" \
            "pkill -f '[p]axosbus-replica' 2>/dev/null || true; pkill -f '[p]axosbus-client' 2>/dev/null || true" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done
}

launch_replicas() {
    echo "==> Launching 3 replicas (US leader, Europe, South America)"
    local region_idx ridx pid rc=0
    local pids=()
    STARTED_REMOTE=1
    for region_idx in 0 1 3; do
        ridx="$(replica_index_for_region "$region_idx")"
        ssh_to "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" "
            rm -f /tmp/paxosbus-replica-$ridx.log
            rm -rf /tmp/paxosbus-durable && mkdir -p /tmp/paxosbus-durable
            nohup ~/$REMOTE_DIR/paxosbus-replica \\
              -c $REMOTE_CONF -i $ridx -l ${REGION_LABELS[$region_idx]} -d /tmp/paxosbus-durable \\
              -drop-mode $DROP_MODE -drop-every $DROP_EVERY \\
              -sync-interval-ms $SYNC_INTERVAL_MS -suspect-timeout-ms $SUSPECT_TIMEOUT_MS \\
              -view-change-timeout-ms $VIEW_CHANGE_TIMEOUT_MS \\
              -view-change-fallback-timeout-ms $VIEW_CHANGE_FALLBACK_TIMEOUT_MS \\
              -gap-retry-timeout-ms $GAP_RETRY_TIMEOUT_MS \\
              -retain-slots $RETAIN_SLOTS -retain-mb $RETAIN_MB \\
              </dev/null >/tmp/paxosbus-replica-$ridx.log 2>&1 &
            echo \$! >/tmp/paxosbus-replica-$ridx.pid
            sleep 1
            if pgrep -f '[p]axosbus-replica' >/dev/null; then
                echo '  [replica $ridx @ ${REGION_LABELS[$region_idx]}] running'
            else
                echo '  [replica $ridx] NOT RUNNING'
                cat /tmp/paxosbus-replica-$ridx.log 2>/dev/null || true
                exit 1
            fi" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    [[ "$rc" == 0 ]] || die "at least one replica failed to launch"
    sleep 3
}

launch_clients() {
    local total=$((4 * CLIENTS_PER_REGION))
    echo "==> Launching $total clients ($CLIENTS_PER_REGION per region; one IAP SSH per host)"
    local region_idx ids pid rc=0
    local pids=()
    for region_idx in 0 1 2 3; do
        ids="$(client_ids_for_region "$region_idx")"
        ssh_to "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" "
            for id in $ids; do
                rm -f /tmp/paxosbus-client-\$id.log
                nohup ~/$REMOTE_DIR/paxosbus-client \\
                  -c $REMOTE_CONF -I \$id -p $INTERVAL_MS -g $GEN_INTERVAL_US \\
                  -t $RESEND_MS -w $START_DELAY_MS -l ${REGION_LABELS[$region_idx]} \\
                  </dev/null >/tmp/paxosbus-client-\$id.log 2>&1 &
                echo \$! >/tmp/paxosbus-client-\$id.pid
            done
            sleep 1
            failed=0
            for id in $ids; do
                if pgrep -f \"[p]axosbus-client.*-I \$id -p\" >/dev/null; then
                    echo \"  [client \$id @ ${REGION_LABELS[$region_idx]}] running\"
                else
                    echo \"  [client \$id @ ${REGION_LABELS[$region_idx]}] NOT RUNNING\"
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
    local wait_s=$(((START_DELAY_MS + 999) / 1000 + FAILURE_AT_S))
    (
        sleep "$wait_s"
        local output remote_ts
        output="$(ssh_to "${REGION_VMS[0]}" "${REGION_ZONES[0]}" \
            "date +%s.%N; pkill -9 -f '[p]axosbus-replica' 2>/dev/null || true" 2>/dev/null || true)"
        remote_ts="${output%%$'\n'*}"
        {
            echo "kill_leader_idx=0"
            echo "kill_leader_label=${REGION_LABELS[0]}"
            echo "kill_leader_host=${REGION_VMS[0]}"
            echo "kill_at_data_phase_s=$FAILURE_AT_S"
            echo "detection_time_ms=$SUSPECT_TIMEOUT_MS"
            echo "kill_wall_local=$(date +%s.%N)"
            echo "kill_wall_leader=$remote_ts"
        } > "$KILL_INFO_FILE"
        echo ""
        echo "══════ KILLED leader replica 0 (${REGION_LABELS[0]}) at +${FAILURE_AT_S}s of data phase ══════"
    ) &
    LOCAL_BG_PIDS+=($!)
}

tail_for_run() {
    local warmup_s=$(((START_DELAY_MS + 999) / 1000))
    local run_for=$((warmup_s + DURATION_S + 6))
    local region_idx ridx first_id pid
    local tail_pids=()

    if [[ "$SCENARIO" == "failure" ]]; then
        echo "==> Failure armed: kill leader at +${FAILURE_AT_S}s; detection=${SUSPECT_TIMEOUT_MS}ms"
        schedule_leader_failure
    fi
    echo "==> Live tail for ${run_for}s (${warmup_s}s sync + ${DURATION_S}s data + slack)"
    echo "----------------------------------------------------------------"
    for region_idx in 0 1 3; do
        ridx="$(replica_index_for_region "$region_idx")"
        ssh_to "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" \
            "tail -n +1 -F /tmp/paxosbus-replica-$ridx.log | sed -u 's/^/[r$ridx] /'" &
        tail_pids+=($!)
    done
    for region_idx in 0 1 2 3; do
        first_id=$((region_idx * CLIENTS_PER_REGION + 1))
        ssh_to "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" \
            "tail -n +1 -F /tmp/paxosbus-client-$first_id.log | sed -u 's/^/[c$first_id] /'" &
        tail_pids+=($!)
    done
    LOCAL_BG_PIDS+=("${tail_pids[@]}")

    sleep "$run_for"
    for pid in "${tail_pids[@]}"; do kill_tree "$pid"; done
    for pid in "${tail_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    # The registered tails (and, in failure mode, the scheduled kill) are done;
    # do not leave their PIDs around long enough for the OS to reuse them.
    LOCAL_BG_PIDS=()
    echo "----------------------------------------------------------------"
}

collect_logs() {
    [[ -n "$RUN_LOG_DIR" ]] || return 0
    echo "==> Copying replica + client logs to $RUN_LOG_DIR"
    local region_idx ridx id pid
    local pids=()
    for region_idx in 0 1 2 3; do
        (
            if ridx="$(replica_index_for_region "$region_idx")"; then
                scp_from "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" \
                    "/tmp/paxosbus-replica-$ridx.log" "$RUN_LOG_DIR/replica-$ridx.log" \
                    || echo "  WARN: missing replica-$ridx log on ${REGION_VMS[$region_idx]}"
            fi
            for id in $(client_ids_for_region "$region_idx"); do
                scp_from "${REGION_VMS[$region_idx]}" "${REGION_ZONES[$region_idx]}" \
                    "/tmp/paxosbus-client-$id.log" "$RUN_LOG_DIR/paxosbus-client-$id.log" \
                    || echo "  WARN: missing client-$id log on ${REGION_VMS[$region_idx]}"
            done
        ) &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" || true; done

    if [[ "$COLLECT_DURABLE" == 1 ]]; then
        echo "==> Copying durable replica logs (this can be large)"
        mkdir -p "$RUN_LOG_DIR/durable"
        pids=()
        for region_idx in 0 1 3; do
            ridx="$(replica_index_for_region "$region_idx")"
            gcloud compute scp "${REGION_VMS[$region_idx]}:/tmp/paxosbus-durable" \
                "$RUN_LOG_DIR/durable/replica-$ridx" \
                --tunnel-through-iap --zone="${REGION_ZONES[$region_idx]}" \
                --project="$PROJECT_ID" --recurse --compress --quiet \
                || echo "  WARN: missing durable logs on ${REGION_VMS[$region_idx]}" &
            pids+=($!)
        done
        for pid in "${pids[@]}"; do wait "$pid" || true; done
    fi
    # Report against the expected file count rather than assuming the copies
    # worked: an scp that failed above only printed a WARN, and a run directory
    # silently missing a client is a dataset that looks complete but is not.
    local want=$((3 + 4 * CLIENTS_PER_REGION)) got
    got=$(ls "$RUN_LOG_DIR"/replica-*.log "$RUN_LOG_DIR"/paxosbus-client-*.log \
        2>/dev/null | wc -l | tr -d ' ')
    if [[ "$got" == "$want" ]]; then
        echo "  collected $got/$want log files into $RUN_LOG_DIR"
    else
        echo "  WARN: collected only $got/$want log files into $RUN_LOG_DIR" >&2
    fi

    LOGS_COLLECTED=1
    if [[ -n "${RUN_DIR_POINTER:-}" ]]; then
        echo "$RUN_LOG_DIR" > "$RUN_DIR_POINTER"
    fi
}

analyze_run() {
    echo ""
    python3 "$SCRIPT_DIR/cloudlab/aggregate-stats.py" "$RUN_LOG_DIR" \
        | tee "$RUN_LOG_DIR/metrics.txt" || echo "  WARN: aggregate-stats.py failed"
    if [[ "$SCENARIO" == "failure" ]]; then
        echo ""
        python3 "$SCRIPT_DIR/cloudlab/aggregate-viewchange-stats.py" "$RUN_LOG_DIR" \
            | tee "$RUN_LOG_DIR/viewchange-metrics.txt" \
            || echo "  WARN: aggregate-viewchange-stats.py failed"
    fi
}

# `gcloud` execs python, which forks its own ssh (and, with
# --tunnel-through-iap, a tunnel helper) as a CHILD. Killing just the PID we
# backgrounded therefore orphans that child and leaves a stray ssh running on
# the laptop after the script exits, so always take the whole descendant tree.
kill_tree() {
    local pid="$1" child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        kill_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
}

# Backstop for everything kill_tree is never told about. Only the live tails and
# the scheduled leader kill register in LOCAL_BG_PIDS; the parallel start,
# deploy, launch, stop, and collect fan-outs do not, because they are waited on
# inline. An interrupt DURING one of those waits would otherwise leak a gcloud
# process. Every one of them is a direct background child of this shell, and
# `jobs -p` is a builtin, so it lists exactly those without forking a helper
# that could list itself.
reap_local_children() {
    local joblist pid
    joblist="$(jobs -p 2>/dev/null || true)"
    [[ -n "$joblist" ]] || return 0
    while read -r pid; do
        [[ -n "$pid" ]] || continue
        kill_tree "$pid"
    done <<< "$joblist"
}

# Runs on EVERY exit: success, error, and Ctrl-C. The order matters — remote
# processes stop before the logs are copied, and the logs are copied before
# anything stops the VMs they live on.
cleanup() {
    local ec=$? pid
    trap - EXIT INT TERM

    # 1. Silence the live tails so the rest of the teardown is readable.
    if ((${#LOCAL_BG_PIDS[@]})); then
        for pid in "${LOCAL_BG_PIDS[@]}"; do kill_tree "$pid"; done
        LOCAL_BG_PIDS=()
    fi

    # 2. Stop the replicas/clients. Logs are left on the VMs for step 3.
    if [[ "$STARTED_REMOTE" == 1 ]]; then
        stop_remote || true
    fi

    # 3. Copy the logs down while the VMs are still up.
    if [[ "$RUN_ACTIVE" == 1 && "$LOGS_COLLECTED" == 0 ]]; then
        echo "==> Attempting to preserve partial logs after an interrupted/failed run"
        collect_logs || true
    fi

    # 4. Only now is it safe to shut the VMs down.
    if [[ "$STOP_VMS_ON_EXIT" == 1 ]]; then
        stop_configured_vms || true
    fi

    # 5. Nothing above needs to spawn a subprocess any more, so sweep up every
    #    background job still alive, registered or not.
    reap_local_children

    if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
        rm -rf -- "$WORK_DIR"
    fi
    exit "$ec"
}
trap cleanup EXIT INT TERM

discover_instances
print_topology

if [[ "$SUBCMD" == list || "$DRY_RUN" == 1 ]]; then
    [[ "$DRY_RUN" == 1 ]] && echo "==> Dry run complete; no VM was started or changed"
    exit 0
fi

if [[ "$SUBCMD" == run && "$STOP_VMS_AFTER_RUN" == 1 ]]; then
    # Arm this before startup so a failed or interrupted run does not leave
    # chargeable experiment VMs behind. setup intentionally leaves VMs running.
    STOP_VMS_ON_EXIT=1
fi

ensure_running
prepare_artifacts
deploy_artifacts

if [[ "$SUBCMD" == setup ]]; then
    echo "==> Setup complete; no PaxosBus process was launched"
    exit 0
fi

initialize_run_dir
kill_stale_processes
launch_replicas
launch_clients
tail_for_run
stop_remote
collect_logs
analyze_run
if [[ "$STOP_VMS_AFTER_RUN" == 1 ]]; then
    stop_configured_vms
fi

echo ""
if [[ "$STOP_VMS_AFTER_RUN" == 1 ]]; then
    echo "==> Done. VMs stopped. Logs: $RUN_LOG_DIR"
else
    echo "==> Done. VMs left running. Logs: $RUN_LOG_DIR"
fi
