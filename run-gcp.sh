#!/usr/bin/env bash
set -euo pipefail

# GCP run of the Go PaxosBus implementation (normal processing only).
# Mirrors prevImplementation/run-gcp.sh, with the C++ toolchain build replaced
# by a static Go build (no shared-library fan-out needed).
#
# Optional: REPO_URL (default github.com/austint903/paxos-bus.git),
#           INTERVAL_MS (default 1), DURATION_S (default 60)
#
# Architecture:
#   pb-controller  (us-east1-c, external IP)  → jump host / orchestrator
#     ├─ pb-useast1        (us-east1-d)          replica 0
#     ├─ pb-europenorth1   (europe-north1-c)     replica 1
#     ├─ pb-southamerica   (southamerica-east1-b) replica 2
#     └─ pb-asia1          (asia-east1-c)        2 clients
#
# Only pb-controller has an external IP. All other VMs are reached via
# `gcloud compute ssh --internal-ip` from the controller (gcloud is
# pre-installed on GCE VMs and authenticates via the VM's service account;
# the SA needs compute.osLogin or compute scope).

REPO_URL="${REPO_URL:-https://github.com/austint903/paxos-bus.git}"
INTERVAL_MS="${INTERVAL_MS:-1}"
DURATION_S="${DURATION_S:-60}"
DROP_MODE="${DROP_MODE:-none}"   # artificial drop scenario: none|leader|followers|all
DROP_EVERY="${DROP_EVERY:-0}"    # drop a slot when reqId % DROP_EVERY == 0 (0 = disabled)
REQUEST_GEN="${REQUEST_GEN:-0}"      # 1 = request-generator mode (= local -r)
GEN_INTERVAL_US="${GEN_INTERVAL_US:-1}"  # request generation interval in µs (= local -g; REQUEST_GEN only)

CONTROLLER_VM="pb-controller"
CONTROLLER_ZONE="us-east1-c"

echo "==> gcloud compute instances list (discovering pb-* VMs)"
REPLICA0_VM= REPLICA0_ZONE= REPLICA0_IP=  # us-east  (not controller)
REPLICA1_VM= REPLICA1_ZONE= REPLICA1_IP=  # europe
REPLICA2_VM= REPLICA2_ZONE= REPLICA2_IP=  # south america
CLIENT_VM=   CLIENT_ZONE=   CLIENT_IP=    # asia (2 clients)

while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  read -r name zone ip <<< "$line"
  [[ "$name" == "$CONTROLLER_VM" ]] && continue
  case "$zone" in
    us-east*)      REPLICA0_VM=$name; REPLICA0_ZONE=$zone; REPLICA0_IP=$ip ;;
    europe*)       REPLICA1_VM=$name; REPLICA1_ZONE=$zone; REPLICA1_IP=$ip ;;
    southamerica*) REPLICA2_VM=$name; REPLICA2_ZONE=$zone; REPLICA2_IP=$ip ;;
    asia*)         CLIENT_VM=$name;   CLIENT_ZONE=$zone;   CLIENT_IP=$ip ;;
  esac
done < <(gcloud compute instances list \
  --filter="name~^pb-" \
  --format="value(name,zone,networkInterfaces[0].networkIP)")

for slot in REPLICA0_VM REPLICA1_VM REPLICA2_VM CLIENT_VM; do
  [[ -n "${!slot}" ]] || { echo "MISSING $slot — check VM zones / names"; exit 1; }
done

printf "  CTRL  %-20s %-22s (entry point)\n" "$CONTROLLER_VM" "$CONTROLLER_ZONE"
printf "  R0    %-20s %-22s %s\n" "$REPLICA0_VM" "$REPLICA0_ZONE" "$REPLICA0_IP"
printf "  R1    %-20s %-22s %s\n" "$REPLICA1_VM" "$REPLICA1_ZONE" "$REPLICA1_IP"
printf "  R2    %-20s %-22s %s\n" "$REPLICA2_VM" "$REPLICA2_ZONE" "$REPLICA2_IP"
printf "  CL    %-20s %-22s %s\n" "$CLIENT_VM"   "$CLIENT_ZONE"   "$CLIENT_IP"

echo "==> Ensuring all VMs are RUNNING"
for vm_zone in "$CONTROLLER_VM:$CONTROLLER_ZONE" \
               "$REPLICA0_VM:$REPLICA0_ZONE" \
               "$REPLICA1_VM:$REPLICA1_ZONE" \
               "$REPLICA2_VM:$REPLICA2_ZONE" \
               "$CLIENT_VM:$CLIENT_ZONE"; do
  vm="${vm_zone%%:*}"; zone="${vm_zone##*:}"
  status=$(gcloud compute instances describe "$vm" --zone="$zone" --format="value(status)")
  if [[ "$status" != "RUNNING" ]]; then
    echo "  starting $vm (was $status)"
    gcloud compute instances start "$vm" --zone="$zone" --quiet &
  fi
done
wait
sleep 10  # give sshd time to come up on freshly-started VMs

# ---------------------------------------------------------------------------
# Generate the orchestrator script that will run *on* pb-controller.
# Quoted heredoc → no local variable expansion; all inputs come via env vars
# passed on the gcloud ssh command line.
# ---------------------------------------------------------------------------
ORCH=$(mktemp)
cat > "$ORCH" <<'ORCH_EOF'
#!/usr/bin/env bash
set -euo pipefail

: "${REPO_URL:?}" "${INTERVAL_MS:?}" "${DURATION_S:?}" "${DROP_MODE:?}" "${DROP_EVERY:?}"
: "${REQUEST_GEN:?}" "${GEN_INTERVAL_US:?}"
: "${REPLICA0_VM:?}" "${REPLICA0_ZONE:?}" "${REPLICA0_IP:?}"
: "${REPLICA1_VM:?}" "${REPLICA1_ZONE:?}" "${REPLICA1_IP:?}"
: "${REPLICA2_VM:?}" "${REPLICA2_ZONE:?}" "${REPLICA2_IP:?}"
: "${CLIENT_VM:?}"   "${CLIENT_ZONE:?}"   "${CLIENT_IP:?}"

ssh_to()   { gcloud compute ssh "$1" --zone="$2" --internal-ip --quiet -- "$3"; }
scp_to()   { gcloud compute scp --zone="$2" --internal-ip --quiet "$3" "$1":"$4"; }
scp_from() { gcloud compute scp --zone="$2" --internal-ip --quiet "$1":"$3" "$4"; }

echo "[ctrl] Build on controller (only VM with outbound internet)"

GO_VERSION=1.22.5
if [[ ! -x /usr/local/go/bin/go ]]; then
  echo "[ctrl]   installing Go $GO_VERSION"
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | sudo tar -C /usr/local -xz
fi
export PATH="$PATH:/usr/local/go/bin"
go version

if [[ ! -d "$HOME/paxos-bus" ]]; then
  git clone "$REPO_URL" "$HOME/paxos-bus"
else
  git -C "$HOME/paxos-bus" pull --ff-only
fi
mkdir -p "$HOME/paxosbus"
( cd "$HOME/paxos-bus" && \
  CGO_ENABLED=0 go build -o "$HOME/paxosbus/paxosbus-replica" ./paxosbus/cmd/paxosbus-replica && \
  CGO_ENABLED=0 go build -o "$HOME/paxosbus/paxosbus-client"  ./paxosbus/cmd/paxosbus-client )
chmod +x "$HOME/paxosbus/paxosbus-replica" "$HOME/paxosbus/paxosbus-client"

echo "[ctrl] Fan out static binaries to R0/R1/R2/CL (no shared libs needed)"
for vm_zone in "$REPLICA0_VM:$REPLICA0_ZONE" "$REPLICA1_VM:$REPLICA1_ZONE" "$REPLICA2_VM:$REPLICA2_ZONE" "$CLIENT_VM:$CLIENT_ZONE"; do
  vm="${vm_zone%%:*}"; zone="${vm_zone##*:}"
  ssh_to "$vm" "$zone" "mkdir -p ~/paxosbus"
  scp_to "$vm" "$zone" "$HOME/paxosbus/paxosbus-replica" "paxosbus/"
  scp_to "$vm" "$zone" "$HOME/paxosbus/paxosbus-client"  "paxosbus/"
  ssh_to "$vm" "$zone" "chmod +x ~/paxosbus/paxosbus-replica ~/paxosbus/paxosbus-client"
done

echo "[ctrl] Generate + distribute paxosbus.conf"
CONFFILE=$(mktemp)
cat > "$CONFFILE" <<CONF
f 1
replica $REPLICA0_IP:7000
replica $REPLICA1_IP:7000
replica $REPLICA2_IP:7000
CONF

for vm_zone in "$REPLICA0_VM:$REPLICA0_ZONE" "$REPLICA1_VM:$REPLICA1_ZONE" "$REPLICA2_VM:$REPLICA2_ZONE" "$CLIENT_VM:$CLIENT_ZONE"; do
  vm="${vm_zone%%:*}"; zone="${vm_zone##*:}"
  scp_to "$vm" "$zone" "$CONFFILE" "paxosbus/paxosbus.conf"
done
rm "$CONFFILE"

echo "[ctrl] Pre-warm gcloud SSH (serializes OS Login key propagation)"
for vm_zone in "$REPLICA0_VM:$REPLICA0_ZONE" "$REPLICA1_VM:$REPLICA1_ZONE" "$REPLICA2_VM:$REPLICA2_ZONE" "$CLIENT_VM:$CLIENT_ZONE"; do
  vm="${vm_zone%%:*}"; zone="${vm_zone##*:}"
  ssh_to "$vm" "$zone" "true"
done

echo "[ctrl] Kill any stale processes"
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  ssh_to "${!vm_var}" "${!zone_var}" "pkill -f '[p]axosbus-replica' || true"
done
ssh_to "$CLIENT_VM" "$CLIENT_ZONE" "pkill -f '[p]axosbus-client' || true"
sleep 2

echo "[ctrl] Launch replicas (us-east, europe, south-america)"
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  region="${!zone_var}"; region="${region%-*}"   # us-east1-d -> us-east1
  ssh_to "${!vm_var}" "${!zone_var}" "
    rm -f /tmp/paxosbus.log
    rm -rf /tmp/paxosbus-durable && mkdir -p /tmp/paxosbus-durable
    cd \$HOME/paxosbus
    nohup ./paxosbus-replica \
      -c paxosbus.conf -i $slot -l $region -d /tmp/paxosbus-durable \
      -drop-mode $DROP_MODE -drop-every $DROP_EVERY </dev/null >/tmp/paxosbus.log 2>&1 &
    disown
    sleep 1
    if pgrep -f '[p]axosbus-replica' >/dev/null; then
      echo '[replica $slot] running, pid='\$(pgrep -f '[p]axosbus-replica')
    else
      echo '[replica $slot] NOT RUNNING — startup log:'
      cat /tmp/paxosbus.log 2>/dev/null || echo '(no log)'
    fi
  "
done
sleep 3

echo "[ctrl] Launch 2 clients on $CLIENT_VM (asia) — pinging replicas"
CLIENT_REGION="${CLIENT_ZONE%-*}"   # asia-east1-c -> asia-east1
CLIENT_EXTRA=""
if [[ "$REQUEST_GEN" == "1" ]]; then
  CLIENT_EXTRA="-r -g $GEN_INTERVAL_US"
fi
for id in 1 2; do
  ssh_to "$CLIENT_VM" "$CLIENT_ZONE" "
    rm -f /tmp/paxosbus-client-$id.log
    cd \$HOME/paxosbus
    nohup ./paxosbus-client \
      -c paxosbus.conf -I $id -p $INTERVAL_MS -l $CLIENT_REGION $CLIENT_EXTRA \
      </dev/null >/tmp/paxosbus-client-$id.log 2>&1 &
    disown
    sleep 1
    if pgrep -f '[p]axosbus-client.*-I $id' >/dev/null; then
      echo '[client $id] running'
    else
      echo '[client $id] NOT RUNNING — startup log:'
      cat /tmp/paxosbus-client-$id.log 2>/dev/null || echo '(no log)'
    fi
  "
done

echo ""
echo "[ctrl] Live tail of all replicas + clients (running for $((DURATION_S + 6))s)"
echo "----------------------------------------------------------------"
TAIL_PIDS=()
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  gcloud compute ssh "${!vm_var}" --zone="${!zone_var}" --internal-ip --quiet \
    -- "tail -f /tmp/paxosbus.log | sed -u 's/^/[r${slot}] /'" &
  TAIL_PIDS+=($!)
done
for id in 1 2; do
  gcloud compute ssh "$CLIENT_VM" --zone="$CLIENT_ZONE" --internal-ip --quiet \
    -- "tail -f /tmp/paxosbus-client-${id}.log | sed -u 's/^/[c${id}] /'" &
  TAIL_PIDS+=($!)
done
sleep $((DURATION_S + 6))
for pid in "${TAIL_PIDS[@]}"; do
  kill "$pid" 2>/dev/null || true
done
wait 2>/dev/null || true

echo "----------------------------------------------------------------"
echo "[ctrl] Stopping replicas + clients"
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  ssh_to "${!vm_var}" "${!zone_var}" "pkill -f '[p]axosbus-replica' || true"
done
ssh_to "$CLIENT_VM" "$CLIENT_ZONE" "pkill -f '[p]axosbus-client' || true"

echo "[ctrl] Collecting logs into ~/paxosbus-logs/ on controller"
rm -rf ~/paxosbus-logs && mkdir -p ~/paxosbus-logs
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  scp_from "${!vm_var}" "${!zone_var}" "/tmp/paxosbus.log" "$HOME/paxosbus-logs/${!vm_var}.log" \
    || echo "  WARN: no /tmp/paxosbus.log on ${!vm_var}"
done
scp_from "$CLIENT_VM" "$CLIENT_ZONE" "/tmp/paxosbus-client-1.log" "$HOME/paxosbus-logs/" \
  || echo "  WARN: no /tmp/paxosbus-client-1.log on $CLIENT_VM"
scp_from "$CLIENT_VM" "$CLIENT_ZONE" "/tmp/paxosbus-client-2.log" "$HOME/paxosbus-logs/" \
  || echo "  WARN: no /tmp/paxosbus-client-2.log on $CLIENT_VM"

echo "[ctrl] Collecting durable per-client logs into ~/paxosbus-durable/ on controller"
rm -rf ~/paxosbus-durable && mkdir -p ~/paxosbus-durable
for slot in 0 1 2; do
  vm_var="REPLICA${slot}_VM"; zone_var="REPLICA${slot}_ZONE"
  # --recurse onto a not-yet-existing dest makes scp create replica-$slot as a
  # copy of the remote dir (so files land in replica-$slot/client-*.log).
  gcloud compute scp --zone="${!zone_var}" --internal-ip --recurse --quiet \
    "${!vm_var}":/tmp/paxosbus-durable "$HOME/paxosbus-durable/replica-$slot" \
    || echo "  WARN: no durable logs on ${!vm_var}"
done

echo ""
echo "[ctrl] Per-replica RTT summary"
for c in 1 2; do
  echo "=== client $c ==="
  for r in 0 1 2; do
    grep -oE "REPLY from replica=$r  rtt=[0-9]+us" "$HOME/paxosbus-logs/paxosbus-client-$c.log" 2>/dev/null \
      | grep -oE "[0-9]+" \
      | awk -v r=$r '{a[NR]=$1; s+=$1} END {
          n=NR; if (!n) { print "  replica="r" no data"; exit }
          asort(a);
          printf "  replica=%d  n=%d  avg=%.0fus  p50=%dus  p99=%dus\n",
                 r, n, s/n, a[int(n*0.5)], a[int(n*0.99)] }'
  done
done

echo "[ctrl] Done. Logs in ~/paxosbus-logs/ on $(hostname)"
ORCH_EOF

echo "==> Uploading orchestrator to $CONTROLLER_VM"
gcloud compute scp --zone="$CONTROLLER_ZONE" --quiet "$ORCH" "$CONTROLLER_VM":~/orchestrator.sh
rm "$ORCH"

echo "==> Running orchestrator on $CONTROLLER_VM (fans out to R0/R1/R2/CL)"
gcloud compute ssh "$CONTROLLER_VM" --zone="$CONTROLLER_ZONE" --quiet -- "
  REPO_URL='$REPO_URL' \
  INTERVAL_MS='$INTERVAL_MS' \
  DURATION_S='$DURATION_S' \
  DROP_MODE='$DROP_MODE' \
  DROP_EVERY='$DROP_EVERY' \
  REQUEST_GEN='$REQUEST_GEN' \
  GEN_INTERVAL_US='$GEN_INTERVAL_US' \
  REPLICA0_VM='$REPLICA0_VM' REPLICA0_ZONE='$REPLICA0_ZONE' REPLICA0_IP='$REPLICA0_IP' \
  REPLICA1_VM='$REPLICA1_VM' REPLICA1_ZONE='$REPLICA1_ZONE' REPLICA1_IP='$REPLICA1_IP' \
  REPLICA2_VM='$REPLICA2_VM' REPLICA2_ZONE='$REPLICA2_ZONE' REPLICA2_IP='$REPLICA2_IP' \
  CLIENT_VM='$CLIENT_VM'     CLIENT_ZONE='$CLIENT_ZONE'     CLIENT_IP='$CLIENT_IP' \
  bash ~/orchestrator.sh
"

# Each run is archived in its own directory so results are never overwritten.
RUN_LOG_DIR="./paxosbus/logs/gcp/gcp-run-$(date +%Y%m%d-%H%M%S)"
echo "==> Copying logs from $CONTROLLER_VM to $RUN_LOG_DIR/"
mkdir -p "$RUN_LOG_DIR"
gcloud compute scp --zone="$CONTROLLER_ZONE" --quiet --recurse \
  "$CONTROLLER_VM":~/paxosbus-logs/. "$RUN_LOG_DIR/"
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "implementation=go"
  echo "interval_ms=$INTERVAL_MS"
  echo "duration_s=$DURATION_S"
  echo "drop_mode=$DROP_MODE"
  echo "drop_every=$DROP_EVERY"
  echo "request_gen=$REQUEST_GEN"
  echo "gen_interval_us=$GEN_INTERVAL_US"
  if [[ "$DROP_MODE" != "none" && "$DROP_EVERY" -gt 0 ]]; then
    echo "mode=drop-$DROP_MODE"
  else
    echo "mode=normal"
  fi
} > "$RUN_LOG_DIR/run-meta.txt"

# Durable per-client logs are archived separately, mirroring the local layout.
DURABLE_DIR="./paxosbus/logs/durable/gcp/$(basename "$RUN_LOG_DIR")"
echo "==> Copying durable per-client logs to $DURABLE_DIR/"
mkdir -p "$DURABLE_DIR"
gcloud compute scp --zone="$CONTROLLER_ZONE" --quiet --recurse \
  "$CONTROLLER_VM":~/paxosbus-durable/. "$DURABLE_DIR/" \
  || echo "  WARN: no durable logs collected on $CONTROLLER_VM"

echo "==> Done. VMs left running. Logs in $RUN_LOG_DIR/ (durable: $DURABLE_DIR/)"
