#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# PaxosBus leader-failure recovery campaign on CloudLab.
#
# Runs the standard 9-client / 3-replica topology N times, killing the leader
# replica partway through each data phase, and reports how long the system took
# to go back to committing. Each repetition is a full run: fresh replicas, fresh
# clients, fresh sync — so the kills are independent samples, not one run with
# several failures in it.
#
# The recovery figure is deliberately measured from the CLIENTS, as the longest
# stretch a client saw nothing commit. That is the number a user of the system
# would feel, and it stays inside one machine's clock (see
# cloudlab/aggregate-viewchange-stats.py for why that matters here). The
# protocol-internal view-change time is reported alongside it.
#
# Usage:
#   ./run-viewchange-cloudlab.sh                 # 5 reps, kill at +20s of a 60s phase
#   REPS=3 KILL_AT_S=15 ./run-viewchange-cloudlab.sh
#   ./run-viewchange-cloudlab.sh analyze         # re-analyse the existing campaign, no runs
#
# Knobs (all optional):
#   REPS            repetitions (default 5)
#   KILL_AT_S       seconds into the data phase to SIGKILL the leader (default 20)
#   DURATION_S      data-phase length (default 60; needs enough runway after the
#                   kill for the recovered system to show steady state again)
#   INTERVAL_MS     bus interval (default 1)
#   GEN_INTERVAL_US per-client request generation interval (default 500)
#   CAMPAIGN        subdirectory under paxosbus/logs/cloudlab (default viewchange)
#   SETTLE_S        pause between repetitions (default 20)
# ─────────────────────────────────────────────────────────────────────────────

REPS="${REPS:-5}"
KILL_AT_S="${KILL_AT_S:-20}"
DURATION_S="${DURATION_S:-60}"
INTERVAL_MS="${INTERVAL_MS:-1}"
GEN_INTERVAL_US="${GEN_INTERVAL_US:-500}"
CAMPAIGN="${CAMPAIGN:-viewchange}"
SETTLE_S="${SETTLE_S:-20}"
# On by default here even though --scale large defaults it off: a kill run is
# exactly the case where the on-disk log is the evidence, and the node deletes
# /tmp/paxosbus-durable at the start of the next run.
COLLECT_DURABLE="${COLLECT_DURABLE:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CAMPAIGN_DIR="$SCRIPT_DIR/paxosbus/logs/cloudlab/$CAMPAIGN"
MANIFEST="$CAMPAIGN_DIR/viewchange-manifest.txt"
ANALYZER="$SCRIPT_DIR/cloudlab/aggregate-viewchange-stats.py"

analyze() {
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "Campaign analysis: $CAMPAIGN_DIR"
    echo "════════════════════════════════════════════════════════════════════"
    python3 "$ANALYZER" "$CAMPAIGN_DIR" | tee "$CAMPAIGN_DIR/viewchange-summary.txt"
}

if [[ "${1:-}" == "analyze" ]]; then
    analyze
    exit 0
fi

mkdir -p "$CAMPAIGN_DIR"
echo "==> PaxosBus view-change campaign"
echo "    reps=$REPS  kill at +${KILL_AT_S}s of a ${DURATION_S}s data phase"
echo "    load: bus=${INTERVAL_MS}ms  gen=${GEN_INTERVAL_US}us  (9 clients, 3 replicas)"
echo "    logs: $CAMPAIGN_DIR"
echo ""

{
    echo "# PaxosBus view-change campaign"
    echo "# started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "# reps=$REPS kill_at_s=$KILL_AT_S duration_s=$DURATION_S"
    echo "# interval_ms=$INTERVAL_MS gen_interval_us=$GEN_INTERVAL_US"
} >> "$MANIFEST"

ptr="$(mktemp -t paxosbus-rundir)"
ok=0
for rep in $(seq 1 "$REPS"); do
    echo ""
    echo "────────────────────────────────────────────────────────────────────"
    echo "==> rep $rep/$REPS"
    echo "────────────────────────────────────────────────────────────────────"
    : > "$ptr"
    rc=0
    RUN_SUBDIR="$CAMPAIGN" RUN_TAG="vc-rep$rep" RUN_DIR_POINTER="$ptr" \
    KILL_LEADER_AT_S="$KILL_AT_S" COLLECT_DURABLE="$COLLECT_DURABLE" \
    INTERVAL_MS="$INTERVAL_MS" GEN_INTERVAL_US="$GEN_INTERVAL_US" \
    DURATION_S="$DURATION_S" \
        "$SCRIPT_DIR/run-cloudlab.sh" --scale large || rc=$?

    run_dir="$(cat "$ptr" 2>/dev/null || true)"
    if [[ "$rc" == 0 && -n "$run_dir" && -d "$run_dir" ]]; then
        ok=$((ok + 1))
        echo "rep=$rep rc=$rc dir=$run_dir" >> "$MANIFEST"
    else
        # A run that died mid-flight (an ssh dropping is enough) must not be
        # averaged in: exclude it explicitly rather than silently keeping a
        # partial log directory.
        echo "rep=$rep rc=$rc dir=${run_dir:-NONE} EXCLUDED" >> "$MANIFEST"
        echo "  !! rep $rep failed (rc=$rc) — excluded from the campaign"
    fi

    if [[ "$rep" -lt "$REPS" ]]; then
        echo "==> settling ${SETTLE_S}s before the next rep"
        sleep "$SETTLE_S"
    fi
done
rm -f "$ptr"

echo ""
echo "==> $ok/$REPS reps completed. Manifest: $MANIFEST"
analyze
