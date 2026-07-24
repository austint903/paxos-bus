#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Closed-loop client sweep for the swiftpaxos protocols (paxos | epaxos) on
# CloudLab — the Figure-8 methodology from the SwiftPaxos paper (NSDI'24):
# ramp the number of concurrent closed-loop clients and record one
# (throughput, latency) sample per run; saturation is where throughput
# plateaus while latency climbs.
#
# Topology is fixed to the PaxosBus 9-client testbed: --scale large with 3
# client processes on each of the 3 replica hosts. Concurrency scales via
# clones (goroutines inside each process), so total clients = 9 x (CLONES+1)
# and every step's client count is rounded UP to a multiple of 9.
#
# Each closed-loop client sends REQS timed requests and exits, so run length
# = REQS x avg latency. To keep every step ~TARGET_S seconds of steady state,
# REQS is sized per step from the PREVIOUS step's measured avg latency
# (bootstrapped by LAT_EST_MS), clamped to [200, 5000].
#
# Every run appends one line to paxosbus/logs/swiftpaxos/<proto>-sweep-manifest.txt:
#   clients=<N> clones=<C> reqs=<R> rep=<K> rc=<RC> no_took=<J> dir=<run-dir-basename>
# (same manifest-driven provenance as paxosbus/logs/cloudlab/sweep-manifest.txt;
# dir is relative to the manifest's directory, rc!=0 or dir=MISSING = failed run,
# no_took = clients killed by the DURATION_S cap before finishing — >0 taints
# the run's throughput, treat like a failure for curve fitting.)
#
# Usage:
#   ./run-swiftpaxos-sweep.sh -P paxos  -c "9 45 90 180 360 720"      # 1 rep each
#   ./run-swiftpaxos-sweep.sh -P epaxos -c "360 720" -r 5             # 5 reps each
#
# Env knobs:
#   TARGET_S      steady-state seconds per run (75)
#   LAT_EST_MS    initial avg-latency estimate for REQS sizing (70)
#   DURATION_S    per-run hard cap passed through to the run script (600)
#   REQS_MIN/MAX  clamp for the per-step REQS (200 / 5000)
# ─────────────────────────────────────────────────────────────────────────────

PROTOCOL=""
CLIENT_COUNTS=""
REPS=1
TARGET_S="${TARGET_S:-75}"
LAT_EST_MS="${LAT_EST_MS:-70}"
DURATION_S="${DURATION_S:-600}"
REQS_MIN="${REQS_MIN:-200}"
REQS_MAX="${REQS_MAX:-5000}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -P|--protocol) PROTOCOL="${2:?-P needs paxos|epaxos}"; shift ;;
        -c|--clients)  CLIENT_COUNTS="${2:?-c needs a list like \"9 45 90\"}"; shift ;;
        -r|--reps)     REPS="${2:?-r needs a number}"; shift ;;
        -t|--target)   TARGET_S="${2:?-t needs seconds}"; shift ;;
        *) echo "usage: $0 -P paxos|epaxos -c \"9 45 90 ...\" [-r reps] [-t target_s]"; exit 1 ;;
    esac
    shift
done
[[ "$PROTOCOL" =~ ^(paxos|epaxos)$ ]] || { echo "ERROR: -P must be paxos|epaxos"; exit 1; }
[[ -n "$CLIENT_COUNTS" ]] || { echo "ERROR: -c is required"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS="$SCRIPT_DIR/paxosbus/logs/swiftpaxos"
# MANIFEST_NAME=<proto>-final-manifest.txt routes the 5-loads x 5-reps final
# measurement runs to their own manifest (same line format).
MANIFEST="$LOGS/${MANIFEST_NAME:-$PROTOCOL-sweep-manifest.txt}"
mkdir -p "$LOGS"

echo "# sweep started $(date -u +%Y-%m-%dT%H:%M:%SZ)  protocol=$PROTOCOL scale=large procs=9 target_s=$TARGET_S reps=$REPS counts=[$CLIENT_COUNTS]" >> "$MANIFEST"
echo "==> Sweep $PROTOCOL: counts [$CLIENT_COUNTS] x $REPS rep(s) -> $MANIFEST"

lat_est="$LAT_EST_MS"
for n in $CLIENT_COUNTS; do
    clones=$(( (n + 8) / 9 - 1 ))
    actual=$(( 9 * (clones + 1) ))
    reqs=$(awk -v t="$TARGET_S" -v l="$lat_est" -v lo="$REQS_MIN" -v hi="$REQS_MAX" \
        'BEGIN { r = int(t * 1000 / l); if (r < lo) r = lo; if (r > hi) r = hi; print r }')
    for rep in $(seq 1 "$REPS"); do
        echo ""
        echo "==> [$PROTOCOL] clients=$actual (clones=$clones) reqs=$reqs rep=$rep/$REPS (lat est ${lat_est}ms)"
        step_log="$(mktemp)"
        rc=0
        SCALE=large CLIENTS_PER_HOST=3 CLONES="$clones" REQS="$reqs" \
            DURATION_S="$DURATION_S" PROTOCOL="$PROTOCOL" \
            "$SCRIPT_DIR/run-swiftpaxos-cloudlab.sh" >"$step_log" 2>&1 || rc=$?

        run_dir="$(sed -n 's/^==> Done\. Logs: //p' "$step_log" | tail -n1)"
        dir_base=MISSING
        no_took=0
        if [[ -n "$run_dir" && -d "$run_dir" ]]; then
            dir_base="$(basename "$run_dir")"
            cp "$step_log" "$run_dir/sweep-step.log"
            metrics="$run_dir/metrics.txt"
            if [[ -f "$metrics" ]]; then
                no_took="$(sed -n "s/.*WARN: \([0-9]*\) client(s) missing 'Test took'.*/\1/p" "$metrics")"
                no_took="${no_took:-0}"
                tput="$(awk '/sum of per-client closed-loop/ { print $3 }' "$metrics")"
                avg="$(sed -n 's/.*latency ms *: n=[0-9]* avg=\([0-9.]*\).*/\1/p' "$metrics" | tail -n1)"
                echo "    tput=${tput:-?} req/s  avg=${avg:-?} ms  no_took=$no_took  ($dir_base)"
                # feed the measured latency into the next step's REQS sizing
                if [[ -n "${avg:-}" && "$no_took" == 0 ]]; then
                    lat_est="$(awk -v a="$avg" 'BEGIN { print (a < 1) ? 1 : int(a + 0.5) }')"
                fi
            else
                echo "    WARN: no metrics.txt in $run_dir"
            fi
        else
            echo "    FAILED (rc=$rc) — tail of step log:"
            tail -n 20 "$step_log" | sed 's/^/      /'
        fi
        rm -f "$step_log"
        echo "clients=$actual clones=$clones reqs=$reqs rep=$rep rc=$rc no_took=$no_took dir=$dir_base" >> "$MANIFEST"
        sleep 3   # let sockets drain between runs
    done
done

echo ""
echo "==> Sweep done. Manifest: $MANIFEST"
