#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Open-loop load sweep for PaxosBus on CloudLab: one run-cloudlab.sh run per
# request-generation interval (-g, GEN_INTERVAL_US), appending one manifest
# line per run — the same format the throughput-latency notebook parses:
#   gen_interval_us=<G> rep=<K> rc=<RC> dir=<run-dir-basename>
#
# Topology is the 9-client testbed: --scale large, 3 clients per replica host,
# bus generation interval INTERVAL_MS (default 1 ms), DURATION_S (default 60)
# of data phase per run. Offered load = 9 x 1e6/G req/s.
#
# Usage:
#   ./run-paxosbus-sweep.sh -g "1000 667 500 400"            # 1 rep each
#   ./run-paxosbus-sweep.sh -g "100 50" -r 5                 # 5 reps each
#
# Env knobs:
#   MANIFEST_NAME   manifest file under paxosbus/logs/cloudlab/
#                   (default sweep-manifest.txt)
#   INTERVAL_MS     bus generation interval, ms (1)
#   DURATION_S      data-phase seconds per run (60)
# ─────────────────────────────────────────────────────────────────────────────

GENS=""
REPS=1
INTERVAL_MS="${INTERVAL_MS:-1}"
DURATION_S="${DURATION_S:-60}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -g|--gens) GENS="${2:?-g needs a list like \"1000 667 500\"}"; shift ;;
        -r|--reps) REPS="${2:?-r needs a number}"; shift ;;
        *) echo "usage: $0 -g \"1000 667 500 ...\" [-r reps]"; exit 1 ;;
    esac
    shift
done
[[ -n "$GENS" ]] || { echo "ERROR: -g is required"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS="$SCRIPT_DIR/paxosbus/logs/cloudlab"
MANIFEST="$LOGS/${MANIFEST_NAME:-sweep-manifest.txt}"
mkdir -p "$LOGS"

code="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "# sweep started $(date -u +%Y-%m-%dT%H:%M:%SZ)  scale=large clients=9 interval_ms=$INTERVAL_MS duration_s=$DURATION_S code=$code gens=[$GENS] reps=$REPS" >> "$MANIFEST"
echo "==> PaxosBus sweep: gens [$GENS] x $REPS rep(s) -> $MANIFEST"

for g in $GENS; do
    for rep in $(seq 1 "$REPS"); do
        echo ""
        echo "==> [paxosbus] gen_interval_us=$g rep=$rep/$REPS (offered $((9 * 1000000 / g)) req/s)"
        step_log="$(mktemp)"
        rc=0
        SCALE=large CLIENTS_PER_HOST=3 INTERVAL_MS="$INTERVAL_MS" \
            DURATION_S="$DURATION_S" REQUEST_GEN=1 GEN_INTERVAL_US="$g" \
            "$SCRIPT_DIR/run-cloudlab.sh" >"$step_log" 2>&1 || rc=$?

        # collect() ends with '==> Done. Logs: <dir>  (durable ...)' — strip the tail
        run_dir="$(sed -n 's/^==> Done\. Logs: \([^ ]*\).*/\1/p' "$step_log" | tail -n1)"
        dir_base=MISSING
        if [[ -n "$run_dir" && -d "$run_dir" ]]; then
            dir_base="$(basename "$run_dir")"
            cp "$step_log" "$run_dir/sweep-step.log"
            tput="$(sed -n 's/.*committed-in-window throughput *: *\([0-9.]*\).*/\1/p' "$step_log" | tail -n1)"
            echo "    done: $dir_base ${tput:+(tput $tput req/s)}"
        else
            echo "    FAILED (rc=$rc) — tail of step log:"
            tail -n 20 "$step_log" | sed 's/^/      /'
        fi
        rm -f "$step_log"
        echo "gen_interval_us=$g rep=$rep rc=$rc dir=$dir_base" >> "$MANIFEST"
        sleep 3   # let sockets drain between runs
    done
done

echo ""
echo "==> Sweep done. Manifest: $MANIFEST"
