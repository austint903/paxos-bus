#!/usr/bin/env python3
"""Recovery cost of a PaxosBus leader failure.

Usage:
  aggregate-viewchange-stats.py <run-dir> [<run-dir> ...]   # one line per run + a summary
  aggregate-viewchange-stats.py <campaign-dir>              # every run under it

What "recovery" means here, and why it is measured this way
-----------------------------------------------------------
The number that matters to a client is how long it saw nothing commit. So the
recovery gap is the largest interval between two consecutive COMMITTED lines in
ONE client's own log. That keeps the measurement inside a single machine's
clock: the three CloudLab nodes are tens of milliseconds apart and their clocks
are not disciplined against each other, so any kill-time-on-node-A to
commit-time-on-node-B subtraction would be measuring skew as much as recovery.

Reported per run:

  client gap    max inter-commit gap, per client, summarised across the 9
                clients (this is the headline recovery cost)
  protocol      VIEW-CHANGE start -> VIEW-CHANGE done on the new leader's own
                log: the view change proper — collecting a quorum of reports,
                merging them, pulling the entries it lacks, installing. Excludes
                detection, which precedes it by ~0.5ms (suspicion calls
                startViewChange directly), and excludes the followers, which
                finish about one one-way delay later when StartView reaches them.

The client gap necessarily exceeds the protocol time: it also contains failure
detection, the frozen cursor draining, and the in-flight requests that had to be
re-boarded. The three together say where the time actually goes.

Steady-state throughput and latency come from aggregate-stats.py's definitions,
except that latency is reported both including and excluding the recovery
window, since a handful of multi-second outliers otherwise dominate the mean.
"""

import glob
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module

_agg = import_module("aggregate-stats".replace("-", "_")) if False else None
# aggregate-stats.py is not an importable module name (hyphen), so load it by path.
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "agg_stats", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "aggregate-stats.py"))
agg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(agg)

TS_RE = re.compile(r"^(\d{8})-(\d{2})(\d{2})(\d{2})-(\d{4}) ")
SUSPECT_RE = re.compile(r"SUSPECT leader (\d+) \(([^,]+),")
VC_START_RE = re.compile(r"VIEW-CHANGE start view=(\d+)")
VC_DONE_RE = re.compile(r"VIEW-CHANGE done view=(\d+)")
MERGE_RE = re.compile(r"view (\d+) merge: quorum=(\d+) stable=(\d+) max=(\d+) "
                      r"noops=(\d+) entries_needed=(\d+)")


def ts_of(line):
    m = TS_RE.match(line)
    if not m:
        return None
    day, hh, mm, ss, frac = m.groups()
    return (int(day) * 86400 + int(hh) * 3600 + int(mm) * 60
            + int(ss) + int(frac) / 10000.0)


def read_meta(run_dir, name="run-meta.txt"):
    path = os.path.join(run_dir, name)
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return dict(l.strip().split("=", 1) for l in f if "=" in l)


def parse_replica_events(run_dir):
    """Per replica: the view-change milestones, each on that replica's clock."""
    ev = {}
    for path in sorted(glob.glob(os.path.join(run_dir, "replica-*.log"))):
        idx = int(re.search(r"replica-(\d+)", os.path.basename(path)).group(1))
        e = ev.setdefault(idx, {"suspect": None, "start": None, "done": None,
                                "merge": None, "reason": None, "last": None})
        with open(path, errors="replace") as f:
            for line in f:
                t = ts_of(line)
                if t is None:
                    continue
                e["last"] = t
                m = SUSPECT_RE.search(line)
                if m and e["suspect"] is None:
                    e["suspect"], e["reason"] = t, m.group(2)
                if VC_START_RE.search(line) and e["start"] is None:
                    e["start"] = t
                if VC_DONE_RE.search(line) and e["done"] is None:
                    e["done"] = t
                m = MERGE_RE.search(line)
                if m and e["merge"] is None:
                    e["merge"] = {"view": int(m.group(1)), "quorum": int(m.group(2)),
                                  "noops": int(m.group(5)), "entries": int(m.group(6))}
    return ev


STATS_RE = re.compile(r"1s: view=(\d+) status=(\w+) executed=(\d+) stable=(\d+)")
RECOV_RE = re.compile(r"1s: received=(\d+) dropped=(\d+).*?gaps=(\d+) recovered=(\d+) "
                      r"noops=(\d+)")


def replica_timeline(run_dir, before=4, after=8):
    """Per-second progress on each replica across its own view change.

    This is the direct test of whether a view change costs seconds: `executed` is
    a cumulative slot counter printed once a second, so its per-second delta is
    the replica's actual processing rate. A recovery that really took seconds
    would show several consecutive samples at (or near) zero.

    Anchored per replica at its OWN view-change instant and reported in relative
    seconds, because the three nodes' clocks are not disciplined against each
    other — for the killed leader, at its last log line instead.
    """
    out = {}
    for path in sorted(glob.glob(os.path.join(run_dir, "replica-*.log"))):
        idx = int(re.search(r"replica-(\d+)", os.path.basename(path)).group(1))
        samples, anchor, last = [], None, None
        with open(path, errors="replace") as f:
            for line in f:
                t = ts_of(line)
                if t is None:
                    continue
                last = t
                if anchor is None and (VC_START_RE.search(line)
                                       or SUSPECT_RE.search(line)):
                    anchor = t
                m = STATS_RE.search(line)
                if m:
                    samples.append({"t": t, "view": int(m.group(1)),
                                    "status": m.group(2), "executed": int(m.group(3)),
                                    "stable": int(m.group(4)), "recovered": None})
                m = RECOV_RE.search(line)
                if m and samples:
                    samples[-1]["recovered"] = int(m.group(4))
        if not samples:
            continue
        if anchor is None:          # the killed leader never gets to suspect anyone
            anchor = last
        rows = []
        for prev, cur in zip(samples, samples[1:]):
            rel = cur["t"] - anchor
            if not (-before <= rel <= after):
                continue
            dt = cur["t"] - prev["t"]
            rows.append({"rel": rel,
                         "rate": (cur["executed"] - prev["executed"]) / dt if dt > 0 else 0,
                         "status": cur["status"], "view": cur["view"],
                         "lag": cur["executed"] - cur["stable"]})
        out[idx] = {"rows": rows, "anchor": anchor, "died": anchor == last}
    return out


def client_gaps(clients, warmup_s=2.0):
    """Largest inter-commit gap per client, ignoring the first warmup_s."""
    out = {}
    for cid, c in sorted(clients.items()):
        ts = sorted(c["commits"])
        if len(ts) < 3:
            continue
        t0 = ts[0] + warmup_s
        best, at = 0.0, None
        for a, b in zip(ts, ts[1:]):
            if a < t0:
                continue
            if b - a > best:
                best, at = b - a, a
        out[cid] = {"label": c["label"], "gap": best, "at": at,
                    "first": ts[0], "last": ts[-1]}
    return out


def summarize_run(run_dir):
    meta = read_meta(run_dir)
    kill = read_meta(run_dir, "kill-info.txt")
    clients = agg.parse_clients(run_dir)
    if not clients:
        return None
    gaps = client_gaps(clients)
    if not gaps:
        return None

    duration = agg.read_duration_s(run_dir)
    gvals = [g["gap"] for g in gaps.values()]

    # Throughput over the same fixed window aggregate-stats.py uses.
    tput = 0.0
    lat_all, lat_clean = [], []
    for cid, c in clients.items():
        t0 = c["gen0"]
        t1 = t0 + duration if duration else c["t1"]
        n = sum(1 for t in c["commits"] if t0 <= t <= t1)
        tput += n / (duration if duration else max(c["t1"] - t0, 1e-9))
        lat_all.extend(c["lat"])
        # "clean" = drop the tail inflated by the stall: any request whose
        # latency exceeds a second is one that was in flight across the view
        # change, not a steady-state sample.
        lat_clean.extend(x for x in c["lat"] if x < 1_000_000)

    ev = parse_replica_events(run_dir)
    leader_idx = int(meta.get("leader_idx", 0))
    # The new leader is the one that logged the merge.
    new_leader = next((i for i, e in ev.items() if e["merge"]), None)
    protocol_s = detect_s = None
    if new_leader is not None:
        e = ev[new_leader]
        if e["start"] is not None and e["done"] is not None:
            protocol_s = e["done"] - e["start"]
    # Detection: on whichever replica logged SUSPECT, kill->SUSPECT needs the
    # leader's clock, so only report it when the suspecting node is also the one
    # whose own last pre-kill log line anchors it. Kept simple: report the
    # SUSPECT reason and let the client gap carry the timing.
    reasons = {i: e["reason"] for i, e in ev.items() if e["reason"]}

    return {
        "dir": run_dir,
        "meta": meta,
        "kill": kill,
        "gaps": gaps,
        "gap_max": max(gvals),
        "gap_med": statistics.median(gvals),
        "gap_min": min(gvals),
        "tput": tput,
        "lat_all": lat_all,
        "lat_clean": lat_clean,
        "protocol_s": protocol_s,
        "new_leader": new_leader,
        "leader_idx": leader_idx,
        "reasons": reasons,
        "merge": ev[new_leader]["merge"] if new_leader is not None else None,
        "n_clients": len(clients),
    }


def fmt_lat(vals):
    if not vals:
        return "no samples"
    s = sorted(vals)
    return (f"p50={agg.pct(s, 0.50)/1000:7.2f}ms  p99={agg.pct(s, 0.99)/1000:8.2f}ms  "
            f"avg={statistics.fmean(s)/1000:7.2f}ms  n={len(s)}")


def print_run(r):
    name = os.path.basename(r["dir"].rstrip("/"))
    print(f"\n=== {name} ===")
    m = r["meta"]
    print(f"  load: bus={m.get('interval_ms','?')}ms gen={m.get('gen_interval_us','?')}us  "
          f"clients={r['n_clients']}  duration={m.get('duration_s','?')}s  commit={m.get('commit','?')}")
    if r["kill"]:
        print(f"  killed: replica {r['kill'].get('kill_leader_idx')} "
              f"({r['kill'].get('kill_leader_label')}) at +{r['kill'].get('kill_at_data_phase_s')}s of data phase")
    print(f"  throughput      {r['tput']/1000:.3f} kreq/s")
    print(f"  latency (all)   {fmt_lat(r['lat_all'])}")
    print(f"  latency (<1s)   {fmt_lat(r['lat_clean'])}")
    print(f"  recovery gap    max={r['gap_max']*1000:8.1f}ms  median={r['gap_med']*1000:8.1f}ms  "
          f"min={r['gap_min']*1000:8.1f}ms   (across {len(r['gaps'])} clients)")
    n_str = len(r["lat_all"]) - len(r["lat_clean"])
    worst = max(r["lat_all"]) / 1e6 if r["lat_all"] else 0.0
    print(f"  stranded        {n_str} requests took >1s (worst {worst:.2f}s) — "
          f"in flight across the kill, invisible to the gap above")
    if r["protocol_s"] is not None:
        print(f"  view change     {r['protocol_s']*1000:.1f}ms on replica {r['new_leader']} "
              f"(start -> done, its own clock)")
    if r["merge"]:
        mg = r["merge"]
        print(f"  merge           view={mg['view']} quorum={mg['quorum']} "
              f"noops={mg['noops']} entries_needed={mg['entries']}")
    if r["reasons"]:
        who = ", ".join(f"r{i}:{v}" for i, v in sorted(r["reasons"].items()))
        print(f"  detected by     {who}")
    per = "  ".join(f"c{c}({g['label']}) {g['gap']*1000:.0f}ms"
                    for c, g in sorted(r["gaps"].items()))
    print(f"  per client      {per}")

    tl = replica_timeline(r["dir"])
    if tl:
        print("  per-second executed rate, each replica on its own clock "
              "(t=0 is its own view change; k/s):")
        for idx, info in sorted(tl.items()):
            cells = []
            for row in info["rows"]:
                mark = "" if row["status"] == "normal" else "!"
                cells.append(f"{row['rel']:+.0f}s:{row['rate']/1000:.1f}{mark}")
            tag = "DIED" if info["died"] else f"view->{info['rows'][-1]['view']}" \
                if info["rows"] else "?"
            print(f"    r{idx} {tag:>8}  " + " ".join(cells))
        print("    (! = status was viewchange at that sample)")

    dur = sorted(glob.glob(os.path.join(r["dir"], "durable", "replica-*")))
    if dur:
        sizes = []
        for d in dur:
            n = sum(os.path.getsize(os.path.join(dp, f))
                    for dp, _, fs in os.walk(d) for f in fs)
            sizes.append(f"{os.path.basename(d)}={n/1e6:.0f}MB")
        print(f"  durable logs    {'  '.join(sizes)}")
    else:
        print("  durable logs    NOT COLLECTED for this run")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__.strip())
        sys.exit(1)

    run_dirs = []
    for a in args:
        if os.path.isdir(a) and glob.glob(os.path.join(a, "*client-*.log")):
            run_dirs.append(a)
        else:
            run_dirs.extend(sorted(d for d in glob.glob(os.path.join(a, "*"))
                                   if os.path.isdir(d)
                                   and glob.glob(os.path.join(d, "*client-*.log"))))
    if not run_dirs:
        print("no run directories with client logs found")
        sys.exit(1)

    results = []
    for d in run_dirs:
        r = summarize_run(d)
        if r is None:
            print(f"\n=== {os.path.basename(d)} ===\n  no COMMITTED lines; skipped")
            continue
        print_run(r)
        results.append(r)

    if len(results) < 2:
        return

    print("\n" + "=" * 72)
    print(f"SUMMARY over {len(results)} runs")
    print("=" * 72)

    def stat(vals, unit, scale=1.0, prec=2):
        v = [x * scale for x in vals]
        s = f"mean={statistics.fmean(v):.{prec}f}{unit}  median={statistics.median(v):.{prec}f}{unit}"
        if len(v) > 1:
            s += f"  sd={statistics.stdev(v):.{prec}f}{unit}"
        s += f"  min={min(v):.{prec}f}  max={max(v):.{prec}f}"
        return s

    print(f"  throughput          {stat([r['tput'] for r in results], ' kreq/s', 1/1000, 3)}")
    # Recovery: median-across-clients per run is the per-run figure; the max is
    # the worst client in that run.
    print(f"  recovery gap (med)  {stat([r['gap_med'] for r in results], 'ms', 1000, 1)}")
    print(f"  recovery gap (max)  {stat([r['gap_max'] for r in results], 'ms', 1000, 1)}")
    print(f"  stranded reqs       {stat([len(r['lat_all']) - len(r['lat_clean']) for r in results], '', 1, 0)}")
    prot = [r["protocol_s"] for r in results if r["protocol_s"] is not None]
    if prot:
        print(f"  view change proper  {stat(prot, 'ms', 1000, 1)}  ({len(prot)}/{len(results)} runs)")
    pooled = [x for r in results for x in r["lat_clean"]]
    print(f"  latency (<1s pooled) {fmt_lat(pooled)}")


if __name__ == "__main__":
    main()
