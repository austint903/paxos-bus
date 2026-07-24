#!/usr/bin/env python3
"""Aggregate stats for a run-swiftpaxos-cloudlab.sh run directory.

Usage: aggregate-swiftpaxos-stats.py <run-dir>

Layout produced by the run script:
  <run-dir>/swiftpaxos-client-c<id>/client.log   clone 0 (process stdout), plus
                                                 RUN_START/RUN_END epoch markers
                                                 written by the launch wrapper
  <run-dir>/swiftpaxos-client-c<id>/client_<j>   clones 1..N (one file each)
  <run-dir>/swiftpaxos-client-c<id>/host         host label (utah/wisc/...)

Every closed-loop client emits one dlog line per timed request
('YYYY/MM/DD HH:MM:SS latency <ms>', reply RTT; the warm-up request is
excluded by the binary) and a final 'Test took <go-duration>'.

Three throughput estimates, most to least conservative:
  closed-loop sum   sum over clients of n_i / TestTook_i — skew-free, each
                    client measured over exactly its own send window
  dlog-ts window    total committed / (last - first latency-line timestamp)
                    across all clients — 1 s resolution, needs NTP-sane clocks
  wall window       total committed / (max RUN_END - min RUN_START) — includes
                    master rendezvous + connection setup, so a lower bound
"""

import glob
import os
import re
import sys
from datetime import datetime

LAT_RE = re.compile(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) latency ([0-9.eE+-]+)\s*$")
TOOK_RE = re.compile(r"Test took\s+(\S+)")
START_RE = re.compile(r"^RUN_START ([0-9.]+)")
END_RE = re.compile(r"^RUN_END ([0-9.]+)")
# ms before m/s so '150ms' doesn't parse as minutes+seconds
DUR_RE = re.compile(r"([0-9.]+)(ms|us|µs|ns|h|m|s)")
UNITS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}


def parse_go_duration(s):
    parts = DUR_RE.findall(s)
    if not parts:
        return None
    return sum(float(v) * UNITS[u] for v, u in parts)


def pctl(xs, p):
    """xs sorted ascending; nearest-rank percentile."""
    if not xs:
        return float("nan")
    i = min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1))))
    return xs[i]


def parse_client_file(path, host):
    lats, stamps = [], []
    took = rstart = rend = None
    with open(path, errors="replace") as f:
        for line in f:
            m = LAT_RE.match(line)
            if m:
                stamps.append(datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S"))
                lats.append(float(m.group(2)))
                continue
            if "Test took" in line:
                m = TOOK_RE.search(line)
                if m:
                    took = parse_go_duration(m.group(1))
            elif line.startswith("RUN_START"):
                m = START_RE.match(line)
                if m:
                    rstart = float(m.group(1))
            elif line.startswith("RUN_END"):
                m = END_RE.match(line)
                if m:
                    rend = float(m.group(1))
    return {
        "path": path,
        "host": host,
        "lats": lats,
        "stamps": stamps,
        "took": took,
        "rstart": rstart,
        "rend": rend,
    }


def lat_line(lats):
    if not lats:
        return "n=0"
    s = sorted(lats)
    return (
        f"n={len(s)} avg={sum(s) / len(s):.2f} p50={pctl(s, 50):.2f} "
        f"p90={pctl(s, 90):.2f} p99={pctl(s, 99):.2f} min={s[0]:.2f} max={s[-1]:.2f}"
    )


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip())
        sys.exit(1)
    run_dir = sys.argv[1]

    print(f"== swiftpaxos run: {os.path.basename(os.path.abspath(run_dir))} ==")
    meta = os.path.join(run_dir, "run-meta.txt")
    if os.path.exists(meta):
        with open(meta) as f:
            for line in f:
                print(f"  {line.rstrip()}")
    print()

    records = []
    client_dirs = sorted(glob.glob(os.path.join(run_dir, "swiftpaxos-client-c*")))
    for d in client_dirs:
        host = "?"
        hostf = os.path.join(d, "host")
        if os.path.exists(hostf):
            host = open(hostf).read().strip()
        for path in [os.path.join(d, "client.log")] + sorted(
            glob.glob(os.path.join(d, "client_[0-9]*"))
        ):
            if os.path.exists(path):
                records.append(parse_client_file(path, host))

    if not records:
        print(f"no swiftpaxos-client-c*/ logs under {run_dir}")
        sys.exit(1)

    # ── per host ─────────────────────────────────────────────────────────────
    print("-- per host --")
    hosts = sorted({r["host"] for r in records})
    for h in hosts:
        rs = [r for r in records if r["host"] == h]
        procs = len({os.path.dirname(r["path"]) for r in rs})
        lats = [x for r in rs for x in r["lats"]]
        closed = sum(len(r["lats"]) / r["took"] for r in rs if r["took"])
        print(
            f"  {h:<8} procs={procs} clients={len(rs)} committed={len(lats)} "
            f"closed-loop thr={closed:8.1f} req/s  lat ms: {lat_line(lats)}"
        )
    print()

    # ── aggregate ────────────────────────────────────────────────────────────
    all_lats = [x for r in records for x in r["lats"]]
    all_stamps = [t for r in records for t in r["stamps"]]
    committed = len(all_lats)
    no_took = [r for r in records if r["took"] is None]

    print("-- aggregate --")
    print(f"  client processes  : {len(client_dirs)}  ({len(records)} closed-loop clients)")
    print(f"  committed requests: {committed}")
    print(f"  latency ms        : {lat_line(all_lats)}")

    closed = sum(len(r["lats"]) / r["took"] for r in records if r["took"])
    print(f"  throughput        : {closed:10.1f} req/s  [sum of per-client closed-loop]")
    if all_stamps:
        window = (max(all_stamps) - min(all_stamps)).total_seconds()
        if window > 0:
            print(
                f"                      {committed / window:10.1f} req/s  "
                f"[dlog-ts window {window:.0f}s]"
            )
        else:
            print("                      dlog-ts window <1s — too short to estimate")
    starts = [r["rstart"] for r in records if r["rstart"]]
    ends = [r["rend"] for r in records if r["rend"]]
    if starts and ends:
        wall = max(ends) - min(starts)
        if wall > 0:
            print(
                f"                      {committed / wall:10.1f} req/s  "
                f"[RUN_START->RUN_END wall {wall:.1f}s]"
            )
    if no_took:
        print(
            f"  WARN: {len(no_took)} client(s) missing 'Test took' "
            f"(crashed or killed by the DURATION_S cap):"
        )
        for r in no_took:
            print(f"    {os.path.relpath(r['path'], run_dir)}")


if __name__ == "__main__":
    main()
