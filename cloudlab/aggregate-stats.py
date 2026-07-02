#!/usr/bin/env python3
"""Aggregate stats for a paxosbus cloudlab run directory.

Usage: aggregate-stats.py <run_dir>

Parses:
  paxosbus-client-*.log  COMMITTED lines -> request RTT (total= gen->quorum,
                         includes the wait to board a bus) + committed
                         throughput, per client / per region / aggregated.
  replica-*.log          per-second "1s: received=" lines -> bus-message
                         delivery rate per replica + gap/noop/backlog health.

Works on both request-gen (-r) runs (log_index commits) and plain runs
(slot commits): anything with "COMMITTED ... total=<n>us" counts.
"""

import glob
import os
import re
import statistics
import sys

COMMIT_RE = re.compile(
    r"^(\d{8})-(\d{2})(\d{2})(\d{2})-(\d{4}) .*"
    r"\[Client (\d+) ?([^\]]*)\] COMMITTED .*total=(\d+)us")
REPLICA_RE = re.compile(
    r"\[Replica (\d+) ?([^\]]*)\] 1s: received=(\d+) dropped=(\d+) .*"
    r"gaps=(\d+) recovered=(\d+) noops=(\d+)"
    r"(?: reply_backlog_max=(\d+) durable_backlog_max=(\d+))?")


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0
    i = int((len(sorted_vals) + 1) * p)
    i = min(max(i, 1), len(sorted_vals))
    return sorted_vals[i - 1]


def fmt_lat(vals):
    if not vals:
        return "no samples"
    s = sorted(vals)
    return (f"n={len(s)}  avg={statistics.fmean(s)/1000:.2f}ms  "
            f"p50={pct(s, 0.50)/1000:.2f}ms  p99={pct(s, 0.99)/1000:.2f}ms  "
            f"min={s[0]/1000:.2f}ms  max={s[-1]/1000:.2f}ms")


def parse_clients(run_dir):
    clients = {}  # cid -> {"label": str, "lat": [us], "t0": s, "t1": s}
    for path in sorted(glob.glob(os.path.join(run_dir, "*client-*.log"))):
        with open(path, errors="replace") as f:
            for line in f:
                m = COMMIT_RE.match(line)
                if not m:
                    continue
                day, hh, mm, ss, frac, cid, label, total = m.groups()
                # log timestamps: day + HHMMSS + 4 frac digits of 100us units
                t = (int(day) * 86400 + int(hh) * 3600 + int(mm) * 60
                     + int(ss) + int(frac) / 10000.0)
                c = clients.setdefault(int(cid), {
                    "label": label or "?", "lat": [], "t0": t, "t1": t})
                c["lat"].append(int(total))
                c["t1"] = t
    return clients


def parse_replicas(run_dir):
    reps = {}  # idx -> {"label", "recv": [per-sec], "dropped","gaps","noops","recovered","rmax","dmax"}
    for path in sorted(glob.glob(os.path.join(run_dir, "replica-*.log"))):
        with open(path, errors="replace") as f:
            for line in f:
                m = REPLICA_RE.search(line)
                if not m:
                    continue
                idx, label, recv, dropped, gaps, recovered, noops, rmax, dmax = m.groups()
                r = reps.setdefault(int(idx), {
                    "label": label or "?", "recv": [], "dropped": 0,
                    "gaps": 0, "noops": 0, "recovered": 0, "rmax": 0, "dmax": 0})
                r["recv"].append(int(recv))
                r["dropped"] += int(dropped)
                r["gaps"] += int(gaps)
                r["noops"] += int(noops)
                r["recovered"] += int(recovered)
                if rmax:
                    r["rmax"] = max(r["rmax"], int(rmax))
                if dmax:
                    r["dmax"] = max(r["dmax"], int(dmax))
    return reps


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip())
        sys.exit(1)
    run_dir = sys.argv[1]
    clients = parse_clients(run_dir)
    reps = parse_replicas(run_dir)

    print("==> Aggregated run stats:", os.path.basename(run_dir.rstrip("/")))

    if not clients:
        print("  no COMMITTED lines found in client logs")
    else:
        print("\n-- per client (request RTT = generation -> quorum commit) --")
        total_tput = 0.0
        by_region = {}
        for cid in sorted(clients):
            c = clients[cid]
            span = max(c["t1"] - c["t0"], 1e-9)
            tput = (len(c["lat"]) - 1) / span if len(c["lat"]) > 1 else 0.0
            total_tput += tput
            by_region.setdefault(c["label"], []).extend(c["lat"])
            print(f"  client {cid:2d} [{c['label']:8s}]  committed/s={tput:7.1f}  {fmt_lat(c['lat'])}")

        print("\n-- per region --")
        for label in sorted(by_region):
            print(f"  {label:8s}  {fmt_lat(by_region[label])}")

        all_lat = [v for c in clients.values() for v in c["lat"]]
        print("\n-- aggregate (all clients, all regions) --")
        print(f"  request throughput (committed): {total_tput:.1f} req/s")
        print(f"  request RTT: {fmt_lat(all_lat)}")

    if not reps:
        print("\n  no replica stats lines found")
    else:
        print("\n-- bus messages (per replica; every replica receives every bus) --")
        for idx in sorted(reps):
            r = reps[idx]
            recv = sorted(r["recv"])
            med = recv[len(recv) // 2] if recv else 0
            peak = recv[-1] if recv else 0
            print(f"  replica {idx} [{r['label']:8s}]  buses/s median={med}  peak={peak}  "
                  f"dropped={r['dropped']}  gaps={r['gaps']}  recovered={r['recovered']}  "
                  f"noops={r['noops']}  reply_backlog_max={r['rmax']}  durable_backlog_max={r['dmax']}")
        meds = []
        for r in reps.values():
            recv = sorted(r["recv"])
            if recv:
                meds.append(recv[len(recv) // 2])
        if meds:
            print(f"  bus delivery rate, summed over replicas: {sum(meds)} buses/s")


if __name__ == "__main__":
    main()
