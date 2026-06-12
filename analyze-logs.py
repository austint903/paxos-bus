#!/usr/bin/env python3
"""Consolidate and analyze PaxosBus logs (Go implementation; the line formats
are identical to the C++ prevImplementation, so logs from either parse).

Usage:
    ./analyze-logs.py [logdir]

logdir defaults to the newest run directory (by mtime) under logs/local/ and
logs/gcp/, where run-paxosbus.sh and run-gcp.sh archive one directory per run.

Per-node raw logs are left untouched (each file is one node's view; cross-VM
wall clocks are only NTP-synced, so a merged ordering is approximate). This
script writes <logdir>/merged.log — every line tagged with its source node,
sorted by wall-clock timestamp where one is present — and prints RTT / gap /
throughput statistics parsed from the client and replica measurement lines.
"""

import os
import re
import sys
import glob
from datetime import datetime, timezone

# Docker `--timestamps` prefix: 2026-06-11T01:02:03.456789012Z
DOCKER_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z?\s+(.*)$")
# lib/message.cc TIMESTAMP_NUMERIC prefix: 20260511-170203-1234 12345
# (month is tm_mon, i.e. 0-based — a quirk of message.cc; only ordering matters)
NUMERIC_TS = re.compile(r"^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})-(\d{4}) \d+\s+(.*)$")

RE_CLIENT    = re.compile(r"\[Client (\d+)([^\]]*)\]")
RE_REPLY     = re.compile(r"REPLY from replica=(\d+)\s+rtt=(\d+)us")
RE_COMMITTED = re.compile(r"COMMITTED seq=(\d+) app_req=(\d+) rtt=(\d+)us total=(\d+)us attempts=(\d+)")
RE_NOQUORUM  = re.compile(r"NO-QUORUM seq=(\d+)")
RE_SENT_1S   = re.compile(r"1s: sent=(\d+) committed=(\d+)")
RE_DROP      = re.compile(r"DROP seq=(\d+)")
RE_GAP       = re.compile(r"GAP detected seq=(\d+)")
RE_RECOVERED = re.compile(r"recovery_latency=(\d+)us")
RE_NOOP      = re.compile(r"noop_latency=(\d+)us")
RE_REPLICA   = re.compile(r"\[Replica (\d+)([^\]]*)\]")


def parse_ts(line):
    """Return (epoch_seconds_or_None, rest_of_line)."""
    m = DOCKER_TS.match(line)
    if m:
        ts = m.group(1)[:26]  # truncate ns -> us for fromisoformat
        try:
            t = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()
            return t, m.group(2)
        except ValueError:
            pass
    m = NUMERIC_TS.match(line)
    if m:
        y, mo, d, h, mi, s, tenths = (int(m.group(i)) for i in range(1, 8))
        try:
            t = datetime(y, mo + 1, d, h, mi, s,
                         tzinfo=timezone.utc).timestamp() + tenths / 10000.0
            return t, m.group(8)
        except ValueError:
            pass
    return None, line


def pctl(sorted_vals, p):
    if not sorted_vals:
        return 0
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def fmt_us(us):
    return f"{us / 1000.0:.2f}ms" if us >= 1000 else f"{us}us"


def stat_line(label, vals):
    if not vals:
        return f"    {label:<38} no data"
    v = sorted(vals)
    return (f"    {label:<38} n={len(v):<7} avg={fmt_us(sum(v) // len(v)):<10}"
            f" p50={fmt_us(pctl(v, 0.50)):<10} p99={fmt_us(pctl(v, 0.99)):<10}"
            f" max={fmt_us(v[-1])}")


def main():
    if len(sys.argv) > 1:
        logdir = sys.argv[1]
    else:
        runs = (glob.glob("logs/local/local-run-*") +
                glob.glob("logs/gcp/gcp-run-*"))
        logdir = max(runs, key=os.path.getmtime) if runs else "logs"
    if not os.path.isdir(logdir):
        sys.exit(f"error: {logdir} is not a directory")

    files = [f for f in sorted(glob.glob(os.path.join(logdir, "*.log")))
             if os.path.basename(f) != "merged.log"]
    if not files:
        sys.exit(f"error: no .log files in {logdir}")

    merged = []          # (ts, node, text)
    replies = {}         # client -> replica -> [rtt_us]
    quorum = {}          # client -> [rtt_us]
    totals = {}          # client -> [total_us] for requests with attempts > 1
    attempts = {}        # client -> count of NO-QUORUM resends
    sent_per_s = {}      # client -> [sent in each 1s window]
    drops = {}           # replica -> count
    gaps = {}            # replica -> count
    recoveries = {}      # replica -> [recovery_us]
    noops = []           # [noop_us] (leader only)
    regions = {}         # replica -> region label (from "-l", if used)

    for path in files:
        node = os.path.basename(path)[:-len(".log")]
        last_ts = 0.0
        with open(path, errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                ts, text = parse_ts(line)
                if ts is None:
                    ts = last_ts + 1e-6  # keep file order for untimestamped lines
                last_ts = ts
                merged.append((ts, node, text))

                cm = RE_CLIENT.search(text)
                rm = RE_REPLICA.search(text)
                if cm:
                    cid = int(cm.group(1))
                    m = RE_REPLY.search(text)
                    if m:
                        replies.setdefault(cid, {}).setdefault(
                            int(m.group(1)), []).append(int(m.group(2)))
                        continue
                    m = RE_COMMITTED.search(text)
                    if m:
                        quorum.setdefault(cid, []).append(int(m.group(3)))
                        if int(m.group(5)) > 1:
                            totals.setdefault(cid, []).append(int(m.group(4)))
                        continue
                    if RE_NOQUORUM.search(text):
                        attempts[cid] = attempts.get(cid, 0) + 1
                        continue
                    m = RE_SENT_1S.search(text)
                    if m:
                        sent_per_s.setdefault(cid, []).append(int(m.group(1)))
                elif rm:
                    rid = int(rm.group(1))
                    if rm.group(2).strip():
                        regions[rid] = rm.group(2).strip()
                    if RE_DROP.search(text):
                        drops[rid] = drops.get(rid, 0) + 1
                    elif RE_GAP.search(text):
                        gaps[rid] = gaps.get(rid, 0) + 1
                    else:
                        m = RE_RECOVERED.search(text)
                        if m:
                            recoveries.setdefault(rid, []).append(int(m.group(1)))
                            continue
                        m = RE_NOOP.search(text)
                        if m:
                            noops.append(int(m.group(1)))

    merged.sort(key=lambda e: e[0])
    merged_path = os.path.join(logdir, "merged.log")
    with open(merged_path, "w") as out:
        for ts, node, text in merged:
            out.write(f"[{node}] {text}\n")

    print(f"Analyzed {len(files)} log files in {logdir}/  "
          f"({len(merged)} lines -> {merged_path})\n")

    def rname(rid):
        return f"replica {rid} ({regions[rid]})" if rid in regions else f"replica {rid}"

    for cid in sorted(set(list(replies) + list(quorum))):
        print(f"== Client {cid} ==")
        for rid in sorted(replies.get(cid, {})):
            print(stat_line(f"{rname(rid)} reply RTT", replies[cid][rid]))
        print(stat_line("quorum (COMMITTED) RTT", quorum.get(cid, [])))
        if totals.get(cid):
            print(stat_line("total latency (resent reqs)", totals[cid]))
        n_commits = len(quorum.get(cid, []))
        n_resends = attempts.get(cid, 0)
        rates = sent_per_s.get(cid, [])
        rate_txt = (f"  send rate avg={sum(rates) // len(rates)}/s"
                    f" max={max(rates)}/s" if rates else "")
        print(f"    committed={n_commits}  resends={n_resends}{rate_txt}")
        print()

    if drops or gaps or recoveries or noops:
        print("== Gap agreement ==")
        for rid in sorted(set(list(drops) + list(gaps) + list(recoveries))):
            print(f"    {rname(rid)}: dropped={drops.get(rid, 0)}"
                  f"  gaps_detected={gaps.get(rid, 0)}")
            if recoveries.get(rid):
                print(stat_line(f"{rname(rid)} recovery latency",
                                recoveries[rid]))
        print(stat_line("NoOp agreement latency", noops))


if __name__ == "__main__":
    main()
