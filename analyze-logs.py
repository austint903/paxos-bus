#!/usr/bin/env python3
"""Consolidate and analyze PaxosBus logs (Go implementation; the line formats
are identical to the C++ prevImplementation, so logs from either parse).

Usage:
    ./analyze-logs.py [logdir]

logdir defaults to the newest run directory (by mtime) under paxosbus/logs/local/
and paxosbus/logs/gcp/, where run-paxosbus.sh and run-gcp.sh archive one directory
per run.

Per-node raw logs are left untouched (each file is one node's view; cross-VM
wall clocks are only NTP-synced, so a merged ordering is approximate). This
script writes <logdir>/merged.log — every line tagged with its source node,
sorted by wall-clock timestamp where one is present — and prints RTT / gap /
throughput statistics parsed from the client and replica measurement lines.

Slot model: each replica keeps ONE global log, so the `slot=` field on REPLY /
COMMITTED / gap lines is the global log slot (no longer == the per-client req id).
Gap-agreement lines now report `slot=` (global); the legacy per-client `seq=`
form is still accepted so older archived runs continue to parse.
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
# Request-generator (-r) REPLY line also carries the bus slot the request rode.
# Counting distinct bus_slot values gives the number of buses that actually
# carried a (committed-or-not) passenger, i.e. the layer-1 throughput, while the
# committed-request count is the layer-2 throughput. The ratio of the two is the
# headline "~1000 requests per bus" batching factor.
RE_REPLY_BUS = re.compile(r"req=\d+\s+bus_slot=(\d+)\s+log_index=\d+")
RE_COMMITTED = re.compile(r"COMMITTED req=(\d+) slot=(\d+) rtt=(\d+)us total=(\d+)us attempts=(\d+)")
# Request-generator (-r) COMMITTED line is keyed on log_index, not the bus slot,
# and has no attempts field (retry granularity is the request, not the bus).
RE_COMMITTED_R = re.compile(r"COMMITTED req=(\d+) log_index=(\d+) rtt=(\d+)us total=(\d+)us")
RE_NOQUORUM  = re.compile(r"NO-QUORUM req=(\d+)")
# -r retry is per request: requests that miss quorum re-board the next bus.
RE_REQTIMEOUT = re.compile(r"REQ-TIMEOUT reboarding=(\d+)")
RE_SENT_1S   = re.compile(r"1s: sent=(\d+) committed=(\d+)")
# Gap-agreement events are keyed on the global log slot now; accept the new
# `slot=` form and the legacy per-client `seq=` form (older archived runs).
RE_DROP      = re.compile(r"DROP (?:slot|seq)=(\d+)")
RE_GAP       = re.compile(r"GAP detected (?:slot|seq)=(\d+)")
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
        runs = (glob.glob("paxosbus/logs/local/local-run-*") +
                glob.glob("paxosbus/logs/gcp/gcp-run-*"))
        logdir = max(runs, key=os.path.getmtime) if runs else "logs"
    if not os.path.isdir(logdir):
        sys.exit(f"error: {logdir} is not a directory")

    files = [f for f in sorted(glob.glob(os.path.join(logdir, "*.log")))
             if os.path.basename(f) != "merged.log"]
    if not files:
        sys.exit(f"error: no .log files in {logdir}")

    merged = []          # (ts, node, text)
    replies = {}         # client -> replica -> [rtt_us]
    buses = {}           # client -> set of distinct bus slots seen (-r only)
    quorum = {}          # client -> [rtt_us]
    commit_span = {}     # client -> [first_committed_ts, last_committed_ts]
    # Throughput window is anchored to the SEND phase, not to commit timestamps:
    #   start = when the client began the open-loop data phase (first send)
    #   end   = the last successful send, i.e. the moment just before sends
    #           started failing because the replicas were killed. This is taken
    #           from the first "send to replica N failed" warning. If a run shut
    #           down cleanly (no failed sends), fall back to the last 1s window
    #           that still committed anything.
    # This keeps the warm-up fill inside the denominator and trims the teardown
    # tail (client still sending after the replicas were killed), which would
    # otherwise be counted as dead time.
    send_start = {}      # client -> ts of "starting open-loop data phase"
    send_end = {}        # client -> ts of last 1s line with committed > 0 (fallback)
    send_fail = {}       # client -> ts of first failed send (≈ last successful send)
    totals = {}          # client -> [total_us] for requests with attempts > 1
    attempts = {}        # client -> count of NO-QUORUM resends
    reboards = {}        # client -> count of requests re-boarded by per-request timeouts (-r)
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
                        bm = RE_REPLY_BUS.search(text)
                        if bm:  # -r mode: track which buses carried passengers
                            buses.setdefault(cid, set()).add(int(bm.group(1)))
                        continue
                    m = RE_COMMITTED.search(text)
                    if m:
                        quorum.setdefault(cid, []).append(int(m.group(3)))
                        if int(m.group(5)) > 1:
                            totals.setdefault(cid, []).append(int(m.group(4)))
                        # Track the actual data-phase span from the first to the
                        # last committed reply's wall-clock timestamp, so throughput
                        # uses the real elapsed time (e.g. 60.21s) rather than the
                        # nominal configured duration.
                        span = commit_span.setdefault(cid, [ts, ts])
                        if ts < span[0]:
                            span[0] = ts
                        if ts > span[1]:
                            span[1] = ts
                        continue
                    m = RE_COMMITTED_R.search(text)
                    if m:
                        rtt, total = int(m.group(3)), int(m.group(4))
                        quorum.setdefault(cid, []).append(rtt)
                        if total > rtt:  # took more than one bus → re-boarded
                            totals.setdefault(cid, []).append(total)
                        span = commit_span.setdefault(cid, [ts, ts])
                        if ts < span[0]:
                            span[0] = ts
                        if ts > span[1]:
                            span[1] = ts
                        continue
                    if RE_NOQUORUM.search(text):
                        attempts[cid] = attempts.get(cid, 0) + 1
                        continue
                    m = RE_REQTIMEOUT.search(text)
                    if m:
                        reboards[cid] = reboards.get(cid, 0) + int(m.group(1))
                        continue
                    if ("starting open-loop data phase" in text or
                            "starting request-gen data phase" in text):
                        send_start.setdefault(cid, ts)
                        continue
                    if (("send to replica" in text or "send bus to replica" in text)
                            and "failed" in text):
                        send_fail.setdefault(cid, ts)  # first failure = last good send
                        continue
                    m = RE_SENT_1S.search(text)
                    if m:
                        sent_per_s.setdefault(cid, []).append(int(m.group(1)))
                        if int(m.group(2)) > 0:  # last second with live commits
                            send_end[cid] = ts
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
        reboard_txt = f"  reboarded={reboards[cid]}" if cid in reboards else ""
        print(f"    committed={n_commits}  resends={n_resends}{reboard_txt}{rate_txt}")
        start = send_start.get(cid)
        end = send_fail.get(cid, send_end.get(cid))  # last good send, else last commit
        # Fallback: under heavy -r load the bus goroutine can stall before it ever
        # emits a "1s:" line (or a failed send), so send_start/send_end are absent.
        # Use the commit span (first->last committed reply) so req/bus throughput
        # still reports — it's the real elapsed time over which commits landed.
        end_label = "last successful send" if cid in send_fail else "last live commit"
        span = commit_span.get(cid)
        if (start is None or end is None) and span and span[1] > span[0]:
            start, end = span
            end_label = "last committed (commit-span fallback)"
        n_buses = len(buses.get(cid, ()))
        if start is not None and end is not None and end > start:
            elapsed = end - start
            t0 = datetime.fromtimestamp(start, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            t1 = datetime.fromtimestamp(end, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            print(f"    send window    {t0} -> {t1} UTC  ({elapsed:.2f}s, "
                  f"data-phase start to {end_label})")
            print(f"    req throughput ={n_commits / elapsed:.1f} req/s "
                  f"(committed/send_window = {n_commits}/{elapsed:.2f}s)")
            if n_buses:  # -r mode: also report the layer-1 (bus) throughput
                print(f"    bus throughput ={n_buses / elapsed:.1f} bus/s "
                      f"(distinct_buses/send_window = {n_buses}/{elapsed:.2f}s)")
                print(f"    batching       ={n_commits / n_buses:.1f} committed reqs/bus "
                      f"({n_commits}/{n_buses})")
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
