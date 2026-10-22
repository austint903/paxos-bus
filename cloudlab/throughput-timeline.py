import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path


PLAN = re.compile(r"schedule-plan first-start=(\d+) pause-start=(\d+) pause-end=(\d+)")
CLIENT = re.compile(r"\[Client (\d+)(?: |\])")


@lru_cache(maxsize=4096)
def second_us(stamp):
    return int(datetime(int(stamp[:4]), int(stamp[4:6]) + 1, int(stamp[6:8]),
                        int(stamp[9:11]), int(stamp[11:13]), int(stamp[13:15]),
                        tzinfo=timezone.utc).timestamp()) * 1_000_000


def timestamp_us(line):
    return second_us(line[:15]) + int(line[16:20]) * 100


def timeline(run_dir, output, bin_us, origin_us=None, duration_us=None, expected_clients=8, start_us=0):
    files = sorted(run_dir.glob("*client-*.log"))
    if len(files) != expected_clients:
        raise ValueError(f"expected {expected_clients} client logs, found {len(files)}")
    starts, identities, plans = [], {}, {}
    for path in files:
        with path.open() as stream:
            for line in stream:
                match = CLIENT.search(line)
                if match:
                    identities[path] = int(match[1])
                plan = PLAN.search(line)
                if plan:
                    plans[identities[path]] = dict(zip(("first_start_ns", "pause_start_ns", "pause_end_ns"),
                                                       map(int, plan.groups())))
                if "sync wait done" in line:
                    starts.append(timestamp_us(line))
                    break
        if path not in identities:
            raise ValueError(f"missing client identity: {path}")
    if len(set(identities.values())) != expected_clients:
        raise ValueError("duplicate client identities")
    source = "explicit Unix timestamp" if origin_us is not None else "earliest client sync wait done"
    if origin_us is None:
        if len(plans) == expected_clients:
            origin_us = min(plan["first_start_ns"] for plan in plans.values()) // 1000
            source = "earliest schedule-plan first-start"
        else:
            if len(starts) != expected_clients:
                raise ValueError("missing sync marker; supply --origin-ns")
            origin_us = min(starts)
    counts = {cid: Counter() for cid in identities.values()}
    last_us, matched, latencies = origin_us, 0, Counter()
    for path, cid in identities.items():
        with path.open() as stream:
            for line in stream:
                if " COMMITTED req=" not in line:
                    continue
                matched += 1
                stamp = timestamp_us(line)
                last_us = max(last_us, stamp)
                offset = stamp - origin_us
                if offset >= start_us and (duration_us is None or offset < start_us + duration_us):
                    counts[cid][(offset - start_us) // bin_us] += 1
                    latencies[int(line.rsplit("total=", 1)[1].split("us", 1)[0])] += 1
    if not matched:
        raise ValueError("no committed requests found")
    if duration_us is None:
        duration_us = max(0, ((last_us - origin_us - start_us) // bin_us + 1) * bin_us)
    clients = sorted(counts)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["time_s", "bin_end_s", "committed_requests", "throughput_req_s"] +
                        [f"client_{cid}_req_s" for cid in clients])
        for start in range(0, duration_us, bin_us):
            end = min(start + bin_us, duration_us)
            values = [counts[cid][start // bin_us] for cid in clients]
            scale = 1_000_000 / (end - start)
            writer.writerow([f"{(start + start_us) / 1e6:.6f}", f"{(end + start_us) / 1e6:.6f}", sum(values),
                             f"{sum(values) * scale:.3f}"] + [f"{n * scale:.3f}" for n in values])
    samples, cumulative, percentiles = sum(latencies.values()), 0, {}
    for latency, count in sorted(latencies.items()):
        cumulative += count
        for name, fraction in (("p50_ms", 0.5), ("p99_ms", 0.99)):
            if name not in percentiles and cumulative >= min(samples, max(1, int((samples + 1) * fraction))):
                percentiles[name] = latency / 1000
    metadata = {"origin_unix_ns": origin_us * 1000, "origin_source": source,
                "log_timezone": "UTC", "log_month_encoding": "zero-based",
                "start_seconds": start_us / 1e6, "bin_seconds": bin_us / 1e6, "duration_seconds": duration_us / 1e6,
                "interval": "[time_s, bin_end_s)", "measurement": "client quorum commit log timestamps",
                "clock_alignment": "shared wall clock; no per-client time shifts",
                "latency_definition": "generation to quorum commit; requests committed inside the selected window",
                "schedule_plans": plans, "latency": {"samples": samples, **percentiles},
                "throughput_req_s": samples * 1e6 / duration_us if duration_us else 0,
                "client_logs": {str(cid): str(path) for path, cid in identities.items()},
                "committed_requests": sum(sum(c.values()) for c in counts.values())}
    output.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def plot_timeline(output, metadata):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with output.open() as stream:
        rows = list(csv.DictReader(stream))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot([float(row["time_s"]) for row in rows],
            [float(row["throughput_req_s"]) / 1000 for row in rows], linewidth=0.8,
            label=f"Committed throughput ({metadata['bin_seconds']:g}s bins)")
    origin = metadata["origin_unix_ns"]
    windows = sorted({((plan["pause_start_ns"] - origin) / 1e9,
                       (plan["pause_end_ns"] - origin) / 1e9)
                      for plan in metadata["schedule_plans"].values() if plan["pause_start_ns"]})
    for index, (start, end) in enumerate(windows):
        ax.axvspan(start, end, color="tab:orange", alpha=0.2,
                   label="Reduced-load schedule" if index == 0 else None)
    ax.set(xlabel="Time since first schedule activation (s)" if
           metadata["origin_source"] == "earliest schedule-plan first-start" else "Time since common origin (s)",
           ylabel="Throughput (kreq/s)", ylim=(0, None))
    ax.set_xlim(metadata["start_seconds"], metadata["start_seconds"] + metadata["duration_seconds"])
    ax.grid(alpha=0.25)
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(output.with_suffix(".png"), dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Bin client quorum commits on a common UTC wall-clock timeline.")
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plot", action="store_true")
    parser.add_argument("--bin-ms", type=int, default=100)
    parser.add_argument("--origin-ns", type=int)
    parser.add_argument("--start-s", type=float, default=0)
    parser.add_argument("--duration-s", type=float)
    parser.add_argument("--expected-clients", type=int, default=8)
    args = parser.parse_args()
    if args.start_s < 0 or args.bin_ms <= 0 or (args.duration_s is not None and args.duration_s <= 0):
        parser.error("bin and duration must be positive")
    output = args.output or args.run_dir / "throughput-100ms.csv"
    result = timeline(args.run_dir, output, args.bin_ms * 1000,
                      args.origin_ns // 1000 if args.origin_ns is not None else None,
                      round(args.duration_s * 1e6) if args.duration_s is not None else None,
                      args.expected_clients, round(args.start_s * 1e6))
    if args.plot:
        plot_timeline(output, result)
    print(f"{output}: {result['committed_requests']} commits, origin={result['origin_unix_ns']}ns")


if __name__ == "__main__":
    main()
