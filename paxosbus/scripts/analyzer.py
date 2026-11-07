import collections
import datetime
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
pattern = re.compile(r"COMMITTED .*total=(\d+)us")
meta = dict(line.split("=", 1) for line in (root / "run-meta.txt").read_text().splitlines())
duration = int(meta["duration_s"])

def timestamp(line):
    day, clock, frac = line.split(" ", 1)[0].split("-")
    date = datetime.date(int(day[:4]), int(day[4:6]) + 1, int(day[6:]))
    return ((date.toordinal() - 719163) * 86400 + int(clock[:2]) * 3600
            + int(clock[2:4]) * 60 + int(clock[4:]) + int(frac) / 10000)

pooled = collections.Counter()

def report(name, counts):
    total = sum(counts.values())
    if not total:
        raise SystemExit(f"No committed requests in {name}")
    ranks = [max(1, min(total, (total + 1) * p // 100)) for p in (50, 95, 99)]
    values = []
    seen = 0
    for latency, count in sorted(counts.items()):
        seen += count
        while len(values) < 3 and seen >= ranks[len(values)]:
            values.append(latency / 1000)
    mean = sum(k * v for k, v in counts.items()) / total / 1000
    print(f"{name}: committed={total} committed/s={total / duration:.1f} mean={mean:.3f}ms "
          f"p50={values[0]:.3f}ms p95={values[1]:.3f}ms p99={values[2]:.3f}ms")

for path in sorted(root.glob("paxosbus-client-*.log")):
    counts = collections.Counter()
    start = None
    with path.open() as stream:
        for line in stream:
            if "sync wait done, starting request-gen data phase" in line:
                start = timestamp(line)
            match = pattern.search(line)
            if match:
                latency = int(match[1])
                committed = timestamp(line)
                generated = committed - latency / 1e6
                if start is None:
                    raise SystemExit(f"Missing data-phase marker in {path.name}")
                if start <= generated < start + duration and committed <= start + duration:
                    counts[latency] += 1
    report(path.stem, counts)
    pooled.update(counts)
report("all-clients", pooled)
