#!/usr/bin/env python3
"""Compares a fresh benchmark run against the committed baseline.

The baseline is `results/latest/` as committed to the repository. Run the
benchmarks into a different directory and point this at both.

It deliberately does not fail on small differences. This machine's run-to-run
spread reaches 3x on some configurations, so a tight threshold here would cry
wolf constantly and get ignored, which is worse than having no check. The
default threshold is 40%, and the point is to notice a change of regime -- a
queue that lost an order of magnitude, a latency that doubled -- not to police
noise.

usage:
  python3 scripts/run_benchmarks.py --out results/candidate
  python3 scripts/check_regressions.py --candidate results/candidate
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Which column matters for which benchmark, and which direction is bad.
# "higher_is_better" says how to read a change, not how large it has to be.
METRICS = [
    ("throughput_msg_s", "throughput", True),
    ("p50_ns", "p50", False),
    ("p99_ns", "p99", False),
    ("p999_ns", "p99.9", False),
]

# Keys that identify a comparable configuration across runs.
IDENTITY = ("benchmark", "variant", "payload_bytes", "producers", "consumers",
            "capacity", "target_rate")


def load(directory: Path) -> dict[tuple, list[dict]]:
    rows: dict[tuple, list[dict]] = defaultdict(list)
    for path in sorted(directory.glob("*.csv")):
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if not all(k in row for k in IDENTITY):
                    continue  # sustained.csv has its own shape
                rows[tuple(row[k] for k in IDENTITY)].append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, default=REPO / "results" / "latest")
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--threshold", type=float, default=40.0,
                        help="percent change worth reporting (default 40)")
    parser.add_argument("--fail", action="store_true",
                        help="exit non-zero when a regression is found")
    args = parser.parse_args()

    baseline = load(args.baseline)
    candidate = load(args.candidate)
    if not baseline:
        sys.exit(f"no comparable rows in {args.baseline}")
    if not candidate:
        sys.exit(f"no comparable rows in {args.candidate}")

    regressions: list[str] = []
    improvements: list[str] = []
    missing = 0

    for key in sorted(baseline):
        if key not in candidate:
            missing += 1
            continue
        for column, label, higher_is_better in METRICS:
            before = statistics.median(float(r[column]) for r in baseline[key])
            after = statistics.median(float(r[column]) for r in candidate[key])
            if before == 0.0:
                continue
            change = (after - before) / before * 100.0
            if abs(change) < args.threshold:
                continue
            worse = (change < 0) if higher_is_better else (change > 0)
            name = f"{key[0]} / {key[1]}"
            detail = (f"{name}: {label} {before:,.0f} -> {after:,.0f} "
                      f"({change:+.0f}%)")
            (regressions if worse else improvements).append(detail)

    print(f"compared {len(baseline)} baseline configurations against "
          f"{args.candidate}, threshold {args.threshold:.0f}%")
    if missing:
        print(f"  {missing} baseline configurations absent from the candidate run")

    if improvements:
        print(f"\nimproved ({len(improvements)}):")
        for line in improvements:
            print(f"  {line}")
    if regressions:
        print(f"\nREGRESSED ({len(regressions)}):")
        for line in regressions:
            print(f"  {line}")
    if not regressions and not improvements:
        print("\nno change beyond the threshold")

    print("\nBefore acting on any of this: re-run the candidate. A single run on a shared\n"
          "machine can move a number by more than this threshold on its own, and the\n"
          "benchmarks that repeat internally report their spread for exactly that reason.")

    if regressions and args.fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
