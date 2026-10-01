#!/usr/bin/env python3
"""Runs the FlashBus benchmark suite and writes CSVs under results/.

The sweeps live here rather than in more C++ mains because they differ only in
their arguments: flashbus-bench is the end-to-end benchmark, and a payload
sweep, a rate sweep and a batching sweep are the same program three times.

Nothing in this script invents a number. Every row comes from a binary that
measured it, and every CSV carries the machine description that produced it.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_BUILD = REPO / "build" / "default"


class Runner:
    def __init__(self, build: Path, out: Path, dry_run: bool, quick: bool):
        self.build = build
        self.out = out
        self.dry_run = dry_run
        self.quick = quick
        self.log: list[dict] = []

    def binary(self, *parts: str) -> Path:
        path = self.build.joinpath(*parts)
        if not path.exists():
            sys.exit(f"missing {path}\nbuild first:  CXX=g++ cmake --preset default && "
                     f"cmake --build build/default -j")
        return path

    def run(self, label: str, binary: Path, args: list[str]) -> None:
        command = [str(binary), *args]
        print(f"\n=== {label}\n    {' '.join(command)}", flush=True)
        if self.dry_run:
            return
        started = datetime.datetime.now(datetime.timezone.utc)
        result = subprocess.run(command, capture_output=True, text=True)
        print(result.stdout, end="")
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr, end="")
            sys.exit(f"{label} failed with exit code {result.returncode}")
        (self.out / "logs").mkdir(parents=True, exist_ok=True)
        safe = label.replace("/", "_").replace(" ", "_")
        (self.out / "logs" / f"{safe}.txt").write_text(result.stdout + result.stderr)
        self.log.append({
            "label": label,
            "command": command,
            "started_utc": started.isoformat(),
            "seconds": (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds(),
        })

    def scale(self, full: int, quick: int) -> int:
        return quick if self.quick else full

    def messages_for(self, rate: int, seconds: float) -> int:
        """Message count that makes a paced run last roughly `seconds`.

        Sizing by count rather than duration is how a 100k msg/s run ends up
        taking twenty times as long as a 2M msg/s run for no extra information.
        """
        target = int(rate * seconds) if rate > 0 else int(2_000_000 * seconds / 5)
        return max(200_000, int(target * (0.25 if self.quick else 1.0)))


def clock(runner: Runner) -> None:
    """First, because every later number is two clock reads wide."""
    runner.run("00_clock", runner.binary("benchmarks", "clock_bench"),
               ["--samples", str(runner.scale(20_000_000, 2_000_000))])


def queues(runner: Runner) -> None:
    """Case study 1, plus the false-sharing and cache-locality variants."""
    bench = runner.binary("benchmarks", "queue_bench")
    csv = str(runner.out / "queue.csv")
    messages = runner.scale(10_000_000, 1_000_000)
    repeat = str(runner.scale(3, 2))

    # Saturated: how fast the handoff can go, and what the latency looks like
    # when a queue that cannot keep up simply stays full.
    runner.run("01_queue_saturated", bench,
               ["--messages", str(messages), "--capacity", "4096", "--repeat", repeat,
                "--payload", "16,48,112,240,1008", "--csv", csv])

    # Paced below the mutex queue's saturation point, so both queues stay
    # shallow and the latency number is the handoff cost rather than the depth.
    runner.run("02_queue_paced", bench,
               ["--messages", str(runner.messages_for(1_000_000, 3)), "--capacity", "4096",
                "--rate", "1000000", "--repeat", repeat, "--mode", "latency",
                "--payload", "16,48,112,240,1008", "--csv", csv])

    # Same payload, different ring capacity: shows where the ring stops fitting
    # in cache (48 B user bytes means a 64 B slot, so capacity x 64 = bytes).
    for capacity in (64, 1024, 65536, 1048576):
        runner.run(f"03_queue_capacity_{capacity}", bench,
                   ["--messages", str(messages), "--capacity", str(capacity),
                    "--repeat", repeat, "--mode", "throughput", "--payload", "48", "--csv", csv])

    # The false-sharing experiment, controlled. Threads pinned so scheduler
    # placement is not a free variable, slot size fixed at 128 B, and only the
    # ring's footprint changes. The padded and unpadded variants respond to that
    # sweep completely differently, which is what identifies the effect.
    fs_csv = str(runner.out / "false_sharing.csv")
    for capacity in (256, 1024, 4096, 16384, 65536, 262144):
        runner.run(f"04_false_sharing_cap_{capacity}", bench,
                   ["--messages", str(messages), "--capacity", str(capacity),
                    "--repeat", repeat, "--mode", "throughput", "--payload", "112",
                    "--producer-cpu", "2", "--consumer-cpu", "4", "--csv", fs_csv])


def allocation(runner: Runner) -> None:
    """Case study 2: new/delete per order against a preallocated pool."""
    runner.run("03_allocation", runner.binary("benchmarks", "alloc_bench"),
               ["--events", str(runner.scale(5_000_000, 1_000_000)),
                "--csv", str(runner.out / "allocation.csv")])


def affinity(runner: Runner) -> None:
    runner.run("04_affinity", runner.binary("benchmarks", "affinity_bench"),
               ["--messages", str(runner.scale(5_000_000, 1_000_000)),
                "--csv", str(runner.out / "affinity.csv")])


def end_to_end(runner: Runner) -> None:
    """Payload and rate sweeps through the real TCP path."""
    bench = runner.binary("apps", "flashbus-bench")
    csv = str(runner.out / "end_to_end.csv")

    for payload in (32, 64, 128, 256, 1024):
        # The 64-byte run also dumps the full histogram, so the distribution
        # chart shows the whole shape rather than five percentiles.
        extra = (["--histogram", str(runner.out / "latency_histogram.csv")]
                 if payload == 64 else [])
        runner.run(f"05_payload_{payload}B", bench,
                   ["--messages", str(runner.messages_for(200_000, 5)), "--payload", str(payload),
                    "--rate", "200000", "--variant", f"payload-{payload}B", "--csv", csv] + extra)

    # The overload curve: latency, loss and queue depth against offered load.
    #
    # Four publishers, not one. A publisher issuing one write() per 64-byte
    # event tops out near 400k msg/s on its syscall rate alone, so a single
    # publisher cannot offer the broker more load than it can take and the
    # sweep comes out perfectly flat with no drops -- a measurement of the load
    # generator, not of FlashBus.
    overload_csv = str(runner.out / "overload.csv")
    for rate in (200_000, 400_000, 800_000, 1_200_000, 1_600_000, 2_400_000, 0):
        label = "unthrottled" if rate == 0 else f"{rate // 1000}k"
        runner.run(f"06_rate_{label}", bench,
                   ["--messages", str(runner.messages_for(rate, 5)), "--payload", "64",
                    "--producers", "4", "--rate", str(rate),
                    "--variant", f"rate-{label}", "--csv", overload_csv])

    # Where the broker's own ceiling is, and what sets it. Publisher batching
    # changes how many frames arrive per read, which is the quantity the
    # broker's throughput actually depends on.
    ceiling_csv = str(runner.out / "ceiling.csv")
    for producers, batch in ((1, 1), (1, 8), (1, 32), (2, 8), (4, 1), (4, 8)):
        runner.run(f"06b_ceiling_{producers}p_batch{batch}", bench,
                   ["--messages", str(runner.messages_for(0, 10)), "--payload", "64",
                    "--rate", "0", "--producers", str(producers),
                    "--publisher-batch", str(batch),
                    "--variant", f"{producers}p-batch{batch}", "--csv", ceiling_csv])

    # Fan-out: one topic, several publishers and subscribers.
    fanout_csv = str(runner.out / "fanout.csv")
    for producers, consumers in ((1, 1), (1, 2), (1, 4), (2, 2), (4, 1), (4, 4)):
        runner.run(f"07_fanout_{producers}p{consumers}c", bench,
                   ["--messages", str(runner.messages_for(400_000, 5)), "--payload", "64",
                    "--rate", "400000", "--producers", str(producers),
                    "--consumers", str(consumers), "--variant", f"{producers}p{consumers}c",
                    "--csv", fanout_csv])


def batching(runner: Runner) -> None:
    """Case study 3: the throughput/latency trade in one table."""
    bench = runner.binary("apps", "flashbus-bench")
    csv = str(runner.out / "batching.csv")
    messages = str(runner.messages_for(500_000, 5))
    for batch in (1, 8, 32, 128):
        runner.run(f"08_batch_{batch}", bench,
                   ["--messages", messages, "--payload", "64", "--rate", "500000",
                    "--egress-batch", str(batch), "--variant", f"fixed-batch-{batch}",
                    "--csv", csv])
    runner.run("08_batch_adaptive", bench,
               ["--messages", messages, "--payload", "64", "--rate", "500000",
                "--egress-batch", "32", "--adaptive-batching",
                "--variant", "adaptive-batch", "--csv", csv])
    # Batching matters most when the link is saturated, so repeat unthrottled.
    for batch in (1, 32, 128):
        runner.run(f"08_batch_{batch}_saturated", bench,
                   ["--messages", str(runner.messages_for(0, 5)), "--payload", "64", "--rate", "0",
                    "--egress-batch", str(batch), "--variant", f"saturated-batch-{batch}",
                    "--csv", str(runner.out / "batching_saturated.csv")])


def spin_vs_park(runner: Runner) -> None:
    """What giving the core back costs in latency."""
    bench = runner.binary("apps", "flashbus-bench")
    csv = str(runner.out / "spin_vs_park.csv")
    messages = str(runner.messages_for(200_000, 5))
    for spin_us in (0, 50, 500, 5000):
        runner.run(f"09_idle_spin_{spin_us}us", bench,
                   ["--messages", messages, "--payload", "64", "--rate", "200000",
                    "--idle-spin-us", str(spin_us), "--variant", f"idle-spin-{spin_us}us",
                    "--csv", csv])


def market_data(runner: Runner) -> None:
    demo = runner.binary("examples", "market_data", "market-data-demo")
    csv = str(runner.out / "market_data.csv")
    runner.run("10_market_steady", demo,
               ["--events", str(runner.messages_for(400_000, 5)), "--rate", "400000",
                "--csv", csv])
    runner.run("11_market_burst", demo,
               ["--events", str(runner.messages_for(400_000, 5)), "--rate", "200000",
                "--burst", "--csv", csv])
    runner.run("12_market_burst_small_queue", demo,
               ["--events", str(runner.messages_for(400_000, 5)), "--rate", "200000",
                "--burst", "--egress-capacity", "1024", "--csv", csv])
    runner.run("13_market_slow_consumer", demo,
               ["--events", str(runner.messages_for(200_000, 5)), "--rate", "200000",
                "--slow-consumer-us", "50", "--csv", csv])
    runner.run("14_market_heap_store", demo,
               ["--events", str(runner.messages_for(400_000, 5)), "--rate", "400000",
                "--pool=0", "--csv", csv])


def sustained(runner: Runner) -> None:
    runner.run("15_sustained", runner.binary("benchmarks", "sustained_bench"),
               ["--seconds", str(runner.scale(300, 20)), "--rate", "300000",
                "--payload", "64", "--interval", str(runner.scale(15, 5)),
                "--csv", str(runner.out / "sustained.csv")])


SUITES = {
    "clock": clock,
    "queues": queues,
    "allocation": allocation,
    "affinity": affinity,
    "end_to_end": end_to_end,
    "batching": batching,
    "spin": spin_vs_park,
    "market": market_data,
    "sustained": sustained,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build", type=Path, default=DEFAULT_BUILD)
    parser.add_argument("--out", type=Path, default=REPO / "results" / "latest")
    parser.add_argument("--suite", action="append", choices=sorted(SUITES),
                        help="run only these suites (repeatable); default is all")
    parser.add_argument("--quick", action="store_true",
                        help="shorter runs, for checking the harness rather than measuring")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep", action="store_true",
                        help="append to an existing results directory instead of replacing it")
    args = parser.parse_args()

    if args.out.exists() and not args.keep and not args.dry_run:
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True, exist_ok=True)

    runner = Runner(args.build, args.out, args.dry_run, args.quick)
    for name in (args.suite or sorted(SUITES)):
        SUITES[name](runner)

    if not args.dry_run:
        (args.out / "manifest.json").write_text(json.dumps({
            "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "quick": args.quick,
            "build": str(args.build),
            "runs": runner.log,
        }, indent=2) + "\n")
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
