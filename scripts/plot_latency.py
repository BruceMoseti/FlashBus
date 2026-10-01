#!/usr/bin/env python3
"""Turns the CSVs under results/ into the charts the README refers to.

Every chart is drawn from a measured CSV. There is no hard-coded data anywhere
in this file, which is the point: a chart that does not come from a run is a
drawing, and a drawing has no place in a performance report.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib is required:  pip install matplotlib")

REPO = Path(__file__).resolve().parent.parent
PERCENTILES = [("p50_ns", "p50"), ("p95_ns", "p95"), ("p99_ns", "p99"), ("p999_ns", "p99.9")]


def read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def us(row: dict, column: str) -> float:
    return float(row[column]) / 1000.0


def finish(figure, axes, path: Path, title: str, note: str | None = None) -> None:
    axes.set_title(title)
    axes.grid(True, alpha=0.3, linestyle=":")
    if note:
        figure.text(0.5, -0.02, note, ha="center", fontsize=8, color="#555")
    figure.tight_layout()
    figure.savefig(path, dpi=140, bbox_inches="tight")
    plt.close(figure)
    print(f"  {path.name}")


def queue_comparison(rows: list[dict], out: Path) -> None:
    """Mutex baseline against the SPSC ring, by payload size."""
    throughput = [r for r in rows if r["benchmark"] == "queue_throughput"]
    latency = [r for r in rows if r["benchmark"] == "queue_latency"]
    if not throughput:
        return

    by_variant: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in throughput:
        by_variant[row["variant"]].append(
            (int(row["payload_bytes"]), float(row["throughput_msg_s"]) / 1e6))

    figure, axes = plt.subplots(figsize=(7, 4))
    for variant in sorted(by_variant):
        points = sorted(set(by_variant[variant]))
        axes.plot([p[0] for p in points], [p[1] for p in points], marker="o", label=variant)
    axes.set_xscale("log", base=2)
    axes.set_xlabel("slot size (bytes)")
    axes.set_ylabel("throughput (M msg/s)")
    axes.legend()
    finish(figure, axes, out / "queue_throughput.png",
           "Queue throughput: mutex baseline vs SPSC ring")

    if not latency:
        return
    variants = sorted({r["variant"] for r in latency})
    figure, axes = plt.subplots(figsize=(7, 4))
    width = 0.8 / max(len(variants), 1)
    for index, variant in enumerate(variants):
        row = next(r for r in latency if r["variant"] == variant)
        offsets = [i + index * width for i in range(len(PERCENTILES))]
        axes.bar(offsets, [us(row, column) for column, _ in PERCENTILES], width, label=variant)
    axes.set_xticks([i + 0.4 - width / 2 for i in range(len(PERCENTILES))])
    axes.set_xticklabels([name for _, name in PERCENTILES])
    axes.set_yscale("log")
    axes.set_ylabel("latency (us, log scale)")
    axes.legend()
    finish(figure, axes, out / "queue_latency.png",
           "Queue handoff latency by percentile",
           "includes two steady_clock reads per message; see clock_bench for their cost")


def allocation(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    variants = sorted({r["variant"] for r in rows})
    figure, axes = plt.subplots(figsize=(7, 4))
    width = 0.8 / max(len(variants), 1)
    for index, variant in enumerate(variants):
        row = next(r for r in rows if r["variant"] == variant)
        offsets = [i + index * width for i in range(len(PERCENTILES))]
        axes.bar(offsets, [us(row, column) for column, _ in PERCENTILES], width, label=variant)
    axes.set_xticks([i + 0.4 - width / 2 for i in range(len(PERCENTILES))])
    axes.set_xticklabels([name for _, name in PERCENTILES])
    axes.set_yscale("log")
    axes.set_ylabel("order-book apply latency (us, log scale)")
    axes.legend()
    finish(figure, axes, out / "allocation.png",
           "Order-book apply: new/delete per order vs preallocated pool")


def overload(rows: list[dict], out: Path) -> None:
    """The curve that shows where the system saturates, and what it does there."""
    paced = sorted((r for r in rows if int(r["target_rate"]) > 0),
                   key=lambda r: int(r["target_rate"]))
    if not paced:
        return
    offered = [int(r["target_rate"]) / 1e6 for r in paced]

    figure, axes = plt.subplots(1, 3, figsize=(14, 4))
    axes[0].plot(offered, [float(r["throughput_msg_s"]) / 1e6 for r in paced], marker="o")
    axes[0].plot(offered, offered, linestyle="--", color="#999", label="offered = delivered")
    axes[0].set_xlabel("offered load (M msg/s)")
    axes[0].set_ylabel("delivered (M msg/s)")
    axes[0].set_title("Throughput")
    axes[0].legend(fontsize=8)

    for column, name in PERCENTILES:
        axes[1].plot(offered, [us(r, column) for r in paced], marker="o", label=name)
    axes[1].set_xlabel("offered load (M msg/s)")
    axes[1].set_ylabel("latency (us)")
    axes[1].set_yscale("log")
    axes[1].set_title("Latency")
    axes[1].legend(fontsize=8)

    axes[2].plot(offered, [int(r["messages_dropped"]) for r in paced], marker="o", color="#c0392b")
    axes[2].set_xlabel("offered load (M msg/s)")
    axes[2].set_ylabel("events dropped")
    axes[2].set_title("Loss")

    for axis in axes:
        axis.grid(True, alpha=0.3, linestyle=":")
    figure.suptitle("Overload behaviour: bounded queues plus a drop policy")
    figure.tight_layout()
    figure.savefig(out / "overload.png", dpi=140, bbox_inches="tight")
    plt.close(figure)
    print("  overload.png")


def payload_sweep(rows: list[dict], out: Path) -> None:
    rows = sorted((r for r in rows if r["benchmark"] == "end_to_end"),
                  key=lambda r: int(r["payload_bytes"]))
    if not rows:
        return
    sizes = [int(r["payload_bytes"]) for r in rows]
    figure, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].plot(sizes, [float(r["throughput_msg_s"]) / 1e6 for r in rows], marker="o")
    axes[0].set_xscale("log", base=2)
    axes[0].set_xlabel("payload (bytes)")
    axes[0].set_ylabel("delivered (M msg/s)")
    axes[0].set_title("Throughput vs payload")
    for column, name in PERCENTILES:
        axes[1].plot(sizes, [us(r, column) for r in rows], marker="o", label=name)
    axes[1].set_xscale("log", base=2)
    axes[1].set_xlabel("payload (bytes)")
    axes[1].set_ylabel("latency (us)")
    axes[1].set_title("Latency vs payload")
    axes[1].legend(fontsize=8)
    for axis in axes:
        axis.grid(True, alpha=0.3, linestyle=":")
    figure.suptitle("End-to-end over TCP, 1 publisher / 1 subscriber")
    figure.tight_layout()
    figure.savefig(out / "payload_sweep.png", dpi=140, bbox_inches="tight")
    plt.close(figure)
    print("  payload_sweep.png")


def batching(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    def sort_key(row: dict) -> tuple[int, int]:
        variant = row["variant"]
        if "adaptive" in variant:
            return (1, 0)
        return (0, int(variant.rsplit("-", 1)[-1]))

    rows = sorted(rows, key=sort_key)
    labels = [r["variant"].replace("fixed-batch-", "batch ").replace("adaptive-batch", "adaptive")
              for r in rows]
    figure, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].bar(labels, [float(r["throughput_msg_s"]) / 1e6 for r in rows], color="#2980b9")
    axes[0].set_ylabel("delivered (M msg/s)")
    axes[0].set_title("Throughput")
    positions = range(len(rows))
    width = 0.8 / len(PERCENTILES)
    for index, (column, name) in enumerate(PERCENTILES):
        axes[1].bar([p + index * width for p in positions],
                    [us(r, column) for r in rows], width, label=name)
    axes[1].set_xticks([p + 0.4 - width / 2 for p in positions])
    axes[1].set_xticklabels(labels)
    axes[1].set_ylabel("latency (us)")
    axes[1].set_title("Latency")
    axes[1].legend(fontsize=8)
    for axis in axes:
        axis.grid(True, alpha=0.3, linestyle=":")
    figure.suptitle("Egress batching: frames coalesced into one write()")
    figure.tight_layout()
    figure.savefig(out / "batching.png", dpi=140, bbox_inches="tight")
    plt.close(figure)
    print("  batching.png")


def sustained(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    at = [float(r["at_s"]) for r in rows]
    figure, axes = plt.subplots(1, 3, figsize=(14, 4))
    for column, name in (("p50_ns", "p50"), ("p99_ns", "p99"), ("p999_ns", "p99.9")):
        axes[0].plot(at, [us(r, column) for r in rows], marker="o", label=name)
    axes[0].set_ylabel("latency (us)")
    axes[0].set_title("Latency over time")
    axes[0].legend(fontsize=8)
    axes[1].plot(at, [float(r["resident_kib"]) / 1024.0 for r in rows], marker="o", color="#8e44ad")
    axes[1].set_ylabel("resident memory (MiB)")
    axes[1].set_title("Memory over time")
    axes[2].plot(at, [int(r["allocations"]) for r in rows], marker="o", color="#16a085")
    axes[2].set_ylabel("heap allocations per interval")
    axes[2].set_title("Allocation over time")
    for axis in axes:
        axis.set_xlabel("elapsed (s)")
        axis.grid(True, alpha=0.3, linestyle=":")
    figure.suptitle("Sustained load: what a five-second benchmark cannot show")
    figure.tight_layout()
    figure.savefig(out / "sustained.png", dpi=140, bbox_inches="tight")
    plt.close(figure)
    print("  sustained.png")


def spin_vs_park(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda r: int(r["variant"].split("-")[-1].removesuffix("us")))
    labels = [r["variant"].replace("idle-spin-", "") for r in rows]
    figure, axes = plt.subplots(figsize=(7, 4))
    for column, name in PERCENTILES:
        axes.plot(labels, [us(r, column) for r in rows], marker="o", label=name)
    axes.set_xlabel("idle spin window before parking")
    axes.set_ylabel("latency (us)")
    twin = axes.twinx()
    twin.plot(labels, [float(r["cpu_cores_used"]) for r in rows], marker="s",
              linestyle="--", color="#7f8c8d", label="CPU cores")
    twin.set_ylabel("CPU cores used")
    axes.legend(loc="upper right", fontsize=8)
    twin.legend(loc="center right", fontsize=8)
    finish(figure, axes, out / "spin_vs_park.png",
           "Spinning versus giving the core back")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=REPO / "results" / "latest")
    args = parser.parse_args()
    if not args.results.exists():
        sys.exit(f"{args.results} does not exist; run scripts/run_benchmarks.py first")

    print(f"charts from {args.results}:")
    queue_comparison(read(args.results / "queue.csv"), args.results)
    allocation(read(args.results / "allocation.csv"), args.results)
    overload(read(args.results / "overload.csv"), args.results)
    payload_sweep(read(args.results / "end_to_end.csv"), args.results)
    batching(read(args.results / "batching.csv"), args.results)
    sustained(read(args.results / "sustained.csv"), args.results)
    spin_vs_park(read(args.results / "spin_vs_park.csv"), args.results)


if __name__ == "__main__":
    main()
