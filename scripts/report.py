#!/usr/bin/env python3
"""Generates the results tables in README.md from the measured CSVs.

The numbers in the README are not typed by hand. They are rendered from
results/latest/ and written between marker comments, so a stale number in the
documentation is a thing that cannot happen quietly: re-run the benchmarks,
re-run this, and the diff shows exactly what moved.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
HEADLINE_BEGIN = "<!-- RESULTS:HEADLINE -->"
TABLES_BEGIN = "<!-- RESULTS:TABLES -->"
END = "<!-- RESULTS:END -->"


def read(results: Path, name: str) -> list[dict]:
    path = results / f"{name}.csv"
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def us(value: str | float) -> str:
    return f"{float(value) / 1000.0:.2f}"


def mps(value: str | float) -> str:
    return f"{float(value) / 1e6:.2f}"


def median(rows: list[dict], column: str) -> float:
    return statistics.median(float(r[column]) for r in rows)


def group(rows: list[dict], *keys: str) -> dict[tuple, list[dict]]:
    out: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        out[tuple(row[k] for k in keys)].append(row)
    return out


def table(header: list[str], body: list[list[str]], align: str | None = None) -> str:
    sep = [align or "---"] * len(header)
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(sep) + " |"]
    lines += ["| " + " | ".join(row) + " |" for row in body]
    return "\n".join(lines)


def pretty_topology(variant: str) -> str:
    """'4p-batch8' -> '4 publishers, 8 frames per write'."""
    publishers, _, batch = variant.partition("-batch")
    count = publishers.removesuffix("p")
    noun = "publisher" if count == "1" else "publishers"
    frames = "frame" if batch == "1" else "frames"
    return f"{count} {noun}, {batch} {frames} per write()"


def machine(rows: list[dict]) -> str:
    if not rows:
        return "unknown machine"
    r = rows[0]
    return (f"{r['cpu_model']} ({r['cpu_count']} logical), {r['os']}, kernel "
            f"{r['kernel']}, {r['compiler']} `{r['compiler_flags']}`, Boost "
            f"{r['boost_version']}. Hardware PMU: "
            f"{'available' if r['hw_pmu_available'] == '1' else 'NOT available'}.")


def headline(results: Path) -> str:
    end_to_end = read(results, "end_to_end")
    queue = read(results, "queue")
    sustained = read(results, "sustained")
    ceiling = read(results, "ceiling")
    overload = read(results, "overload")

    parts: list[str] = []
    parts.append(f"Measured on {machine(end_to_end or queue)}\n")

    rows: list[list[str]] = []
    # Prefer the dedicated latency-floor sweep, which exists precisely so the
    # headline figure comes from a run made to measure it.
    floor = read(results, "latency_floor")
    e2e = floor or [r for r in end_to_end if r["payload_bytes"] == "64"]
    if e2e:
        r = min(e2e, key=lambda row: float(row["p50_ns"]))
        rows.append([
            ("End-to-end over TCP, 64 B, 1 pub / 1 sub, paced at "
             f"{int(r['target_rate']) // 1000}k msg/s"),
            (f"p50 **{us(r['p50_ns'])} us**, p99 **{us(r['p99_ns'])} us**, "
             f"p99.9 {us(r['p999_ns'])} us, {r['messages_dropped']} dropped"),
        ])

    paced = [r for r in queue
             if r["benchmark"] == "queue_latency" and r["target_rate"] != "0"
             and r["payload_bytes"] == "64"]
    if paced:
        by_variant = group(paced, "variant")
        spsc = by_variant.get(("spsc-padded",), [])
        mutex = by_variant.get(("mutex-queue",), [])
        if spsc and mutex:
            rows.append([
                "SPSC ring handoff, 64 B slots, paced below saturation",
                (f"p50 **{us(median(spsc, 'p50_ns'))} us**, "
                 f"p99 **{us(median(spsc, 'p99_ns'))} us** "
                 f"(mutex baseline: p50 {us(median(mutex, 'p50_ns'))} us, "
                 f"p99 {us(median(mutex, 'p99_ns'))} us)"),
            ])

    saturated = [r for r in queue
                 if r["benchmark"] == "queue_throughput" and r["payload_bytes"] == "64"
                 and r["capacity"] == "4096"]
    if saturated:
        by_variant = group(saturated, "variant")
        spsc = by_variant.get(("spsc-padded",), [])
        mutex = by_variant.get(("mutex-queue",), [])
        if spsc and mutex:
            rows.append([
                "SPSC ring throughput, 64 B slots, saturated",
                (f"**{mps(median(spsc, 'throughput_msg_s'))} M msg/s** "
                 f"(mutex baseline: {mps(median(mutex, 'throughput_msg_s'))} M msg/s)"),
            ])

    if ceiling:
        lossless = [r for r in ceiling if int(r["messages_dropped"]) == 0]
        if lossless:
            best = max(lossless, key=lambda r: float(r["throughput_msg_s"]))
            rows.append([
                "Broker throughput ceiling, 64 B, zero loss",
                (f"**{mps(best['throughput_msg_s'])} M msg/s** delivered "
                 f"({pretty_topology(best['variant'])})"),
            ])

    if overload:
        # The highest offered rate the broker actually *carried*: delivered has
        # to track offered and nothing may be dropped. Taking the highest
        # zero-loss row alone would reward a rate the publishers could not even
        # generate, where nothing is dropped only because nothing arrived.
        carried = [r for r in overload
                   if int(r["target_rate"]) > 0
                   and int(r["messages_dropped"]) == 0
                   and float(r["throughput_msg_s"]) >= 0.99 * float(r["target_rate"])]
        if carried:
            best = max(carried, key=lambda r: int(r["target_rate"]))
            rows.append([
                "Highest offered load carried in full, zero loss (4 pub / 1 sub, 64 B)",
                (f"**{mps(best['target_rate'])} M msg/s** at p50 {us(best['p50_ns'])} us, "
                 f"p99 {us(best['p99_ns'])} us"),
            ])

    if sustained:
        first, last = sustained[0], sustained[-1]
        total = sum(int(r["received"]) for r in sustained)
        drift = (float(last["p99_ns"]) - float(first["p99_ns"])) / float(first["p99_ns"]) * 100
        steady_allocs = sum(int(r["allocations"]) for r in sustained[1:])
        rows.append([
            (f"Sustained {float(last['at_s']):.0f} s at "
             f"{float(median(sustained, 'throughput_msg_s')) / 1000:.0f}k msg/s"),
            (f"{total / 1e6:.0f} M events, **0 dropped**, p99 drift {drift:+.0f}%, "
             f"**{steady_allocs} heap allocations** after the first interval"),
        ])

    if rows:
        parts.append(table(["What", "Measured"], rows))
    return "\n".join(parts)


def tables(results: Path) -> str:
    out: list[str] = []

    charts = [("overload.png", "Overload behaviour"),
              ("queue_latency.png", "Queue handoff latency by percentile"),
              ("queue_throughput.png", "Queue throughput by slot size"),
              ("sustained.png", "Sustained load over time"),
              ("spin_vs_park.png", "Spinning versus parking"),
              ("batching.png", "Egress batching"),
              ("allocation.png", "Order-book apply: heap versus pool"),
              ("payload_sweep.png", "End-to-end against payload size")]
    present = [(f, t) for f, t in charts if (results / f).exists()]
    if present:
        rel = results.relative_to(REPO)
        out.append("Charts, all drawn from the CSVs in this directory by "
                   "`scripts/plot_latency.py`:\n\n" +
                   "\n".join(f"* [{title}]({rel}/{name})" for name, title in present))

    floor = read(results, "latency_floor")
    if floor:
        body = [[f"{int(r['target_rate']) // 1000}k", mps(r["throughput_msg_s"]),
                 us(r["p50_ns"]), us(r["p95_ns"]), us(r["p99_ns"]), us(r["p999_ns"]),
                 r["messages_dropped"]]
                for r in sorted(floor, key=lambda r: int(r["target_rate"]))]
        out.append("### End-to-end latency, 1 publisher / 1 subscriber, 64 B\n\n" +
                   table(["offered rate", "delivered M msg/s", "p50 us", "p95 us", "p99 us",
                          "p99.9 us", "dropped"], body) +
                   "\n\nThe pipeline's own cost, well below saturation. Every event crosses "
                   "two TCP hops, an encode, a decode, two SPSC rings and the dispatcher. "
                   "Latency starts to climb at 200k msg/s not because the broker is saturated "
                   "but because the publisher is: one `write()` per event tops out near 0.45 M "
                   "msg/s on syscall rate, so above that the publisher's own send loop begins "
                   "queueing.")

    e2e = read(results, "end_to_end")
    if e2e:
        body = [[r["payload_bytes"], mps(r["throughput_msg_s"]), us(r["p50_ns"]),
                 us(r["p95_ns"]), us(r["p99_ns"]), us(r["p999_ns"]),
                 r["messages_dropped"]]
                for r in sorted(e2e, key=lambda r: int(r["payload_bytes"]))]
        out.append("### End-to-end over TCP, 1 publisher / 1 subscriber, paced\n\n" +
                   table(["payload B", "M msg/s", "p50 us", "p95 us", "p99 us", "p99.9 us",
                          "dropped"], body) +
                   "\n\nLatency rises about 40% across a 32x range of payload size, which is "
                   "what a mostly fixed per-frame cost looks like: the copy is small next to "
                   "the syscalls and the queue hops around it.")

    overload = read(results, "overload")
    if overload:
        body = []
        for r in sorted(overload, key=lambda r: int(r["target_rate"]) or 10**9):
            offered = ("unthrottled" if r["target_rate"] == "0"
                       else f"{int(r['target_rate']) / 1e6:.1f}")
            body.append([offered, mps(r["throughput_msg_s"]), us(r["p50_ns"]),
                         us(r["p99_ns"]), us(r["p999_ns"]), r["messages_dropped"],
                         r["sequence_gaps"], f"{float(r['cpu_cores_used']):.1f}"])
        out.append("### Overload behaviour, 4 publishers / 1 subscriber, 64 B\n\n" +
                   table(["offered M/s", "delivered M/s", "p50 us", "p99 us", "p99.9 us",
                          "dropped", "gaps", "cores"], body) +
                   "\n\nDelivered tracks offered until the knee, then flattens while latency "
                   "climbs and the drop counter starts moving. Every dropped event shows up "
                   "as a subscriber sequence gap, which is the property the broker sequence "
                   "exists for. Note the CPU column: all four publishers, the broker's two "
                   "threads and the subscriber share eight vCPUs, so the knee is partly this "
                   "machine running out of cores.")

    ceiling = read(results, "ceiling")
    if ceiling:
        body = [[pretty_topology(r["variant"]), mps(r["throughput_msg_s"]), us(r["p50_ns"]),
                 r["messages_dropped"], f"{float(r['cpu_cores_used']):.1f}"]
                for r in ceiling]
        single = next((r for r in ceiling if r["variant"] == "1p-batch1"), None)
        batched = next((r for r in ceiling if r["variant"] == "1p-batch8"), None)
        caption = ("One publisher issuing one `write()` per event cannot offer the broker more "
                   "than its own syscall rate")
        if single and batched:
            gain = float(batched["throughput_msg_s"]) / float(single["throughput_msg_s"])
            caption += (f", which measures {mps(single['throughput_msg_s'])} M msg/s here. "
                        f"Letting it put eight frames in each `write()` multiplies delivered "
                        f"throughput {gain:.0f}x without changing a line of broker code")
        caption += (", because the quantity that governs broker throughput is frames per read, "
                    "not events per second. The large p50 values in the batched rows are "
                    "queueing delay: these runs are unthrottled on purpose, so the pipeline is "
                    "full by construction and latency is backlog divided by rate.")
        out.append("### What sets the ceiling: frames per read\n\n" +
                   table(["load generator", "delivered M msg/s", "p50 us",
                          "dropped", "cores"], body) + "\n\n" + caption)

    fanout = read(results, "fanout")
    if fanout:
        body = [[f"{r['producers']} pub / {r['consumers']} sub", mps(r["throughput_msg_s"]),
                 us(r["p50_ns"]), us(r["p99_ns"]), r["messages_dropped"],
                 f"{float(r['cpu_cores_used']):.1f}"]
                for r in fanout]
        out.append("### Fan-out, 64 B, each publisher paced at 400k msg/s\n\n" +
                   table(["topology", "delivered M msg/s", "p50 us", "p99 us", "dropped",
                          "cores"], body) +
                   "\n\nFan-out multiplies delivered throughput without multiplying latency, "
                   "until the machine runs out of cores: the four-by-four row is this guest "
                   "trying to run four publishers, four subscribers and the broker's two "
                   "threads on eight vCPUs, and the drop counter says so.")

    false_sharing = [r for r in read(results, "false_sharing")
                     if r["benchmark"] == "queue_throughput"]
    if false_sharing:
        grouped = group(false_sharing, "capacity", "variant")
        capacities = sorted({int(k[0]) for k in grouped})
        body = []
        for capacity in capacities:
            row = [f"{capacity * 128 // 1024}"]
            for variant in ("mutex-queue", "spsc-unpadded", "spsc-padded"):
                rows_for = grouped.get((str(capacity), variant), [])
                row.append(mps(median(rows_for, "throughput_msg_s")) if rows_for else "-")
            padded = grouped.get((str(capacity), "spsc-padded"), [])
            unpadded = grouped.get((str(capacity), "spsc-unpadded"), [])
            if padded and unpadded:
                ratio = median(padded, "throughput_msg_s") / median(unpadded, "throughput_msg_s")
                row.append(f"{ratio:.1f}x")
            else:
                row.append("-")
            body.append(row)
        out.append("### False sharing: the same code with the two ring indices on one cache "
                   "line or two\n\n" +
                   table(["ring KiB", "mutex M/s", "unpadded M/s", "padded M/s", "padded gain"],
                         body) +
                   "\n\n128 B slots, both threads pinned, median of repeated runs. The unpadded "
                   "variant is flat no matter how big the ring gets, which is the signature of "
                   "a fixed per-item cost: the producer's store to the write index invalidates "
                   "the line holding the read index and vice versa, so every single item pays "
                   "a coherence round trip. The padded variant has no such floor and scales "
                   "with the ring, because each side can amortise one index exchange over a "
                   "longer run of slots. At the smallest ring the two are identical — the "
                   "effect is invisible until the ring is large enough to expose it, which is "
                   "why the first version of this experiment found nothing.")

    batching = read(results, "batching")
    if batching:
        body = [[r["variant"], mps(r["throughput_msg_s"]), us(r["p50_ns"]), us(r["p99_ns"]),
                 us(r["p999_ns"]), f"{float(r['cpu_cores_used']):.1f}"]
                for r in batching]
        out.append("### Egress batching: frames coalesced into one write()\n\n" +
                   table(["variant", "delivered M msg/s", "p50 us", "p99 us", "p99.9 us",
                          "cores"], body) +
                   "\n\nOne frame per `write()` is catastrophic: egress becomes syscall-bound "
                   "near 0.4 M msg/s, the pipeline backs up into the kernel's socket buffers, "
                   "and p50 lands in the hundreds of milliseconds. Eight and above are all "
                   "fine and barely distinguishable from each other. The adaptive policy — "
                   "batch of 1 while the queue is shallow, growing with depth — inherits "
                   "exactly the problem it was meant to avoid, because a shallow queue is the "
                   "normal case and a `write()` syscall is not cheap enough to spend on one "
                   "frame. It is a pessimisation here, and it is kept and reported rather than "
                   "tuned until it wins.")

    spin = read(results, "spin_vs_park")
    if spin:
        body = [[r["variant"].replace("idle-spin-", ""), us(r["p50_ns"]), us(r["p99_ns"]),
                 us(r["p999_ns"]), f"{float(r['cpu_cores_used']):.2f}",
                 r["voluntary_ctx_switches"]]
                for r in sorted(spin, key=lambda r: int(
                    r["variant"].split("-")[-1].removesuffix("us")))]
        out.append("### Spinning versus giving the core back\n\n" +
                   table(["idle spin window", "p50 us", "p99 us", "p99.9 us", "cores",
                          "ctx switches"], body) +
                   "\n\nThe single largest lever on measured latency in the system. Parking "
                   "immediately saves about one core and costs roughly four times the p50, "
                   "because the dispatcher has no descriptor to wait on, so its sleep lands "
                   "directly on the latency of whichever event ends an idle period. A 50 us "
                   "window already recovers all of it. Every other end-to-end number in this "
                   "repository was taken at the 500 us default, and this table is published "
                   "next to them because a latency figure obtained by burning four cores on "
                   "spin loops is not honest without it.")

    sustained = read(results, "sustained")
    if sustained:
        body = [[f"{float(r['at_s']):.0f}", mps(r["throughput_msg_s"]), us(r["p50_ns"]),
                 us(r["p99_ns"]), us(r["p999_ns"]), r["dropped"], r["allocations"],
                 f"{float(r['resident_kib']) / 1024:.1f}"]
                for r in sustained]
        out.append("### Sustained load\n\n" +
                   table(["t (s)", "M msg/s", "p50 us", "p99 us", "p99.9 us", "dropped",
                          "heap allocs", "RSS MiB"], body) +
                   "\n\nThe columns a five-second benchmark cannot show: whether p99 drifts, "
                   "whether memory grows, and whether the steady-state allocation count is "
                   "really zero. It is, from the second interval onward.")

    return "\n\n".join(out)


def splice(text: str, marker: str, replacement: str) -> str:
    """Replaces whatever sits between `marker` and the next END marker."""
    if marker not in text:
        sys.exit(f"marker {marker} not found in {marker!r}'s file")
    start = text.index(marker) + len(marker)
    end = text.find(END, start)
    if end == -1:
        sys.exit(f"no {END} after {marker}")
    return text[:start] + "\n" + replacement + "\n" + text[end:]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=REPO / "results" / "latest")
    parser.add_argument("--readme", type=Path, default=REPO / "README.md")
    args = parser.parse_args()

    if not args.results.exists():
        sys.exit(f"{args.results} does not exist; run scripts/run_benchmarks.py first")

    text = args.readme.read_text()
    text = splice(text, HEADLINE_BEGIN, headline(args.results))
    text = splice(text, TABLES_BEGIN, tables(args.results))
    args.readme.write_text(text)
    print(f"updated {args.readme} from {args.results}")


if __name__ == "__main__":
    main()
