# Measured results

Generated output, committed on purpose.

These CSVs are the evidence behind every number in the README and in
`docs/PERFORMANCE.md`. The README tables are rendered *from these files* by
`scripts/report.py` and the charts are drawn from them by
`scripts/plot_latency.py`, so a claim in the documentation can be checked
against the data that produced it without running anything. That traceability
is worth more than keeping generated files out of version control.

```
latest/
├── *.csv                 one row per measured configuration
├── *.png                 charts, drawn from those CSVs
├── logs/                 full stdout of every benchmark run
└── manifest.json         what ran, when, and for how long
```

Every CSV row carries the machine that produced it: CPU model and core count,
kernel, OS, compiler and its exact flags, build type, sanitizer, Boost version,
CPU governor, and whether a hardware PMU was available. A row can therefore
always be traced back to the conditions behind it, or dismissed on the evidence.

## What is here

| File | Study |
| --- | --- |
| `latency_floor.csv` | lowest end-to-end latency, 1 publisher / 1 subscriber, by offered rate |
| `end_to_end.csv` | end-to-end against payload size |
| `overload.csv` | the saturation knee: throughput, latency, loss and queue depth against offered load |
| `ceiling.csv` | the broker's throughput ceiling and what sets it (frames per read) |
| `fanout.csv` | publisher/subscriber topologies |
| `queue.csv` | mutex baseline against the SPSC ring, saturated and paced |
| `false_sharing.csv` | the controlled cache-line experiment: only the ring's footprint varies |
| `allocation.csv` | `new`/`delete` per order against a preallocated pool |
| `batching.csv`, `batching_saturated.csv` | frames coalesced into one `write()` |
| `spin_vs_park.csv` | what giving the core back costs in latency |
| `affinity.csv` | thread placement, saturated and paced |
| `sustained.csv` | 300 seconds at 300k msg/s, one row per interval |
| `market_data.csv` | the synthetic feed into the order-book consumer |
| `latency_histogram.csv` | full histogram buckets, for the distribution chart |

## Regenerating

```bash
make bench        # run everything, redraw the charts, regenerate README tables
```

Roughly 25 minutes, most of it the 300-second sustained run. `--quick` shortens
every run and should be used for checking that the harness works, never for
producing a number.

## Comparing a new run against this one

```bash
python3 scripts/run_benchmarks.py --out results/candidate
python3 scripts/check_regressions.py --candidate results/candidate
```

The threshold is deliberately loose at 40%. Run-to-run spread on a shared
machine reaches 3x on some configurations, and a tight threshold would cry wolf
until it was ignored — which is worse than having no check at all. It is looking
for a change of regime, not for noise.

## Reading these numbers

Please read `docs/BENCHMARKING.md` first. The three things that matter most:

- **One machine**, a shared 8-vCPU KVM guest with no virtual PMU, no CPU
  isolation, no cpufreq control and loopback networking. The relative
  comparisons are far more durable than the absolutes.
- **No hardware counters.** `hw_pmu_available` is 0 in every row here, so no
  claim about cycles, branches or cache misses is made anywhere in this
  repository. Where a cache-level explanation is offered it is labelled a
  hypothesis, with the experiment that would settle it named.
- **A clock read costs 24.5 ns** on this host, comparable to the SPSC handoff
  being measured. Benchmarks that would be distorted by that run twice, once
  with no clock in the loop, and print the clock cost beside the result.
