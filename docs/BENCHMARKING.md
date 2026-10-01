# Benchmarking Methodology

A performance claim with no method attached is an opinion. This is the method.

---

## Reproducing

```bash
CXX=g++ cmake --preset default
cmake --build build/default -j

python3 scripts/run_benchmarks.py            # writes results/latest/*.csv
python3 scripts/plot_latency.py              # writes results/latest/*.png
scripts/run_perf.sh                          # perf counters, where they exist
```

`--quick` shortens every run; use it to check that the harness works, never to
produce a number. `--suite NAME` runs one suite. Every CSV row carries the CPU
model, core count, kernel, OS, compiler, exact compiler flags, build type,
sanitizer, Boost version, CPU governor and whether a hardware PMU was
available, so a row can always be traced back to the conditions that made it.

## What is measured

**End-to-end latency** is the subscriber's `steady_clock` reading minus the
publisher's reading, which the publisher wrote into the frame header at
`publish()` time. One machine, one clock, so there is no clock skew to correct
and no chance of getting that correction wrong. It includes everything:
encode, `write()`, the kernel's loopback path, the broker's read, decode,
ingress queue, routing, egress queue, `write()`, and the subscriber's read and
decode.

**Queue handoff latency** is the consumer's reading minus the producer's, with
nothing but the queue in between.

**Throughput** is messages delivered divided by the measured window, where the
window starts after the warmup and ends with the last message.

Samples go into a fixed-memory HDR-style histogram: 8 significant bits, so a
reported percentile is within 0.8% of the true sample, with bounded memory and
no allocation on `record()`. Percentiles are reported as the bucket's highest
equivalent value, clamped to the exact observed maximum, so a latency is never
reported lower than it was. Nothing is printed per sample; printing a latency
per event measures the printer.

## The clock is part of the result

`clock_bench` runs first in the suite, because every latency number here is two
clock reads wide. On the machine in `results/`:

```
steady_clock::now()   24.52 ns per read
rdtscp                17.47 ns per read (calibrated at 2.400 GHz)
agreement over 200 ms: drift -0.00%
gap between consecutive now_ns() calls: min 24  p50 26  p99 29  p99.9 36  max 6909 ns
```

Two consequences, both of which changed how other benchmarks are written:

1. **A clock read costs about as much as an SPSC handoff.** Anything measured
   per-message with two clock reads has roughly 49 ns of instrument in it. For
   results where that matters, the benchmark runs twice: once with no clock in
   the loop at all, where total time over message count gives the true
   per-event cost, and once with per-message timestamps for the distribution.
   `queue_bench` and `alloc_bench` both do this and print the clock cost next
   to the result.

2. **The p99.9 of doing nothing is 36 ns and the max is 6.9 µs.** That is this
   machine interfering with its own measurement, and it bounds how small a tail
   claim can be believed here.

`steady_clock` is the reference throughout. The TSC path exists and is
validated against `steady_clock` before any use — a counter that disagrees over
a long interval is a counter that cannot measure time — but 7 ns per read is
not worth a second clock in the results.

## Pacing, and why saturated latency is not latency

A benchmark with an unthrottled producer and a bounded queue measures something
other than what it looks like. If the consumer cannot keep up, the queue simply
stays full, and

```
    latency ~= capacity / throughput
```

which is a throughput result wearing a latency costume. Measured directly, with
a 4096-slot queue: the mutex baseline reports a 1.6 ms p50 and the SPSC ring
917 µs, and both numbers are just `4096 / throughput`. Neither says anything
about how long a handoff takes.

So latency comparisons are run **paced below saturation**, where the queue stays
shallow and the number is the handoff cost. Throughput comparisons are run
unthrottled. Both appear in `results/latest/queue.csv`, tagged by
`target_rate`, and the saturated rows are kept rather than hidden because
saturation is a real operating regime — it is what the overload sweep explores
on purpose.

## Warmup and repetition

Every run discards a warmup: thread startup, the first touch of every ring page,
and TCP's initial congestion window all belong to setup, not to steady state.
`queue_bench` discards the first 10% of messages; `flashbus-bench` discards a
configurable wall-clock warmup, 500 ms by default.

The machine in `results/` is a shared 8-vCPU KVM guest. A single run of
anything here can be off by a factor of two. `queue_bench` therefore runs each
configuration several times and reports the **median run**, printing the spread
beside it. The median of each column separately would invent a row no run
produced; this reports a row that actually happened.

## Idle backoff affects every latency number

FlashBus loops spin before they park. The spin window is a parameter, and it is
the single biggest lever on measured latency:

| `--idle-spin-us` | effect |
| --- | --- |
| 0 | park immediately; a parked loop adds its timeout to the next event |
| 500 (default) | spin through the gaps at normal rates; park when traffic stops |
| 5000 | effectively always spinning |

`results/latest/spin_vs_park.csv` has the measured curve. Any end-to-end number
quoted anywhere in this repository was taken at the default, and the comparison
to `0` is published next to it, because a latency figure obtained by burning
four cores on spin loops and presented without that context is not an honest
figure.

## Hardware counters

`scripts/run_perf.sh` probes each `perf` event before collecting it, because a
virtual machine without a virtual PMU does not fail — `perf stat -e cycles`
prints `<not supported>` and exits zero, which makes it very easy to publish
cache-miss numbers that were never measured.

**On the machine in `results/`, no hardware PMU is available.** It is a KVM
guest with no vPMU, so cycles, instructions, branch misses and cache misses
cannot be collected, and no microarchitectural claim is made anywhere in this
repository on the basis of them. The counters that do work there are the
software ones, and the ones FlashBus collects itself through `getrusage`:
context switches (voluntary and involuntary), page faults, CPU time, and
resident set size. Those are in every result row.

This is a real limitation and it is the reason the false-sharing experiment is
reported as a latency and throughput difference rather than as a cache-line
transfer count. On bare metal, `scripts/run_perf.sh --record` will collect the
rest.

## Correctness is a separate question

`ctest` is the correctness suite: unit tests, protocol fuzzing, queue
wraparound and stress, end-to-end ordering, backpressure policies,
slow-consumer isolation, and the order book — under no sanitizer and under
AddressSanitizer, UndefinedBehaviorSanitizer and ThreadSanitizer.

```bash
for preset in default asan ubsan tsan; do
  CXX=g++ cmake --preset "$preset" && cmake --build "build/$preset" -j
  ctest --test-dir "build/$preset" --output-on-failure
done
```

"The benchmark ran" is not "the code is correct". They answer different
questions, and the sanitizers have already answered one of them in a way the
plain build did not: AddressSanitizer found a heap overflow in the dispatcher's
per-topic sequence table that every non-sanitized test had passed over.

The three network-driving suites are marked `RUN_SERIAL`. Every FlashBus loop
spins before it parks, so two of them side by side oversubscribe the CPUs and
the subscribers fall behind — which looks exactly like a dropped-event bug and
is not one.

For the same reason, the correctness suite sizes its own load: publisher and
subscriber counts and the paced rate come from `available_cpu_count()`, and
every test that asserts zero loss paces its publishers. An unpaced burst into a
bounded egress ring is entitled to drop events, so a test that forbids it is not
testing delivery, it is testing how fast the machine happens to be.

## Limits of these numbers

* One machine, one configuration: a shared 8-vCPU KVM guest with no vPMU, no
  cpufreq control, no CPU isolation, no hugepages and loopback networking.
  Absolute figures do not transfer to other hardware. The relative comparisons
  are more durable than the absolutes, and the method is what is meant to
  transfer.
* Loopback is not a NIC. There is no driver, no PCIe, no wire. An end-to-end
  figure here is a lower bound on what the same code would show over a network,
  and it omits the costs that usually dominate on real hardware.
* No CPU isolation means every tail number includes whatever else the host was
  doing. The `max` column is mostly a measure of that, which is why the
  analysis leans on p99 and p99.9 and treats `max` as context.
* Benchmarks run the broker, publishers and subscribers in one process on one
  host, so they share the CPU with the thing they measure. CPU cores used is
  reported for exactly this reason.
