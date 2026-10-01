# FlashBus

A low-latency event-streaming engine in C++20: lock-free SPSC queues,
non-blocking TCP, a binary wire protocol, bounded backpressure, and preallocated
memory, built for predictable tail latency under sustained and bursty load.

FlashBus does one thing: **move small binary events from producers to consumers
with low and predictable latency.** It is not a durable broker and does not try
to be one. What it guarantees is written down in [DESIGN.md](DESIGN.md) before
the code, and what it does not guarantee is written down just as plainly.

```
  publisher ─┐                        FlashBus broker
             │   TCP
  publisher ─┼───────►  ┌──────────────────────────────────────────────┐
             │          │  network thread            dispatcher thread │
  publisher ─┘          │                                             │
                        │  read()                                     │
                        │    │                                        │
                        │    ▼                                        │
                        │  decode ──► [ingress SPSC] ──► poll ──┐      │
                        │                                       │      │
                        │                            topic lookup      │
                        │                            sequence number   │
                        │                            fan-out           │
                        │                                       │      │
                        │  write() ◄── [egress SPSC] ◄──────────┘      │
                        │    │          one per subscriber             │
                        └────┼─────────────────────────────────────────┘
                             │ TCP
                   ┌─────────┼──────────┐
                   ▼         ▼          ▼
              subscriber subscriber subscriber
```

Two threads, three stages, and every queue between them strictly
single-producer / single-consumer. There is no MPMC queue anywhere in FlashBus:
several publishers scale by getting one ingress ring each.

---

## Headline numbers

<!-- RESULTS:HEADLINE -->
Measured on Intel(R) Xeon(R) Processor (8 logical), Linux x86_64 / Ubuntu 24.04.4 LTS, kernel 6.12.94+, g++ 13.3.0 `-O2 -g -DNDEBUG`, Boost 1.83.0. Hardware PMU: NOT available.

| What | Measured |
| --- | --- |
| End-to-end over TCP, 64 B, 1 pub / 1 sub, paced at 200k msg/s | p50 **15.29 us**, p99 **22.27 us**, p99.9 26.50 us, 0 dropped |
| SPSC ring handoff, 64 B slots, paced below saturation | p50 **0.41 us**, p99 **0.46 us** (mutex baseline: p50 2.54 us, p99 7.39 us) |
| SPSC ring throughput, 64 B slots, saturated | **15.11 M msg/s** (mutex baseline: 7.18 M msg/s) |
| Broker throughput ceiling, 64 B, zero loss | **5.19 M msg/s** delivered (2 publishers, 8 frames per write()) |
| Highest offered load carried in full, zero loss (4 pub / 1 sub, 64 B) | **1.20 M msg/s** at p50 59.90 us, p99 113.15 us |
| Sustained 300 s at 300k msg/s | 90 M events, **0 dropped**, p99 drift +8%, **0 heap allocations** after the first interval |
<!-- RESULTS:END -->

All figures are measured by the binaries in this repository, on one machine,
and that machine is a shared 8-vCPU KVM guest with **no virtual PMU**, no
cpufreq control and loopback networking. Read
[docs/BENCHMARKING.md](docs/BENCHMARKING.md) before quoting any of them; the
relative comparisons are far more durable than the absolutes, and several of
them changed sign once the benchmark itself was fixed.

---

## Design goals

| Goal | How |
| --- | --- |
| Predictable tail latency | bounded queues everywhere, no allocation on the hot path, no locks on the data path |
| Explicit ownership | one thread per stage, SPSC queues between them, no shared mutable state except two rings |
| Backpressure as a design, not an accident | ingress is flow-controlled through TCP; egress is bounded with a per-subscriber loss policy |
| Honest measurement | every result carries the machine that produced it; every claim has a benchmark behind it |
| Reviewable concurrency | if the ownership rule cannot be stated in one sentence, it is the wrong rule |

## Semantics

Stated fully in [DESIGN.md](DESIGN.md). In short: TCP transport, FIFO per
publisher stream, bounded buffers, **no durability**, at-most-once delivery
while connected, explicit backpressure, binary framing, no heap allocation in
the steady state, and a disconnected or slow subscriber may miss events — and
will be told how many.

## Protocol

32-byte fixed header, explicitly little-endian, then payload.

```
 offset  size  field
      0     2  magic          0xFB55
      2     1  version
      3     1  type           1=DATA 2=SUBSCRIBE 3=HEARTBEAT
      4     4  topic
      8     4  payload_size
     12     2  flags
     14     2  reserved       must be 0
     16     8  sequence       per-topic broker sequence
     24     8  timestamp_ns   publisher's steady_clock at publish
```

TCP is a byte stream, not a message stream. The decoder is a state machine that
tolerates any fragmentation, including a header split across several `recv()`
calls, and it is tested a byte at a time and at every chunk size from 1 to 137.
A malformed header ends the connection; FlashBus never tries to resynchronise a
corrupt stream, because there is no way to know where the next real header
begins. The parser is fuzzed with random bytes, bit-flipped valid frames, every
truncation of a valid frame, and absurd declared lengths.

## Concurrency model

The network thread owns every socket and buffer. The dispatcher thread owns
routing, sequencing and the backpressure decision. They share exactly one
`Channel` per connection: two SPSC rings, a closed flag, and counters, with the
network-written and dispatcher-written counters in separate cache-line groups so
the two threads never contend for a line while counting. Counters are
single-writer and use a relaxed load-add-store rather than `fetch_add`, because
a `lock xadd` per event is tens of cycles of waste when there is only one writer.

`Channel` holds no socket and no `io_context`, which is what keeps teardown
simple: either thread may hold the last reference and destroy it.

## Backpressure

Ingress and egress are deliberately asymmetric.

**Ingress is flow-controlled and lossless.** Before each read, a session
computes how many frames its ingress ring can still absorb and reads at most
that many bytes' worth — every frame is at least a 32-byte header and the
decoder holds at most one partial frame, so reading `(free - 1) × 32` bytes
yields at most `free` frames. With fewer than two free slots nothing is read,
the TCP receive window closes, and the publisher is throttled by the kernel.
This costs nothing in normal operation: a 4096-slot ring with room to spare
permits a larger read than the read buffer, so the bound never binds until
there is real pressure.

**Egress is bounded and lossy by policy**, per subscriber:

| Policy | Behaviour | Who suffers |
| --- | --- | --- |
| `drop-newest` (default) | discard for this subscriber, and count it | only this subscriber |
| `disconnect` | close the slow subscriber | only this subscriber |
| `block` | dispatcher waits for room | **everyone** |

So FlashBus never drops an event because a publisher was fast. It drops events
because a subscriber was slow, and it reports which subscriber and how many.
`drop-oldest` is deliberately absent: it would require the dispatcher to advance
the consumer's read index, costing the SPSC ring the single-writer property
that makes it reviewable. The reasoning is in [DESIGN.md](DESIGN.md).

## Building

Needs a C++20 compiler, CMake 3.20+, Boost 1.70+ (Asio and system), and
GoogleTest for the tests.

```bash
sudo apt-get install -y g++ cmake libboost-system-dev libgtest-dev   # Debian/Ubuntu
pip install matplotlib                                               # for the charts

CXX=g++ cmake --preset default
cmake --build build/default -j
ctest --test-dir build/default --output-on-failure
```

Presets: `default`, `native` (`-march=native`), `asan`, `ubsan`, `tsan`.

> On some distributions `/usr/bin/c++` is a clang that cannot find libstdc++;
> hence the explicit `CXX=g++`. Any working C++20 compiler will do.

## Running

```bash
./build/default/apps/flashbus-server --port 9000
./build/default/apps/flashbus-sub    --topic trades
./build/default/apps/flashbus-pub    --topic trades --rate 100000 --payload 64

# everything in one process, for measuring
./build/default/apps/flashbus-bench --producers 4 --consumers 4 \
    --messages 2000000 --payload 64 --rate 400000
```

Every binary takes `--help`, and every one rejects an option it does not
recognise rather than silently keeping a default — a typo in a benchmark flag is
how people publish numbers for a configuration they never ran.

## Benchmark methodology

The short version; the long version is [docs/BENCHMARKING.md](docs/BENCHMARKING.md).

* Latency is end-to-end: the subscriber's `steady_clock` reading minus the
  publisher's, carried in the frame header. One machine, one clock.
* Samples go into a fixed-memory HDR-style histogram, accurate to 0.8%, which
  never under-reports a latency. Nothing is printed per sample.
* **A clock read costs 24.5 ns on this machine, about as much as an SPSC
  handoff.** Benchmarks where that matters run twice: once with no clock in the
  loop, where total time over message count is the true per-event cost, and
  once with per-message timestamps for the distribution. The clock cost is
  printed next to the result rather than quietly subtracted.
* Latency comparisons are **paced below saturation**. With an unthrottled
  producer, a queue that cannot keep up simply stays full and the measured
  latency becomes capacity divided by throughput — a throughput result wearing
  a latency costume.
* Every configuration whose spread matters is run several times, and the
  **median run** is reported with the spread beside it. Single runs on this
  shared guest vary by up to 3x.

```bash
python3 scripts/run_benchmarks.py     # results/latest/*.csv
python3 scripts/plot_latency.py       # results/latest/*.png
scripts/run_perf.sh                   # perf counters, where they exist
```

## Results

<!-- RESULTS:TABLES -->
Charts, all drawn from the CSVs in this directory by `scripts/plot_latency.py`:

* [Overload behaviour](results/latest/overload.png)
* [Queue handoff latency by percentile](results/latest/queue_latency.png)
* [Queue throughput by slot size](results/latest/queue_throughput.png)
* [Sustained load over time](results/latest/sustained.png)
* [Spinning versus parking](results/latest/spin_vs_park.png)
* [Egress batching](results/latest/batching.png)
* [Order-book apply: heap versus pool](results/latest/allocation.png)
* [End-to-end against payload size](results/latest/payload_sweep.png)

### End-to-end over TCP, 1 publisher / 1 subscriber, paced

| payload B | M msg/s | p50 us | p95 us | p99 us | p99.9 us | dropped |
| --- | --- | --- | --- | --- | --- | --- |
| 32 | 0.20 | 14.97 | 18.69 | 22.02 | 26.88 | 0 |
| 64 | 0.20 | 15.29 | 18.69 | 22.27 | 26.50 | 0 |
| 128 | 0.20 | 15.29 | 18.82 | 22.14 | 25.60 | 0 |
| 256 | 0.20 | 17.79 | 23.93 | 26.62 | 31.74 | 0 |
| 1024 | 0.20 | 20.99 | 29.82 | 36.35 | 52.99 | 0 |

Latency rises about 40% across a 32x range of payload size, which is what a mostly fixed per-frame cost looks like: the copy is small next to the syscalls and the queue hops around it.

### Overload behaviour, 4 publishers / 1 subscriber, 64 B

| offered M/s | delivered M/s | p50 us | p99 us | p99.9 us | dropped | gaps | cores |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.2 | 0.20 | 17.15 | 30.34 | 40.70 | 0 | 0 | 7.0 |
| 0.4 | 0.40 | 25.60 | 42.49 | 50.17 | 0 | 0 | 7.0 |
| 0.8 | 0.80 | 34.05 | 58.62 | 98.30 | 0 | 0 | 7.0 |
| 1.2 | 1.20 | 59.90 | 113.15 | 178.18 | 0 | 0 | 7.0 |
| 1.6 | 1.60 | 92.16 | 221.18 | 2981.89 | 667 | 6 | 6.5 |
| 2.4 | 1.76 | 169.98 | 1269.76 | 1687.55 | 0 | 0 | 6.8 |
| unthrottled | 1.70 | 32636.93 | 64487.42 | 65536.00 | 6014 | 66 | 3.3 |

Delivered tracks offered until the knee, then flattens while latency climbs and the drop counter starts moving. Every dropped event shows up as a subscriber sequence gap, which is the property the broker sequence exists for. Note the CPU column: all four publishers, the broker's two threads and the subscriber share eight vCPUs, so the knee is partly this machine running out of cores.

### What sets the ceiling: frames per read

| load generator | delivered M msg/s | p50 us | dropped | cores |
| --- | --- | --- | --- | --- |
| 1 publisher, 1 frame per write() | 0.45 | 17.79 | 0 | 4.0 |
| 1 publisher, 8 frames per write() | 5.12 | 10616.83 | 0 | 3.4 |
| 1 publisher, 32 frames per write() | 4.83 | 11927.55 | 0 | 3.3 |
| 2 publishers, 8 frames per write() | 5.19 | 27787.26 | 0 | 3.4 |
| 4 publishers, 1 frame per write() | 1.35 | 32505.85 | 7504 | 3.8 |
| 4 publishers, 8 frames per write() | 6.52 | 58982.40 | 906 | 2.4 |

One publisher issuing one `write()` per event cannot offer the broker more than its own syscall rate, which measures 0.45 M msg/s here. Letting it put eight frames in each `write()` multiplies delivered throughput 11x without changing a line of broker code, because the quantity that governs broker throughput is frames per read, not events per second. The large p50 values in the batched rows are queueing delay: these runs are unthrottled on purpose, so the pipeline is full by construction and latency is backlog divided by rate.

### Fan-out, 64 B, each publisher paced at 400k msg/s

| topology | delivered M msg/s | p50 us | p99 us | dropped | cores |
| --- | --- | --- | --- | --- | --- |
| 1 pub / 1 sub | 0.37 | 17.92 | 27.01 | 0 | 4.0 |
| 1 pub / 2 sub | 0.79 | 22.14 | 36.61 | 0 | 5.0 |
| 1 pub / 4 sub | 1.60 | 32.51 | 54.78 | 0 | 7.0 |
| 2 pub / 2 sub | 0.80 | 25.34 | 44.03 | 0 | 6.0 |
| 4 pub / 1 sub | 0.40 | 26.37 | 44.03 | 0 | 7.0 |
| 4 pub / 4 sub | 1.60 | 2572.29 | 11534.33 | 21995 | 7.6 |

Fan-out multiplies delivered throughput without multiplying latency, until the machine runs out of cores: the four-by-four row is this guest trying to run four publishers, four subscribers and the broker's two threads on eight vCPUs, and the drop counter says so.

### False sharing: the same code with the two ring indices on one cache line or two

| ring KiB | mutex M/s | unpadded M/s | padded M/s | padded gain |
| --- | --- | --- | --- | --- |
| 32 | 5.93 | 15.81 | 15.66 | 1.0x |
| 128 | 6.22 | 16.03 | 17.87 | 1.1x |
| 512 | 6.46 | 14.43 | 31.62 | 2.2x |
| 2048 | 6.68 | 15.24 | 78.64 | 5.2x |
| 8192 | 6.28 | 13.11 | 133.88 | 10.2x |
| 32768 | 7.54 | 15.16 | 126.01 | 8.3x |

128 B slots, both threads pinned, median of repeated runs. The unpadded variant is flat no matter how big the ring gets, which is the signature of a fixed per-item cost: the producer's store to the write index invalidates the line holding the read index and vice versa, so every single item pays a coherence round trip. The padded variant has no such floor and scales with the ring, because each side can amortise one index exchange over a longer run of slots. At the smallest ring the two are identical — the effect is invisible until the ring is large enough to expose it, which is why the first version of this experiment found nothing.

### Egress batching: frames coalesced into one write()

| variant | delivered M msg/s | p50 us | p99 us | p99.9 us | cores |
| --- | --- | --- | --- | --- | --- |
| fixed-batch-1 | 0.33 | 306184.19 | 331350.02 | 335544.32 | 2.3 |
| fixed-batch-8 | 0.37 | 18.30 | 26.88 | 29.95 | 4.0 |
| fixed-batch-32 | 0.43 | 18.05 | 26.11 | 30.08 | 4.0 |
| fixed-batch-128 | 0.43 | 17.79 | 25.98 | 30.85 | 4.0 |
| adaptive-batch | 0.50 | 481.28 | 684.03 | 962.56 | 3.9 |

One frame per `write()` is catastrophic: egress becomes syscall-bound near 0.4 M msg/s, the pipeline backs up into the kernel's socket buffers, and p50 lands in the hundreds of milliseconds. Eight and above are all fine and barely distinguishable from each other. The adaptive policy — batch of 1 while the queue is shallow, growing with depth — inherits exactly the problem it was meant to avoid, because a shallow queue is the normal case and a `write()` syscall is not cheap enough to spend on one frame. It is a pessimisation here, and it is kept and reported rather than tuned until it wins.

### Spinning versus giving the core back

| idle spin window | p50 us | p99 us | p99.9 us | cores | ctx switches |
| --- | --- | --- | --- | --- | --- |
| 0us | 68.61 | 122.37 | 127.49 | 2.92 | 248245 |
| 50us | 14.97 | 21.63 | 24.96 | 4.00 | 44 |
| 500us | 16.64 | 24.32 | 28.16 | 4.00 | 35 |
| 5000us | 16.25 | 24.06 | 27.14 | 4.00 | 1 |

The single largest lever on measured latency in the system. Parking immediately saves about one core and costs roughly four times the p50, because the dispatcher has no descriptor to wait on, so its sleep lands directly on the latency of whichever event ends an idle period. A 50 us window already recovers all of it. Every other end-to-end number in this repository was taken at the 500 us default, and this table is published next to them because a latency figure obtained by burning four cores on spin loops is not honest without it.

### Sustained load

| t (s) | M msg/s | p50 us | p99 us | p99.9 us | dropped | heap allocs | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 15 | 0.30 | 16.51 | 24.45 | 26.75 | 0 | 48 | 21.6 |
| 30 | 0.30 | 17.28 | 25.86 | 29.31 | 0 | 0 | 21.6 |
| 45 | 0.30 | 18.05 | 26.62 | 30.34 | 0 | 0 | 21.6 |
| 60 | 0.30 | 18.05 | 26.75 | 30.98 | 0 | 0 | 21.6 |
| 75 | 0.30 | 18.18 | 26.88 | 30.98 | 0 | 0 | 21.6 |
| 90 | 0.30 | 18.18 | 26.75 | 30.85 | 0 | 0 | 21.6 |
| 105 | 0.30 | 17.79 | 26.11 | 30.08 | 0 | 0 | 21.6 |
| 120 | 0.30 | 17.54 | 25.86 | 29.44 | 0 | 0 | 21.6 |
| 135 | 0.30 | 17.41 | 25.73 | 29.44 | 0 | 0 | 21.6 |
| 150 | 0.30 | 18.05 | 26.50 | 30.21 | 0 | 0 | 21.6 |
| 165 | 0.30 | 18.05 | 26.24 | 29.70 | 0 | 0 | 21.6 |
| 180 | 0.30 | 17.92 | 26.11 | 29.31 | 0 | 0 | 21.6 |
| 195 | 0.30 | 18.05 | 26.11 | 29.57 | 0 | 0 | 21.6 |
| 210 | 0.30 | 17.92 | 26.37 | 30.34 | 0 | 0 | 21.6 |
| 225 | 0.30 | 17.92 | 26.11 | 29.57 | 0 | 0 | 21.6 |
| 240 | 0.30 | 18.05 | 26.37 | 29.95 | 0 | 0 | 21.6 |
| 255 | 0.30 | 17.92 | 26.24 | 29.95 | 0 | 0 | 21.6 |
| 270 | 0.30 | 18.18 | 26.75 | 31.10 | 0 | 0 | 21.6 |
| 285 | 0.30 | 18.05 | 26.50 | 30.34 | 0 | 0 | 21.6 |
| 300 | 0.30 | 18.05 | 26.50 | 30.46 | 0 | 0 | 21.6 |

The columns a five-second benchmark cannot show: whether p99 drifts, whether memory grows, and whether the steady-state allocation count is really zero. It is, from the second interval onward.
<!-- RESULTS:END -->

## Performance case studies

Three studies, written up with the measurements and the reasoning in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md):

1. **Mutex queue to SPSC ring** — what lock-free buys, and why the tail
   improves far more than the median.
2. **`new`/`delete` to a preallocated pool** — a 28% per-event win that is
   invisible in the latency percentiles, and why that is a measurement problem
   rather than a contradiction.
3. **Egress batching** — frames per `write()` against latency, including an
   adaptive policy that made things dramatically worse and the reason it did.

Plus two investigations that changed the design: the false-sharing experiment,
where padding helps by up to 8x but only once the ring is large enough for the
effect to be visible at all; and spin-versus-park, which is the single largest
lever on measured latency in the whole system.

## Market-data demo

A synthetic exchange feeds fixed-width 48-byte binary events through FlashBus to
a consumer that maintains a live order book with an intrusive hash index and
pooled order nodes, under steady and bursty load.

```bash
./build/default/examples/market_data/market-data-demo --events 2000000 --burst
```

The generator's stream is self-consistent: every cancel and trade refers to an
order that is currently live. So when the consumer reports an unknown order id,
that is upstream event loss, and it cross-checks against the subscriber's
sequence-gap count — a book that sees unknown orders with no sequence gap is a
bug, and the demo fails rather than printing a plausible number.

## Correctness testing

```bash
for preset in default asan ubsan tsan; do
  CXX=g++ cmake --preset "$preset" && cmake --build "build/$preset" -j
  ctest --test-dir "build/$preset" --output-on-failure
done
```

Eight suites: SPSC ring (capacity 1, 2 and 1024, wraparound, producer-faster,
consumer-faster, random pauses on both sides, batch drain, millions of
operations), protocol round-trip and fragmentation and fuzzing, memory pool
churn, histogram accuracy against exact percentiles, end-to-end ordering and
fan-out and late joiners, backpressure policies and slow-consumer isolation, the
order book, and a full-path soak. All clean under AddressSanitizer,
UndefinedBehaviorSanitizer and ThreadSanitizer.

The sanitizers were worth the trouble. AddressSanitizer found a heap overflow
that every non-sanitized test had passed over: the dispatcher's per-topic
sequence table was declared `std::vector<uint64_t> next_sequence_{kMaxTopicId +
1, 0}`, which selects the initializer-list constructor and builds a two-element
vector rather than 1024 zeros, so every topic id above 1 read and wrote past the
allocation.

Correctness and performance are separate suites on purpose. "The benchmark ran"
is not "the code is correct"; they answer different questions.

## Limitations

* **Not a durable message broker.** No persistence, no replay, no replication,
  no consensus. Nothing is ever written to disk.
* **At-most-once delivery while connected.** A subscriber that falls behind
  loses events according to its policy. It can detect exactly how many from the
  broker sequence, and the broker counts them too.
* An event rejected at ingress — which requires the dispatcher to be the
  bottleneck — never reaches the sequencer, so it produces no subscriber gap and
  is visible only in the broker's counters.
* **1 KiB payload ceiling**, because ring slots are fixed-size and inline. That
  is what buys zero allocation and zero indirection on the queueing path, and
  it wastes most of the ring when payloads are small.
* No UDP multicast, no gap-recovery channel, no multi-threaded `io_context`.
* **No authentication, no encryption, no input rate limiting.** The parser is
  fuzzed and bounded, but FlashBus belongs on a trusted network.
* Topic ids are bounded at 1023 so routing can be an array index.
* **Benchmarks characterise one machine**, a shared 8-vCPU KVM guest with no
  virtual PMU, no CPU isolation, no cpufreq control and loopback networking.
  No microarchitectural claim is made anywhere in this repository, because the
  counters that would support one cannot be read here. Absolute numbers will
  not transfer; the method is meant to.
* Loopback is not a NIC. There is no driver, no PCIe, no wire, and those costs
  usually dominate on real hardware.

## Future work

In the order it would actually be done:

1. **UDP multicast** for one-way fan-out, with sequence-gap detection and no
   pretence that UDP is reliable. The per-subscriber TCP streams are the obvious
   thing to replace when subscriber count grows.
2. **A TCP recovery channel** for the gaps multicast leaves, which is only worth
   building once gap detection has been running long enough to show what the
   gaps look like.
3. **An eventcount wake for the dispatcher**, so it can park without putting its
   wake-up latency on the next event. Today it spins for a window and then
   sleeps, which is the measured trade in
   [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
4. **Re-running the microarchitectural experiments on bare metal**, where the
   PMU exists. The false-sharing result is currently supported by throughput
   and latency only, and it deserves cache-line transfer counts.
5. Variable-size ring slots, to remove the 1 KiB ceiling and the wasted
   footprint — worth it only if the measurements say the footprint matters,
   which for small payloads they do.

## Repository layout

```
include/flashbus/   the library, mostly headers
src/                the parts with out-of-line implementations
apps/               flashbus-server, -pub, -sub, -bench
examples/market_data/   synthetic feed generator and order-book consumer
tests/              correctness, fuzzing, stress
benchmarks/         clock, queue, allocation, affinity, sustained
scripts/            benchmark runner, plotting, perf collection
results/latest/     measured CSVs and the charts drawn from them
docs/               ARCHITECTURE, BENCHMARKING, PERFORMANCE
DESIGN.md           the semantics, written before the code
```
