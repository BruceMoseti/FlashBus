# Performance Studies

Five investigations. Three are the headline case studies; two changed the
design. Every number comes from a CSV under `results/latest/`, produced by a
binary in this repository.

Read [BENCHMARKING.md](BENCHMARKING.md) first if you intend to quote anything.
The short version: one shared 8-vCPU KVM guest, no virtual PMU, no CPU
isolation, loopback networking, and a `steady_clock` read that costs 24.5 ns —
about as much as the SPSC handoff being measured.

**Nothing here is attributed to cache misses, branch misses or cycles**, because
this machine has no hardware PMU and `perf` cannot read those counters on it.
Where a microarchitectural explanation is offered, it is offered as a hypothesis
with the evidence that supports it stated explicitly, and the experiment that
would settle it named.

---

## Case study 1 — mutex queue to SPSC ring

**Question.** What does a lock-free SPSC ring buy over `std::queue` behind a
`std::mutex`, and where does it buy it?

**Setup.** `benchmarks/queue_bench.cpp`. One producer, one consumer, 4096-slot
queue, 10 M messages per run, first 10% discarded as warmup, three runs per
configuration with the median reported. Throughput runs have no clock in the
loop. Latency runs are **paced at 1 M msg/s**, below the mutex queue's
saturation point, so the queue stays shallow and the number is handoff cost.

**Saturated throughput**, 1P/1C, 4096-slot ring:

| slot size | mutex queue | SPSC padded | speedup |
| --- | --- | --- | --- |
| 32 B | 7.0 M msg/s | 13.8 M msg/s | 2.0x |
| 64 B | 7.2 M msg/s | 15.1 M msg/s | 2.1x |
| 128 B | 6.2 M msg/s | 63.7 M msg/s | 10.3x † |
| 256 B | 4.6 M msg/s | 11.6 M msg/s | 2.5x |
| 1024 B | 2.6 M msg/s | 6.3 M msg/s | 2.4x |

† The 128-byte SPSC row is bimodal: individual runs span roughly 30 to 130
M msg/s, so the median is not a summary of anything. Study 4 explains why, and
the honest reading of this table is "2 to 2.5x" with one configuration that
behaves differently for an identifiable reason.

**Paced handoff latency**, the comparison that matters:

| slot size | queue | p50 | p95 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- |
| 32 B | mutex | 1.06 us | 4.25 us | 5.38 us | 7.93 us |
| 32 B | SPSC | **0.35 us** | **0.38 us** | **0.40 us** | **0.67 us** |
| 64 B | mutex | 2.54 us | 5.21 us | 7.39 us | 9.92 us |
| 64 B | SPSC | **0.41 us** | **0.44 us** | **0.46 us** | **0.78 us** |
| 256 B | mutex | 8.26 us | 20.73 us | 31.10 us | 60.41 us |
| 256 B | SPSC | **0.39 us** | **0.47 us** | **0.53 us** | **0.87 us** |
| 1024 B | mutex | 11.33 us | 44.03 us | 71.68 us | 121.34 us |
| 1024 B | SPSC | **0.41 us** | **0.46 us** | **0.50 us** | **0.73 us** |

**What happened.**

At 64-byte slots the median improves 6x and p99 improves 16x. At 1024 bytes the
median improves 28x and p99 improves **144x**. Two things in that pattern
matter more than the headline multiple:

The tail improves far more than the median. A mutex does not make the average
handoff much slower; it makes *some* handoffs much slower, and those are the
ones a latency-sensitive system is judged on.

And the gap *widens with payload size*, because the two queues scale
differently — which is the next section.

Three mechanisms, in rough order of size:

1. **Blocking.** `std::mutex` is a futex. When the lock is contended, a thread
   enters the kernel, is descheduled, and waits to be woken. The mutex rows show
   millions of voluntary context switches per run; the SPSC rows show single
   digits. A wake-up is microseconds, which is exactly where the mutex tail
   lives.
2. **Lock hold time scales with payload.** `std::queue::push` copies the item
   *while holding the lock*, so a bigger item holds the lock longer and widens
   the contention window. The mutex p50 grows from 1.06 us to 11.33 us across
   the payload range — an 11x degradation — while the SPSC ring is flat at
   0.35–0.41 us. The ring copies into a slot it already owns and publishes with
   a single release store, so no payload size is ever inside a critical section.
3. **Allocation.** `std::queue` over `std::deque` allocates and frees blocks as
   it grows and shrinks. The ring allocates once, at construction.

The flatness of the SPSC row across a 32x payload range is the clearest single
result here: **the cost is the cross-core publish, not the bytes.**

**One thing this does not show.** Saturated SPSC throughput is not a stable
number at every configuration — the 128-byte row varies between runs by a factor
of four. That is not noise in the harness; it is explained in study 4.

---

## Case study 2 — `new`/`delete` to a preallocated pool

**Question.** Does a memory pool actually help, or is it cargo cult?

**Setup.** `benchmarks/alloc_bench.cpp`. The workload is the order-book
consumer, because that is where FlashBus genuinely has objects with independent
lifetimes: a `NEW_ORDER` creates an order node and a `CANCEL` or `TRADE`
destroys it. Events are generated up front and replayed from memory. The two
variants differ in exactly one thing — where an `Order` comes from — so the
difference is attributable. 5 M events, first 10% as warmup, run both with and
without a clock in the loop.

| variant | per-event cost | p50 | p99 | p99.9 | max | heap allocations |
| --- | --- | --- | --- | --- | --- | --- |
| `new`/`delete` | 22.1 ns | 0.043 us | 0.099 us | 0.279 us | 23.1 us | 1,320,417 |
| memory pool | **16.4 ns** | 0.039 us | 0.099 us | 0.283 us | 16.3 us | **0** |

The per-event column comes from the run with no clock in the loop; the
percentile columns come from the run with per-event timestamps.

**What happened.** The pool cuts **26% off the per-event cost** and removes
**1.3 million allocations**. And the latency percentiles are identical: p99 is
0.099 us either way.

Both of those statements are true, and the apparent contradiction is a
measurement artifact worth more than either number:

> The per-event difference is 5.7 ns. Collecting one per-event latency sample
> costs two `steady_clock` reads, which is 49 ns. The instrument is **nine times
> larger than the effect.** The percentiles cannot resolve it, and no number of
> extra runs will change that. Note the latency-mode per-event cost in the CSV:
> 72 ns against 22 ns, which is the instrument tripling the cost of the thing
> it is measuring.

This is why `alloc_bench` runs both modes and prints the clock cost above the
results. The throughput mode — no clock in the loop, total time over event count
— is the only one of the two that can see a 4 ns difference.

**Where the pool earns its place anyway**, in descending order of importance:

1. **Zero allocations is a property, not an optimisation.** The pool makes the
   steady-state allocation count exactly zero, which the tests assert with a
   counting `operator new`. A system that never calls the allocator cannot be
   surprised by it — no arena lock, no `brk`/`mmap`, no fragmentation growth over
   a long run. The sustained-load table shows zero allocations per interval for
   every interval after the first, across 300 seconds and 90 M events.
2. **It bounds memory by construction.** The pool returns `nullptr` when
   exhausted and reports its high-water mark, so "how much memory can this
   consumer use" has an answer at startup instead of being discovered in
   production.
3. **26% of a 22 ns operation.** Real, and small.

**The honest headline is not "pools are faster."** glibc's malloc is very good
at this workload: fixed-size blocks, high churn, single thread, tcache-friendly.
On this machine the pool wins 26% of a small number. The reason to keep it is
determinism, and determinism is a property you get by not calling the allocator
at all — which you cannot verify without counting.

---

## Case study 3 — egress batching

**Question.** How many frames should share one `write()` call, and what does
coalescing cost in latency?

**Setup.** `apps/flashbus-bench` through the full TCP path, 64-byte payloads,
one publisher paced at 500k msg/s, one subscriber, varying
`--egress-batch`. The broker's batch size bounds frames per `write()` call, not
frames per event-loop iteration — that distinction was a bug once, and is
explained in [ARCHITECTURE.md](ARCHITECTURE.md).

A caveat on the offered rate: 500k msg/s is just above what a single publisher
can generate with one `write()` per event, so the delivered column is partly
bounded by the load generator and should not be read as a broker throughput
figure. The latency column is unaffected and is what this study is about.

| egress batch | delivered | p50 | p99 | p99.9 | cores |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.33 M msg/s | **306 ms** | 331 ms | 336 ms | 2.3 |
| 8 | 0.37 M msg/s | 18.30 us | 26.88 us | 29.95 us | 4.0 |
| 32 | 0.43 M msg/s | 18.05 us | 26.11 us | 30.08 us | 4.0 |
| 128 | 0.43 M msg/s | 17.79 us | 25.98 us | 30.85 us | 4.0 |
| adaptive | 0.50 M msg/s | 481 us | 684 us | 963 us | 3.9 |

**What happened.**

**One frame per `write()` is catastrophic, by four orders of magnitude.** A
loopback `write()` costs a couple of microseconds, so one syscall per frame caps
egress near 0.4 M msg/s. The publisher is offering 0.5 M, so the pipeline backs
up — first into the egress ring, then into the kernel's socket buffers on both
sides — and p50 lands at a third of a second. Note the CPU column: 2.3 cores
instead of 4.0, because the broker's network thread is blocked in `write()`
rather than doing anything useful.

**Above 8, the batch size barely matters.** 8, 32 and 128 are within noise of
each other on both throughput and latency. The expected trade — bigger batch,
better throughput, worse latency — does not appear, because the pipeline is not
syscall-bound at any of them and a batch only ever contains what was already
waiting. Coalescing costs latency only when you *wait* to fill a batch, and
FlashBus never does: it takes whatever is in the ring and sends it.

**The adaptive policy is a pessimisation, and it was kept.** The policy is the
obvious one: batch of 1 when the queue is shallow, growing with depth, so quiet
periods get minimum latency. Measured, it is 26x worse at p50 than a fixed batch
of 8. The reason is that *a shallow queue is the normal case* — at 500k msg/s
the loop keeps up easily, so depth is almost always below the threshold, and the
policy spends almost all of its time in the batch-of-1 regime it inherits all
the problems of. The premise behind "send immediately when quiet" is that the
syscall is cheap relative to the inter-arrival time. At 2 us between events and
2 us per `write()`, it is not.

This could be fixed by giving the quiet case a floor of 8 rather than 1. It has
not been, because the measured negative result is worth more than a policy tuned
until it wins, and because the finding generalises: an adaptive policy whose
cheap branch is not actually cheap is worse than no policy at all.

---

## Study 4 — false sharing

**Question.** The two ring indices can sit on one cache line or on two. Does it
matter?

This started as the control experiment the project brief asks for — change the
layout, measure, and report whichever way it goes. The first version found
nothing, which turned out to be the most interesting part.

**Setup.** `queue_bench` with `SpscRing<T, true>` and `SpscRing<T, false>`:
identical code, one template parameter, `alignas(64)` on the producer and
consumer index structs or not. Both threads pinned to fixed CPUs so scheduler
placement is not a free variable. 128-byte slots throughout. The only thing
varying is the **ring's capacity**, and therefore its cache footprint.

| ring footprint | mutex | unpadded | padded | padded gain |
| --- | --- | --- | --- | --- |
| 32 KiB | 5.9 M msg/s | 15.8 M msg/s | 15.7 M msg/s | 1.0x |
| 128 KiB | 6.2 M msg/s | 16.0 M msg/s | 17.9 M msg/s | 1.1x |
| 512 KiB | 6.5 M msg/s | 14.4 M msg/s | 31.6 M msg/s | 2.2x |
| 2 MiB | 6.7 M msg/s | 15.2 M msg/s | 78.6 M msg/s | 5.2x |
| 8 MiB | 6.3 M msg/s | 13.1 M msg/s | **133.9 M msg/s** | **10.2x** |
| 32 MiB | 7.5 M msg/s | 15.2 M msg/s | 126.0 M msg/s | 8.3x |

**What happened.** The unpadded variant is **flat** — 13 to 16 M msg/s across a
thousandfold change in ring footprint. The padded variant has no such ceiling
and scales to 134 M msg/s.

A throughput that does not respond at all to cache footprint is the signature of
a **fixed per-item cost**, and 13–16 M msg/s is 60–75 ns per item, which is the
right order for a cache-line round trip between cores. The hypothesis:

* **Unpadded**, the producer's release store to `write` invalidates the line
  that also holds `read`, and the consumer's release store to `read`
  invalidates the line that also holds `write`. Each side's own bookkeeping
  store destroys the other's copy, so **every item** costs a coherence round
  trip, no matter how much work is done between them.
* **Padded**, the two indices are on separate lines. Each side also keeps a
  private cached copy of the other's index and only refreshes it when its
  cached view says the ring is full or empty. With a larger ring, each side can
  go further before that happens, so one index exchange is **amortised over a
  longer run of slots** — and the cost per item falls with capacity.

That explains both columns with one mechanism, and it predicts the shape: flat
for unpadded, rising for padded. The 32 MiB row falling back slightly is
consistent with the ring finally exceeding this CPU's 2 MiB per-core L2.

**The part worth keeping.** At the smallest ring the two variants are
*identical*. The first version of this experiment used the default 4096-slot
ring with small payloads — a footprint of a couple of hundred kilobytes — and
concluded there was no effect, with the unpadded variant sometimes measuring
slightly *faster*. The effect was real the whole time; the experiment just
wasn't looking where it was visible. **A layout experiment at one working-set
size is not a layout experiment.**

**What is missing.** The mechanism above is a hypothesis supported by the shape
of the curve, not by counters. Settling it needs
`perf stat -e cache-misses,mem_load_l3_hit_retired.xsnp_hitm`, and this machine
has no PMU. `scripts/run_perf.sh --record` will collect it on bare metal; until
then this is a hypothesis with good evidence, not a demonstrated cause.

---

## Study 5 — spinning versus giving the core back

**Question.** FlashBus loops spin before they park. How long should they spin?

This is the single largest lever on measured latency in the whole system, which
is why it is published next to every latency number rather than buried.

**Setup.** `flashbus-bench`, 64 B, one publisher paced at 200k msg/s, varying
`--idle-spin-us`, which applies to both the network loop and the dispatcher.

| idle spin window | p50 | p99 | p99.9 | cores | voluntary ctx switches |
| --- | --- | --- | --- | --- | --- |
| 0 (park at once) | 68.6 us | 122.4 us | 127.5 us | 2.9 | 248,245 |
| 50 us | **15.0 us** | **21.6 us** | 25.0 us | 4.0 | 44 |
| 500 us (default) | 16.6 us | 24.3 us | 28.2 us | 4.0 | 35 |
| 5000 us | 16.3 us | 24.1 us | 27.1 us | 4.0 | 1 |

**What happened.** Parking immediately costs **4.5x the p50** and saves about
one core. A 50 us window recovers all of it, and anything longer changes
nothing except the context-switch count.

The asymmetry is in the architecture. The network loop parks in `epoll`, so a
socket becoming readable wakes it immediately — parking costs it almost nothing.
The **dispatcher has no descriptor to wait on**: it polls SPSC rings, so when it
parks it sleeps for a fixed interval and that sleep lands directly on the
latency of whichever event ends the idle period. At 200k msg/s, events arrive
5 us apart, so a dispatcher that parks between them adds its sleep to most
messages.

Two bugs fell out of measuring this, both now fixed and both worth stating
because the fix was not the obvious one:

1. The spin window was originally counted in **loop iterations**, not time. The
   dispatcher's idle iteration costs about 8 ns, so 2000 iterations was 16 us —
   shorter than the gap between events at 100k msg/s. It parked between
   messages. An iteration count is not a unit: it means something different on
   each loop and something different again on the next machine.
2. The network loop called `io_context::run_one_for()` to park and **discarded
   the return value**. A read completing inside that call produces no egress
   frame yet, because the dispatcher has not seen it, so the iteration looked
   idle, so the loop parked again at once — adding the timeout to the latency of
   every single message.

Together those two took p50 from 67 us to 12 us. Neither was a slow algorithm;
both were the idle path leaking into the hot path.

**What is still owed.** The right fix for the dispatcher is an eventcount: let
it park on a futex that the network thread signals when it pushes to an empty
ingress ring, with an atomic "is it sleeping" flag so the signal costs nothing
when it is awake. That would give low latency *and* an idle dispatcher that uses
no CPU. It is in the future-work list rather than the code, and until it exists
the default is to spend 500 us of spinning to buy the latency.

---

## Where the time goes, and what is not known

Honest accounting of what has and has not been established here.

**Established, with measurements:**

* The SPSC ring beats a mutex queue by 2.0-2.5x on throughput and by 13x to
  144x at p99 depending on slot size, and its handoff cost is flat across a 32x
  payload range (349 ns to 407 ns) while the mutex queue's degrades nearly 11x.
* Ingress never drops an event, over every test in the suite, because reads are
  sized to what the ring can absorb and TCP flow control does the rest.
* Egress loss happens only under genuine overload, is bounded by the configured
  policy, and is visible to the subscriber as a sequence gap and to the
  operator as a counter. A slow subscriber does not cost a healthy one anything,
  which is asserted by test rather than by argument.
* Steady-state heap allocations are exactly zero, over 300 seconds and 90 M
  events, measured with a counting `operator new` rather than assumed.
* p99 drift over 300 seconds at 300k msg/s is +8%, and resident memory grows
  0.05%.
* The idle backoff policy is worth 4.5x in p50 latency and about one core.
* Index cache-line placement is worth up to 10x throughput, but only once the
  ring is large enough for the effect to be visible at all.

**Not established, and stated as such:**

* No cycle, branch or cache-miss counts anywhere. This machine has no virtual
  PMU. Every microarchitectural statement in this document is a hypothesis with
  the supporting evidence named, and the experiment that would settle it named
  too.
* Whether the pool-versus-heap difference shows in the end-to-end tail. The
  market-data runs give p99.9 of 549 us pooled against 676 us heap, which is in
  the right direction and well inside this machine's run-to-run spread. It is
  not a result.
* Absolute latency on real hardware. Loopback has no driver, no PCIe and no
  wire, and those costs usually dominate. These numbers are a lower bound.
* Behaviour above roughly 1.6 M msg/s offered with four publishers, where this
  8-vCPU guest runs out of cores to run the load generators, the broker and the
  consumers at the same time. The knee in the overload curve is partly the
  machine's, not FlashBus's, and the CPU column in that table is there to say so.
