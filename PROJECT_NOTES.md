# Project Notes

Working notes on FlashBus: why it exists, what was hard, what broke, and what I
would change. Written for myself and for anyone who wants the reasoning rather
than the result.

`DESIGN.md` is the semantics contract. `docs/ARCHITECTURE.md` is how it is
built. `docs/PERFORMANCE.md` is what it measures. This document is the part
that does not belong in any of those: the decisions, the mistakes, and the
things I would do differently.

---

## 1. Problem statement

Message brokers are overwhelmingly tuned for average throughput. The
interesting question in a latency-sensitive system is different: **what is the
p99.9, and what makes it bad?**

The usual answers are all structural rather than algorithmic:

- heap allocation on the message path, which exposes you to the allocator's
  arena locks and to fragmentation that only appears after hours of running;
- lock contention, where a mutex does not make the average handoff slower so
  much as it makes *some* handoffs much slower;
- one syscall per message, which caps throughput at the syscall rate and leaves
  the pipeline backing up into kernel socket buffers;
- unbounded queues, which do not solve overload so much as defer the crash.

FlashBus is a broker built to avoid all four, with the constraint that **every
claim about it has to be measurable by code in the repository**. That second
half turned out to be the harder and more interesting half.

### Scope, decided up front

I wrote `DESIGN.md` before the implementation specifically so the guarantees
would be decided rather than discovered. In: TCP transport, FIFO per publisher
stream, bounded buffers, at-most-once delivery while connected, explicit
backpressure, binary framing, no steady-state allocation. Out: durability,
replay, replication, consensus, Kafka compatibility.

Deciding the non-goals first is what kept this from becoming a worse Kafka.

---

## 2. Architecture

Two threads, three stages, and every queue between them strictly
single-producer/single-consumer.

```
publisher --TCP--> [network thread] --ingress SPSC--> [dispatcher] --egress SPSC--> [network thread] --TCP--> subscriber
                    read + decode                     route + sequence              batch + write
```

- **Network thread** owns every socket, every buffer, every decoder, the
  sessions and the acceptor.
- **Dispatcher thread** owns the routing table, the per-topic sequence
  counters, and the backpressure decision.
- **Shared**: exactly one `Channel` per connection — two SPSC rings, a closed
  flag, and counters.

The single most useful constraint in the whole design: **`Channel` holds no
socket and no `io_context`.** That means either thread can hold the last
reference and destroy it, which removes the entire class of cross-thread
lifetime problems that otherwise dominate connection teardown in an
asynchronous server.

Multiple publishers scale by getting **one ingress ring each**, so adding
publishers never turns a queue into a contended one. There is no MPMC queue
anywhere in FlashBus.

---

## 3. What I implemented

All of it. There is no framework doing the interesting part.

**Written from scratch:**

| Component | Notes |
| --- | --- |
| SPSC ring buffer | power-of-two masking, cached opposite index, cache-line padded indices, acquire/release pairing, `claim`/`commit` zero-copy write path, batch drain |
| Wire protocol | 32-byte header, explicit little-endian codec, validation of every field that could make the parse unsafe |
| Streaming frame decoder | tolerates arbitrary fragmentation, parses in place out of the caller's buffer when nothing is held over |
| Reactor event loop | `epoll` via Asio, loop-armed reads, egress drained before the next read, time-based idle backoff, parks in `epoll` |
| Ingress flow control | read size bounded by ring space so TCP's window does the throttling |
| Dispatcher | array-indexed topic routing, per-topic broker sequencing, fan-out, backpressure policies |
| Memory pool | preallocated blocks, intrusive free list threaded through the blocks |
| HDR-style histogram | logarithmic bucketing, 0.8% accuracy, allocation-free `record`, never under-reports |
| Single-writer counters | relaxed load-add-store rather than `fetch_add`, cache-line grouped by owning thread |
| Publisher/subscriber clients | batching publisher, gap-detecting subscriber |
| Order-book consumer | intrusive hash chains, Fibonacci hashing, pluggable allocation policy |
| Synthetic market-data feed | self-consistent event stream, steady and bursty profiles |
| Benchmark harness | env capture, pacing, warmup, repetition with median-and-spread |
| Tooling | benchmark runner, chart generation, README table generation, perf collection, regression comparison |

**Used off the shelf:** Boost.Asio, for sockets and the `epoll` reactor only —
not for buffering, not for the protocol, not for the concurrency. GoogleTest.
matplotlib. That is the entire dependency list.

---

## 4. The hardest technical problem

**Making ingress backpressure lossless without a lock, a condition variable, or
an unbounded buffer.**

The naive version is easy and wrong. Read whatever the socket has, decode it,
push frames into the ingress ring, and drop on the floor when the ring is full.
That is what the first version did, and it means a publisher that outruns the
broker silently loses events — which is exactly the failure mode a broker exists
to prevent.

The fix had to satisfy three things at once: no loss, bounded memory, and no
coordination primitive on the hot path.

The insight is that **you can bound the output of a decode by bounding its
input.** Every frame is at least a 32-byte header, and the decoder holds at most
one incomplete frame. So if the ring has `free` slots and I read at most
`(free − 1) × 32` bytes, the decode can produce at most `free` frames:

> The held partial frame can complete and yield at most one frame, consuming at
> least one byte. The remaining `L − 1` bytes yield at most `⌊(L − 1) / 32⌋`
> more. With `L = (free − 1) × 32`, the total is `1 + (free − 2) = free − 1`,
> which fits.

Below two free slots, nothing is read at all. The socket's receive buffer fills,
the TCP window closes, and **the kernel throttles the publisher for me** — flow
control I did not have to implement, exposed by simply declining to read.

What makes this satisfying is that it costs nothing when it is not needed. A
4096-slot ring with room to spare permits a 131 KB read, larger than the 64 KiB
read buffer, so the bound never binds until there is real pressure.

The counter for ingress rejection still exists and should never move. I kept it
rather than asserting, so that a mistake in the bound shows up as a number in
the statistics instead of as events that quietly disappear. A test drives it
with a **two-frame** ingress ring — the pathological case, where every read is
32 bytes — and asserts both that nothing is lost and that the counter is zero.

---

## 5. The most interesting algorithm

**The cached opposite index in the SPSC ring, and what it does to false
sharing.**

The ring's producer owns the write index and the consumer owns the read index.
The naive implementation has each side load the other's index on every
operation, which is a cross-core cache-line transfer per item — roughly 60–75 ns
on this hardware, and a hard ceiling on throughput.

Instead, each side keeps a **private, non-atomic cached copy** of the other's
index, and refreshes it only when its cached view says the ring is full (for the
producer) or empty (for the consumer). In the common case the producer can
publish many items without ever reading the consumer's index.

That optimisation is what makes the false-sharing result interesting. I expected
padding the two indices onto separate cache lines to help a little. Measured at
the default ring size, it did essentially nothing, and I nearly wrote it up as
"no effect, as measured." The experiment was wrong, not the hypothesis: I varied
the layout but held the **working set** fixed.

Varying ring capacity at a fixed slot size, with both threads pinned:

| ring footprint | unpadded | padded | gain |
| --- | --- | --- | --- |
| 32 KiB | 15.8 M msg/s | 15.7 M msg/s | 1.0× |
| 512 KiB | 14.4 M msg/s | 31.6 M msg/s | 2.2× |
| 2 MiB | 15.2 M msg/s | 78.6 M msg/s | 5.2× |
| 8 MiB | 13.1 M msg/s | 133.9 M msg/s | **10.2×** |

The unpadded variant is **flat across a thousandfold change in footprint**. A
throughput that does not respond at all to cache footprint is the signature of a
fixed per-item cost, and 13–16 M msg/s is 60–75 ns per item, which is the right
order for a coherence round trip between cores.

The mechanism that explains both columns with one idea: unpadded, the producer's
release store to the write index invalidates the line that *also* holds the read
index, and vice versa — each side's own bookkeeping store destroys the other's
copy, so every item pays a round trip regardless of how much work happens
between items. Padded, the two indices are on separate lines, so the cached-index
trick can amortise one exchange over a long run of slots, and the cost per item
falls as the ring grows.

**The lesson I actually took from it: a layout experiment at one working-set
size is not a layout experiment.**

I am careful to call that mechanism a hypothesis. It is supported by the shape of
the curve, not by counters, because this machine has no PMU. Settling it needs
`perf stat -e cache-misses,mem_load_l3_hit_retired.xsnp_hitm` on bare metal.

---

## 6. The major engineering decision

**Ingress lossless, egress lossy — deliberately asymmetric.**

Both are bounded queues, and both could have had the same policy. They do not,
and the reason is about *who pays*:

- An **ingress** ring has exactly one publisher behind it. Stalling it
  inconveniences only that publisher, so TCP flow control is the right answer
  and loss is unnecessary.
- An **egress** ring is one leg of a fan-out. Stalling it stops *everyone*, so
  loss has to be an option, and which loss is a per-subscriber policy decision.

The consequence is the sentence I would lead with in an interview:

> FlashBus never drops an event because a publisher was fast. It drops events
> because a subscriber was slow, and it names the subscriber and the count.

That is a stronger and more useful guarantee than "at-most-once" on its own,
and it is testable: a subscriber that stops reading entirely must cost a healthy
subscriber nothing, and `backpressure_test` asserts exactly that.

The same reasoning is why **`DROP_OLDEST` is deliberately absent** even though
it is the obviously right policy for market data, where the newest event is the
valuable one. The oldest queued event sits at the index the *consumer* owns.
Dropping it would require the dispatcher to advance that index — a
compare-exchange on a word that is otherwise single-writer — which would cost
the ring the single-writer property that makes it reviewable in one sitting.
Given that a subscriber which cannot keep up is going to lose events either way,
the gain did not justify complicating the one data structure everything else
depends on. If it were genuinely needed, the honest implementation is a separate
queue type, not a flag on this one.

---

## 7. Alternative designs considered

**A multi-producer queue instead of one SPSC ring per connection.** All
publishers would share one ring, and the dispatcher would not need a round-robin
poll. Rejected because the hot path would acquire a CAS loop with ABA hazards,
on a system whose entire purpose is a predictable tail. The cost I accepted is
polling that grows linearly with connection count.

**Refcounted pool blocks instead of inline ring slots.** Fan-out to N
subscribers would cost one atomic increment instead of N copies. Rejected
because it adds a refcount, a cross-thread free, and an indirection on every
dequeue. The cost I accepted is a 1 KiB payload ceiling and a 64-byte event
sitting in a 1088-byte slot — a 17× cache footprint penalty that the capacity
sweep then showed actually matters. That is the decision I am least sure about.

**`async_write` per frame instead of a batched non-blocking write.** Simpler,
idiomatic Asio. Rejected because it allocates per operation and issues a syscall
per message; the batching study shows one frame per `write()` costs a 306 ms p50
once egress goes syscall-bound.

**Async clients sharing the broker's reactor.** Rejected because a client has
one socket and one job; a blocking `write()` is both simpler and lower latency.
The cost I accepted is written down: a single-threaded client cannot publish and
subscribe on one connection without risking self-deadlock under the `block`
policy.

**A hash map for topic routing.** Rejected in favour of a flat vector indexed by
topic id, which makes routing an array index with no hashing. The cost I
accepted is a bound of 1023 on topic ids.

---

## 8. Performance bottlenecks found

Three, in descending order of how much they taught me.

**The idle backoff was the single biggest lever on latency — larger than any
data structure.** FlashBus loops spin before they park. Parking immediately
costs 4.5× the p50 (68.6 µs against 15.0 µs) and saves about one core, because
the dispatcher has no descriptor to wait on: it polls SPSC rings, so when it
parks it sleeps for a fixed interval and that sleep lands directly on the
latency of whichever event ends the idle period. I publish that table next to
every latency number, because a figure obtained by burning four cores on spin
loops is not honest without it.

**The broker's throughput is governed by frames per read, not events per
second.** One publisher issuing one `write()` per 64-byte event cannot offer the
broker more than its own syscall rate — 0.45 M msg/s measured. Letting it put
eight frames in each `write()` multiplies delivered throughput **11×** without
changing a line of broker code. This is the result I would have been least able
to predict, and it reframes what "optimise the broker" even means.

**The memory pool is 26% faster per event and invisible in the percentiles.**
Both are true. The effect is 5.7 ns, and collecting one latency sample costs two
24.5 ns clock reads, so the instrument is nine times larger than the effect.
That is why `alloc_bench` runs both with and without a clock in the loop. The
reason to keep the pool is not speed, it is that the steady-state allocation
count is exactly **zero** and verified by counting — a system that never calls
the allocator cannot be surprised by it.

---

## 9. How I tested it

Eight suites, run under no sanitizer and under ASan, UBSan and TSan, in CI.
Correctness and performance are separate suites on purpose: "the benchmark ran"
is not "the code is correct."

The tests I would point at as doing real work:

- **Protocol fragmentation.** Every chunk size from 1 to 137, plus byte-at-a-time
  delivery, plus chunks larger than the decoder's own buffer. TCP gives you a
  byte stream, not a message stream, and a header split across three `recv()`
  calls is normal.
- **Protocol fuzzing.** Random bytes, bit-flipped valid frames, *every*
  truncation of a valid frame, and a declared length of 4 GiB. Run under ASan,
  which is where it earns its keep: the assertion is that the parser rejects
  garbage rather than over-reading or sizing an allocation from an attacker's
  integer.
- **Memory-ordering verification.** A test writes a sequence and a checksum
  through `claim()` and verifies both through `front()` across a million
  handoffs. That proves the release/acquire pair publishes the *slot contents*,
  not merely the index — which is the thing that would be silently broken by
  using `relaxed` everywhere.
- **Slow-consumer isolation.** A subscriber that connects, subscribes, and never
  reads again, alongside a healthy one. The assertion is that the healthy
  subscriber loses nothing and sees no gaps, while the broker's drop counter
  attributes the loss to the stalled one.
- **Zero-allocation assertions.** Tests link a counting `operator new` and
  assert the count does not move across 200k operations.
- **Histogram accuracy.** Percentiles checked against exact sorted percentiles
  over a deliberately long-tailed distribution, asserting the documented 0.8%
  bound and that it never under-reports.

The suite is also **portable**, which took work: load scales with
`std::thread::hardware_concurrency()` and the test broker does not spin, so it
is green at 2, 4 and 8 cores. That matters because a correctness test that fails
on a small CI runner teaches you nothing and trains you to ignore red.

---

## 10. Bugs I hit, and how

The failures are the most interesting part of this project, so here they are
with the mechanism rather than just the symptom.

### Reactor starvation (found by an end-to-end test)

The read handler re-armed itself — the idiomatic Asio pattern. But
`io_context::poll()` runs ready handlers until there are none, and a completed
read immediately arms another that is also ready, so **one `poll()` call
consumed a publisher's entire stream** before the loop ever reached the line
that writes anything out. The ingress ring filled, the dispatcher moved
everything to the egress rings, and the egress rings overflowed — while the
socket they were meant to be written to had not been touched.

Fix: reads are armed by the **event loop**, after egress is pumped. The
ownership rule became "at most one read in flight per session, and the loop
decides when."

### A unit error that cost 5× in latency (found by instrumenting)

p50 was 67 µs when it should have been ~12 µs, and it scaled with the idle sleep
interval, which made no sense because the loop was supposed to spin for 2000
iterations before sleeping. I instrumented the loops and found the dispatcher
had slept 6567 times in a 200k-message run.

The spin window was counted in **loop iterations**. The dispatcher's idle
iteration costs about 8 ns, so 2000 of them covered 16 µs — *shorter than the
gap between events at 100k msg/s*. It was parking between messages and putting
its sleep on the critical path. An iteration count is not a unit: it means
something different on each loop and something different again on the next
machine.

There was a second bug in the same area: the network loop called
`io_context::run_one_for()` to park and **discarded the return value**. A read
completing inside that call produces no egress frame yet, because the dispatcher
has not seen it, so the iteration looked idle and the loop parked again
immediately — adding the timeout to the latency of every single message.

Together: 67 µs → 12 µs. Neither was a slow algorithm. Both were the idle path
leaking into the hot path.

### A socket that outlived its own teardown (found by a 40%-flaky test)

`disconnect` policy test failed about 40% of the time. The dispatcher sets the
channel's closed flag, and the network thread is supposed to notice and close
the socket. But `reap_sessions()` erased the session from the session list first,
and the **in-flight read kept the `Session` object alive** via its `shared_ptr`.
So the socket stayed open with nobody left to poll it, and the peer never
learned it had been disconnected.

Fix: close before dropping, unconditionally, in the only thread allowed to touch
the socket. The flakiness came from the race between the dispatcher setting the
flag and the loop's two passes over the session list.

### A heap overflow every non-sanitised run had passed (found by ASan)

```cpp
std::vector<uint64_t> next_sequence_{kMaxTopicId + 1, 0};
```

That is the **initializer-list** constructor. It builds a two-element vector
`{1024, 0}` — not 1024 zeros. Every topic id above 1 read and wrote past the
allocation, and the full test suite passed anyway, because a small heap overflow
usually does nothing visible.

This is the single best argument for the sanitizer presets in this repository.
Both tables are now sized in the constructor with parentheses, with a comment
saying why.

### A benchmark that measured the load generator (found by reading my own output)

The overload sweep came out perfectly flat: delivered tracked offered at every
rate from 100k to 2 M msg/s, zero drops, flat latency. That is not what
saturation looks like, which is the tell.

A single publisher issuing one `write()` per event tops out near 0.4 M msg/s on
syscall rate alone, so it could never offer the broker more than it could take.
**I had measured my load generator.** The sweep now uses four publishers and
shows a real knee near 1.6 M msg/s.

Three sibling bugs in the same harness, all of the same species — the
measurement looked like a result and was not:

- **Latency that was really throughput.** With an unthrottled producer and a
  bounded queue, a queue that cannot keep up simply stays full, so the measured
  latency is `capacity ÷ throughput`. The mutex baseline's "1.6 ms p50" was
  `4096 / 2.5M`, nothing more. Latency comparisons are now paced below
  saturation.
- **Affinity that contaminated itself.** The probe for "is pinning permitted?"
  pinned the main thread for the rest of the process, and threads inherit the
  affinity mask — so one pinned placement contaminated every placement after it,
  including the unpinned baseline, which then ran both threads on CPU 0 and
  reported a quarter of the real throughput.
- **A warmup that swallowed the run.** A fast configuration finished inside the
  wall-clock warmup window and reported `0.000 M msg/s` over a window of
  `0.000 s`. A run with no samples outside the warmup is now an error rather
  than a row of zeros in a CSV.

And one in the tooling: `scripts/run_perf.sh` trusted Ubuntu's `/usr/bin/perf`,
which is a wrapper that prints installation advice and **exits successfully**
when it cannot find a kernel-matched binary. Grepping its output for
`<not supported>` found nothing, so every counter looked available — the exact
mistake the script exists to prevent.

---

## 11. What I would improve with more time

1. **An eventcount wake for the dispatcher.** The measured cost of its having
   nothing to wait on is 4.5× the p50. Park it on a futex signalled by the
   network thread when it pushes to an empty ingress ring, with an atomic "is it
   sleeping" flag so the signal costs nothing when it is awake. That buys low
   latency *and* an idle broker that uses no CPU, instead of trading one for the
   other.
2. **Variable-size ring slots.** A 64-byte event currently occupies a 1088-byte
   slot, so the ring's cache footprint is 17× the useful data — and the capacity
   sweep showed footprint drives throughput. A byte-oriented ring with
   length-prefixed records would fix both the ceiling and the waste, at the cost
   of a harder wraparound case.
3. **UDP multicast egress**, with sequence-gap detection and no pretence that
   UDP is reliable. Per-subscriber TCP streams are the obvious thing to replace
   when subscriber count grows, since the ceiling study shows the broker's cost
   is per-frame work on the egress side.
4. **Re-run the microarchitectural experiments on bare metal.** The
   false-sharing mechanism is a hypothesis supported by the shape of a curve.
   It deserves cache-line transfer counts, and this host has no PMU.
5. **A second egress thread**, to find out whether the network thread or the
   dispatcher is the real ceiling at 5 M msg/s. I currently cannot say which,
   and I would rather know than guess.

---

## 12. Likely interview questions

Fifteen questions I would expect, with the points I should be able to make.

<details>
<summary><strong>1. Why single-producer/single-consumer queues instead of one multi-producer queue?</strong></summary>

Ownership that can be stated in one sentence and checked by reading two
functions. The entire concurrency surface of the broker is two index words per
ring — no CAS loop, no ABA hazard, no retry under contention. Multiple
publishers scale by getting one ring each.

The cost is real and I should name it: polling grows linearly with connection
count, and I gave up a single fair queue. For a system whose purpose is a
predictable tail, putting a contended CAS loop on the hot path is the wrong
trade.
</details>

<details>
<summary><strong>2. Walk me through the memory ordering in the ring buffer. Why not <code>relaxed</code> everywhere?</strong></summary>

The producer writes the slot, then does a **release** store to the write index.
The consumer does an **acquire** load of the write index, then reads the slot.
That pairing is what makes the *slot contents* visible — the release store
publishes everything that happened before it, and the acquire load prevents the
slot reads from being hoisted above it.

Symmetrically, the producer does an acquire load of the read index before
overwriting a slot, so the consumer's reads of that slot are ordered before the
overwrite.

With `relaxed` everywhere the indices would still be atomic and the program
would look like it worked, but there would be no ordering between the index and
the data, so a consumer could see an advanced index and stale slot contents.
A test writes a checksum through `claim()` and verifies it through `front()`
across a million handoffs precisely to check this, rather than trusting it.
</details>

<details>
<summary><strong>3. What happens when a subscriber stops reading?</strong></summary>

Its socket buffer fills, then the broker's `write()` returns `EWOULDBLOCK`, then
its egress ring fills. At that point the dispatcher applies that subscriber's
policy: `drop-newest` (default, counted), `disconnect`, or `block`.

The important part is the blast radius. Each subscriber has its own ring, so a
stalled one affects only itself — `backpressure_test` asserts that a healthy
subscriber alongside a stalled one loses nothing and sees no sequence gaps.
`block` is the exception and that is exactly why it is not the default: it
converts one slow subscriber into a system-wide stall, which the benchmark
measures rather than my asserting it.
</details>

<details>
<summary><strong>4. Why is ingress lossless but egress lossy? Isn't that inconsistent?</strong></summary>

It is deliberate, and the reason is who pays. An ingress ring has one publisher
behind it, so stalling it inconveniences only that publisher — TCP flow control
is free and sufficient. An egress ring is one leg of a fan-out, so stalling it
stops everyone, which means loss has to be an option and which loss is a policy
choice.

The result is a stronger guarantee than at-most-once alone: FlashBus never drops
an event because a publisher was fast; it drops events because a subscriber was
slow, and it names the subscriber and the count.
</details>

<details>
<summary><strong>5. How do you achieve lossless ingress without blocking or an unbounded buffer?</strong></summary>

By bounding the *input* to the decode rather than handling the output. Every
frame is at least a 32-byte header and the decoder holds at most one incomplete
frame, so reading `(free − 1) × 32` bytes can yield at most `free` frames. Below
two free slots I read nothing, the TCP receive window closes, and the kernel
throttles the publisher.

Two things to add unprompted: it costs nothing in the normal case, because a
ring with room to spare permits a read larger than the read buffer, so the bound
never binds until there is real pressure. And the rejection counter still exists
and should never move — counted rather than asserted so a wrong bound surfaces
as a number instead of as vanished events.
</details>

<details>
<summary><strong>6. How does your framing handle partial reads?</strong></summary>

TCP is a byte stream, not a message stream, so one `recv()` may contain three
frames and half of a fourth, or four bytes of a header. The decoder is a state
machine that holds at most one incomplete frame and reassembles across calls.

Two details worth volunteering. When nothing is held over, it parses frames **in
place out of the caller's buffer** and copies only the trailing partial frame, so
the common case does not memcpy every byte through an intermediate buffer. And
it is tested at every chunk size from 1 to 137 plus byte-at-a-time, because the
bug you want to catch is the one that only appears when a boundary lands in a
particular place.

A malformed header closes the connection. There is no resynchronisation, because
there is no way to know where the next real header starts.
</details>

<details>
<summary><strong>7. Where does the project allocate, and how do you know?</strong></summary>

At startup: rings, session buffers, histograms, pools. In the steady state,
nowhere — and I know because the tests and benchmarks link a **counting
`operator new`** and assert the count does not move. The sustained run reports
zero allocations per interval for every interval after the first, across 300
seconds and 90 million events.

That is the real argument for the memory pool, incidentally. It is 26% faster
per event, which is small; the reason to keep it is that a system which never
calls the allocator cannot be surprised by an arena lock or by fragmentation
that only appears after hours.
</details>

<details>
<summary><strong>8. Your memory pool is only 26% faster and shows no difference in the latency percentiles. Why keep it?</strong></summary>

Because the percentile result is a measurement artifact, not a contradiction,
and I can show that. The effect is 5.7 ns. Collecting one latency sample costs
two 24.5 ns clock reads, so the instrument is nine times larger than the effect
and no number of runs will resolve it. That is why `alloc_bench` also runs with
no clock in the loop, where total time over event count can see it.

And the reason to keep it is determinism rather than speed: zero allocations is
a *property*, verified by counting, and it bounds memory at startup instead of
discovering the bound in production.
</details>

<details>
<summary><strong>9. You benchmarked false sharing and first found no effect. What happened?</strong></summary>

I varied the layout but held the working set fixed, at the default ring size.
The effect was real the whole time; the experiment was not looking where it was
visible.

Varying ring capacity at a fixed slot size, the unpadded variant is **flat**
across a thousandfold change in footprint — 13–16 M msg/s throughout — while the
padded variant scales to 134 M msg/s. A throughput that does not respond to
cache footprint is the signature of a fixed per-item cost, and 60–75 ns per item
is the right order for a coherence round trip.

The mechanism I would offer, flagged as a hypothesis: unpadded, each side's own
bookkeeping store invalidates the other's copy of the shared line, so every item
pays a round trip; padded, the cached-index optimisation amortises one exchange
over a long run of slots. I cannot confirm it here because the host has no PMU,
and I would name the counters I would use.
</details>

<details>
<summary><strong>10. What is your throughput bottleneck?</strong></summary>

Frames per read, which is not where I expected it. One publisher issuing one
`write()` per 64-byte event cannot offer the broker more than its own syscall
rate — 0.45 M msg/s measured. Letting it put eight frames per `write()`
multiplies delivered throughput 11× with no change to the broker.

The broker's own ceiling is 5.19 M msg/s at zero loss. I should be honest that I
cannot currently attribute that between the network thread and the dispatcher —
finding out would take a second egress thread, and it is on the list.
</details>

<details>
<summary><strong>11. Your p50 is 11.8 µs over loopback. Is that good, and what dominates it?</strong></summary>

It is reasonable for two TCP hops through a broker on a virtualised host, and I
would not present it as a hardware-level number. Loopback has no driver, no PCIe
and no wire, so this is a lower bound on what the same code would show over a
network — and those omitted costs usually dominate on real hardware.

What dominates it here: two socket round trips, and before I fixed it, the idle
backoff. Parking immediately costs 4.5× the p50. That trade is published next to
every latency figure, because a number obtained by burning four cores on spin
loops is not honest without it.
</details>

<details>
<summary><strong>12. Why did adaptive batching make things worse, and why is it still in the code?</strong></summary>

The policy is the obvious one: batch of 1 while the queue is shallow, growing
with depth, so quiet periods get minimum latency. It measured 26× worse at p50
than a fixed batch of 8.

The premise is that a syscall is cheap relative to the inter-arrival time. At
2 µs between events and ~2 µs per `write()`, it is not — and a shallow queue is
the *normal* case, so the policy spends nearly all its time in the batch-of-1
regime, which is the one that is catastrophic.

It is still there because the measured negative result is worth more than a
policy tuned until it wins, and because the finding generalises: an adaptive
policy whose cheap branch is not actually cheap is worse than no policy at all.
</details>

<details>
<summary><strong>13. How would you scale this to a thousand subscribers?</strong></summary>

Today it would not scale well, and I can say why specifically. Each subscriber
costs a TCP stream, a 4096-slot ring at 1088 bytes per slot (4.5 MB), and a
per-frame copy on the fan-out path — so a thousand subscribers is 4.5 GB of
rings and a thousand copies per event.

In order: variable-size ring slots to kill the footprint; UDP multicast egress so
one packet stream serves many subscribers, with sequence-gap detection and no
pretence that UDP is reliable; then a TCP recovery channel for the gaps, but only
after gap detection has run long enough to show what the gaps look like.
</details>

<details>
<summary><strong>14. What is the worst bug you found, and how?</strong></summary>

An out-of-bounds write that the entire test suite passed over:

```cpp
std::vector<uint64_t> next_sequence_{kMaxTopicId + 1, 0};
```

That selects the initializer-list constructor and builds a two-element vector
`{1024, 0}` rather than 1024 zeros, so every topic id above 1 read and wrote past
the allocation. AddressSanitizer found it; nothing else would have, because a
small heap overflow usually does nothing visible.

That is the argument for the sanitizer presets being in CI rather than a thing I
ran once. The honourable mention is the reactor starvation bug, which an
end-to-end test found and which taught me that `io_context::poll()` running
*all* ready handlers is a starvation hazard when a handler re-arms itself.
</details>

<details>
<summary><strong>15. How do I know your benchmark numbers are real?</strong></summary>

Run them — `make bench`. But more usefully, the things I did so they would be
checkable:

Every result row carries the CPU, kernel, compiler, exact flags, Boost version
and whether a PMU was available. The README tables are **generated** from the
CSVs by `scripts/report.py`, so a stale number shows up as a diff rather than
sitting in prose. Configurations whose spread matters run repeatedly and report
the median with the spread beside it, because single runs here vary up to 3×.
Latency comparisons are paced below saturation, because otherwise latency is
just capacity ÷ throughput.

And the limits are stated rather than buried: no PMU on this host, so **no
microarchitectural claim appears anywhere**; loopback is not a NIC; the
benchmarks characterise one machine. I would rather you trust the method than
the absolutes.
</details>

---

## 13. Things I would want a reviewer to notice

Not the headline numbers — the decisions behind them.

- `DESIGN.md` was written **before** the implementation, and the non-goals are
  as explicit as the goals. The ingress semantics section was rewritten when the
  implementation changed, because the contract is the thing that has to stay
  true.
- Four of the five documented performance studies include a result I did not
  expect, and two are **negative results** that are published rather than tuned
  away.
- The repository documents **four bugs in its own benchmark harness**, because a
  measurement that looks like a result and is not is the most expensive kind of
  mistake in performance work.
- `DROP_OLDEST` is absent with a written reason, rather than present and
  subtly wrong.
- Nothing in this repository claims a cache-miss count, because the host cannot
  measure one.
