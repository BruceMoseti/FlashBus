# FlashBus Design & Semantics

This document is the contract. It is deliberately written before the
implementation so that the guarantees are decided rather than discovered.
If the code and this document disagree, the code is wrong.

FlashBus does one thing: **move small binary events from producers to
consumers with low and predictable latency.** It is not a durable broker.

---

## 1. Guarantees

| Property | Guarantee |
| --- | --- |
| Transport | TCP (`SOCK_STREAM`, `TCP_NODELAY` set) |
| Ordering | FIFO per `(publisher connection, topic)` stream |
| Buffers | Bounded everywhere; capacity fixed at startup |
| Durability | None. Nothing is written to disk, ever |
| Delivery | At-most-once, while connected |
| Backpressure | Explicit, per subscriber, with a configured policy |
| Message format | Binary, fixed 32-byte header, little-endian |
| Allocation | No heap allocation on the steady-state hot path |
| Failure behaviour | A disconnected or slow subscriber may miss events |
| Threading | Explicit ownership: ingress I/O, dispatch, egress I/O |

### What FlashBus does *not* guarantee

* No persistence, no replay, no replication, no consensus.
* No total order across publishers. Two publishers writing to the same topic
  interleave; each publisher's own events stay in order.
* No exactly-once delivery. If a subscriber's queue overflows, its configured
  policy decides what is lost, and the loss is *counted and reported*.
* No delivery to subscribers that connect after an event was published.
* Benchmarks characterise one machine. They are not claimed to generalise.

### Ordering, stated precisely

For a single publisher connection `P` publishing events `e1, e2, ... en` to
topic `T`, any subscriber of `T` that receives both `ei` and `ej` (`i < j`)
receives `ei` before `ej`. Events may be missing from that sequence; the
subscriber detects the gap via the per-stream sequence number.

The dispatcher assigns each accepted event a monotonically increasing
**broker sequence** per topic. Subscribers use it for gap detection, so a
subscriber can distinguish "the publisher never sent it" from "FlashBus
dropped it for me".

One consequence is worth stating rather than discovering: an event rejected at
*ingress*, because the dispatcher was behind and the publisher's ingress ring
was full, never reaches the sequencer and so never gets a sequence number.
Subscribers therefore see no gap for it. Such an event is visible only in the
broker's `frames_rejected` counter. Egress drops, which happen after
sequencing, do show up as subscriber gaps.

---

## 2. Threading model

Thread ownership is explicit. No component is touched by two threads unless
the type is an SPSC queue, and then exactly one thread is the producer and
exactly one is the consumer.

```
  publisher conn ──► [ingress SPSC] ──► dispatcher ──► [egress SPSC] ──► subscriber conn
    (net thread)       1P/1C            (1 thread)       1P/1C            (net thread)
```

* **Network thread(s)** own sockets, decode ingress frames, and write egress
  bytes. One `io_context`; N threads is supported but 1 is the default because
  a single thread removes handler synchronisation entirely.
* **Dispatcher thread** owns topic routing, sequence assignment and the
  backpressure decision. It is the only writer of egress queues.
* Each queue is a strict SPSC ring. There is no MPMC queue in FlashBus, by
  choice: ownership that can be stated in one sentence is worth more than a
  clever lock-free algorithm nobody can review.

Multiple publishers scale by having **one ingress ring per connection**, which
the dispatcher polls round-robin. This keeps every queue SPSC.

---

## 3. Wire protocol

Fixed 32-byte header, then payload. All multi-byte integers are
**little-endian** on the wire, chosen because every target host is
little-endian and it makes the encode a `memcpy` there; the accessors are
byte-wise so a big-endian host stays correct.

```
 offset  size  field
      0     2  magic          0xFB55
      2     1  version        currently 1
      3     1  type           1=DATA 2=SUBSCRIBE 3=HEARTBEAT
      4     4  topic          numeric topic id
      8     4  payload_size   bytes following the header
     12     2  flags          bit 0 = batch continuation hint
     14     2  reserved       must be 0
     16     8  sequence       per-stream, starts at 1
     24     8  timestamp_ns   publisher steady_clock at publish
```

A decoder MUST reject a frame whose magic is wrong, whose version is
unknown, whose `payload_size` exceeds the configured maximum, or whose
`reserved` field is non-zero. Rejection closes the connection; FlashBus never
attempts to resynchronise a corrupt stream.

TCP is a byte stream, not a message stream. The decoder is a state machine
that tolerates any fragmentation, including a header split across several
`recv()` calls. This is tested explicitly, byte at a time.

---

## 4. Backpressure

Every subscriber has its own bounded egress ring. When the dispatcher finds
that ring full, the session's configured policy applies:

| Policy | Behaviour | Who suffers |
| --- | --- | --- |
| `DROP_NEWEST` | Event is not enqueued; the subscriber's `dropped` counter increments | only this subscriber |
| `DISCONNECT` | Subscriber is closed and must reconnect | only this subscriber |
| `BLOCK` | Dispatcher waits for room | **everyone** |

`BLOCK` is implemented because it is the obvious thing to ask for, and it is
not the default because on a fan-out path it converts one slow consumer into a
system-wide stall — which the slow-consumer benchmark measures rather than
asserts. `DROP_NEWEST` is the default. This is the central design statement of
the project: **a bounded queue with a loss policy is a design; an unbounded
queue is a deferred crash.**

`DROP_OLDEST` is deliberately **not** implemented. The oldest queued event sits
at the index the consumer owns, so dropping it would require the dispatcher to
advance the consumer's read index — a compare-exchange on a word that is
otherwise single-writer, which would cost the SPSC ring the property that makes
it reviewable in one sitting. For a market-data feed the newest event is also
the one worth keeping, so the trade buys little. If it were needed, the honest
implementation is a separate queue type, not a flag on this one.

Slow-consumer isolation is a tested property, not an aspiration: a subscriber
that stops reading must not move the p99 of the subscribers that keep reading.

---

## 5. Allocation

Hot path means: a steady-state publish, dispatch, or deliver of one event.

* All rings are allocated once at construction, power-of-two capacity, and
  hold **inline fixed-size slots**. Payload capacity per slot is a startup
  parameter, so an event is copied into its slot with no indirection and no
  refcount.
* Socket read and write buffers are preallocated per session and reused.
* `MemoryPool<T>` provides O(1) `acquire`/`release` from a preallocated block
  with an intrusive free list, for the variable-lifetime objects that the
  ring design cannot cover (order-book order nodes in the market-data demo).
* Steady-state allocation count is asserted to be zero by test, using a
  global `operator new` counter, rather than assumed.

---

## 6. Measurement

A performance claim with no measurement attached is an opinion.

* `steady_clock` is the reference clock. An RDTSC path exists but is treated
  as an experiment that must be validated against `steady_clock`, not as a
  default.
* Latency is end-to-end: subscriber receive timestamp minus the publisher
  timestamp carried in the header. Single machine, single clock, so there is
  no clock-skew correction to get wrong.
* Samples go into a fixed-memory HDR-style histogram. Nothing is printed on
  the hot path; printing a latency per event measures the printer.
* Every result records CPU, kernel, compiler, flags, payload size, topology
  and durations, so a number can be traced back to the conditions that
  produced it.
* Correctness tests and performance tests are separate suites. "The benchmark
  ran" is not "the code is correct".

---

## 7. Non-goals

Kafka compatibility, distributed consensus, replication, a web dashboard,
Kubernetes, a database, or an order-matching engine. Each of those would
dilute the technical identity of the project, which is low-latency C++
systems engineering on Linux.
