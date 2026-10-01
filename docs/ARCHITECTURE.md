# FlashBus Architecture

`DESIGN.md` says what FlashBus guarantees. This says how, and why each piece
looks the way it does.

---

## The shape of it

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

Two threads, three stages, and every queue between them is strictly
single-producer / single-consumer. There is no MPMC queue in FlashBus. Several
publishers scale by getting **one ingress ring each**, which the dispatcher
polls round-robin, so adding publishers never turns a queue into a contended
one.

## Thread ownership

| Owned by | What |
| --- | --- |
| Network thread | every socket, every read and write buffer, every `StreamDecoder`, the `Session` objects, the acceptor |
| Dispatcher thread | the routing table, the per-topic sequence counters, the backpressure decision |
| Shared | exactly one `Channel` per connection: two SPSC rings, a closed flag, and counters |

`Channel` is the only shared mutable state, and it deliberately holds **no
socket and no `io_context`**. That is what keeps teardown simple: either thread
may hold the last reference and destroy it, so there is no cross-thread
lifetime dance around a socket that only one thread is allowed to touch.

Counters are single-writer. `Counter` is a relaxed load-add-store rather than
`fetch_add`, because one writer needs no atomic read-modify-write and a
`lock xadd` per event is tens of cycles of pure waste. The counters the network
thread writes and the counters the dispatcher writes sit in separate
cache-line-aligned groups inside `Channel`, so the two threads never contend
for a line while counting.

## The event loop

The network thread is a reactor:

```
loop:
    io.poll()            run whatever socket handlers are ready
    for each session:
        pump_egress()    drain the egress ring into write() calls
        ensure_read_armed()
    reap closed sessions
    if nothing happened for idle_spin_us:
        io.run_one_for(idle_sleep_us)     park in epoll
```

Three details in that loop were bugs first:

**Reads are armed by the loop, never by the read handler.** A handler that
re-arms itself lets one busy publisher's reads monopolise a single
`io.poll()` call: `poll()` runs ready handlers until there are none, a
completed read immediately arms another that is also ready, and the loop never
reaches the line that writes anything out. The ingress ring fills, the
dispatcher moves it all to the egress rings, and the egress rings overflow —
while the socket they were meant to be written to was never touched.

**Egress is drained before the next read is requested.** Same reason, stated as
an ordering rule.

**The batch size bounds frames per `write()`, not frames per loop iteration.**
Capping the latter looks like the same thing and is not: one 64 KiB read puts
roughly 670 small frames into the ingress ring, so a 32-frame-per-iteration cap
throttles egress to a twentieth of ingress and the ring overflows for no reason
other than the cap.

The idle backoff is measured in **time, not iterations**. The dispatcher's idle
iteration costs about 8 ns and the network loop's costs hundreds, so an
iteration count means something different on each loop and something different
again on the next machine. At 2000 iterations the dispatcher was giving up after
16 µs, which is shorter than the gap between events at 100k msg/s, so it parked
between messages and put its sleep interval on the critical path. Switching the
unit moved p50 from 67 µs to 12 µs.

## Backpressure, in two different places

Ingress and egress are not symmetric, and that asymmetry is the design.

**Ingress is flow-controlled and lossless.** Before each read, the session
computes how many frames its ingress ring could still absorb and reads at most
that many bytes' worth: every frame is at least a 32-byte header and the decoder
holds at most one incomplete frame, so reading `(free - 1) × 32` bytes can
produce at most `free` frames. With fewer than two free slots nothing is read at
all, the TCP receive window closes, and the publisher is throttled by the
kernel. One slow publisher slows only itself.

In normal operation this costs nothing: a 4096-slot ring with room to spare
permits a read of 131 KB, which is larger than the 64 KiB read buffer, so the
bound never binds. It binds only under real pressure, which is when it should.

**Egress is bounded and lossy by policy.** A subscriber gets its own ring, and
when it fills, that subscriber's policy decides. It cannot be made lossless
without blocking the fan-out for everyone, which is exactly what the `BLOCK`
policy does and exactly why it is not the default.

So: *the broker never drops an event because a publisher was fast. It drops
events because a subscriber was slow, and it says which subscriber and how
many.*

## Sequencing

The dispatcher stamps every routed event with a per-topic broker sequence,
replacing the publisher's own. With several publishers on one topic this is the
only sequence a subscriber could use for gap detection — publisher sequences
interleave into nonsense. A subscriber's first event on a topic sets its
baseline, so joining mid-stream is not reported as loss.

The routing table is a flat `vector` indexed by topic id, not a hash map, which
is why topic ids are bounded at 1023 and why topic *names* are resolved to ids
once at startup. Routing is an array index on the hot path.

## Memory

Nothing on the steady-state path allocates, and that is checked rather than
claimed: tests and benchmarks link an `operator new` that counts calls, and
assert the count does not move.

* Rings are allocated once with power-of-two capacity and hold **inline
  fixed-size slots**. A queued frame therefore needs no indirection, no
  refcount and no allocation. The cost is a fixed 1 KiB payload ceiling and a
  ring that is mostly empty space when payloads are small — a 64-byte event
  occupies a 1088-byte slot. That trade is measured, not assumed: the ring
  capacity sweep in `queue_bench` shows what the footprint does to throughput.
* Ring storage is value-initialised at construction, so every page is faulted
  in during startup instead of during the first burst.
* Read and write buffers are per-session and sized once. The write buffer holds
  exactly one maximum-size batch, which is an exact bound rather than a guess
  because a new batch is only started once the previous one has fully left.
* `MemoryPool<T>` serves the objects a fixed-size ring cannot express — order
  nodes in the market-data consumer. It is single-threaded by design: one pool
  per owning thread costs nothing and keeps acquire and release down to a few
  instructions with no atomics. It returns `nullptr` when exhausted, because a
  pool that grows is just a slower malloc.

## Protocol

32-byte fixed header, little-endian, then payload. The header is validated
before anything else uses it: wrong magic, unknown version, unknown type,
non-zero reserved field, or a payload length over the configured maximum all
end the connection. FlashBus never tries to resynchronise a corrupt stream,
because there is no way to know where the next real header starts.

`StreamDecoder` tolerates any fragmentation, including a header split across
several `recv()` calls, and is tested a byte at a time and at every chunk size
from 1 to 137. When nothing is held over from the previous chunk it parses
frames in place out of the caller's buffer and copies only the trailing partial
frame, so the common case does not memcpy every byte through an intermediate
buffer.

## Clients

Publisher and subscriber use blocking sockets. That is not laziness: a client
has one socket and one job, and a blocking `write()` is both the simplest and
the lowest-latency way to do it. Asynchronous machinery earns its complexity
where there are many sockets to multiplex, which is the broker.

The publisher stamps `timestamp_ns` at `publish()` time, before any batching,
so that queueing delay introduced by batching appears in the measured latency
instead of hiding inside it.

## Where the files are

```
include/flashbus/
  clock.hpp        steady_clock reference, a validated TSC path, the load Pacer
  metrics.hpp      HDR-style histogram, single-writer Counter, result + CSV
  platform.hpp     machine facts, CPU affinity, rusage, allocation counting
  message.hpp      header and frame types, topic ids
  protocol.hpp     little-endian codec and the streaming decoder
  ring_buffer.hpp  the SPSC ring, padded and unpadded
  mutex_queue.hpp  the baseline that exists to be beaten
  memory_pool.hpp  preallocated block pool
  transport.hpp    Channel, policies, server config, Server
  session.hpp      one connection, network-thread side
  dispatcher.hpp   routing, sequencing, backpressure
  publisher.hpp    publisher client
  subscriber.hpp   subscriber client with gap detection
  cli.hpp          argument parser that rejects what it does not know
```

Two deviations from the layout sketched in the project brief, both deliberate:
`cli.hpp` is shared by the apps and the benchmarks rather than duplicated, and
the end-to-end, overload, burst and batching sweeps are driven by
`scripts/run_benchmarks.py` over `flashbus-bench` and `market-data-demo` instead
of separate benchmark binaries, because those sweeps differ only in their
arguments.
