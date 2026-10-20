# Architecture comparison: three Rust candidates

The reassessment directive names three separate candidates. This document describes each candidate, why it
takes the shape it does, what was built or reused, and how it fared. The audit that motivated the split is in
REASSESSMENT.md. The measurements are in BENCHMARK_RESULTS.md Part II, and the decision is in
ARCHITECTURE_DECISION.md.

| | A. Rust-native server | B. Production-oriented proxy | C. Rust optimization layer |
|---|---|---|---|
| What | `rvk-server`: a multi-threaded, Valkey-compatible server for a declared string-cache scope | A process between clients and Valkey that multiplexes and pipelines | Client-side functionality in Rust: multiplexed connections, auto-pipelining, client-side caching |
| Hypothesis | One endpoint served from several cores beats Valkey's single-endpoint options and matches sharding without client routing | Batching inside a proxy reduces Valkey's per-request cost | Batching and caching in the client remove server work entirely |
| Built in Part II? | **Yes**, about 4,100 lines of Rust including unit tests | **No**: bounded by Part I measurements (REASSESSMENT.md §3); one continuity run | **No new code**: the maintained `redis` crate (redis-rs 1.7.1) is measured as is |
| Client contract | Unchanged (one endpoint, any Valkey client) | Unchanged | Requires the client library and, for caching, RESP3 |

## 1. Candidate A: `rvk-server`

### 1.1 Why this architecture

Part I found that Valkey's per-request cost is dominated by kernel networking. It also found that Valkey's
multi-core options either require cluster-aware clients (sharding) or performed poorly on 2 vCPUs
(`io-threads`, which still runs every command on the main thread). A Rust server can only beat Valkey if it
changes where that work runs. Options considered:

| Design | Per-core work | Multi-core | Why chosen or not |
|---|---|---|---|
| Single-threaded Rust clone of Valkey | Same system calls per request; user-space work in Rust instead of C | No | Kept as a configuration (`rvk-1t`): it isolates the **language and implementation** effect (ratio R1). Alone it is capped by Part I's per-core bound |
| tokio-based async server | Extra wakeups, task scheduling, and cross-thread handoffs (Part I's proxy spent 7% in `finish_task_switch`) | Yes | Rejected: the measured proxy showed tokio's scheduling overhead on exactly this kind of workload |
| Shared-nothing, thread per core, keys owned by threads (Dragonfly, Seastar) | No locks | Yes | Rejected for the PoC: with N threads, (N−1)/N of requests land on a thread that does not own the key and need a cross-thread message and a wakeup. That is the cost the proxy paid. Worth it at high core counts; not at 2 |
| **Thread per core with its own event loop, shared lock-sharded keyspace** (memcached, KeyDB) | One uncontended mutex per command (estimate: about 20 ns; not measured here), no cross-thread handoff | Yes, one endpoint | **Chosen.** Each connection stays on one thread for its lifetime, and any thread can execute any command |
| `io_uring` | Fewer system calls per request | Either | Deferred: it changes the kernel interface rather than the language, and C can use it equally (this Valkey tree does not). It would confound the language/architecture question |

### 1.2 What was built

| Part | Design |
|---|---|
| Event loop (`server.rs`) | One `mio` poll (edge-triggered `epoll`) per worker thread, each with its own `SO_REUSEPORT` listener, so the kernel spreads connections across workers without a dispatcher. Each chunk read is executed before the next read, and reading stops at a short read (no wasted `EAGAIN` read). **Round 2:** replies are written once per event-loop pass, after every ready connection has been served, as Valkey does in `beforeSleep`. Round 1 wrote each connection's replies as soon as they were produced, which woke the client once per reply, and the profile showed that as kernel time. `WRITABLE` interest only while output is pending; reading stops while more than 64 MB of output is pending |
| Protocol (`resp.rs`) | RESP2 multibulk and inline parsing that reproduces Valkey's limits, error texts and `sdsparsearg` tokenizing; RESP2/RESP3 reply encoding |
| Keyspace (`store.rs`) | 64 shards, each a cache-line-padded `Mutex` around a `hashbrown::HashTable` of 32-byte entries; key and value share one allocation. Round 1 kept entries in a separate slab with a table of slab positions, which cost one more dependent cache miss per lookup. Keyed SipHash (round 1: foldhash); the shard comes from hash bits the table does not use. Lock-free per-shard counters, published only on change. Per-shard min-heap of expiry deadlines |
| Commands (`cmd.rs`) | 61 commands (FEATURE_MATRIX.md) with Valkey's arity, flags and error texts. Multi-key commands lock their shards in ascending order (atomic and deadlock-free). `EXEC` locks the shards of every queued and watched key, then checks the watch flags |
| Expiry | Lazy on access; active expiry per shard every 100 ms with a 25% time budget |
| Eviction | `allkeys-lru` (5 samples, 16-entry candidate pool, as in `evict.c`), `allkeys-random`, `noeviction`; memory measured from jemalloc |
| Allocator | jemalloc, as Valkey uses |

### 1.3 Correctness evidence

Summarized from FEATURE_MATRIX.md:

* 50,358 differential cases against Valkey, 1 mismatch (`INCRBYFLOAT` formatting, declared partial).
* All concurrency invariants pass at 2 and 4 threads, with and without `maxmemory`.
* 68 of Valkey's own Tcl tests pass and 3 fail, all 3 on out-of-scope set commands (`SADD`, `SRANDMEMBER`). Six
  of the 8 files stopped at an out-of-scope command, `unit/info-command` stopped on a missing `INFO` field, and
  `unit/auth` runs only against a self-started server. No in-scope test failed.

### 1.4 What it does not have

Everything in FEATURE_MATRIX.md §8, including persistence, replication, cluster mode, ACL users, TLS, other data
types, Pub/Sub, scripting, `CLIENT TRACKING` and most observability. ARCHITECTURE_DECISION.md costs these. Two
are architectural rather than just missing:

* **Persistence.** Valkey snapshots with `fork()` and copy-on-write, which relies on one thread mutating the
  dataset. A multi-threaded server needs a non-fork snapshot design, such as Dragonfly's versioned snapshotting.
  That is substantial new design work, not a port.
* **`CLIENT TRACKING` and keyspace notifications.** Invalidation must be emitted to connections owned by other
  threads, which reintroduces cross-thread messaging on the write path.

## 2. Candidate B: production-oriented proxy

Not re-implemented. REASSESSMENT.md §3 shows that the proxy's CPU floor is set by measured kernel costs on both
hops, so no implementation can beat sharding on CPU per operation on the measured host. A production version
would also need the restart-propagation fix, TLS, ACL pass-through, cluster topology handling and session pinning
for connection-scoped commands. The `p2-proxy` suite places the Part I prototype on the Part II host for
reference.

## 3. Candidate C: established Rust client functionality

Measured with `rsclient`, a 322-line driver around the `redis` crate 1.7.1. It is not a new implementation.

* **Multiplexing.** 50 logical clients share one `MultiplexedConnection`. The crate pipelines whatever is in
  flight on that socket, so the server sees batches without the application batching. This is the same
  mechanism as the Part I proxy, inside the client process, so it has no extra hop.
* **Client-side caching.** The crate's experimental `cache-aio` feature uses RESP3 `CLIENT TRACKING`, so the
  server sends invalidation pushes. Cached GETs never reach the server.
* **GLIDE.** Valkey GLIDE has a Rust core with the same multiplexing and, since 2.4, a client-side cache. Its Rust
  crate is a preview, and GLIDE is normally used from Python, Java, Node or Go. It was not measured here.
  redis-rs is the measured stand-in for "established Rust client functionality".

## 4. Results by candidate

Measured, from BENCHMARK_RESULTS.md Part II (medians of 3 trials; data service on 2 vCPUs). Round 1 is the
pre-registered round; round 2 re-measured Candidate A after the review fixes and two profile-guided changes,
and is post hoc.

| | A. `rvk-server` | B. Proxy | C. redis-rs client functionality |
|---|---|---|---|
| Hypothesis held? | **Partly.** It beat Valkey's single-endpoint option (`io-threads 2`) by 1.5–3.2× in round 2, but only roughly matched two shards (R3 0.85–1.19), short of the 1.20 needed | **No.** On the measured host its CPU floor sits above sharding's, as predicted (REASSESSMENT.md §3.2) | **Multiplexing: yes.** **Caching: no** |
| Throughput, `read90`, 50 connections | Round 2: 262,324 ops/s (`rvk-2t`) vs 239,153 for two shards and 110,111 for one Valkey | 132,926 ops/s vs 236,016 for two shards (0.56×) | 200,356 ops/s on one Valkey with all clients on one socket, vs 117,740 with a socket each (1.70×) |
| Server CPU per operation | Round 2: 7.00 µs vs 8.10 for two shards, unpipelined; 1.33 vs 1.15 µs at depth 16 (round 1: 7.22 vs 8.22; 1.80 vs 1.37) | 13.18 µs vs 8.22 (1.60×) | 1.89 µs vs 8.26 (−77%) |
| Per-core (language and implementation) effect | Round 1: 0.80–0.91× of Valkey unpipelined, 0.73–0.96× at depth 16. Round 2: 0.97–1.03× and 0.96–1.14×, parity (never the 1.20× that would show an advantage). Both came from implementation choices, not the language | – | – |
| Latency | p99 1.42× single-threaded Valkey at 20k ops/s in round 2 (1.58× in round 1); below every single-endpoint Valkey option at 60k and 100k in both rounds | Part I: p99 1.47–2.04× the best Valkey option at every load measured | p99 455 µs vs 848 µs with a socket per client |
| Memory | Round 1: 172.5 B per key vs Valkey's 169.5 B. Round 2: 197.6 B, so fewer keys held under `maxmemory` | No dataset of its own | Cache: client memory for up to 100,000 entries |
| Correctness | Gate 2 met in both rounds, but the round 1 build had deadlocks only the review found. All 57 confirmed review findings (of 62 reported) fixed or documented | Part I: hid a backend restart from some clients (silent data loss); 17.6% of Valkey's tests pass through it | Multiplexing: 0 stale reads. Cache: 34% of reads stale, from a library race |
| Decision (ARCHITECTURE_DECISION.md §II.1) | **NO-GO** | **NO-GO** | **Adopt multiplexing** through a maintained client; **reject** the experimental cache |

What the three results say together: on this budget (2 vCPUs, loopback, string-cache scope), the server-side
gains that `rvk-server` delivered came from architecture (multi-threading behind one endpoint) and from copying
Valkey's own techniques (write batching), and they were matched by sharding. Rust designs that were not built
(shared-nothing thread-per-core, `io_uring`, batch prefetching, more server cores) are not covered by this result
(ARCHITECTURE_DECISION.md §II.1). The largest gain per server CPU came from batching in the client, which needs no
new server, no new proxy and no new code.

## 5. Multi-agent review of `rvk-server`

Five read-only reviewers with adversarial verifiers reported 62 findings: 57 confirmed (52 from the concurrency,
protocol and I/O, semantics and security reviewers, 5 from the fairness reviewer), 4 refuted and 1 uncertain.
They include two self-deadlocks reachable by any client and a remotely triggerable abort, none of which the
differential test, the concurrency invariants or the benchmarks had found. REASSESSMENT.md §7 summarizes them
and what they mean for the evidence. FEATURE_MATRIX.md §11 says how each was handled. The raw findings are in
`results/poc/review_findings.json`.
