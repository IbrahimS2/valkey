# ADR: Rust alternative to Valkey

| Field | Value |
|---|---|
| Status | **Decided after the reassessment.** A. Rust-native server: **NO-GO**. B. Proxy: **NO-GO**. C. Rust client functionality: **adopt client-side multiplexing from a maintained client (no new code); reject the experimental client-side cache** |
| Date | 2026-10-19 |
| Supersedes | The Part I decision record below, which evaluated only the proxy. It is kept as first committed |
| Scope | Valkey source tree at `741b0159`; `rvk-server` round 1 and round 2 builds; one 4-vCPU KVM guest over loopback, with 2 CPUs for the data service (BENCHMARK_PLAN.md Part II) |
| Inputs | [REASSESSMENT.md](REASSESSMENT.md), [ARCHITECTURE_COMPARISON.md](ARCHITECTURE_COMPARISON.md), [FEATURE_MATRIX.md](FEATURE_MATRIX.md), [BENCHMARK_PLAN.md](BENCHMARK_PLAN.md) Part II, [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) Part II, [THREAT_MODEL.md](THREAT_MODEL.md) Part II |

# Part II: decision after the reassessment

Labels used below: **measured** (traceable to `results/raw2/` or `results/poc/`), **estimate** (judgment, with its
basis), and **hypothesis** (plausible, not tested).

## II.1 Decision

| Candidate | What was evaluated | Decision | Deciding evidence |
|---|---|---|---|
| **A. Rust-native server** | `rvk-server`: a multi-threaded, lock-sharded server for a declared string-cache scope, built for this purpose (about 4,100 lines of Rust) | **NO-GO** | Pre-registered Gate 3 (round 1): GO conditions met on 0 of 5 points, CONDITIONAL GO conditions on 1 of 5 (3 needed). A post-hoc round 2 after fixes met the CONDITIONAL GO throughput condition on 4 of 5 points but failed its latency condition (p99 1.42× at 20k ops/s), and a post-hoc round cannot overturn round 1 anyway. Gate 4 is unmet: no material advantage over sharding, against a feature gap estimated at 18–29 engineer-quarters |
| **B. Production proxy** | Part I's prototype, plus a continuity run on the Part II host | **NO-GO** | An architectural CPU floor on the measured host (REASSESSMENT.md §3.2): with that host's per-request kernel cost, no implementation of this architecture can beat sharding on CPU per operation. Measured today: 0.56× the throughput of two shards at 1.60× their CPU per operation |
| **C. Rust optimization layer** | The maintained `redis` crate (redis-rs 1.7.1): multiplexed connection and experimental client-side cache | **Adopt multiplexing through a maintained client (Valkey GLIDE, or the multiplexing client of the application's language). Build nothing. Reject the experimental cache** | Multiplexing: 1.70× throughput and −77% server CPU per operation on single-threaded Valkey, with no server change and 0 stale reads. Cache: 1.14× more throughput, 9% more server CPU per operation, and 34% of reads stale |

**In one sentence:** keep Valkey, get more from it with client-side batching (multiplexing, pipelining) and use
cluster mode to scale out. Neither a Rust server nor a Rust proxy delivered an advantage, on the measured scope,
that those existing options do not.

**What this NO-GO covers, and what it does not.** It covers `rvk-server` as built: one event loop per thread over
a shared lock-sharded keyspace, string-cache scope, no persistence, 2 vCPUs for the data service, synthetic
workloads over loopback. It does not show that no Rust server can outperform Valkey. These were not built or
measured: shared-nothing thread-per-core designs at high core counts (as in Dragonfly), `io_uring`, batch key
prefetching in `rvk-server`, and any host with more than 2 server cores. §II.9 names the experiment that would test
the most promising of these, and the production evidence that would justify running it.

## II.2 Context and assumptions

* The repository is Valkey's source, not a deployment. It records no production workload, configuration, metrics
  or engineering requirement (ASSESSMENT.md §2). Every workload is synthetic: 100-byte values (16 KiB in one
  scenario), 1M keys, uniform or Zipf 0.99 access (BENCHMARK_PLAN.md §4).
* One 4-vCPU KVM guest: data service on CPUs 0–1, load generator on CPUs 2–3, loopback networking. At this size
  Valkey's `io-threads` is outside its intended regime, so R2 compares `rvk-server` with a weak configuration.
* Decision thresholds were committed (`da0b089`) before any Part II result file (BENCHMARK_PLAN.md Part II). Round 1
  is the pre-registered round. Round 2 was designed after seeing round 1 and is reported as post hoc.
* Part I ran on a different host (Xeon model 85, 2.8 GHz) from Part II (Xeon model 207, 2.1 GHz). Part I and
  Part II numbers are not compared with each other. Within Part II, baselines moved by up to 28% between rounds
  on the same host and binaries (pipelined `valkey-1t` and `valkey-2shard` rose 16–28%; the other baselines moved
  by −11% to +10%), so every ratio uses baselines from the same round.

## II.3 Gates 1–4 by candidate

| Gate | A. `rvk-server` | B. Proxy | C. Client functionality |
|---|---|---|---|
| **1. Architectural feasibility** | **Met.** One endpoint served from several cores could plausibly beat Valkey's single-endpoint options and match sharding without client routing (ARCHITECTURE_COMPARISON.md §1.1). Scope declared in FEATURE_MATRIX.md, written alongside the build (not part of the pre-registration) | **Not met for performance.** The proxy terminates every client socket, so it pays the same per-request kernel cost as Valkey, plus a second hop. Its estimated floor (about 14–16 µs/op in Part I) met or exceeded two shards' 12.5–14.8 µs/op on the same host, and on the Part II host the measured proxy used 1.60× two shards' CPU per operation. So it was not rebuilt | **Met.** Batching in the client removes per-request system calls at the server without a hop, and maintained implementations exist |
| **2. Functional validity** | **Met as pre-registered, in both rounds:** 1 differential mismatch in 50,358 cases (`INCRBYFLOAT` formatting, declared partial); concurrency invariants pass at 2 and 4 threads; 0 errors, timeouts or reply misattributions in every run. **But** the round 1 build had two self-deadlocks and a remotely triggerable abort that only code review found (REASSESSMENT.md §7). The round 2 build fixes or documents every confirmed review finding (57 of the 62 reported: 52 on correctness and security, 5 on benchmark fairness), and 22 regression scenarios pass | **Not met as built in Part I.** The proxy hid a backend restart from 16 and 5 of 50 clients, which then ran against an empty server: silent data loss, a blocker under the directive. The defect is fixable. Only 17.6% of Valkey's tests pass through it, because multiplexing breaks connection-scoped commands, which is architectural | **Multiplexing: met.** 0 errors and 0 stale reads in every run. **Cache: not met.** 34% stale reads, caused by a race in the library (REASSESSMENT.md §6.3). That is a blocker for any data where staleness matters |
| **3. Performance validity** | **Not met.** Round 1 (pre-registered): GO 0 of 5, CONDITIONAL GO 1 of 5. Round 2 (post hoc): GO 0 of 5; CONDITIONAL GO throughput condition 4 of 5, latency condition failed (p99 1.42× the best single-endpoint option at 20k ops/s) | **Not met.** On the Part II host: 0.56× two shards at 1.60× CPU per operation on `read90` | **Multiplexing: met.** The pre-registered adoption bar is ≥ 1.20× throughput or ≥ 20% less server CPU per operation; measured 1.70× and −77%. **Cache: not met.** 1.14× over multiplexing alone, with server CPU per operation 9% higher |
| **4. Production and economic validity** | **Not met.** Against Valkey's existing scale-out option (sharding or cluster), no material advantage: R3 0.72–1.17 in round 1 and 0.85–1.19 in round 2 on the Gate 3 points, never ≥ 1.20 there (outside them, round 1 measured 1.29× at 10 connections and 1.27× on `SET EX 5`). The only large measured advantage (R2) is against `io-threads` on 2 vCPUs, outside its regime, and is unverified on 4 or more cores. The measured advantage exists mainly in the unpipelined regime, which Candidate C removes (§II.8). Closing the feature gap is estimated at 18–29 engineer-quarters plus 1–2 FTE ongoing (§II.7). No production workload exists to set against that | **Not met.** More CPU per operation than sharding means negative infrastructure savings, so there is no break-even point (Part I §8) | **Met for multiplexing.** No development; GLIDE is Valkey's own client project with a Rust core. Cost: a client migration, and dedicated connections for connection-scoped commands (THREAT_MODEL.md §8.1). GLIDE itself was not measured here |

## II.4 Evidence summary

All numbers are measured, medians of 3 trials (startup: median of 5), from BENCHMARK_RESULTS.md Part II. R1 = `rvk-1t`
÷ `valkey-1t`, R2 = `rvk-2t` ÷ `valkey-io2`, R3 = `rvk-2t` ÷ `valkey-2shard`. Ranges in the first table cover the five
Gate 3 points (four core workloads at 50 connections, and `read90` at 200 connections).

| Question | Round 1 (pre-registered) | Round 2 (post hoc) |
|---|---|---|
| Does Rust help per core? (R1, same architecture and CPU as `valkey-1t`) | No: 0.80–0.91× unpipelined, 0.73–0.96× at depth 16 (outside the Gate 3 points: 1.14× and 1.18× at 1 and 10 connections, below the 1.20× bar) | Parity: 0.97–1.03× unpipelined, 0.96–1.14× at depth 16, at the same CPU per operation (8.74 vs 8.77 µs on `read90`) |
| Does one multi-threaded endpoint beat Valkey's single-endpoint option? (R2) | Yes on 2 vCPUs: 2.2–2.8× unpipelined, 1.1–1.9× at depth 16 | Yes: 2.8–3.2× and 1.5–2.9× |
| Does it beat two shards on the same 2 CPUs? (R3) | No: 0.99–1.17× unpipelined (mostly limited by the load generator), 0.72–0.88× at depth 16 | Roughly level: 1.05–1.19× unpipelined, 0.85–1.06× at depth 16 |
| p99 at a fixed offered load, `rvk-2t` ÷ best single-endpoint Valkey | 1.58× at 20k ops/s; 0.35× at 60k; 0.66× at 100k | 1.42× at 20k; 0.55× at 60k; 0.33× at 100k |
| Memory per key (1M × 100 B, both jemalloc) | 172.5 B vs Valkey 169.5 B | 197.6 B (RSS 254.5 B vs 173.7 B) |

| Other measured comparisons | Result |
|---|---|
| Pipelining (depth 16) on `valkey-1t`, `read90` | 6.3× (round 1) and 7.9× (round 2) the unpipelined throughput, at 1.13–1.32 µs/op instead of 8.3–8.8 |
| Multiplexing client (redis-rs, 50 logical clients on one socket) on `valkey-1t` | 1.70× throughput, 8.26 → 1.89 µs/op. On `rvk-2t`: 1.84 µs/op at 191,582 ops/s, no better than `valkey-1t` |
| 16 KiB values, `read90` | `rvk-2t` 0.97× two shards |
| `SET EX 5`, `mixed50` | `rvk-2t` 1.27× two shards (higher throughput also keeps more keys alive) |
| `maxmemory 256mb allkeys-lru`, uniform (round 2) | Valkey held 1.61M keys, hit rate 0.322; `rvk-1t` held 1.00M keys, hit rate 0.200 |
| Low concurrency (1 connection) | `rvk` p50 37–38 µs vs Valkey 42 µs |
| Startup to first PONG, empty | `rvk` 5.8 ms vs Valkey 37.5 ms |
| Server killed under load and restarted | Both drop every connection, so clients see the failure, and both come back empty (persistence was off for Valkey; `rvk` has none) |
| Part I proxy on the Part II host | 132,926 ops/s at 13.18 µs/op vs two shards 236,016 at 8.22 |

## II.5 Performance and resource efficiency: what came from Rust and what from the architecture

| Effect | Source | Evidence |
|---|---|---|
| Per-core speed | **Implementation, not language.** Round 1's deficit came from writing replies per connection (33.4% of CPU in the kernel's wakeup path vs Valkey's 18.7%) and from dependent cache misses in lookups. Round 2 copied Valkey's `beforeSleep` write batching and reached parity. At depth 16 that cut the wakeup share from 23.6% to 5.9%; unpipelined it fell only to 29.6% (Valkey: 18.0%), so the profiles do not fully explain the unpipelined parity, during a round in which Valkey's own unpipelined `read90` baseline also fell 7.5% | Profiles, `results/profile2/`, `results/profile2_r2/`; calibration ladder 520k → 575k → 594k → 861k ops/s (BENCHMARK_RESULTS.md §II.4) |
| Multi-core single endpoint | **Architecture.** Any thread runs any command under an uncontended shard lock. Valkey's `io-threads` still executes every command on the main thread | R2 1.5–3.2× (round 2) on 2 vCPUs |
| Two shards versus one multi-threaded endpoint | **Architecture, roughly a draw.** Shards need no locks but need client routing; `rvk` needs locks but serves one endpoint | R3 0.85–1.19× (round 2) |
| Pipelined lookups | **Valkey's prior optimization.** Valkey prefetches keys for a whole batch (`memory_prefetch.c`); its lookups are about 6% of pipelined CPU. `rvk` does not, and lookups plus key comparisons are 54% of its pipelined CPU | Depth-16 profiles |
| Bookkeeping | **Feature difference.** About 5% of Valkey's pipelined CPU goes to per-command timing for statistics and slow-command logging, which `rvk` does not have. This favors `rvk` | `getMonotonicUs` 5.0% in the depth-16 profile |
| Memory safety | **Rust.** 0 memory-safety defects among the 57 confirmed review findings (52 of them correctness and security findings) on the round 1 build, about 3,500 lines (about 4,100 after the fixes). But the defects found (deadlocks, a panic-abort, unbounded resource use, quadratic work) are classes Rust does not prevent | THREAT_MODEL.md §7.4 |
| Low startup time | **Scope.** `rvk` has no persistence to load and few subsystems to initialize | 5.8 ms vs 37.5 ms |

**Resource costs of `rvk-server` measured here:** about 17% more memory per small key in round 2 (47% more RSS),
which in a memory-bounded cache means fewer keys held and a lower hit rate; about 100 µs higher p99 than
single-threaded Valkey for the 2-thread server at low load (cause not investigated; hypothesis: cross-CPU
wakeups between unpinned workers and lock handoff); and, at depth 16, more CPU per operation than two shards on
2 of 4 core workloads in round 2 (all 4 in round 1).

## II.6 Correctness, reliability and security (summary of THREAT_MODEL.md Part II)

* **Candidate A.** No extra hop or secret holder, but every client byte is parsed and executed by new code. The
  review found two self-deadlocks reachable by any client, a process abort, unbounded memory paths and two quadratic
  CPU paths (request re-parsing and the WATCH duplicate check), all fixed. Remaining known exposures: whole-value
  copies in `APPEND`/`SETRANGE`, `KEYS` and `FLUSHALL` holding every shard lock, lock-held work stalling other
  threads, and any remaining panic aborting the process and losing all data. Missing security features: ACL users,
  TLS, audit and command logs.
* **Candidate B.** Unchanged from Part I: an extra parser and secret holder, cross-client reply leakage as a new
  failure class, and connection-scoped semantics lost.
* **Candidate C.** Multiplexing keeps head-of-line blocking and shared fate inside the application process. The
  experimental cache serves stale data that can stay stale until LRU eviction or its 30-minute TTL.
* **Gate 2 was weaker than it looked.** The pre-registered checks were met by a build that any client could hang
  with a four-command transaction (for example `MULTI`, `SET`, `INFO`, `EXEC`). A new server needs adversarial
  review, fault injection and fuzzing as standing practice, which §II.7 costs.

## II.7 Development and maintenance effort (estimates, not measurements)

What it would take to turn `rvk-server` into a server that could replace Valkey for general use. These are
judgment estimates by area, based on the gaps in FEATURE_MATRIX.md §8 and on the effort the string-cache scope
itself took. One engineer-quarter (EQ) is one engineer for three months.

| Area | Estimate | Basis and main risk |
|---|---|---|
| Data types (hashes, lists, sets, sorted sets, streams, HyperLogLog, bitmaps, geo) with Valkey's compact encodings and memory efficiency | 4–6 EQ | Each type has dozens of commands with exact reply and error semantics; memory parity needs Valkey's listpack and intset style encodings |
| Persistence: RDB load and save compatible with Valkey's format, AOF, without `fork()` | 3–5 EQ | Valkey's snapshots rely on `fork()` and one mutating thread. A multi-threaded server needs a new snapshot design (ARCHITECTURE_COMPARISON.md §1.4) |
| Replication (`PSYNC`, backlog), and Sentinel compatibility | 2–3 EQ | Wire compatibility with Valkey replicas would allow mixed fleets during migration |
| Cluster mode (slots, cluster bus, `MOVED`/`ASK`, resharding) | 3–5 EQ | Required for scale beyond one host |
| Lua scripting and Functions | 1–2 EQ | Embedding Lua with Valkey's sandbox and atomicity |
| ACL users, TLS, Pub/Sub, `CLIENT TRACKING`, keyspace notifications, blocking commands, `SCAN` family | 2–3 EQ | `CLIENT TRACKING` and notifications need cross-thread messaging on the write path |
| Observability and operations (`INFO` parity, `COMMANDLOG`, `LATENCY`, `MONITOR`, `CONFIG REWRITE`, buffer memory accounting) | 1–2 EQ | Operators expect Valkey's tooling to work unchanged |
| Hardening: fuzzing, fault injection, passing Valkey's Tcl suite, security review | 2–3 EQ, then continuing | Round 1 showed that differential and load tests alone missed hang and crash defects |
| **Total to approach feature parity (excluding the Modules API)** | **18–29 EQ** | Valkey's `src/` is about 203k lines of C (ASSESSMENT.md); `rvk-server` covers 61 commands |
| Ongoing: tracking Valkey's command additions, security patching, on-call | 1–2 FTE | Valkey adds commands and behavior every release |
| **Recommended alternative:** multiplexing client (GLIDE), pipelining in batch paths, cluster mode | Days to weeks of application and configuration work | Existing, supported software |

## II.8 Infrastructure cost and break-even (computed from measured throughput and CPU per operation)

At an assumed $0.05 per vCPU-hour (illustrative, not a quote) and assuming linear scale-out (unverified):

* **Unpipelined `read90`, round 2** (the workload of the cost table, BENCHMARK_RESULTS.md §II.10): from measured
  throughput per provisioned vCPU, `rvk-2t` needs 7.6 vCPUs per 1M ops/s against 8.4 for two shards, a saving of
  about $27 per month per sustained 1M ops/s. The most favorable unpipelined Gate 3 point for `rvk-2t`, `write90`
  (R3 1.19×), would save about $50.
* **Pipelined:** in round 2, mixed: two shards used less CPU per operation on 2 of 4 core workloads (1.15 vs
  1.33 µs on `read90`), and `rvk-2t` on the other 2, by 2–6%. In round 1, two shards used less on all 4, by
  13–24%.
* **With a multiplexing client:** server CPU per operation fell from 8.26 to 1.89 µs on `valkey-1t`, a saving of
  77% with no development. `rvk-2t` behind the same client used 1.84 µs, so `rvk`'s unpipelined advantage
  disappears once clients batch.
* **Break-even (illustrative):** at an assumed $250k per engineer-year, ongoing maintenance alone (1–2 FTE,
  about $21k–42k per month) would need roughly 770M–1.5B ops/s of sustained, unpipelined, small-request traffic
  for the $27 per 1M ops/s saving to pay for it (about 420M–840M ops/s at `write90`'s $50), before any
  development cost (18–29 EQ, about $1.1M–1.8M at the same rate). A fleet that size would save far more, sooner,
  by adopting multiplexing clients.

So Candidate A has no break-even point at any realistic scale on this evidence, Candidate B has none at all
(negative savings), and Candidate C's multiplexing pays back immediately.

## II.9 Migration, rollback, next steps and what would change the decision

**Recommended path (reversible, no new components):**

1. **Adopt a multiplexing client** where the application's request pattern allows it: Valkey GLIDE, or the
   multiplexing connection of the language's established client. Canary it on one service, compare server CPU
   per operation and p99 against the current client, and roll back by reverting the client change. Keep
   transactions, blocking commands and Pub/Sub on dedicated connections.
2. **Pipeline batch paths** (bulk loads, fan-out reads). Depth 16 gave 6.3–7.9× single-threaded throughput.
3. **Scale out with cluster mode** and a cluster-aware client. Two shards matched or beat `rvk-2t` on most
   pipelined points.
4. **Do not use** redis-rs 1.7.1's experimental client-side cache for data where staleness matters. If
   client-side caching is wanted, measure GLIDE's implementation for the same race first (not measured here).

**Recommended reporting, not done here because it is an outward-facing action:** the redis-rs `cache-aio`
insert-after-invalidate race (REASSESSMENT.md §6.3) should be reported to the library's maintainers.

**Evidence that would reopen Candidate A, and the experiment it would justify.** All triggers must hold in a real
deployment:

| Trigger (measured in production) | Experiment | Success criteria (all required, pre-registered) | Stop condition |
|---|---|---|---|
| Valkey nodes CPU-bound on hosts with 4 or more cores; many heterogeneous clients that cannot use cluster mode or multiplexing; a command mix inside, or close to, the string-cache scope | Replicate R2 and R3 on 4, 8 and 16 server cores against `io-threads` at its best thread count and cluster mode with a cluster-aware client. Add batch key prefetching to `rvk-server` and restore the compact table. Estimate: 1–2 engineer-months | R2 ≥ 1.20 against the best `io-threads` setting and R3 ≥ 0.90 on 3 of 5 points; p99 ≤ 1.10× at production offered load; memory per key ≤ 1.05× Valkey's; Gate 2 plus fuzzing and fault injection | `io-threads` at its best setting comes within 1.20×; or the projected saving at the organization's fleet size does not exceed the §II.7 estimate |
| A capability Valkey lacks that costs several round trips per request | A Rust module (`valkeymodule-rs`) rather than a new server | ≥ 2× fewer round trips per business operation; p99 ≤ 1.0× the current path | A Lua or Functions implementation meets the same targets |

**Evidence that would change the other decisions:**

* **Candidate B:** only a host where per-request kernel cost is much lower than measured here (for example
  kernel-bypass networking), together with clients that cannot change. Part I §9 gives the sidecar experiment.
* **Candidate C cache:** a client-side cache measured with no stale reads beyond its documented invalidation
  delay, on a read-heavy workload over a real network, where saved round trips matter more than they do on
  loopback.
* **Candidate C multiplexing:** workloads dominated by blocking commands, large values or long-running commands,
  where head-of-line blocking on a shared socket would cost more than batching saves.

## II.10 Answers to the nine questions

**1. Was the original proxy a suitable representation of the intended Rust solution?** No, for the question asked;
yes, for its own candidate. It was a fair test of a multiplexing proxy, and its result stands: on the measured host,
that architecture loses to sharding on CPU per operation by construction. It was not a Rust server, so it could not
show whether a Rust server could beat Valkey. Part I's broader claim relied on an Amdahl bound that applies per
core, not to a multi-threaded single endpoint (REASSESSMENT.md §3, §4.1).

**2. Which limitations were implementation defects, and which were architectural constraints?**

| | Implementation defects (fixable) | Architectural constraints |
|---|---|---|
| Proxy (Part I) | No batching in v1 (fixed in Part I); hiding a backend restart from clients | Per-request kernel cost on both hops; an extra hop in latency, bytes and copies; connection-scoped semantics lost under multiplexing; cross-client reply leakage as a failure class |
| Part I evidence | Open-loop latency inflated by 1 ms pacing ticks and no intended-time latency; unequal allocators in the memory comparison; a near-saturated load generator on the two-shard baseline | – |
| `rvk-server` round 1 | Per-connection reply writes (wakeup cost); a slab layout with an extra cache miss; 52 confirmed correctness and security review findings (fewer distinct defects, since reviewers overlapped), including two deadlocks; false sharing on counters and an `Arc` clone per command | – |
| `rvk-server` round 2 | Missing batch prefetching (a known technique, not built); a vector allocation per command and each key hashed twice; the flattened table's memory cost | `fork()`-based snapshots are unavailable to a multi-threaded mutator; `CLIENT TRACKING` needs cross-thread messaging; lock-held work stalls other threads; a panic aborts the whole process |

Not classified: the 2-thread server's higher p99 at low load, whose cause was not investigated.

**3. Which Rust-based candidate was actually implemented and tested?** Candidate A, `rvk-server`
(`poc/rvk-server`, about 4,100 lines), in two builds, with a differential test, concurrency invariants, 22
regression scenarios, Valkey's Tcl tests and unit and property tests. Candidate B was not rebuilt; Part I's
prototype was re-run for continuity. Candidate C was tested as maintained software (redis-rs 1.7.1) through a
322-line driver.

**4. How does it compare with tuned Valkey, pipelining and sharding?** Per core it matched Valkey in round 2,
after being 4–27% slower in round 1 at the Gate 3 points (R1 0.73–0.96×; at 1 and 10 connections it was 1.14–1.18×
faster). Against `io-threads 2` it was 1.5–3.2× faster on 2 vCPUs in round 2. Against two shards on the same CPUs
it was roughly level: ahead unpipelined, behind or level pipelined, and never 1.20× ahead on a Gate 3 point
(outside them, round 1 measured 1.29× at 10 connections and 1.27× on `SET EX 5`). Pipelining on plain Valkey
(6.3–7.9×) and a multiplexing client (1.70× on the same single server CPU, −77% CPU per operation) were each larger
gains per server CPU than anything the server change delivered.

**5. What performance improvements were measured, and at what resource cost?** In round 2, over Valkey's
single-endpoint option, R2 1.5–3.2×, at lower CPU per operation (unpipelined `read90`: 7.00 vs 20.80 µs). Over two
shards, 1.05–1.19× unpipelined at 0.83–0.91× the CPU per operation, with the load generator limiting most of those
points. Costs: memory per key 197.6 B vs 169.5 B in round 2, a lower hit rate under `maxmemory`, about 100 µs higher
p99 at low load for the 2-thread server, and the missing features in §II.6–§II.7.

**6. What protocol, correctness, security and recovery requirements remain unmet?** Protocol: RESP2 and RESP3 for 61
commands only; no other data types, scripting, Pub/Sub, blocking commands, `SCAN` or `CLIENT TRACKING`;
`INCRBYFLOAT` formatting differs. Correctness: the behaviors declared partial in FEATURE_MATRIX.md §2–§7 (the
review-driven ones are in §11), and the residual exposures in THREAT_MODEL.md §7.3. Security: no ACL users, TLS or
audit logging. Recovery: no persistence or replication, so every restart is empty; clients are disconnected on
failure, as with Valkey.

**7. Are the results sufficiently representative to support the decision?** For a NO-GO on building toward
production, yes: per core, the result is parity, and against sharding, no candidate reached the pre-registered
bar. For anything beyond that, no. One 4-vCPU guest over loopback, 2 server CPUs, synthetic workloads, a load
generator that limited several points, baselines that drifted by up to 28% between rounds, and a post-hoc second
round. The largest measured advantage (R2) is the least representative, because `io-threads` is outside its
regime on 2 vCPUs.

**8. What does the evidence justify building next, if anything?** Nothing new. Adopt a maintained multiplexing
client, pipeline batch paths and use cluster mode (§II.9). If, and only if, a deployment shows the triggers in
§II.9, run the bounded 4+-core replication experiment described there before any product work.

**9. What exact production evidence would change the recommendation?** For Candidate A: Valkey nodes measured
CPU-bound on 4 or more cores; clients that cannot use cluster mode or multiplexing; a command mix within or near
the string-cache scope; and a pre-registered replication on that hardware showing R2 ≥ 1.20 against the best
`io-threads` setting, R3 ≥ 0.90, p99 ≤ 1.10× at production load and memory per key ≤ 1.05×, with a fleet large
enough that the projected saving exceeds the §II.7 cost. For B and C, see §II.9.

## II.11 Remaining uncertainties

* Whether `rvk-server`'s lead over `io-threads` survives on 4 or more server cores (not measured; the host has
  4 vCPUs and the load generator needs 2).
* How a cluster-aware client (GLIDE) against real Valkey cluster mode compares with client-routed shards (not
  measured; the shards stand in for it).
* GLIDE's own multiplexing and client-side cache (not measured; redis-rs stands in).
* The cause of `rvk-2t`'s higher p99 at low load (not investigated).
* How much batch prefetching would add to `rvk-server` at depth 16 (hypothesis: most of its remaining pipelined
  per-core gap to Valkey, given that lookups and key comparisons are 54% of its CPU there).
* Behavior on real networks, over hours, on a second independent host, and under true overload (the overload
  suite did not overload anything).


# Part I decision record (superseded by Part II above; kept as first committed)

| Field | Value |
|---|---|
| Status | **Decided: NO-GO** (implementation not authorized) |
| Date | 2026-10-19 |
| Scope | Valkey source tree at `741b0159`; benchmarks on one 4-vCPU KVM host (BENCHMARK_PLAN.md §2) |
| Inputs | [ASSESSMENT.md](ASSESSMENT.md), [BENCHMARK_PLAN.md](BENCHMARK_PLAN.md), [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md), [THREAT_MODEL.md](THREAT_MODEL.md) |

## 1. Decision

**NO-GO.** Do not build a Rust replacement for Valkey, a Rust proxy or sidecar, or Rust rewrites of Valkey
components. Keep Valkey and use its native scaling and integration options, which were measured to be better on
the same resources. No implementation plan or production code is authorized.

Answers to the six required questions:

| Question | Answer |
|---|---|
| What problem does Rust solve that Valkey tuning cannot? | **None was found.** The measured bottleneck is per-request kernel work: 64–71% of Valkey's CPU on small requests. Batching removes it; changing language does not. Valkey already offers batching (pipelining, multiplexing clients) and scale-out (sharding and cluster) |
| Which exact component should be implemented in Rust, and why? | **None.** The best candidate, a multiplexing auto-pipelining proxy (Option A), was prototyped, tuned and measured. It lost to plain Valkey sharding on the same 2 vCPUs. The same mechanism already exists in Rust without the extra hop, inside the Valkey GLIDE client's core |
| What measurable improvement was demonstrated? | **None against the tuned baseline.** The proxy improved on single-threaded Valkey alone (1.04× throughput on `read90`), but only by using a second CPU. On the same 2 CPUs, two Valkey shards gave 1.49× the proxy's throughput at 0.74× its CPU per operation. Client-side pipelining gave 5.7× on one CPU |
| Trade-offs | Performance: higher CPU per operation and an extra hop (§5). Correctness: connection-scoped semantics lost (transactions, `SELECT`, blocking commands, Pub/Sub, `CLIENT TRACKING`, per-user ACLs), and a new class of cross-client reply-leak bugs. Security: one more parser and one more secret holder, while Valkey's attack surface is unchanged. Operations: one more process to deploy, monitor and page on (§6) |
| Is the improvement sufficient to justify total cost of ownership? | **No.** There is no improvement to set against the cost. The prototype's extra CPU per operation alone makes the infrastructure cost higher, before any development or maintenance cost (§7, §8) |
| Proceed, limited experiment, or reject? | **Reject** Options A, B, C and D as proposed. §9 lists the specific evidence that would justify reopening a **limited experiment**, with predefined success and stop criteria |

## 2. Context

* The repository is the Valkey server source (`unstable`, `741b0159`), not a deployment. It has no workload, no
  production configuration, no metrics, and no recorded business or engineering requirement (ASSESSMENT.md §2).
  The GO criterion's requirement that the benefit "address an established business or engineering requirement"
  cannot be met from this repository alone.
* All measurements come from one 4-vCPU KVM host over loopback, with synthetic workloads
  (BENCHMARK_PLAN.md §2, §4, §10). Absolute numbers will differ elsewhere. The comparisons hold the CPU budget,
  workload and host constant.

## 3. Options considered

| Option | What was evaluated | Evidence | Outcome |
|---|---|---|---|
| Tuned Valkey: client pipelining | `read90-p16` on `valkey-1t` | Measured: 5.7× single-threaded throughput on the same CPU | **Recommended first** when the application can batch |
| Tuned Valkey: sharding (cluster) | `valkey-2shard`, one process per CPU | Measured: 1.55–1.93× across the core workloads, with low CPU per op | **Recommended** for scaling out |
| Tuned Valkey: `io-threads` | `valkey-io2` on 2 vCPUs | Measured: 0.82–0.85× and 28.5–32.6 µs/op, worse on this budget. Consistent with Valkey's guidance to enable it on 3–4+ cores | Re-evaluate on hosts with 4 or more cores; not on 2 vCPUs |
| A. Rust multiplexing proxy | `proxy-poc`, tuned (BENCHMARK_PLAN.md §3) | Measured: loses to sharding on throughput and CPU per op; adds latency at low load; breaks connection-scoped semantics | **Reject** |
| A. Rust client-side component | Valkey GLIDE already has a Rust core that multiplexes with automatic pipelining, one connection per node | Existing, maintained software | **Adopt GLIDE if a client change is needed; build nothing** |
| A. Request coalescing (single-flight) | Duplicate misses cost Valkey the same as hits; the benefit is protecting a backing database | Application-level pattern available in every language | Not a Rust question |
| B. Rust sidecar cache | Analysis; best case equals a local Valkey hop | Measured local hop 63 µs p50; remote RTT benefit not measurable here | **Reject**; prefer an in-process near cache with `CLIENT TRACKING`, or a local replica |
| C. Standalone Rust server | Amdahl bound, feature inventory | Ceiling 1.45× per core even with zero user-space cost; about 203k lines of C to match | **Reject**; configuration 4 not run |
| D. Rust inside Valkey | Profile shares; Modules API | No single user-space component is large enough; Rust modules are possible (`valkey-bloom`) | **Reject rewrites**; a Rust module stays an option for a specific missing capability |

## 4. Benchmark summary

Full tables, raw data and variance are in [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md). Every configuration had
CPUs 0–1 for the data service and CPUs 2–3 for the load generator. Each point is the median of 3 independent
trials.

| Scenario | valkey-1t | valkey-io2 | valkey-2shard | rust-proxy | proxy ÷ best native |
|---|---|---|---|---|---|
| `read90`, 50 conns: ops/s (server CPU µs/op) | 83,603 (11.8) | 68,758 (28.5) | **129,832 (14.8)** | 86,920 (20.1) | 0.67× ops, 1.36× CPU |
| `mixed50`, 50 conns | 73,308 (13.4) | 60,457 (32.6) | **136,586 (14.0)** | 81,654 (21.2) | 0.60×, 1.51× |
| `write90`, 50 conns | 74,540 (13.2) | 62,769 (31.3) | **133,962 (14.2)** | 84,500 (20.2) | 0.63×, 1.43× |
| `skew-read90` (Zipf 0.99), 50 conns | 80,529 (12.2) | 68,750 (28.7) | **155,086 (12.5)**¹ | 96,075 (18.5) | 0.62×, 1.49× |
| `read90`, 1000 conns | 58,635 (16.8) | 53,440 (36.9) | **124,052 (15.7)** | 94,636 (20.6) | 0.76×, 1.31× |
| 16 KiB values, `read90` | 45,074 (21.8) | 44,540 (44.2) | **89,273 (21.9)** | 30,935 (52.5) | 0.35×, 2.40× |
| client pipeline 16, `read90` | 479,265 (2.0) | – | **996,709 (2.0)** | 479,646 (3.3) | 0.48×, 1.66× |
| p50 / p99 µs at 40k ops/s offered | 322 / 3,391 | 246 / 3,539 | **201 / 2,773** | 441 / 4,088 | 2.19× / 1.47× |
| p50 µs at 1 connection | **63** | 66 | n/a | 150 | 2.38× vs `valkey-1t` |

¹ Load generator near saturation (176% CPU), so this is a lower bound for the shards.

## 5. Performance and resource efficiency

### Advantages Rust could have, and where they actually come from

| Claimed advantage | Comes from Rust or from the architecture? | Observed here |
|---|---|---|
| Lower overhead per request | Architecture (batching, fewer system calls) | Batching cut **Valkey's** CPU per op behind the proxy to 3.2–4.1 µs. The proxy re-incurred the per-request system calls at 15.3–17.0 µs per op, 70–72% of it in the kernel |
| Memory safety | Rust | Real for new code. It does not reduce Valkey's attack surface behind a proxy, and does not prevent logic or availability bugs (THREAT_MODEL.md §2) |
| Predictable resource management | Mostly the design (bounded queues, backpressure) | The prototype needed explicit limits; Valkey already has them (`client-output-buffer-limit`, `maxmemory-clients`) |
| Efficient concurrency | Architecture | Two plain Valkey shards used 2 CPUs better than proxy plus Valkey |
| Fewer allocations and copies | Implementation | The proxy adds copies (client buffer to batch to Valkey and back) |
| Isolation through process boundaries | Architecture | Adds a failure domain and a hop. The direct path was simpler |

### Disadvantages and risks found

* **More total CPU for the same work:** 1.20× the total CPU per operation of `valkey-2shard`
  on `read90`, client included.
* **More network traffic:** 289 loopback bytes per operation, against 144.5 direct. Every
  byte crosses two hops.
* **Higher latency at low load:** at 1 connection, p50 150 µs against 63 µs.
* **Lost semantics and new bug classes:** see §6.

### Where Valkey is already more efficient than a custom Rust component

Valkey's single-threaded event loop handles a non-pipelined request in 11.8 µs of CPU, of which only
3.7 µs is user space. Its `hashtable` stores a 100-byte value with 53.5 bytes of overhead,
including expiry and eviction metadata. When batched (pipelining), its CPU per operation falls to 2.0 µs.
None of this leaves meaningful room for a reimplementation.

## 6. Correctness, reliability and security (summary of THREAT_MODEL.md)

* **Semantics:** the proxy must refuse or specially handle transactions, `SELECT`, blocking commands, Pub/Sub,
  `CLIENT *`, `HELLO`/RESP3, `WAIT` and per-user ACLs. Valkey's own test suite run through the proxy:
  141 of 799 tests passed (17.6%). 6 of 8 files stopped at the first refused command, 2 tests hung, and 0 tests returned a wrong answer.
* **Failure behavior (measured):** killing Valkey under load disconnected every direct client. Through the proxy, in 2 of 3 trials, 16 and 5 of the 50 clients never saw the failure and silently continued against the empty restarted instance, with hit rates falling to 0.675 and 0.873. For counters, locks or sessions that is silent data loss (THREAT_MODEL.md §6.2).
* **New risk:** a framing bug in a multiplexer leaks one client's replies to another; without the proxy, the same
  bug corrupts only its own connection.
* **Supply chain:** 13 third-party crates in the prototype, pinned with checksums. A product would need
  RustSec auditing and a patch process.

## 7. Development and maintenance effort (estimates, not measurements)

| Item | Rough estimate | Basis |
|---|---|---|
| Production-grade multiplexing proxy: TLS, AUTH/ACL handling, cluster topology and `MOVED`/`ASK`, RESP3, a command allowlist generated from `commands.def`, transaction checkout, blocking commands, Pub/Sub, metrics, config reload, fuzzing, conformance suite | 2–4 engineer-quarters to first production use | Judgment, from the features the prototype had to refuse (THREAT_MODEL.md §3) |
| Ongoing maintenance: Valkey command additions, dependency and security patching, on-call | 0.25–0.5 FTE | Judgment; Valkey adds commands every release (this tree has 9.2.0 commands) |
| Recommended alternative: client pipelining or batching, or cluster mode, or GLIDE | Days to weeks of application and configuration work | Uses supported, existing features |

## 8. Infrastructure cost (computed from measured CPU per operation)

Using the measured data-service CPU per operation on `read90` and an assumed price of $0.05 per vCPU-hour (an
illustrative parameter, not a quote):

| topology | consumed CPU-s per Mop | throughput per provisioned vCPU | vCPUs for 1M ops/s (provisioned) | monthly cost at $0.05/vCPU-h, 730 h | vs `valkey-2shard` |
|---|---|---|---|---|---|
| valkey-1t | 11.8 | 83,603 ops/s | 12.0 | $437 | −$126 (0.78×) |
| valkey-io2 | 28.5 | 34,379 ops/s | 29.1 | $1,062 | +$499 (1.89×) |
| valkey-2shard | 14.8 | 64,916 ops/s | 15.4 | $562 | +$0 (1.00×) |
| rust-proxy | 20.1 | 43,460 ops/s | 23.0 | $840 | +$278 (1.49×) |

At 1M ops/s sustained, assuming linear scale-out of the measured unit (unverified), the proxy architecture needs about 7.6 more vCPUs than two-shard Valkey, about $278 more per month at the assumed price. Per vCPU, single-threaded Valkey on its own core was the most efficient configuration measured. That argues for scaling out with one Valkey process per core (cluster mode), not for adding components.

The proxy needs more vCPUs than the Valkey-native option for the same throughput, so infrastructure savings are
negative and **no break-even point exists**. Development and maintenance (§7) only add cost. Failure cost: a
reply-misattribution bug is a data-confidentiality incident whose cost depends on the data stored, which is
unknown here.

## 9. Migration, rollback and conditions for revisiting

**Recommended path (reversible, no new components):**

1. If throughput per node is the problem: batch or pipeline in the application, then shard (cluster mode).
   Re-evaluate `io-threads` only on hosts with 4 or more cores.
2. If connection count or churn is the problem: use persistent connections with a pooled or multiplexing client
   (for example GLIDE). The `churn` suite measures the effect of per-request connections (BENCHMARK_RESULTS.md).
3. If remote read latency is the problem: an in-process near cache with `CLIENT TRACKING`, or a local read
   replica.

Each of these is a configuration or client change. Rollback means reverting that change, and the existing Valkey
deployment stays in service throughout.

**Evidence that would justify a CONDITIONAL GO (a limited proof of concept):**

| Trigger (must be measured in the real deployment) | Experiment | Success criteria (all required) | Stop condition |
|---|---|---|---|
| Valkey nodes saturated on per-request kernel time (sys share above 60%), with clients that cannot batch, pipeline or adopt a multiplexing client | Sidecar multiplexer on one application host, shadowing 5% of traffic, 4 weeks | Against the best Valkey-native alternative on the same vCPUs: ≥ 1.20× throughput **or** ≤ 0.80× total CPU per op; p99 ≤ 1.10× at production load; zero reply-attribution errors under fuzzing and differential testing; Valkey's test suite passing for every command family the application uses | Any reply misattribution; or criteria unmet after 4 weeks; or the GLIDE/pipelining alternative becomes possible |
| A required data type or server-side operation that Valkey lacks, currently costing several round trips per request | A Rust module (via `valkeymodule-rs`) in staging | ≥ 2× fewer round trips per business operation; p99 ≤ 1.0× the current path; module passes fuzzing and `RESTORE`/RDB robustness tests | A Lua or Function implementation meets the same targets |

## 10. Acceptance criteria applied

The thresholds were set in BENCHMARK_PLAN.md §8 before the formal runs:

| # | Criterion | Measured (best case for the proxy) | Met? |
|---|---|---|---|
| 1 | Throughput ≥ 1.20× best native, or CPU/op ≤ 0.80× at equal or better throughput | ≤ 0.76× throughput; CPU/op 1.30–2.40× the shards' wherever both ran. Under client-limited churn: 0.86× `valkey-io2` but 1.68× `valkey-1t`, at equal throughput | **No** |
| 2 | p99 ≤ 1.10× best native at the same offered load | 1.47–2.04× | **No** |
| 3 | Zero errors and timeouts; recovery regression ≤ 1 s | Zero; +4.6 ms | Yes, on timing (a restart can be hidden from clients) |
| 4 | No loss of semantics for commands the workload uses | GET/SET only; 17.6% of Valkey's tests pass | Only for the synthetic workload |

Result: **NO-GO**, because criteria 1 and 2 fail on every core workload. The other gates in the prompt are
also unmet: there is no established requirement, and there is no improvement over the tuned baseline.

