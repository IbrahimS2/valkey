# Reassessment: audit of Part I and the basis for Part II

Part I of this assessment (ASSESSMENT.md, BENCHMARK_RESULTS.md §1–13, ARCHITECTURE_DECISION.md as first
committed) reached **NO-GO** after building and measuring one Rust candidate, a multiplexing proxy. The
reassessment directive asked whether that proxy was a suitable test of "a properly architected Rust-based
alternative", which of its problems were fixable defects and which were inherent, and then to build and measure
the right candidates. This document is the audit. The new measurements are in BENCHMARK_RESULTS.md Part II and
the new decision is in ARCHITECTURE_DECISION.md.

No Part I raw data was changed. Part I results stay in `results/raw/`. Part II results are in `results/raw2/`,
from a harness extended under the same rules (`bench/harness.py`, suites prefixed `p2-`). Where this audit says
a Part I number was misleading, the Part I tables are left as they were and this document says why.

## 1. Summary

1. **The Part I proxy was a valid test of one hypothesis, not of the question asked.** It tested whether moving
   batching into a separate process helps. It does not, for an architectural reason (§3). It could not answer
   whether a Rust *server* could beat Valkey, and Part I did not build one. Part I rejected that option
   ("configuration 4") on an Amdahl argument instead.
2. **That Amdahl argument was stated too broadly.** It bounds a single-threaded reimplementation per core. It
   says nothing about a multi-threaded server that serves one endpoint from several cores. That architecture
   was never measured, and Valkey's own single-endpoint multi-core option (`io-threads`) measured poorly on
   2 vCPUs. That left an untested gap between "faster code" and "more processes" (§4.1).
3. **Part I's open-loop latency figures were inflated by the load generator.** The generator paced sends on
   tokio's 1 ms timer, so all connections sent in bursts on each tick. It also never reported the latency
   measured from the intended send time. Relative comparisons are fair, because every topology saw the same
   generator. Absolute p50 and p99 values are not. At 60k ops/s on today's host, precise pacing gives
   `valkey-1t` p50 32 µs instead of 236 µs (§4.2).
4. **Several smaller methodological issues** did not change the Part I decision but are corrected in Part II
   (§4.3–4.6). The memory comparison used unequal allocators. The two-shard baseline ran its load generator at
   146–177% CPU. The kernel share came from a host with heavier CPU-vulnerability mitigations than the
   Part II host.
5. **Part II measures three candidates separately** (ARCHITECTURE_COMPARISON.md):
   * A: a Rust-native, multi-threaded, Valkey-compatible server, `rvk-server`, built for this purpose.
   * B: the proxy, bounded by Part I's measurements plus one continuity run today.
   * C: established Rust client functionality (redis-rs multiplexing and client-side caching), measured rather
     than rebuilt.

   Decision thresholds were committed (`da0b089`) before any measurement of these candidates.
6. **The first `rvk-server` build passed every pre-registered correctness check and still had defects that
   could hang or crash it.** A multi-agent code review found 52 confirmed defects after the round 1
   measurements, two of them self-deadlocks (§7). All are fixed or documented, and a regression suite now
   reproduces them against Valkey. The round 1 performance numbers stand, because no benchmark exercised those
   paths. But "passed Gate 2" turned out to be weaker evidence than it sounds.
7. **The profiles explained the first build's per-core deficit, and fixing it was an implementation matter.**
   rvk-1t used more kernel time per request than Valkey, because it woke the client once per reply instead of
   once per event-loop pass. The second build writes replies as Valkey does in `beforeSleep`. That change, and
   a flatter hash table, were measured as a disclosed post hoc round 2 with every baseline re-run
   (BENCHMARK_RESULTS.md Part II).

## 2. How the audit was done, and process disclosures

* **What was checked.** Every Part I table was regenerated from `results/raw/` with `bench/summarize.py`. K1–K3
  matched the published tables exactly, so the numbers trace back to raw data. Each Part I conclusion was checked
  for whether its evidence covered its scope (§4). The proxy's measured costs were decomposed into the parts
  that a better implementation could remove and the parts it could not (§3).
* **Who did it.** The directive's multi-agent review workflow could not run at first: sub-agent launches failed
  on an organization spend limit. The audit, the `rvk-server` implementation and its test harnesses were
  therefore produced by a single agent, without the independent cross-checking a workflow would have provided.
  After the limit reset, a read-only multi-agent review of `rvk-server` ran in five dimensions (concurrency,
  protocol and I/O, command semantics, security, benchmark fairness). Each dimension had an adversarial verifier.
  Its confirmed findings and their handling are in §6.
* **The host changed between Part I and Part II.** The container was rescheduled. Part I ran on an
  Intel Xeon at 2.8 GHz (model 85, 33 MB L3) with Retbleed, TAA and MMIO-stale-data mitigations active. Part II
  runs on an Intel Xeon at 2.1 GHz (model 207, 260 MB L3) that reports itself not affected by those. Both are
  KVM guests with 4 vCPUs, the same kernel and the same binaries (`results/environment.json`,
  `results/environment_part2.json`). On Part II's host every baseline is 1.3–1.8× faster (for example
  `valkey-1t` GET-only went from 78.5k to about 125k ops/s). **Part I and Part II numbers must not be compared
  with each other.** Every Part II comparison uses baselines re-run in the same Part II matrix, on the same
  host, interleaved by trial.

## 3. Was the original proxy suitable? Defects versus architectural constraints

### 3.1 What the proxy was

`proxy-poc` was a benchmark-only tokio proxy. It accepted client connections, multiplexed their requests onto
one upstream connection to a single-threaded Valkey (auto-pipelining), and demultiplexed replies. It shared
CPUs 0–1 with Valkey (2 tokio workers; Valkey pinned to CPU 0), the best shape found in calibration.

### 3.2 Classification of every problem Part I recorded

| Problem | Class | Why |
|---|---|---|
| v1 never batched (tokio LIFO slot ran the writer after every request) | **Defect, fixed in Part I** | One `yield_now` fixed it: 1.0 → 13–56 commands per upstream write |
| Proxy CPU per op 15.3–17.0 µs, 70–72% kernel | **Architectural** | The proxy terminates every client socket, so it makes the same per-request `read`/`write` calls Valkey made. Batching helps only the upstream side. A perfect proxy with zero user-space cost still pays about 11–12 µs/op in the kernel. Add Valkey's 3.2–4.1 µs behind it and the floor is about 14–16 µs/op, against 12.5–14.8 µs/op for two shards on the same host. **No implementation of this architecture can beat sharding on CPU per operation there** |
| Extra hop: +87 µs p50 at 1 connection, 2× loopback bytes, 2× copies of large values | **Architectural** | Inherent to any proxy. 16 KiB values made it worse than one Valkey |
| Connection-scoped semantics lost (`MULTI`/`WATCH`, `SELECT`, blocking, Pub/Sub, `CLIENT TRACKING`, per-user ACLs), so 17.6% of Valkey's tests pass | **Architectural for a multiplexer; scope for the prototype** | A multiplexer can support these only by pinning a client to a dedicated upstream connection for the session, which removes the batching that was its only benefit for those clients |
| Clients not told when the backend restarted (16 and 5 of 50 clients kept running against an empty Valkey) | **Defect** | Fixable: close every client connection when the upstream connection fails. A production proxy must do this |
| New failure class: one framing bug leaks replies across clients | **Architectural risk** | Inherent to multiplexing, controllable by testing; zero misattributions were measured |
| Proxy and Valkey shared CPU 0 | **Configuration choice** | Chosen in calibration as the proxy's best shape. Giving the proxy a dedicated CPU would leave Valkey alone on one CPU, which bounds the proxy at Valkey's single-thread capacity |

**Answer to "was the proxy suitable?"** It was a fair test of the proxy candidate, and its result stands: the
architecture loses to sharding by construction on CPU per operation, independently of implementation quality.
It was not a test of a Rust server, so it did not support Part I's broader claim that no Rust implementation
could help.

### 3.3 Why Candidate B is not re-implemented in Part II

The floor in §3.2 comes from measured kernel costs, not from the prototype's code. A rebuilt proxy could at best
remove the proxy's ~4.7 µs/op of user-space work and the restart defect. Fixing the restart defect matters for
safety, but it changes no performance number. Part II reruns the existing proxy on `read90` (`p2-proxy`) only so
that it is placed on today's host next to the other candidates.

## 4. Issues in Part I's evidence and claims

### 4.1 The Amdahl bound was applied beyond its scope

ASSESSMENT.md §4.2 computed `speedup ≤ 1 / kernel share ≈ 1.45×` per core and used it to reject a standalone
Rust server. The computation is correct for its premise: one thread, one `read` and one `write` per request. Two
things fall outside that premise:

* **Multi-core architecture.** A server whose worker threads each run an event loop over a shared, lock-sharded
  keyspace serves one endpoint from N cores, with no client routing. Valkey's equivalent single-endpoint option,
  `io-threads`, offloads I/O but keeps command execution on the main thread. On 2 vCPUs Part I measured it at
  0.82–0.85× single-threaded throughput. Part I treated "sharding requires cluster-aware clients" and
  "`io-threads` loses on 2 vCPUs" as Valkey's whole multi-core story, and never measured a multi-threaded
  server.
* **System calls per request are not fixed.** Under load, an event loop that reads until a short read and writes
  once per batch makes fewer calls per request. So does one that handles many ready sockets per `epoll_wait`.
  The kernel share is a property of the implementation and the load, not a constant.

The statement "language is not the lever" survives (§5). The statement "architecture is the lever, and Valkey
already provides it" did not cover every architecture. Part II builds the missing one: Candidate A, `rvk-server`.

### 4.2 Open-loop latency: generator pacing and coordinated omission

* `loadgen` paced open-loop sends with `tokio::time::sleep_until`. tokio's timer resolves to 1 ms, so all 50
  connections woke on the same tick and sent together. At 60k ops/s that is bursts of about 60 requests per
  millisecond, which queue in the server.
* The schedule lag (actual minus intended send time) was recorded in every Part I run but not reported. It was
  p50 0.78–1.09 ms, p99 2.1–4.5 ms, and up to 78 ms max. Latency was measured from the actual send time, so
  that lag was excluded. This is partial coordinated omission.
* **Effect.** On today's host at 60k ops/s, timer pacing gave `valkey-1t` p50 236 µs and p99 4,190 µs. Precise
  pacing (`--spin-pacing`: coarse sleep, then yield until the exact time) gave p50 32 µs and p99 372 µs
  (`results/raw2/dev/pacing-smoke.jsonl`, single 5 s runs). Part I's open-loop **absolute** values are therefore
  dominated by the generator. Its **relative** findings (proxy p50 and p99 above every native topology) compared
  topologies under the same arrivals and still stand as relative statements. They are not evidence about
  latency under smooth load.
* **Part II** reports both the actual-send and intended-send latency. It uses `--spin-pacing` for every
  open-loop point, plus timer pacing at one rate for continuity. Spin pacing still has a p99 send lag of about
  0.75 ms on this host (the client spins at about 195% CPU), so intended-time p99 includes up to that much
  generator lag. This is reported with the results.

### 4.3 The memory comparison used unequal allocators

ASSESSMENT.md §4.3 compared Valkey (jemalloc) with a naive Rust map using glibc malloc, and concluded that "a Rust
rewrite does not get compact memory for free". The direction is plausible, but the comparison mixed language
with allocator. Part II measures `rvk-server`, which uses jemalloc, against Valkey under the same procedure
(`recovery.py p2-memory`).

### 4.4 The two-shard baseline was close to client saturation

In Part I's core and connection suites, the load generator used 146–177% CPU when driving `valkey-2shard`, but
only the 176.6% point crossed the 170% flag. The shards were probably understated by a few percent on most
points. This favored the proxy, so it does not weaken Part I's NO-GO. Part II has the same exposure, because
faster servers make the client the bottleneck sooner. The pre-registered rule therefore decides client-bound
comparisons on pipelined (`-p16`) runs.

### 4.5 The kernel share is host-dependent

Part I's 64–71% kernel share was measured on a host with active Retbleed, TAA and MMIO mitigations. Those make
system calls more expensive. Part II re-profiles on its own host (`bench/profile_part2.sh`). Any Amdahl-style
reasoning in Part II uses Part II's profile.

### 4.6 Smaller points

* The cost model's linear scale-out assumption was flagged in Part I and still holds as a caveat.
* Part I described `io-threads` correctly as "not evidence against `io-threads` on larger hosts", but then used
  its 2-vCPU result as Valkey's single-endpoint multi-core baseline. Part II keeps it as a baseline. Any
  advantage over it is labelled as specific to 2 vCPUs, and the pre-registered CONDITIONAL GO requires a repeat
  on ≥ 4 server cores.
* Part I's correctness evidence for the proxy (Tcl suite through the proxy) was sound and is unchanged.

## 5. What Part I got right and Part II retains

* The bottleneck analysis method: per-process CPU accounting, user/sys split, `perf` profiles, and commandstats
  cross-checks.
* The rule that all configurations get the same CPUs (0–1 for the data service, 2–3 for the generator).
* Client-side batching as the first lever. Part II measures it again, as Candidate C's multiplexing.
* "Language is not the lever" as a per-core statement. Part II tests it directly: `rvk-1t` against `valkey-1t`,
  same architecture and same CPU, is ratio R1.

## 6. Defects found while building and testing Part II

Defects in new code are listed so the evidence for them is not lost.

### 6.1 `rvk-server` defects found by its own test harnesses (all fixed before the formal round 1 matrix)

| Defect | Found by | Fix |
|---|---|---|
| `WATCH` compare-and-set lost updates: 1,596–1,598 of 1,600 increments survived | `concurrency.py watch_cas` | The watch flag was checked before taking the shard locks. `EXEC` now locks the shards of every queued **and** watched key, then checks |
| Edge-triggered `epoll` stall when output was paused for backpressure and input was already buffered | Development testing with pipelined large replies | Process-and-flush loop that resumes buffered input after a flush |
| Spurious `OOM` errors under `allkeys-lru` with concurrent writers | `concurrency.py memory_bound` | A command proceeds while eviction makes progress, as in Valkey's `EVICT_RUNNING` |
| Spurious `OOM` (second cause): a check-then-act race between two threads' evictions and writes, at 84 bytes over the limit | `valkey-benchmark` SET stress under `maxmemory` (10 errors in 5M) | `make_room` decides from what it observed (memory fitted at some point, or it evicted). `memory_bound` now also runs 100-byte values. 0 errors in 3 × 5M SETs after the fix |
| `-0` accepted as an integer; 1M multibulk cap instead of `INT_MAX`; missing unauthenticated limits | Differential test | Ported `string2ll` rules and Valkey's limits |
| Inline tokenizer did not concatenate quoted segments as `sdsparsearg` does; CRLF checks too lax | Valkey's `unit/protocol` | Rewritten to `sdsparsearg` semantics; strict CRLF |
| `INCRBYFLOAT` formatting | Differential test | Improved to shortest round-trip; still differs from `long double` (declared partial) |

The multi-agent review's findings are in §7. None of the harnesses above found them.

### 6.2 Measurement-tool defects found and fixed

| Defect | Effect | Fix |
|---|---|---|
| `rsclient` stale-read metric counted concurrent writers' reordering as staleness | 5 false "stale reads" without any cache | Single writer per key, so the newest acknowledged write is what the server holds. 0 false positives without a cache afterwards |
| `loadgen` open-loop pacing on 1 ms ticks | §4.2 | `--spin-pacing` |
| Intended-time latency not reported | §4.2 | `lat_from_intended_us` in every open-loop record |

### 6.3 A defect found in established software (Candidate C)

redis-rs 1.7.1's experimental client-side cache (`cache-aio`) inserts a GET reply into the cache in the
caller's future, after the reply has arrived (`caching/cmd.rs`, `resolve` → `sharded_lru.rs`, `insert`). Its only
guard is a per-connection epoch. If the server's invalidation push for that key is processed between the reply's
arrival and the insert, the invalidation finds nothing to remove. The stale value is then inserted and served
until LRU eviction or the 30-minute default client TTL. The server has meanwhile stopped tracking the key for
this client, so no further invalidation arrives. Measured consequences are in BENCHMARK_RESULTS.md Part II
(Candidate C). This is a correctness defect in an experimental feature of a maintained library, and it is the
reason Part II does not recommend that cache for data where staleness matters.

## 7. Multi-agent review of `rvk-server`

**How it ran.** One workflow with five read-only reviewers: concurrency, protocol and I/O, command semantics
against Valkey's C source, security and denial of service, and benchmark fairness. Each reviewer's findings went
to an adversarial verifier told to refute them by tracing the code. The reviewers could not build or run
anything, because the round 1 matrix was running on the same machine. The first launch lost four agents to the
organization spend limit; the workflow was resumed after the reset, with the security reviewer's prompt
sharpened because its first pass returned nothing. Full findings, verdicts and verifier reasoning are in
[`results/poc/review_findings.json`](results/poc/review_findings.json).

**What it found.** 57 findings: 52 confirmed, 4 refuted (all from the fairness reviewer), 1 uncertain (the timing
side channel of a non-constant-time password comparison, which the verifier judged too small to measure
remotely; it is fixed anyway). The most serious:

| Finding | Severity | Why the harnesses missed it |
|---|---|---|
| `EXEC` self-deadlocks when `INFO` or `UNWATCH` is queued: both lock shards the transaction already holds, and `std::sync::Mutex` is not reentrant. The stuck worker then blocks every thread that touches those shards | critical | The differential test never queued INFO or UNWATCH inside MULTI |
| `MULTI`/`EXEC` bypasses `maxmemory` entirely: queued writes are never checked, so memory grows without bound | high | The invariants and benchmarks never wrote through transactions under `maxmemory` |
| `DEBUG SLEEP inf` panics, and `panic = "abort"` kills the process. DEBUG was open to every client | high | No test sent a non-finite float |
| Never-authenticated clients had no input or output caps, so a client without the password could make the server buffer gigabytes | high | No test pipelined without reading while unauthenticated |
| An incomplete multibulk request was re-parsed from its start on every read: quadratic CPU for large argument counts | medium | Requests in tests were small |
| A FIN arriving with the last data was never seen, so half-closing clients hung and dead connections leaked | medium | Test clients never half-close |

All confirmed findings are fixed or documented as partial (FEATURE_MATRIX.md §11). The 22 scenarios in
[`poc/tests/regressions.py`](poc/tests/regressions.py) compare Valkey and rvk-server on the findings that a client
can observe. All 22 pass on the round 2 build. On the round 1 binary, 17 fail and 4 cannot start (that binary
lacks `--enable-debug-command`), which shows the scenarios detect the defects.

**What it means for the evidence.** The review was the only check that found any of these. Gate 2 as
pre-registered (differential test, concurrency invariants, error-free benchmarks) was met by a build that could
be hung by any client in two commands. Two lessons for the decision:

* The correctness evidence for a new server has to include adversarial review and fault-oriented tests, not only
  differential and load tests. That is a recurring cost of Candidate A that Valkey users do not pay
  (ARCHITECTURE_DECISION.md §II.7).
* The round 1 performance numbers are valid for what they measured. No benchmark sends MULTI with INFO or UNWATCH,
  writes through transactions, half-closes connections, or runs unauthenticated, so the defective paths were never
  on the measured path.

**Fairness findings.** Two were confirmed and affect how the comparison is read, though no result changed
direction. Valkey spends about 5% of pipelined CPU on per-command timing and statistics (`getMonotonicUs` 5.0% in
the depth-16 profile), which rvk does not do, and that favors rvk. rvk's round 1 build paid costs of its own: false
sharing on shared counters, an `Arc` clone and a vector allocation per command, and each key hashed twice. Those
disfavored it. The verifier refuted the claim that foldhash invited hash flooding, as speculative. The round 2
build uses keyed SipHash anyway, as Valkey does, so that difference no longer exists.

## 8. What the Part II profiles say about Part I's conclusions

* **The kernel share holds on the new host.** `valkey-1t` spends 72% of its unpipelined CPU in the kernel on the
  Part II host (`results/profile2/valkey-1t-p1.dso.txt`), against 70.6% in Part I. The per-core Amdahl bound
  is about 1.4× on both hosts. The host change did not undermine it.
* **A Rust server is subject to the same bound, and the first build was below Valkey.** rvk-1t spent 83% in the
  kernel. The excess was in the wakeup path (`_raw_spin_unlock_irqrestore`, 33% against Valkey's 19%), not in
  the language. Writing replies once per event-loop pass, as Valkey does, removed most of it.
* **With pipelining, user space matters and Valkey's prior optimization shows.** At depth 16 the first build
  spent about 40% of its CPU in hash lookups and key comparisons, against about 6% for Valkey, which prefetches
  keys for a whole batch (`memory_prefetch.c`). Flattening rvk's table helped a little. Batch prefetching was not
  built, so lookups and key comparisons are still 54% of rvk-1t's pipelined CPU in round 2. rvk-1t nevertheless
  reached 0.96–1.14× of Valkey's pipelined throughput there, because it avoids other costs Valkey pays.
* **"Language is not the lever" survives, with a qualification.** The first build lost to Valkey per core for
  implementation reasons, and the second reached parity for implementation reasons. Neither result is about
  Rust. What a multi-threaded architecture can add over one core is measured by R2 and R3, not R1.
