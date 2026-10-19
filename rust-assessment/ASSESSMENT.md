# Assessment: current architecture, bottleneck, and Rust opportunities

Phase One and Phase Two of the engineering assessment. Numbers marked *measured* come from the runs in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md). Numbers marked *theoretical* are models and were not measured.

## 1. Summary

* **This repository is the Valkey server's source code, not a deployment of Valkey.** It contains no
  application, no deployment topology, no production configuration, no traffic, and no metrics. Most Phase One
  questions (topology, production hardware, workload, observed bottlenecks) therefore **cannot be answered from
  the repository**. They are listed as such in §2, not guessed. All later conclusions are conditional on the
  synthetic workloads defined in BENCHMARK_PLAN.md.
* **The measured bottleneck is the kernel's per-request network path, not Valkey's code.** On a small-value
  cache workload, Valkey's main thread saturates one core, and 64–71% of that core's time is kernel time
  (system calls and loopback TCP). Executing the command itself (`GET`) takes about 1.85 µs of the roughly
  12 µs of CPU each operation costs (§4).
* **So language choice is not the lever.** A user-space rewrite in any language can only touch the user-space
  share. An infinitely fast reimplementation would give at most 1.45× per core (Amdahl bound, measured
  inputs). The levers that work on this bottleneck are fewer system calls per operation (pipelining,
  multiplexing) and more cores (sharding). Valkey and its clients already provide both.
* **The one Rust option with a mechanism aimed at that bottleneck was prototyped and measured:** a multiplexing,
  auto-pipelining proxy (Option A). It cuts Valkey's own CPU per operation to 3.2–4.1 µs, but the proxy
  spends 15.3–17.0 µs per operation, mostly in the kernel. On the same 2 vCPUs it reaches 0.60–0.67× the
  throughput of two plain Valkey shards, at 1.36–1.51× the CPU per operation.
* Options B, C and D are assessed in §5. None has a measured or workload-backed justification.

## 2. Phase One: what could and could not be determined

| Item | Finding | Status |
|---|---|---|
| Valkey version | Source build of the `unstable` branch at `741b0159` (2026-10-17). `VALKEY_VERSION` `255.255.255`, release stage `dev`. Commands up to 9.2.0 are present, for example `HOTKEYS`. Redis compatibility version 7.2.4 | Determined |
| Deployment topology | None in the repository. The tree supports standalone, primary-replica, Sentinel (`sentinel.conf`) and cluster (`utils/create-cluster`) modes | **Not determinable** |
| Managed service? | No evidence either way | **Not determinable** |
| Production hardware, OS, network, storage | None in the repository. Test host: 4 vCPU Cascade Lake KVM guest, 16 GB, Ubuntu 24.04, loopback only (BENCHMARK_PLAN.md §2) | Test host only |
| Configuration | Only the shipped `valkey.conf` and built-in defaults: `io-threads 1`; no `maxmemory`, so `noeviction`; RDB save points `3600 1 300 100 60 10000`; `appendonly no`; all `lazyfree-*` set to yes; `maxclients 10000`; `protected-mode yes`; bound to loopback | Defaults only |
| Persistence, replication, eviction in use | Unknown. Defaults as above | **Not determinable** |
| Client libraries, pooling, pipelining, TLS, auth | No application. The tree ships `deps/libvalkey` (used by `valkey-cli` and `valkey-benchmark`). TLS is optional (`BUILD_TLS`); ACLs are built in | **Not determinable** |
| Workload: command mix, key and value distributions, TTLs, read/write ratio, concurrency, peaks | No traces, logs or metrics | **Not determinable**; synthetic mixes used |
| Current bottlenecks (production) | No observations available | **Not determinable**; test-host bottleneck measured (§4) |
| Monitoring, deployment automation, failover, backup | Valkey exposes `INFO`, `LATENCY`, `COMMANDLOG`, `HOTKEYS`, `CLIENT LIST`, `MEMORY STATS`; systemd units and init scripts ship in `utils/`. No monitoring stack, runbooks or backup policy in the repository | Capabilities only |
| Upstream performance process | CI has `benchmark-on-label`, `benchmark-on-demand` and `benchmark-release` workflows using `valkey-io/valkey-perf-benchmark` on self-hosted ARM64 EC2 runners | Determined |

**Implication:** the prompt's GO criterion requires "an established business or engineering requirement".
None is recorded in this repository, so no option can reach GO here regardless of benchmark results. The rest of
this document asks a narrower question: *if* a deployment had the measured bottleneck, would Rust be the right
tool?

## 3. Architecture relevant to the decision (from the source)

| Area | Implementation in this tree | Relevance |
|---|---|---|
| Execution model | One main thread owns all data and runs commands (`server.c`, `networking.c`) | Command execution is serialized; scaling uses I/O threads or more processes |
| I/O threads | `io_threads.c`, `queues.c`, [design-docs/io-threads.md](../design-docs/io-threads.md): workers do socket reads and writes, parsing, accepts, polls and deferred frees over lock-free SPMC/SPSC/MPSC queues; the main thread applies results | Already moves per-connection system calls off the main thread, which is what a proxy would do |
| Memory prefetch | `memory_prefetch.c` batches key lookups to hide cache misses | Already optimizes the data path a Rust rewrite would target |
| Keyspace | `hashtable.c` (open addressing, keys embedded in objects), `kvstore.c` per slot | Per-key overhead is already low; measured in §4.3 |
| Client-side caching | `tracking.c`: `CLIENT TRACKING` in default, `BCAST`, `OPTIN` and `OPTOUT` modes, RESP3 push invalidations | Server-assisted near cache already exists (relevant to Option B) |
| Hot keys | `hotkeys.c` (Space-Saving top-K, `HOTKEYS GET`) | Hot-key detection already exists (relevant to Options A and B) |
| Persistence | `rdb.c`, `aof.c` (multi-part AOF), `forkless.c` (background-iterator snapshot without fork) | Durability a replacement would have to match |
| Limits and protection | `client-output-buffer-limit`, `client-query-buffer-limit` (1 GB), `proto-max-bulk-len` (512 MB), `maxmemory-clients`, ACLs | A proxy would have to replicate these; behind it, Valkey sees only the proxy |
| Extensibility | Modules API (`module.c`). Rust modules are possible through `valkeymodule-rs`; the Valkey project's `valkey-bloom` is written in Rust | The supported path for Rust inside Valkey (Option D) |
| Size | about 203k lines of C in `src/*.[ch]`, excluding `deps/` | Scale of what a replacement would reimplement |

## 4. Bottleneck analysis (measured on the test host)

### 4.1 Where the CPU goes

Measured on `valkey-1t` at saturation, 50 connections, 100-byte values
(BENCHMARK_RESULTS.md §8, `results/profile/`):

| Measurement | Value | Source |
|---|---|---|
| Valkey main thread utilization | 98–99% of one CPU | `/proc` per run |
| Kernel share of Valkey's CPU | **64–71%** across the four core workloads (median 69%) | `/proc` utime/stime, 12 runs |
| Kernel share of profile samples | 70.6% kernel, 22.5% `valkey-server`, 6.8% libc (mostly `read`/`write` wrappers and `memmove`) | `perf record -e cpu-clock` |
| Total Valkey CPU per operation, `read90` | 11.8 µs | harness |
| of which user space | 3.7 µs | harness |
| of which executing `GET` itself | 1.85 µs per call | `INFO commandstats` during the profile run |
| Largest user-space symbols | `hashtableObjectGetKey` 3.5%, `findBucket` 3.3% (hash lookups that miss cache on a 1M-key table), `isCopyAvoidPreferred` 2.5%, `string2ll` 1.0% | `perf` |
| Largest kernel symbol | `_raw_spin_unlock_irqrestore` 17% (interrupt re-enable in the KVM guest, where loopback softirq work lands), then the TCP send, receive and ACK paths | `perf` |

The load generator was not the limit: it used 92–107% of its 200% CPU in these runs.

The same pattern holds for the Rust proxy, which does the same per-connection socket work: 70–72% of its CPU is
kernel time, and 73.5% of its profile samples are kernel samples. The cost is per system call and per packet. It
does not depend on the language of the code making the calls.

When requests arrive batched, Valkey's own cost per operation collapses. Behind the proxy (about 13 commands per
upstream write), Valkey spent 3.2–4.1 µs per operation instead of 11.8–13.4, and `GET` itself took 0.48 µs instead of
1.85, because batched input lets `memory_prefetch.c` overlap the hash-table cache misses. With client pipelining
at depth 16, Valkey's total cost was 2.0 µs per operation (§6).

### 4.2 Why that rules out "rewrite it in Rust" as a throughput lever

If a reimplementation, in any language, made all of Valkey's user-space work free and still made one `read` and
one `write` per request, the best case would be:

`speedup ≤ 1 / kernel share = 1 / 0.69 ≈ 1.45×` per core, measured on this host.

That bound is unreachable in practice. A real implementation still parses, looks up and builds replies, and the
measured Rust proxy spent 4.7 µs per operation in user space just to frame and forward requests. Compare this
1.45× ceiling with what existing Valkey features delivered on the same host:

| Lever | Measured effect | Requires new code? |
|---|---|---|
| Client pipelining, depth 16 | **5.7×** per core (83.6k → 479k ops/s) | No |
| One more Valkey process (sharding), 2 vCPUs | **1.55–1.93×** over one process (K1) | No |
| Hypothetical zero-cost user space, any language | ≤ 1.45× per core | A full server |

The bottleneck sits in a layer (kernel networking per request) that Rust cannot reach, and the tools that do
reach it already exist.

### 4.3 Memory per key

Measured with 1,000,000 keys and 100-byte values, 3 trials each, identical in every trial:

| Implementation | Bytes per key | Overhead above the 116-byte payload (16-byte key + 100-byte value) |
|---|---|---|
| Valkey (`used_memory`) | 169.5 | **53.5** |
| Valkey (RSS) | 173.8 | 57.8 |
| Naive Rust `HashMap<Box<[u8]>, (Box<[u8]>, u64)>` (RSS, glibc malloc) | 230.0 | **114.0** |

Valkey's overhead includes expiry and LRU/LFU metadata and the object header; the Rust map has none of these
and still uses twice the overhead. With 16 KiB values, the naive Rust map was smaller: 16,486 bytes per key
against Valkey's 20,534. Valkey's 16,400-byte allocation falls just past jemalloc's 16 KiB size class and is
rounded up to 20 KiB. This is an allocator size-class effect, not a language effect. A Rust service using
jemalloc would see the same thing, and it matters only for values just above a size-class boundary.

## 5. Opportunity assessment

Each option is evaluated separately. For each: the hypothesis, what already exists, the evidence, and the
verdict.

### 5.1 Option A: optimize the Valkey integration with a Rust component

Four sub-options were examined.

**(i) Multiplexing, auto-pipelining proxy: prototyped and measured.** `proxy-poc` accepts many client
connections, coalesces their requests into large pipelined writes on one upstream connection, and splits the
replies. It was profiled and tuned before measurement (BENCHMARK_PLAN.md §3). Results on the same 2 vCPUs
(BENCHMARK_RESULTS.md):

| Comparison | Result |
|---|---|
| vs single-threaded Valkey, 50 connections | 1.04–1.19× throughput, but using a second CPU |
| vs two Valkey shards on the same 2 vCPUs, 50 connections | **0.60–0.67× throughput, 1.36–1.51× CPU per op** |
| vs two shards, 200 and 1000 connections | 0.73× and 0.76× throughput, 1.34× and 1.31× CPU per op |
| 16 KiB values | 0.69× single-threaded Valkey, 0.35× two shards |
| Clients already pipelining (depth 16) | 1.00× single-threaded Valkey, using 1.55 vCPUs instead of 1 |
| Latency, 1 connection | p50 150 µs against 63 µs direct (+87 µs) |
| Latency at fixed load (20k–60k ops/s) | p50 2.2–2.3×, p99 1.5–2.0× the best native topology |
| Connection churn (new connection per request) | Same throughput, which is limited by client connection setup; 1.7× the server-side CPU per op |
| Semantics | Transactions, `SELECT`, blocking commands, Pub/Sub, `CLIENT *`, RESP3 and per-user ACLs refused (THREAT_MODEL.md §3) |

The mechanism does what it claims for Valkey: 3.2–4.1 µs per operation behind the proxy instead of 11.8–13.4.
The proxy then pays 15.3–17.0 µs per operation, 70–72% of it in the kernel, for the per-client system calls it
took over. That is more than Valkey paid for the same calls. The system as a whole does more work per operation
than plain Valkey, and much more than two shards.

**(ii) Rust client-side connection manager and multiplexer: already exists.** The Valkey project's own client,
[Valkey GLIDE](https://glide.valkey.io/concepts/architecture/multiplexing), has a Rust core shared by its
language bindings. It sends all requests to a node over a single multiplexed connection and relies on pipelining,
which is the proxy's mechanism, in-process and without an extra hop. GLIDE's documentation lists the same
caveats the prototype ran into: blocking commands, `WATCH`/`MULTI` and large values delay other requests on a
shared connection
([General Concepts](https://github.com/valkey-io/valkey-glide/wiki/General-Concepts)). If an application
suffers from connection count or round trips, adopting GLIDE (or pipelining in its current client) is the
Rust-backed option. It needs no new component.

**(iii) Request coalescing for duplicate cache misses (single-flight):** a `GET` that misses costs Valkey the
same as a hit (cold-cache runs: 83.9k ops/s at 8.5% hit rate, against 83.6k fully warm). The benefit of
coalescing is protecting the backing database during a stampede. It is an application-level pattern of a few
dozen lines in any language, not a reason for a Rust component.

**(iv) Reducing serialization and allocations:** RESP encoding and decoding is below 0.5% of profile samples
per function (§4.1). The `memmove` and allocation costs that are visible come from moving bytes through sockets, which a proxy
adds rather than removes.

**Verdict: reject** building a Rust proxy or client component. If a client-side change is needed, adopt GLIDE
or pipelining.

### 5.2 Option B: Rust caching sidecar or specialized cache

**Hypothesis.** A Rust in-memory cache on the application host, with Valkey still authoritative, could cut
latency and Valkey load for a hot read set. This is an **additional cache layer, not a replacement**. Valkey keeps
the data, persistence and replication.

| Criterion | Assessment |
|---|---|
| Hit rate and miss penalty | A sidecar hit costs one local hop; a miss costs the local hop plus the remote trip. It pays off only if `hit rate > local hop / remote latency`. On this host the local hop is the loopback round trip, measured at 63 µs p50 for one connection to `valkey-1t`. Against a same-host Valkey there is nothing to save. The benefit exists only against a remote Valkey with real RTT (§7), which could not be measured here |
| TTL accuracy | The sidecar must copy Valkey's TTL (an extra `PTTL`, or `GETEX`-style reads) or apply its own shorter TTL, which allows stale reads up to that TTL |
| Memory per entry | A second copy of every hot entry. Valkey measured 53.5 bytes of overhead per 100-byte entry. A naive Rust map, with less functionality, measured 114 (§4.3) |
| Eviction | LRU or LFU would have to be reimplemented. Valkey already has approximated LRU and LFU; under `maxmemory` with skewed access its hit rate was measured at 0.699 (`evict-skew`) |
| Concurrency | A sidecar shared by several application processes needs a concurrent map. This is solvable with existing crates, but it is new code on the hot path |
| Invalidation and consistency | A sidecar must subscribe to Valkey `CLIENT TRACKING` (BCAST or OPTIN) to learn of writes. Invalidations are lost during a disconnect, so the sidecar must flush on reconnect. Without tracking, it serves stale data until its local TTL expires. Either way consistency gets weaker than reading Valkey directly; this has to be documented and accepted, not silently introduced |
| Cold start | After a restart or a reconnect flush the hit rate is 0 and the full load returns to Valkey at once |
| Multi-process and multi-node | One copy per application host; BCAST invalidations fan out to every host |

**What already exists:**

* **In-process near cache with server-assisted invalidation** (`CLIENT TRACKING`, `tracking.c`): hits cost
  nanoseconds instead of a local socket round trip, using a protocol Valkey already implements.
* **A Valkey replica on the application host:** the same local-hop latency as a sidecar, with Valkey's own
  eviction, TTL and replication semantics, and no new code. Replication is asynchronous, so staleness is similar
  to the sidecar's.

**Verdict: not justified.** The benefit depends on a remote, RTT-bound, read-heavy, high-locality workload that
the repository does not show. If one appears, try an in-process near cache with `CLIENT TRACKING` first, then a
local read replica. Consider a Rust sidecar only if both fail for a reason they cannot address.

### 5.3 Option C: specialized Rust data service

**Hypothesis.** A purpose-built Rust server for a narrow command subset could be cheaper per operation than
Valkey.

**What it would have to reimplement to be a drop-in for even a simple cache:** RESP2 and RESP3 parsing; strings
plus whichever of hashes, lists, sets, sorted sets and streams the workload uses; lazy and active expiry with
Valkey's semantics; approximated LRU and LFU eviction under `maxmemory`; RDB and AOF persistence (or an explicit
decision to have none); replication and failover (Sentinel or cluster); ACLs and TLS; client output-buffer
limits; and the observability operators rely on (`INFO`, `COMMANDLOG`, `LATENCY`, `HOTKEYS`, `CLIENT LIST`).
Valkey's implementation of all this is about 203k lines of C, refined over more than 15 years of Redis and Valkey history.

**Evidence against.**

* **Amdahl ceiling (measured inputs, §4.2):** 64–71% of Valkey's per-operation CPU is kernel time. Any
  user-space server doing one read and one write system call per request pays that cost too. Even with zero
  user-space cost, the ceiling is 1.45× per core on this workload.
* **The kernel cost is not Valkey-specific.** The measured Rust proxy, which does the same client-side socket
  work, spends 70–72% of its CPU in the kernel, with tokio, the most widely used Rust async runtime.
* **Gains seen in multi-threaded servers come from architecture, not language.** Shared-nothing threads,
  io_uring and batching are available to C as well. Valkey itself is adding them (I/O threads, memory prefetch,
  `forkless.c`). Valkey's own scaling path for more cores is cluster mode, which this assessment measured as
  `valkey-2shard`.
* **Memory:** Valkey's measured overhead is 53.5 bytes per key above the raw payload, including
  expiry and eviction metadata. A naive Rust `HashMap` with no eviction metadata, no concurrency and no protocol
  layer measured 114 bytes (§4.3). A Rust rewrite does not get compact memory for free.

**Configuration 4 (standalone Rust alternative) was therefore not run.** The prompt allows it only "where a
replacement is technically justified and functionally comparable". Neither condition holds, and building a toy
server to produce a flattering number would compare unequal feature sets.

**Verdict: reject.** It would reimplement Valkey for a ceiling that is small even in theory, and it would give up
durability, replication and ecosystem compatibility. Compatibility with Valkey or Redis could not be claimed
without conformance testing.

### 5.4 Option D: Rust for selected Valkey functionality

**Hypothesis.** Rewriting a hot component of Valkey in Rust (for example the RESP parser, the reply path, or a
data type) could make that component faster or safer.

**Measured share of candidates.** In the Valkey profile (§4.1), all of Valkey's user-space code together is
about 30% of its CPU. The largest user-space items are hash-table lookups, about 7%, dominated by cache misses
rather than instructions. No request-parsing or reply-building function reaches 0.5% of samples; the only one in
the top 40 is `_writeToClient` at 0.49%. Replacing any of them in Rust, behind an FFI boundary, could recover at
most a fraction of a percent. It would also add `cargo` to a `make`/CMake
build, a second toolchain for every packager, and FFI-boundary bugs, in a project whose contributors write C.

**Where Rust inside Valkey does make sense: new functionality as a module.** The Modules API supports Rust
through `valkeymodule-rs`. The Valkey project's own Bloom filter module,
[`valkey-bloom`](https://github.com/valkey-io/valkey-bloom), is written in Rust. If a workload needs a data type
or server-side operation that Valkey lacks (today emulated with several round trips or a read-modify-write
loop), a module removes round trips and gives memory safety to new code that runs inside the server process. The
performance gain there comes from moving the computation to the data, which a C module, Lua script or Valkey
Function would also achieve. Rust's distinct contribution is safety for new in-process code. Even that is
partial: `valkey-bloom` still had a crash bug, CVE-2026-21864, an assertion reachable through a crafted
`RESTORE` (THREAT_MODEL.md §2).

**Verdict:** no rewrite of existing Valkey components. A Rust module remains a sound choice for a specific
missing capability, but no such capability is identified, because no workload is defined. Compare it first with
Valkey Functions or Lua.

## 6. Simpler alternatives, ranked by measured effect

Measured on the same host and budget (BENCHMARK_RESULTS.md K1, K2, K4):

| Rank | Alternative | Measured effect | Cost and caveats |
|---|---|---|---|
| 1 | **Client pipelining or batching** (or a multiplexing client such as GLIDE) | 5.7× throughput per core; 2.0 µs CPU per op instead of 11.8 | Application change; latency per batch rises (p50 1.5 ms at depth 16 with 50 connections) |
| 2 | **Persistent connections** instead of one connection per request | 76.6k against 12.0k ops/s (6.4×) on `valkey-1t`, same tool and GET-only workload; 12.6 against 72 µs CPU per op | Use client connection pooling or multiplexing |
| 3 | **Sharding** (Valkey cluster) | 1.55–1.93× on 2 vCPUs; lowest p50 at every fixed load and lowest p99 at 40k and 60k ops/s | Cluster-aware client; multi-key commands limited to one slot |
| 4 | **`io-threads`** | Negative on 2 vCPUs (0.82–0.85×, 2.3–2.4× CPU per op). Valkey's guidance is 2–3 threads on 4+ cores | Re-test on the target host size before enabling |
| 5 | **Configuration for a pure cache** | Persistence off vs AOF `everysec`: `read90` 83.6k against 75.3k, `write90` 74.5k against 47.7k ops/s on `valkey-1t` | Only where losing data on restart is acceptable |
| – | Rust proxy (Option A) | Below ranks 1–3 on every workload; see §5.1 | New component, lost semantics |

## 7. Network round trips (theoretical model, not measured)

Loopback hides network round-trip time (RTT), and RTT is where client-side batching pays off most. The RTT values
below are illustrative assumptions for typical cloud networks, not measurements.

A connection with one request in flight completes at most `1 / (RTT + S)` requests per second, where `S` is the
server-side service and queueing time. With pipelining (or multiplexing) depth `P`, this becomes about
`P / (RTT + P·S)`.

| Assumed RTT | Max ops/s per connection, P=1 | P=16 | Connections needed for 80k ops/s at P=1 |
|---|---|---|---|
| 50 µs (same host or rack) | about 16,000 | about 66,000 | 5 |
| 250 µs (same zone) | about 3,800 | about 36,000 | 21 |
| 1 ms (cross-zone) | about 1,000 | about 13,000 | 81 |

`S` is set to 12 µs, the measured server CPU per operation of `valkey-1t` on `read90`, and held constant. That
is conservative: pipelining also lowers `S` (§6).

Implications:

* For a remote Valkey, batching in the client (pipelining, or a multiplexing client such as GLIDE) recovers most
  of the RTT cost without adding a hop.
* A Rust **sidecar proxy** on the application host gets the same batching effect. It adds a local hop:
  87 µs of extra p50 latency at one connection (measured, §5.1) and 15.3–17.0 µs of CPU per operation.
* A Rust **sidecar cache** on the application host turns a remote hit (RTT plus service time) into a local hop.
  Its best case is the loopback round trip measured for `valkey-1t` at one connection, 63 µs p50.
  That is the same cost as running a Valkey replica on the application host, which needs no new code.
  An in-process cache avoids even that hop.

These are the reasons the decision record asks for a measured RTT-bound workload before reconsidering
Option A or B.
