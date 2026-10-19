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
  cache workload, Valkey's main thread saturates one core, and ⟪KSHARE⟫ of that core's time is kernel time
  (system calls and loopback TCP). Executing the command itself takes ⟪CMDUS⟫ µs of the ⟪TOTALUS⟫ µs of CPU
  each operation costs (§4).
* **So language choice is not the lever.** A user-space rewrite in any language can only touch the user-space
  share. An infinitely fast reimplementation would give at most ⟪AMDAHL⟫× per core (Amdahl bound, measured
  inputs). The levers that work on this bottleneck are fewer system calls per operation (pipelining,
  multiplexing) and more cores (sharding). Valkey and its clients already provide both.
* **The one Rust option with a mechanism aimed at that bottleneck was prototyped and measured:** a multiplexing,
  auto-pipelining proxy (Option A). It cuts Valkey's own CPU per operation to ⟪PROXYVALKEYUS⟫ µs, but the proxy
  spends ⟪PROXYUS⟫ µs per operation, mostly in the kernel. On the same 2 vCPUs it reaches ⟪PROXYVSSHARD⟫× the
  throughput of two plain Valkey shards, at ⟪PROXYCPUVSSHARD⟫× the CPU per operation.
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

⟪SECTION_4_1⟫

### 4.2 Why that rules out "rewrite it in Rust" as a throughput lever

⟪SECTION_4_2⟫

### 4.3 Memory per key

⟪SECTION_4_3⟫

## 5. Opportunity assessment

Each option is evaluated separately. For each: the hypothesis, what already exists, the evidence, and the
verdict.

### 5.1 Option A: optimize the Valkey integration with a Rust component

⟪SECTION_5_1⟫

### 5.2 Option B: Rust caching sidecar or specialized cache

**Hypothesis.** A Rust in-memory cache on the application host, with Valkey still authoritative, could cut
latency and Valkey load for a hot read set. This is an **additional cache layer, not a replacement**. Valkey keeps
the data, persistence and replication.

| Criterion | Assessment |
|---|---|
| Hit rate and miss penalty | A sidecar hit costs one local hop; a miss costs the local hop plus the remote trip. It pays off only if `hit rate > local hop / remote latency`. On this host the local hop is the loopback round trip, measured at ⟪LOOPBACK_P50⟫ µs p50 for one connection to `valkey-1t`. Against a same-host Valkey there is nothing to save. The benefit exists only against a remote Valkey with real RTT (§7), which could not be measured here |
| TTL accuracy | The sidecar must copy Valkey's TTL (an extra `PTTL`, or `GETEX`-style reads) or apply its own shorter TTL, which allows stale reads up to that TTL |
| Memory per entry | A second copy of every hot entry. Valkey measured ⟪VALKEY_OVERHEAD⟫ bytes of overhead per 100-byte entry. A naive Rust map, with less functionality, measured ⟪RUST_OVERHEAD⟫ (§4.3) |
| Eviction | LRU or LFU would have to be reimplemented. Valkey already has approximated LRU and LFU; under `maxmemory` with skewed access its hit rate was measured at ⟪EVICT_SKEW_HIT⟫ (`evict-skew`) |
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
Valkey's implementation of all this is about 203k lines of C, refined over 15 years of Redis and Valkey history.

**Evidence against.**

* **Amdahl ceiling (measured inputs, §4.2):** ⟪KSHARE⟫ of Valkey's per-operation CPU is kernel time. Any
  user-space server doing one read and one write system call per request pays that cost too. Even with zero
  user-space cost, the ceiling is ⟪AMDAHL⟫× per core on this workload.
* **The kernel cost is not Valkey-specific.** The measured Rust proxy, which does the same client-side socket
  work, spends ⟪PROXYKSHARE⟫ of its CPU in the kernel, with tokio, the most widely used Rust async runtime.
* **Gains seen in multi-threaded servers come from architecture, not language.** Shared-nothing threads,
  io_uring and batching are available to C as well. Valkey itself is adding them (I/O threads, memory prefetch,
  `forkless.c`). Valkey's own scaling path for more cores is cluster mode, which this assessment measured as
  `valkey-2shard`.
* **Memory:** Valkey's measured overhead is ⟪VALKEY_OVERHEAD⟫ bytes per key above the raw payload, including
  expiry and eviction metadata. A naive Rust `HashMap` with no eviction metadata, no concurrency and no protocol
  layer measured ⟪RUST_OVERHEAD⟫ bytes (§4.3). A Rust rewrite does not get compact memory for free.

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
⟪USHARE⟫ of its CPU. Request parsing and reply building are each a few percent of samples. Replacing one of them
in Rust, behind an FFI boundary, could at best recover a few percent. It would also add `cargo` to a `make`/CMake
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

⟪SECTION_6⟫

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
  ⟪HOP_LAT⟫ µs extra p50 latency at one connection (measured, §5.1) and ⟪PROXYUS⟫ µs of CPU per operation.
* A Rust **sidecar cache** on the application host turns a remote hit (RTT plus service time) into a local hop.
  Its best case is the loopback round trip measured for `valkey-1t` at one connection, ⟪LOOPBACK_P50⟫ µs p50.
  That is the same cost as running a Valkey replica on the application host, which needs no new code.
  An in-process cache avoids even that hop.

These are the reasons the decision record asks for a measured RTT-bound workload before reconsidering
Option A or B.
