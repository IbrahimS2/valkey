# Benchmark plan

Status: executed on 2026-10-19. Results are in [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md); raw data is in
[`results/raw/`](results/raw/).

## 1. Questions the benchmark must answer

1. Where does Valkey spend CPU on a small-value cache workload: in Valkey's own code, or in the kernel?
   This sets an upper bound on what any user-space rewrite, in any language, could gain.
2. On a fixed CPU budget, does putting a Rust component in front of Valkey (Option A: a multiplexing,
   auto-pipelining proxy) give more throughput, lower latency or lower CPU per operation than the best
   Valkey-only use of the same CPUs?
3. How do the simplest alternatives compare: Valkey `io-threads`, more Valkey processes (sharding), and
   client-side pipelining?
4. What does the extra component cost in correctness, recovery behavior and memory?

## 2. Test environment

Everything runs on one host. `bench/harness.py env` records the details in
[`results/environment.json`](results/environment.json).

| Item | Value |
|---|---|
| Host | KVM guest (Firecracker kernel `6.18.44-fc-v139`), Ubuntu 24.04.5 LTS |
| CPU | 4 vCPU Intel Xeon (Cascade Lake, family 6 model 85) at 2.80 GHz, 1 thread per core, 33 MiB L3 |
| Memory | 16 GB, no swap; THP `madvise` |
| Network | Loopback only. `sch_netem` is not compiled into the kernel, so network latency cannot be emulated |
| Valkey | Source tree at `741b0159` (unstable, `255.255.255`), `make` defaults: `-O3`, jemalloc 5.3.0, gcc 13.3 |
| Rust | rustc/cargo 1.97.0 (pinned in `bench/rust/rust-toolchain.toml`), release profile, thin LTO |
| Rust deps | tokio 1.53.3, bytes 1.12.1, hdrhistogram 7.6.0 (pinned in `bench/rust/Cargo.lock`) |
| Limits | `nofile` 20000, `somaxconn` 4096 |

### CPU layout (same for every configuration)

| CPUs | Role |
|---|---|
| 0–1 | Data service: Valkey process(es) and, when present, the Rust proxy. This is the budget under comparison |
| 2–3 | Load generator only |

Every configuration gets the same 2-vCPU budget. The single-threaded baseline uses only one of them by design;
that is part of what is being measured.

## 3. Configurations

The prompt's four required configurations map onto these topologies:

| Required configuration | Topology name | Definition |
|---|---|---|
| 1. Baseline | `valkey-1t` | Valkey with default threading (`io-threads 1`), pinned to CPU 0, persistence off (`save ""`, `appendonly no`) |
| 2. Tuned baseline | `valkey-io2` | Same, with `io-threads 2`, on CPUs 0–1 |
| 2. Tuned baseline | `valkey-2shard` | Two single-threaded Valkey processes, one per CPU. The client routes each key to one shard (`index % 2`), as a cluster-aware client would |
| 2. Tuned baseline (integration) | `*-p16` workloads | Client-side pipelining, depth 16, against `valkey-1t`, `valkey-2shard` and `rust-proxy` |
| 3. Rust-assisted | `rust-proxy` | `valkey-1t` on CPU 0, behind `proxy-poc` (2 tokio worker threads on CPUs 0–1, 1 upstream connection) |
| 4. Standalone Rust | not run | Not technically justified and no functionally comparable implementation exists. See [ASSESSMENT.md §5.3](ASSESSMENT.md#53-option-c-specialized-rust-data-service) |

Persistence is off in the main matrix because the repository has no deployment telling us what durability is
required. The `aof` suite repeats two workloads with `appendonly yes`, `appendfsync everysec`. The proxy does not
change durability: Valkey still does the persisting.

### Why the Rust candidate is a proxy

Option A is the only option with a concrete, testable mechanism that does not reimplement Valkey: it batches
requests from many client connections into pipelined writes on few upstream connections. That cuts Valkey's
per-request system calls. The open question is whether the proxy's own system calls and copies cost less than
they save. `proxy-poc` is a benchmark instrument, not a product. See its header comment for what it leaves out.

### Fairness tuning applied to the Rust candidate

Before the formal runs, the proxy was profiled and tuned so the comparison is not against a strawman:

* **Batching bug fixed.** tokio runs a freshly woken task next (its LIFO slot). The upstream writer therefore
  flushed after every single request: 1.00 commands per upstream write. Yielding once before draining the
  queue raised this to 12–16 commands per write and throughput by about 2×.
* **Shape sweep.** Upstream connections {1, 2, 4} × worker threads {1, 2} × CPU placement were tried at
  50 connections. The best shape, 2 threads sharing CPUs 0–1 with Valkey and 1 upstream connection, is the one
  benchmarked.

## 4. Workloads

All keys use the `key:%012d` format (16 bytes), the same as `valkey-benchmark`. Values are filled with `x`.
Unless noted, the dataset is preloaded before each run so every GET hits.

| Name | GET:SET | Keyspace | Value | Distribution | Other |
|---|---|---|---|---|---|
| `get100` | 100:0 | 1M | 100 B | uniform | cross-check only |
| `read90` | 90:10 | 1M | 100 B | uniform | read-heavy |
| `mixed50` | 50:50 | 1M | 100 B | uniform | mixed |
| `write90` | 10:90 | 1M | 100 B | uniform | write-heavy |
| `skew-read90` | 90:10 | 1M | 100 B | Zipf θ=0.99 | hot keys |
| `large-read90` | 90:10 | 100k | 16 KiB | uniform | large values (1.6 GB dataset) |
| `ttl-mixed50` | 50:50 | 1M | 100 B | uniform | `SET … EX 5`, starts empty, 8 s warmup to reach steady state |
| `cold-read90` | 90:10 | 1M | 100 B | uniform | empty dataset (cold cache) |
| `read90-p16` | 90:10 | 1M | 100 B | uniform | client pipeline depth 16 |
| `read90-aof`, `write90-aof` | 90:10, 10:90 | 1M | 100 B | uniform | `appendonly yes`, `everysec` |
| `evict-uniform` | 50:50 | 5M | 100 B | uniform | `maxmemory 256mb`, `allkeys-lru`; preload overfills, so the run is steady-state eviction |
| `evict-skew` | 50:50 | 5M | 100 B | Zipf θ=0.99 | as above |
| `get100-churn` | 100:0 | 1M | 100 B | uniform | `valkey-benchmark -k 0`: a new TCP connection per request (`churn` suite) |
| `write90-aof-p16` | 10:90 | 1M | 100 B | uniform | AOF `everysec`, client pipeline depth 16 (`aof2` suite) |

**Suites added during the run, disclosed here:** `churn` (connection churn is the classic case where a proxy with
persistent upstream connections helps a server, so it is the proxy's best case and had to be tested) and `aof2`
(the first AOF runs covered only `valkey-1t` and `rust-proxy`; §8 requires a comparison with the best
Valkey-native topology). Both were added before their results were known. No run was repeated or discarded
because of its result. The only excluded points are the not-applicable 1-connection `valkey-2shard` runs.

### Load shapes

* **Closed loop** (default): each connection keeps `pipeline` requests outstanding. Used for saturation
  throughput. Concurrency levels are 1, 10, 50, 200 and 1000 connections.
* **Open loop** (`openloop` suite): requests are sent on a fixed schedule at 20k, 40k and 60k ops/s,
  whether or not replies have arrived. This compares latency at the same offered load. Latency is measured
  from the actual send time. Schedule lag is recorded to detect coordinated omission. The tokio timer has
  1 ms granularity, so sends come in small bursts.

### Scenarios that cannot be represented here

| Scenario | Why not | Treatment |
|---|---|---|
| Real application workload, command mix, key and TTL distributions | The repository is the Valkey source tree. There is no deployment, traffic capture or metrics | Synthetic mixes above; all conclusions are conditional on them |
| Network round trips (same AZ, cross AZ) | Loopback only; no `netem` | Analytical model in ASSESSMENT.md, labeled *theoretical* |
| Concurrent cache misses hitting a backing database (stampede) | No backing database exists; Valkey's cost per miss equals its cost per hit | Discussed as an application-side pattern (single-flight) |
| Replication and failover | Single host; would measure the kernel, not the design | Recovery is limited to restart and reconnect behavior (`recovery.py`) |
| TLS | `proxy-poc` has no TLS, so comparing it with TLS Valkey would be unequal | Not run; noted as a cost the proxy would add |

## 5. Metrics and how each is obtained

| Required metric | Method |
|---|---|
| Throughput | Load generator, replies completed inside the measurement window ÷ window. Cross-checked against Valkey's `total_commands_processed` sampled at the window edges |
| Latency p50/p95/p99/p99.9 | Load generator HDR histogram (1 ns–120 s, 3 significant digits), per op type and overall. Client-side, from send to reply parsed |
| CPU utilization | `/proc/<pid>/stat` utime and stime per process, sampled at the window edges. Reported per process and summed for the data-service side |
| CPU per operation | Data-service CPU seconds ÷ operations. µs/op equals CPU-seconds per million ops |
| Memory | Valkey `used_memory` and RSS, process RSS; bytes per key in `recovery.py memory` |
| Network traffic | Load generator byte counters, plus Valkey `total_net_*_bytes` for the proxy's second hop. Reported as loopback bytes per op |
| Cache hit rate | GET replies: bulk = hit, nil = miss. Valkey `keyspace_hits` and `keyspace_misses` are also recorded |
| Error and timeout rates | Load generator: RESP error replies, request timeouts (5 s), lost connections. Valkey `total_error_replies` |
| Startup and warm-up | `recovery.py rdb`, `aof`: restart to `loading:0`; proxy cold start to first PONG |
| Recovery time | `recovery.py proxy`: how long after Valkey is ready the proxy serves again. `failover`: in-flight outcome when Valkey is killed under load |
| Cost per million operations | (a) consumed: server CPU-seconds per Mop; (b) provisioned: 2 vCPU × price ÷ throughput, at an assumed price of $0.05 per vCPU-hour (a parameter, not a quote) |

### Bottleneck attribution (one-off profiling)

* `perf record -g` (software `cpu-clock` event; this VM exposes no hardware PMU) on Valkey and on the proxy
  under the `get100` load.
* The user/kernel split from `/proc` for every run.
* Valkey `INFO commandstats` `usec_per_call`: time spent executing the command itself.

## 6. Procedure per run (`bench/harness.py run_one`)

1. Start the topology fresh on an empty temporary directory, pinned with `taskset`.
2. Preload the dataset with `loadgen --preload` (pipelined SETs) and check `DBSIZE`. Skipped for the
   cold, TTL and eviction workloads.
3. `CONFIG RESETSTAT`; take an INFO snapshot.
4. Start the load generator on CPUs 2–3. It opens connections, waits for its start time, runs `warmup`
   seconds unrecorded, then records for `duration` seconds.
5. At the start and end of the recording window, sample CPU for every process and Valkey's command counter.
6. Collect the client JSON, INFO deltas, RSS and proxy counters, then stop everything. Write one JSON line.

Defaults: 3 independent trials per point (fresh processes each time), 3 s warmup, 15 s recording, different
random seed per trial.

## 7. Validity checks

* **Client saturation:** a run is flagged `client-bound` if the load generator used more than 170% of its
  200% CPU.
* **Independent load generator:** `valkey-benchmark` and `loadgen` both run `get100` against `valkey-1t`
  (`harness.py crosscheck`). Agreement within run-to-run noise is required before trusting `loadgen`'s
  other numbers.
* **Server-side throughput:** Valkey's own command counter across the window must agree with the client's
  throughput.
* **Correctness:** errors, timeouts and lost connections must be zero in every throughput run, and the
  preload `DBSIZE` must match.
* **Noise:** throughput CV is reported for every point. A difference smaller than the trial range is not
  treated as a difference.

## 8. Acceptance thresholds (set before the formal matrix)

The repository has no service-level objectives, so the thresholds come from engineering requirements alone.
They were fixed after the calibration runs, which showed throughput spreads of up to about ±10% between
repeated runs on this VM, and before the formal matrix ran. The formal runs confirmed this: the largest
throughput CV was 10.1%.

A Rust-assisted configuration earns **GO** only if, against the **best** Valkey-native topology on the same
2 vCPUs, it achieves **all** of:

1. Throughput ≥ 1.20× (twice the calibration noise band, so it also pays for running one more component), **or**
   data-service CPU per op ≤ 0.80× at equal or better throughput;
2. p99 latency no worse than 1.10× at the same offered load (open loop);
3. Zero errors and timeouts, with no regression in recovery time beyond 1 s;
4. No loss of semantics for commands the workload uses.

Meeting 1–3 but not 4, or meeting them on only some workloads, gives at most **CONDITIONAL GO**. Missing 1 or
2 on the core workloads is **NO-GO**.

## 9. Reproduction

```sh
make -j"$(nproc)"                                        # Valkey, from the repo root
cd rust-assessment/bench/rust && cargo build --release   # loadgen, proxy-poc, mapmem
cargo test --release -p resp-common                      # RESP framing tests
cd .. && ./run_all.sh                                    # about 2.5 hours on 4 vCPUs
./summarize.py                                           # writes ../results/summary.md and key_tables.md
```

Individual suites: `./harness.py list`, `./harness.py run core`, `./recovery.py memory rdb`,
`PERF=/path/to/perf ./profile.sh`, `./conformance.sh`.
Requirements: Linux, `taskset`, Python 3.8+, 4 or more CPUs (edit `SERVER_CPUS` and `CLIENT_CPUS` in
`harness.py` for larger hosts). Profiles need `perf`; conformance needs `tclsh` 8.5+. Raw results are appended,
so delete `results/raw/` for a clean rerun.

## 10. Known limitations

* **One host, loopback networking.** Absolute numbers are specific to this VM. KVM makes system calls and
  loopback TCP unusually expensive (about 75% of Valkey's CPU is kernel time here), which **favors**
  architectures that cut system calls, such as the proxy and pipelining. Relative results are the point.
* **Only 2 vCPUs for the data service.** `io-threads` is designed for larger hosts. Valkey's guidance is 2–3
  I/O threads on 4 cores. The `valkey-io2` result shows behavior at the bottom of that range, not its ceiling.
* **Sharding without cluster mode.** `valkey-2shard` routes keys in the client without cluster mode. Real
  cluster mode adds slot checks, the cluster bus, and MOVED and ASK handling. Each shard's connections are
  fixed, so hot-key load imbalance between shards is not modeled.
* **No real workload.** Every conclusion is conditional on the synthetic mixes.
* **Short runs.** 15 s windows show steady-state CPU and latency, not hours-long effects such as
  fragmentation or rehashing.
* **Not independently re-run.** The cross-check uses a second tool on the same host; no second machine or
  person repeated the runs.

---

# Part II: reassessment plan (pre-registered 2026-10-19, before any measurement of the new candidates)

Part I (above) evaluated one candidate, a multiplexing proxy. The reassessment directive asks whether a properly
architected Rust implementation could outperform Valkey. [REASSESSMENT.md](REASSESSMENT.md) explains why the proxy
did not answer that, and [ARCHITECTURE_COMPARISON.md](ARCHITECTURE_COMPARISON.md) explains how the candidates were
chosen. This part fixes the configurations, workloads and decision thresholds **before** any performance number for
the new candidates was taken. The commit that adds this section precedes every new result file in the git history.

## II.1 Configurations (same host and CPU layout as Part I: data service on CPUs 0–1, load generator on CPUs 2–3)

| Name | What it is | Client contract | Question it answers |
|---|---|---|---|
| `valkey-1t` | Valkey, default threading, on CPU 0 | one endpoint | baseline |
| `valkey-io2` | Valkey `io-threads 2` on CPUs 0–1 | one endpoint | Valkey's own single-endpoint multi-core option |
| `valkey-2shard` | two Valkey processes, one per CPU | client routes keys (cluster-aware client) | Valkey's scale-out option |
| `rvk-1t` | `rvk-server --threads 1` on CPU 0 | one endpoint | **implementation/language effect**: same architecture (one event loop) and same CPU as `valkey-1t` |
| `rvk-2t` | `rvk-server --threads 2` on CPUs 0–1 | one endpoint | **architecture effect**: a multi-threaded, lock-sharded server on the same 2 CPUs |
| `*-p16` | client pipelining, depth 16 | as above | capacity without per-request system call costs; avoids client saturation |
| `rust-proxy` | Part I proxy, `read90` only | one endpoint | continuity with Part I under today's conditions |
| redis-rs clients | the `redis` crate's MultiplexedConnection and experimental client-side cache | application-side | Candidate C, using maintained functionality rather than new code |

## II.2 Workloads and measurements

The Part I workloads and procedure are reused unchanged (§4–§7 above). The changes are:

* `rvk-server` is started by the harness like `valkey-server`. It has no persistence, so the AOF scenarios compare
  it only with Valkey's persistence-off runs, and say so.
* **Open-loop latency** is reported twice: from the actual send time (as in Part I) and from the *intended* send
  time. The second includes any lateness of the load generator itself, which removes coordinated omission. The
  Part I audit found send lag of 0.8–1.1 ms p50 and up to 42 ms max that had not been reported.
* Memory per key, profiles (`perf`) and correctness results are collected for `rvk-server` too.

## II.3 Decision thresholds (pre-registered)

**Gate 2 (functional validity), required before any performance number counts:**

* Differential test against Valkey: mismatches only in features the feature matrix declares partial.
* All concurrency invariants (`poc/tests/concurrency.py`) pass at 2 and 4 threads, including under `maxmemory`.
* Zero errors, timeouts and reply misattributions in every benchmark run.

**Gate 3 (performance validity), for Candidate A (`rvk-server`).** Ratios use medians of 3 trials on the core
workloads (`read90`, `mixed50`, `write90`, `skew-read90`, 50 connections) plus 200 connections on `read90`.

| Ratio | Definition | Reading |
|---|---|---|
| R1 | `rvk-1t` ÷ `valkey-1t` throughput (and CPU/op) | implementation effect. Within ±10%: no material language/implementation advantage. ≥ 1.20× at equal or lower CPU/op: advantage demonstrated |
| R2 | `rvk-2t` ÷ `valkey-io2` | architecture effect at the same client contract (one endpoint) |
| R3 | `rvk-2t` ÷ `valkey-2shard` | against Valkey's best measured use of 2 CPUs, which needs cluster-aware clients |

* **GO** (build toward production) requires R3 ≥ 1.20 on at least 3 of 5 points, with CPU/op no higher than the
  best native option, p99 at the same offered load ≤ 1.10× the best native option, Gate 2 passed, and Gate 4.
* **CONDITIONAL GO** (a bounded experiment) requires R2 ≥ 1.20 and R3 ≥ 0.90 on at least 3 of 5 points, p99 at
  the same offered load ≤ 1.10× the best single-endpoint Valkey option, and Gate 2 passed. This means: it matches
  sharding without client routing and beats Valkey's single-endpoint option. The experiment must then be repeated
  on a host where `io-threads` is in its intended regime (≥ 4 cores for the server), because on 2 vCPUs
  `io-threads` is known to be weak (Part I).
* **NO-GO** otherwise.
* If a comparison is limited by the load generator (> 170% client CPU) on either side, it is decided on the
  pipelined (`-p16`) runs instead, where the client is not the bottleneck.

**Candidate B (proxy).** Not re-implemented: Part I measurements bound it (REASSESSMENT.md §4). Thresholds are
unchanged from Part I §8.

**Candidate C (optimization layer).** Only maintained functionality is measured (redis-rs). The question is
"adopt", never "build": adopting is recommended for a workload class if application throughput rises ≥ 1.20× or
data-service CPU per successful operation falls ≥ 20%, with staleness behavior documented.

**Gate 4 (production and economic validity).** A GO also needs a measured advantage that existing solutions do not
provide (Valkey `io-threads`, cluster mode with a cluster-aware client such as GLIDE), and a feature gap that can be
closed at an estimated cost the advantage pays for. The repository has no production workload, so Gate 4 can at
best be assessed conditionally.
