# ADR: Rust alternative to Valkey

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

