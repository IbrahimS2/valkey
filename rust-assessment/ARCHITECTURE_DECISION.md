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
| What problem does Rust solve that Valkey tuning cannot? | **None was found.** The measured bottleneck is per-request kernel work: ⟪KSHARE⟫ of Valkey's CPU on small requests. Batching removes it; changing language does not. Valkey already offers batching (pipelining, multiplexing clients) and scale-out (sharding and cluster) |
| Which exact component should be implemented in Rust, and why? | **None.** The best candidate, a multiplexing auto-pipelining proxy (Option A), was prototyped, tuned and measured. It lost to plain Valkey sharding on the same 2 vCPUs. The same mechanism already exists in Rust without the extra hop, inside the Valkey GLIDE client's core |
| What measurable improvement was demonstrated? | **None against the tuned baseline.** The proxy improved on single-threaded Valkey alone (⟪PROXY_VS_1T⟫× throughput on `read90`), but only by using a second CPU. On the same 2 CPUs, two Valkey shards gave ⟪SHARD_VS_PROXY⟫× the proxy's throughput at ⟪SHARD_CPU_VS_PROXY⟫× its CPU per operation. Client-side pipelining gave ⟪P16_VS_1T⟫× on one CPU |
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
| Tuned Valkey: client pipelining | `read90-p16` on `valkey-1t` | Measured: ⟪P16_VS_1T⟫× single-threaded throughput on the same CPU | **Recommended first** when the application can batch |
| Tuned Valkey: sharding (cluster) | `valkey-2shard`, one process per CPU | Measured: ⟪SHARD_VS_1T⟫× on `read90`, with low CPU per op | **Recommended** for scaling out |
| Tuned Valkey: `io-threads` | `valkey-io2` on 2 vCPUs | Measured: ⟪IO2_VS_1T⟫× and ⟪IO2_CPU⟫ µs/op, worse on this budget. Consistent with Valkey's guidance to enable it on 3–4+ cores | Re-evaluate on hosts with 4 or more cores; not on 2 vCPUs |
| A. Rust multiplexing proxy | `proxy-poc`, tuned (BENCHMARK_PLAN.md §3) | Measured: loses to sharding on throughput and CPU per op; adds latency at low load; breaks connection-scoped semantics | **Reject** |
| A. Rust client-side component | Valkey GLIDE already has a Rust core that multiplexes with automatic pipelining, one connection per node | Existing, maintained software | **Adopt GLIDE if a client change is needed; build nothing** |
| A. Request coalescing (single-flight) | Duplicate misses cost Valkey the same as hits; the benefit is protecting a backing database | Application-level pattern available in every language | Not a Rust question |
| B. Rust sidecar cache | Analysis; best case equals a local Valkey hop | Measured local hop ⟪LOOPBACK_P50⟫ µs p50; remote RTT benefit not measurable here | **Reject**; prefer an in-process near cache with `CLIENT TRACKING`, or a local replica |
| C. Standalone Rust server | Amdahl bound, feature inventory | Ceiling ⟪AMDAHL⟫× per core even with zero user-space cost; about 203k lines of C to match | **Reject**; configuration 4 not run |
| D. Rust inside Valkey | Profile shares; Modules API | No single user-space component is large enough; Rust modules are possible (`valkey-bloom`) | **Reject rewrites**; a Rust module stays an option for a specific missing capability |

## 4. Benchmark summary

Full tables, raw data and variance are in [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md). Every configuration had
CPUs 0–1 for the data service and CPUs 2–3 for the load generator. Each point is the median of 3 independent
trials.

⟪ADR_HEADLINE_TABLE⟫

## 5. Performance and resource efficiency

### Advantages Rust could have, and where they actually come from

| Claimed advantage | Comes from Rust or from the architecture? | Observed here |
|---|---|---|
| Lower overhead per request | Architecture (batching, fewer system calls) | Batching cut **Valkey's** CPU per op behind the proxy to ⟪PROXYVALKEYUS⟫ µs. The proxy re-incurred the per-request system calls at ⟪PROXYUS⟫ µs per op, ⟪PROXYKSHARE⟫ of it in the kernel |
| Memory safety | Rust | Real for new code. It does not reduce Valkey's attack surface behind a proxy, and does not prevent logic or availability bugs (THREAT_MODEL.md §2) |
| Predictable resource management | Mostly the design (bounded queues, backpressure) | The prototype needed explicit limits; Valkey already has them (`client-output-buffer-limit`, `maxmemory-clients`) |
| Efficient concurrency | Architecture | Two plain Valkey shards used 2 CPUs better than proxy plus Valkey |
| Fewer allocations and copies | Implementation | The proxy adds copies (client buffer to batch to Valkey and back) |
| Isolation through process boundaries | Architecture | Adds a failure domain and a hop. The direct path was simpler |

### Disadvantages and risks found

* **More total CPU for the same work:** ⟪PROXY_TOTAL_VS_SHARD⟫× the total CPU per operation of `valkey-2shard`
  on `read90`, client included.
* **More network traffic:** ⟪PROXY_BYTES⟫ loopback bytes per operation, against ⟪DIRECT_BYTES⟫ direct. Every
  byte crosses two hops.
* **Higher latency at low load:** at 1 connection, p50 ⟪PROXY_C1_P50⟫ µs against ⟪DIRECT_C1_P50⟫ µs.
* **Lost semantics and new bug classes:** see §6.

### Where Valkey is already more efficient than a custom Rust component

Valkey's single-threaded event loop handles a non-pipelined request in ⟪SVC_US⟫ µs of CPU, of which only
⟪USERUS⟫ µs is user space. Its `hashtable` stores a 100-byte value with ⟪VALKEY_OVERHEAD⟫ bytes of overhead,
including expiry and eviction metadata. When batched (pipelining), its CPU per operation falls to ⟪P16_US⟫ µs.
None of this leaves meaningful room for a reimplementation.

## 6. Correctness, reliability and security (summary of THREAT_MODEL.md)

* **Semantics:** the proxy must refuse or specially handle transactions, `SELECT`, blocking commands, Pub/Sub,
  `CLIENT *`, `HELLO`/RESP3, `WAIT` and per-user ACLs. Valkey's own test suite run through the proxy:
  ⟪CONF_SUMMARY⟫.
* **Failure behavior (measured):** ⟪FAILOVER_SUMMARY⟫
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

⟪COST_TABLE⟫

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

⟪CRITERIA_TABLE⟫

