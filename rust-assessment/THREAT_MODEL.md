# Threat model and correctness assessment

This file has two parts. Part I (§1–6) covers the benchmarked proxy (Part I's Option A, Part II's Candidate B,
prototype `bench/rust/proxy-poc`) and any Rust sidecar in the request path. Part II (§7–8) covers `rvk-server`
(Candidate A) and the client-side options (Candidate C).

Part I scope: the Rust component that was benchmarked, a multiplexing, auto-pipelining RESP proxy in front of Valkey
(Option A, prototype `bench/rust/proxy-poc`). The same analysis applies, with small changes, to any Rust sidecar
placed in the request path (Option B). It also covers the general claim that Rust would make the data path safer.

Correctness guarantees have to be defined before any performance optimization. This document defines them, says
which ones the prototype breaks, and lists the tests a real implementation would need. Measured behavior is in
[§6](#6-measured-behavior).

## 1. System and trust boundaries

```text
            (1)                         (2)                      (3)
 app ──── RESP over TCP ────► proxy ──── RESP over TCP ────► Valkey ──► RDB/AOF on disk
 many connections,           few shared upstream           unchanged: still parses,
 many principals             connections, ONE principal    executes, persists everything
```

| Boundary | Today (direct) | With a proxy |
|---|---|---|
| (1) client → data service | Valkey authenticates each connection (AUTH or HELLO, ACL user) and enforces ACLs per connection | The proxy must authenticate clients itself, or pass AUTH through. A multiplexer cannot pass AUTH through, because upstream connections are shared |
| (2) proxy → Valkey | n/a | New hop. Upstream connections authenticate as one service identity. Needs TLS if not on the same host |
| (3) Valkey internals | Unchanged | Unchanged. Valkey still parses every forwarded byte |

**Assets:** cached data (confidentiality, integrity), Valkey credentials, availability of the data service, and
the correctness of application logic that relies on Valkey semantics such as atomicity, TTL, ordering and
transactions.

## 2. STRIDE analysis of the proxy

| Threat | Mechanism | Severity | Mitigation available | Present in prototype |
|---|---|---|---|---|
| **Spoofing / elevation:** identity collapse | All clients share the upstream identity. Per-user ACLs in Valkey (`ACL SETUSER … ~prefix:* -@dangerous`) can no longer tell clients apart. Every client gets the union of permissions | High | Reimplement ACLs in the proxy, or one upstream pool per ACL user (which reduces multiplexing) | No: AUTH and HELLO are refused |
| **Information disclosure:** cross-client reply leak | A framing or accounting bug (for example, miscounting the replies to a command) shifts replies by one. Client A then receives client B's data. Without a proxy, a framing bug can only corrupt that one connection | **High**: the worst failure a multiplexer can have | Strict framing, refusing every command whose reply count is not exactly 1, property and fuzz tests on framing, closing the upstream connection on any parse anomaly | Partial: refuses connection-scoped commands, tears down on parse error. No fuzzing done |
| **Tampering** | Plaintext hop (2) on a network | Medium | TLS (`rustls`) on both hops, or a Unix socket on the same host | No TLS |
| **Tampering / integrity:** silent backend substitution | When Valkey restarts (or fails over to another node), idle client connections to the proxy stay open. Their next commands go to the new, possibly empty, instance with no error. A direct client would see the disconnect | High for non-cache data (counters, locks, sessions) | Track Valkey's `run_id` and replication offset; disconnect every client when it changes | **No: measured in §6.2** |
| **Denial of service:** memory | Requests are buffered whole before forwarding, and the in-flight cap counts commands, not bytes. A client can pipeline 1024 × `GET` of large values; the replies sit in an unbounded per-client channel. Valkey's protections (`client-output-buffer-limit`, `maxmemory-clients`, `client-query-buffer-limit`) see only the proxy's one connection | High | Byte-based limits per client and globally, disconnecting slow readers, an equivalent of `client-output-buffer-limit` | Command-count limit only |
| **Denial of service:** head-of-line blocking | A slow command (large `LRANGE`, `KEYS`, long Lua) or a large reply on a shared upstream connection delays every client multiplexed onto it. Valkey's single main thread already serializes command execution, but a proxy adds a second queue that also serializes reply transfer | Medium | More upstream connections (less batching) or per-command routing | Measured: with 16 KiB values the proxy reached only 0.69× single-threaded Valkey's throughput (BENCHMARK_RESULTS.md §7) |
| **Denial of service:** proxy crash | A panic or OOM drops every client at once, and the proxy becomes an extra single point of failure | Medium | Supervisor, multiple proxy instances, health checks | Panics abort the task, not the process; there is no supervisor |
| **Repudiation / audit** | Valkey's `CLIENT LIST`, `MONITOR`, the command log and ACL logs show the proxy, not the real client | Medium | Proxy-side audit logging | No |

**The security argument for Rust is weaker than it looks.** Rust removes memory-unsafety bugs (buffer overflows,
use-after-free) from the proxy's own code. The prototype has no `unsafe` blocks; its dependencies (tokio, mio,
bytes, libc) do contain reviewed `unsafe`. But:

* Valkey still parses everything the proxy forwards, so Valkey's attack surface is unchanged. The proxy adds a
  second parser in front of it.
* The proxy's most serious failure mode is a logic error (reply misattribution), which memory safety does not
  prevent.
* Memory-safe code still crashes. [`valkey-bloom`](https://github.com/valkey-io/valkey-bloom), the Valkey
  project's own Rust module built on `valkeymodule-rs`, had
  [CVE-2026-21864](https://releasealert.dev/cve/CVE-2026-21864): a crafted `RESTORE` triggered an assertion that
  shut the server down. Availability bugs remain.
* Command filtering, the usual defense-in-depth reason for a proxy, is already native: Valkey ACLs can deny
  commands, categories (`-@dangerous`) and key patterns per user.

## 3. Semantics the proxy changes

Each row is a guarantee a Valkey client has today and loses or weakens behind a multiplexing proxy.

| Guarantee | Direct Valkey | Behind `proxy-poc` | Behind a production-grade multiplexer |
|---|---|---|---|
| Per-connection command order | Yes | Yes: each client is pinned to one upstream connection, so FIFO is preserved | Yes if pinned; lost if commands are spread across connections |
| `MULTI`/`EXEC`/`WATCH` atomicity | Yes | **Refused** | Needs a dedicated upstream connection per transaction (connection checkout), which reduces multiplexing |
| `SELECT` (multiple databases) | Yes | **Refused** | Per-database pools |
| Blocking commands (`BLPOP`, `XREAD BLOCK`, `WAIT`) | Yes | **Refused**: they would block every client on that connection | Dedicated connections |
| Pub/Sub, `MONITOR` | Yes | **Refused**: one request, many replies | A separate subsystem |
| `CLIENT TRACKING` (client-side caching) | Yes | **Refused**: invalidations are tied to the connection that read the key | Very hard: the proxy would have to track keys per client |
| `CLIENT REPLY OFF/SKIP`, `CLIENT NO-EVICT`, `CLIENT SETNAME` | Yes | **Refused** (`CLIENT` blocked) | Per-command emulation |
| RESP3 (`HELLO 3`) | Yes | **Refused** | Protocol translation or per-protocol pools |
| Per-user ACLs | Yes | Lost (see §2) | Reimplemented in the proxy |
| Lua and Functions (`EVAL`, `FCALL`) | Atomic | Forwarded, still atomic in Valkey. Long scripts cause head-of-line blocking for other clients | Same |
| TTL and expiry | Valkey-authoritative | Unchanged: the proxy holds no data | Unchanged |
| Durability (RDB/AOF, `WAIT`, `WAITAOF`) | As configured | `WAIT` and `WAITAOF` refused, otherwise unchanged | `WAIT` needs connection affinity |
| Outcome of in-flight requests on failure | Client sees a connection error and knows the outcome is unknown | Proxy closes the client connection, so the outcome is unknown and the client sees an error. Never retried | Retrying would duplicate non-idempotent writes such as `INCR` and `LPUSH`. It must not retry without knowing a command is idempotent |
| Inline protocol (`PING\r\n`) | Supported | Rejected (connection closed) | Supported |

Because of these losses, the proxy is "Valkey-compatible" only for an application that uses single-key,
non-blocking, single-database commands with a single ACL identity. **No compatibility claim is made.** §6 has the
measured conformance result.

## 4. Correctness properties a real implementation would have to guarantee and test

| Property | Required behavior | Test approach |
|---|---|---|
| Reply attribution | Every reply reaches exactly the client and position that issued the command | Property tests: random interleavings of many clients, requests split at random byte boundaries (proptest); fuzz the request and reply parsers (cargo-fuzz); differential test against direct Valkey |
| Reply count per command | Forward only commands with exactly one reply | Allowlist (not denylist) derived from `COMMAND DOCS` / `commands.def`, plus unit tests per command family |
| Ordering | Per-client FIFO | Sequence-numbered `SET`/`GET` chains per client under load |
| Atomicity | Transactions either unsupported (refused) or isolated on a dedicated connection | `MULTI` with `WATCH` contention tests |
| Failure handling | In-flight requests fail and are never retried; requests queued but not yet sent may wait for reconnection, within a bounded queue and timeout | Kill or `SIGSTOP` Valkey under load; network partition with iptables; check that `INCR` counters equal the acknowledged increments |
| Overload | Bounded memory; reject or shed with an error instead of OOM | Slow-reader clients, large pipelined replies, 10k+ idle connections |
| TTL | No caching in the proxy; TTL reads (`TTL`, `PTTL`) pass through | Valkey's `unit/expire` tests through the proxy |
| AuthN/AuthZ | No client can act with permissions it was not granted | ACL test suite through the proxy |
| Graceful shutdown | Stop accepting, drain in-flight requests, close upstream | `SIGTERM` under load, zero lost acknowledged writes |
| Protocol | Reject malformed input without affecting other clients | Valkey's `unit/protocol` tests through the proxy, plus fuzzing |

The prototype has unit tests for RESP framing only (`cargo test -p resp-common`: complete and partial frames,
offsets, malformed input, nesting limits). It is not fit for production. That is by design, because no
implementation was authorized.

## 5. Supply chain and operations

* **Rust dependency surface (prototype):** 13 third-party crates (tokio, mio, bytes, libc, socket2, errno,
  signal-hook-registry, pin-project-lite, tokio-macros, syn, quote, proc-macro2, unicode-ident). All are widely
  used and maintained. Versions are pinned in `Cargo.lock` with checksums. A production component would also need
  `cargo audit` or `cargo deny` in CI, a policy on unsafe code in dependencies, and a patch process that tracks
  RustSec.
* **New things to operate:** a process to deploy, monitor, scale, patch and page on. Its metrics need to be
  correlated with Valkey's, since Valkey's per-client metrics now describe the proxy.
* **Secrets:** the proxy would hold the Valkey credential, and client credentials if it authenticates clients.
  That is a new secret-distribution path.
* **TLS:** use `rustls` (an established library), never custom cryptography. TLS on both hops roughly doubles TLS
  CPU cost compared with TLS terminated in Valkey.

## 6. Measured behavior

Measured on the same host as the benchmarks. Raw data is in `results/raw/recovery.jsonl` and
`results/conformance/`.

### 6.1 Conformance: Valkey's own test suite through the proxy

`bench/conformance.sh` runs eight files of Valkey's Tcl test suite in external-server mode (`--singledb`), first
directly against Valkey, then through `proxy-poc`. In this suite an exception stops the rest of a file, so tests
after the first refused command never run.

| Test file | Passed direct | Passed through proxy | What stopped the proxy run |
|---|---|---|---|
| `unit/type/string` | 125 | 21 | A test that attaches as a replica (`SYNC` refused) hung until timeout |
| `unit/type/incr` | 82 | 27 | `PSUBSCRIBE` (keyspace-notification test) refused |
| `unit/type/hash` | 86 | 4 | `HELLO 3` (RESP3) refused |
| `unit/type/list` | 284 | 50 | `HELLO` (RESP3) refused |
| `unit/expire` | 79 | 39 | Replication-stream test hung (`SYNC` refused) |
| `unit/multi` | 68 | 0 | `MULTI` refused |
| `unit/pubsub` | 38 | 0 | `SUBSCRIBE` refused |
| `unit/protocol` | 37 | 0 | Connection closed on malformed or inline input |
| **Total** | **799** | **141 (17.6%)** | |

Every proxy failure was a refusal, a closed connection or a hang. No test that ran got a wrong answer (`[err]`
count 0), so the prototype fails closed. The suite also uses connection-scoped commands for its own plumbing, so a
real application would hit fewer of these. The result still rules out any claim of compatibility: whole command
families (transactions, Pub/Sub, RESP3, replication) do not work through a multiplexer without a substantially
larger design.

### 6.2 Failure of Valkey under load (`recovery.py failover`)

An open-loop load of 20,000 ops/s ran over 50 connections. Valkey was killed with `SIGKILL` after 4 s and
restarted at once **without persistence**, so it came back empty. Three trials per path:

| Path | Ops completed (of 200,000 offered) | Client connections that saw the failure | GET hit rate over the whole run | Errors / timeouts |
|---|---|---|---|---|
| Direct | 79,404 / 79,398 / 79,395 | 50 / 50 / 50 | 1.000 (no requests after the failure) | 0 / 0 |
| Through proxy | 79,386 / 120,397 / 91,460 | 50 / **34** / **45** | 1.000 / **0.675** / **0.873** | 0 / 0 |

**Finding: the proxy hides a backend restart from some clients.** Clients with no request in flight when Valkey
died kept their proxy connection. Their next requests waited in the proxy's queue until it reconnected (about
5 ms after Valkey was ready), then went to an empty Valkey without any error. With a direct connection, every
client sees the disconnect and can react: re-warm, re-acquire locks, or treat counters as reset. Behind the
proxy, in two of three trials, 5 or 16 of the 50 clients silently started reading an empty dataset. For a pure
cache this only costs misses. For counters, rate limiters, locks or session data it is silent data loss.
Valkey's `run_id` changes on restart; a production proxy would have to watch it and disconnect every client on
change, adding still more code.

### 6.3 Recovery timing

| Measurement | Value (median of 3) |
|---|---|
| `proxy-poc` cold start to first PONG through it | 29.9 ms |
| Proxy serving again after a restarted Valkey is ready | 4.6 ms (reconnect backoff starts at 50 ms, but the first retry succeeded) |
| Valkey restart, RDB load, 1M keys (31 MB RDB) | 1.60 [1.57–1.79] s |
| Valkey restart, RDB load, 5M keys (155 MB RDB) | 8.90 [8.76–9.18] s |
| Valkey restart, AOF load (no rewrite), 1M keys | 2.37 [2.33–2.75] s |

The proxy adds little time to recovery itself. Its effect on recovery is the semantic one in §6.2.

### 6.4 Correctness checks during the throughput runs

* Across 198 load-generator runs, every GET reply was a bulk string or nil, so no reply was visibly misattributed.
  This check is weak: every key holds the same value, so a reply swapped between two GETs would go unnoticed.
  §4's differential and property tests would be needed for a real claim.
* Zero RESP errors, zero timeouts, zero lost connections in every throughput run, direct or through the proxy.

---

# Part II: threat model for the reassessment candidates

Part I covered the proxy (Candidate B). This part covers `rvk-server` (Candidate A) and the client-side options
measured as Candidate C. FEATURE_MATRIX.md lists what `rvk-server` implements. REASSESSMENT.md §7 covers the
multi-agent review, which is the main source of the security evidence below.

## 7. Candidate A: `rvk-server`

### 7.1 Boundaries

`rvk-server` would replace Valkey at the same boundary. There is no extra hop and no new secret holder, which
avoids the proxy's main structural risks (Part I §1–2). The trade is that every byte from every client is now
parsed and executed by about 4,100 lines of new code instead of Valkey's long-tested parser and command
implementations.

### 7.2 STRIDE

| Threat | `rvk-server` (round 2 build) | Compared with Valkey |
|---|---|---|
| **Spoofing** | `requirepass` for the `default` user only, compared in constant time. No ACL users, no TLS, so credentials and data cross the network in clear text | Weaker: Valkey has ACL users, TLS and client certificates |
| **Tampering** | Multi-key commands and transactions are atomic across threads (concurrency invariants). No TLS | Same guarantees; no transport protection |
| **Repudiation** | No `ACL LOG`, `COMMANDLOG`, `MONITOR` or audit trail | Weaker |
| **Information disclosure** | Every authenticated client can read every key (no key-pattern ACLs). `KEYS`, `INFO` and `CLIENT LIST` behave like Valkey's for the `default` user | Weaker: no per-user permissions |
| **Denial of service** | See §7.3. Every panic is an abort of the whole process (`panic = "abort"`), and with no persistence the whole dataset is lost | Valkey also exits on assertion failures, but restarts with its RDB/AOF data |
| **Elevation of privilege** | No scripting, modules, `CONFIG SET` of paths, or `DEBUG` unless enabled, so the classic Valkey escalation routes (Lua sandbox escapes, `CONFIG SET dir`, `MODULE LOAD`) do not exist | Smaller surface, because there are fewer features |

### 7.3 Denial of service: what the review found and what remains

The review confirmed several ways for one client to stop or exhaust the server. They are fixed and covered by
regression scenarios or unit tests (FEATURE_MATRIX.md §11):

* two self-deadlocks inside `EXEC` (any client, four commands, for example `MULTI`, `SET`, `INFO`, `EXEC`);
* a process abort through `DEBUG SLEEP inf`;
* unbounded memory: transactions bypassing `maxmemory`, unbounded transaction queues, no caps for
  never-authenticated clients, and a read buffer that could grow to twice the configured limit;
* quadratic CPU: re-parsing a partly received request on every read, and an O(n²) WATCH duplicate check under
  every shard lock;
* leaked connections: a FIN arriving with data never seen, and no TCP keepalive.

Remaining known exposure:

| Exposure | Effect | Mitigation if this were a product |
|---|---|---|
| Whole-value copies in APPEND/SETRANGE | Repeated small APPENDs to one large value cost quadratic CPU under that shard's lock | Grow values in place |
| `KEYS` and `FLUSHALL` hold all 64 shard locks | One `KEYS *` on a large dataset stalls every thread, where Valkey stalls its one thread | Same advice as for Valkey (use `SCAN`, which rvk lacks); or iterate shard by shard |
| Lock-held work generally | A slow command on one key stalls every thread that needs that shard | Bound per-command work, as Valkey's single thread implicitly does by being single |
| Any panic left in the code | Aborts the process and loses all data | Fuzzing of the parser and commands; a supervisor; persistence |
| 100 ms memory snapshot | New reply buffers can push memory past `maxmemory` for up to one tick | Count buffer allocations, as Valkey does |

### 7.4 What Rust prevented and what it did not

The review found 52 confirmed correctness and security findings (fewer distinct defects, because reviewers
overlapped) in the round 1 build, about 3,500 lines, and **no memory-safety defect**. `rvk-server`'s own
code has one `unsafe` block (the `getrlimit`/`setrlimit` call). Rust's guarantees ruled out buffer overflows,
use-after-free and data races. The defects that were found are the classes Rust does not prevent:

| Class | Count among the most severe findings | Example |
|---|---|---|
| Deadlock | 2 | Re-locking a non-reentrant mutex inside `EXEC` |
| Panic turned into a crash | 1 | `Duration::from_secs_f64(inf)` |
| Unbounded resource use | 5 | Transaction queues, unauthenticated buffers, read-buffer growth |
| Algorithmic complexity | 3 | Quadratic re-parsing, O(n²) WATCH, whole-value copies |
| Semantic divergence from Valkey | many (low severity) | WATCH expiry rules, EXPIRETIME rounding, error ordering |

So the security argument for a Rust rewrite ("memory safety") is real but narrow. It removes the bug class behind
many historical Redis CVEs: integer overflows that became heap corruption in protocol and data-type code. It does
nothing for availability, which is what a new implementation got wrong here. A product would need the hardening
Valkey has accumulated over many years: fuzzing, fault injection and a long tail of resource limits.

### 7.5 Supply chain

Nine third-party runtime crates in `rvk-server` (plus three build-time crates, `cc`, `shlex` and `find-msvc-tools`,
used to compile jemalloc): `mio`, `socket2`, `hashbrown`, `libc`, `log`, `paste`, and the
`tikv-jemallocator`, `tikv-jemalloc-ctl` and `tikv-jemalloc-sys` crates (the last bundles jemalloc's C code, the
allocator Valkey also uses). The versions are pinned in `poc/Cargo.lock`. The `rsclient` measurement tool pulls in
about 80 crates through the `redis` crate and tokio. That is the dependency surface an application takes on when
adopting Candidate C.

## 8. Candidate C: client-side options

### 8.1 Multiplexing (redis-rs `MultiplexedConnection`, GLIDE)

* **Head-of-line blocking:** all logical clients on a socket share one reply stream, so one slow command (a large
  `MGET`, a blocking command) delays every request queued behind it.
* **Shared fate:** a dropped connection fails every in-flight request of every logical client on it. The client
  library must surface that as an error, never retry non-idempotent writes, and must not hide a server restart
  the way the Part I proxy did (Part I §6.2).
* **Connection-scoped state:** `MULTI`/`WATCH`, `SELECT`, blocking commands and Pub/Sub need a dedicated
  connection. Multiplexing libraries handle this by checking out a connection, which the application must use.
* These are the same semantic risks as the proxy (Part I §3), but they stay inside the application's own process
  and failure domain, with no extra network hop or secret.

### 8.2 Client-side caching (redis-rs 1.7.1 `cache-aio`, experimental)

* **Staleness, measured:** the cache served stale values on about 34% of GETs on a skewed read-heavy workload,
  with a stale age of up to seconds (BENCHMARK_RESULTS.md Part II). Most of that comes from a race in the
  library. A GET reply is inserted after an invalidation for the same key has already been processed, and the
  server has by then stopped tracking the key for this client, so no further invalidation will come
  (REASSESSMENT.md §6.3). Such an entry stays stale until LRU eviction or the 30-minute default TTL.
* **Consequence:** this is a correctness failure for any data where a stale read matters (sessions, permissions,
  counters, feature flags). An application could adopt this feature only for data that tolerates unbounded
  staleness, and then a plain TTL cache gives the same guarantee more simply.
* **Data exposure:** cached values live in application memory for up to the client TTL, outside the server's
  access control and `FLUSHALL`.
* **Not measured:** GLIDE's client-side cache, which is a separate implementation. Whether it has the same race
  is unknown.
