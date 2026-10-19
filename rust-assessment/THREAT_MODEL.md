# Threat model and correctness assessment

Scope: the Rust component that was benchmarked, a multiplexing, auto-pipelining RESP proxy in front of Valkey
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
| **Denial of service:** memory | Requests are buffered whole before forwarding, and the in-flight cap counts commands, not bytes. A client can pipeline 1024 × `GET` of large values; the replies sit in an unbounded per-client channel. Valkey's protections (`client-output-buffer-limit`, `maxmemory-clients`, `client-query-buffer-limit`) see only the proxy's one connection | High | Byte-based limits per client and globally, disconnecting slow readers, an equivalent of `client-output-buffer-limit` | Command-count limit only |
| **Denial of service:** head-of-line blocking | A slow command (large `LRANGE`, `KEYS`, long Lua) or a large reply on a shared upstream connection delays every client multiplexed onto it. Valkey's single main thread already serializes command execution, but a proxy adds a second queue that also serializes reply transfer | Medium | More upstream connections (less batching) or per-command routing | Measured: large-value workload, see results |
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

*(Filled in from the measurement runs; see below.)*
