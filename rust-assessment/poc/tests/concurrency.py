#!/usr/bin/env python3
"""Concurrency invariants for a multi-threaded server (stdlib only).

Usage: concurrency.py PORT [--label NAME] [--out FILE] [--scale N]

Each test runs many client connections in parallel threads and checks an
invariant that single-threaded Valkey guarantees by construction and that a
lock-sharded multi-threaded server must guarantee by locking:

  incr_atomic     concurrent INCRs lose no updates
  mset_atomic     MGET never observes a half-applied MSET across shards
  exec_isolation  MGET never observes a half-applied MULTI/EXEC
  watch_cas       WATCH/MULTI/EXEC compare-and-set loses no increments
  expiry_race     GET/SET with 1 ms TTLs and active expiry: no protocol errors,
                  replies are always a full value or nil
  memory_bound    writes under maxmemory + allkeys-lru keep used_memory bounded
                  and are never refused with OOM, for 1000- and 100-byte values
                  (skipped unless the server reports maxmemory > 0)
Exit status 0 if every invariant held.
"""

import json
import socket
import sys
import threading
import time


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.f = self.s.makefile("rb")

    def send(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.s.sendall(b"".join(out))

    def read(self):
        line = self.f.readline()[:-2]
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            return d[:-2]
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise RuntimeError("bad reply %r" % line)

    def cmd(self, *args):
        self.send(*args)
        return self.read()


def run_threads(n, fn):
    errors = []

    def wrap(i):
        try:
            fn(i)
        except Exception as e:  # recorded as a failure, not swallowed
            errors.append(repr(e))

    ts = [threading.Thread(target=wrap, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return errors


def t_incr_atomic(port, scale):
    c = Conn(port)
    c.cmd("del", "ia0", "ia1", "ia2", "ia3")
    clients, per = 16, 1000 * scale

    def work(i):
        k = Conn(port)
        for j in range(per):
            k.send("incr", "ia%d" % ((i + j) % 4))
        for _ in range(per):
            k.read()

    errs = run_threads(clients, work)
    total = sum(int(c.cmd("get", "ia%d" % i) or 0) for i in range(4))
    return {"ok": not errs and total == clients * per, "expected": clients * per, "observed": total, "errors": errs[:3]}


def t_mset_atomic(port, scale):
    keys = ["ma%d" % i for i in range(16)]  # spans many shards
    c = Conn(port)
    c.cmd("mset", *sum([[k, "0"] for k in keys], []))
    stop = threading.Event()
    torn = []

    def writer(i):
        w = Conn(port)
        for j in range(500 * scale):
            v = "%d:%d" % (i, j)
            w.cmd("mset", *sum([[k, v] for k in keys], []))
        stop.set()

    def reader(i):
        r = Conn(port)
        n = 0
        while not stop.is_set() or n < 100:
            vals = r.cmd("mget", *keys)
            if len(set(vals)) != 1:
                torn.append([v.decode() for v in vals])
            n += 1

    errs = run_threads(8, lambda i: writer(i) if i < 4 else reader(i))
    return {"ok": not errs and not torn, "torn_reads": len(torn), "example": torn[:1], "errors": errs[:3]}


def t_exec_isolation(port, scale):
    c = Conn(port)
    c.cmd("mset", "ex_a", "0", "ex_b", "0")
    stop = threading.Event()
    torn = []

    def writer(i):
        w = Conn(port)
        for _ in range(500 * scale):
            w.send("multi")
            w.send("incr", "ex_a")
            w.send("incr", "ex_b")
            w.send("exec")
            for _ in range(4):
                w.read()
        stop.set()

    def reader(i):
        r = Conn(port)
        n = 0
        while not stop.is_set() or n < 100:
            a, b = r.cmd("mget", "ex_a", "ex_b")
            if a != b:
                torn.append([a.decode(), b.decode()])
            n += 1

    errs = run_threads(8, lambda i: writer(i) if i < 4 else reader(i))
    a, b = c.cmd("mget", "ex_a", "ex_b")
    final_ok = int(a) == int(b) == 4 * 500 * scale
    return {"ok": not errs and not torn and final_ok, "torn_reads": len(torn), "final": [int(a), int(b)], "errors": errs[:3]}


def t_watch_cas(port, scale):
    c = Conn(port)
    c.cmd("set", "cas", "0")
    per = 200 * scale
    retries = [0]
    lock = threading.Lock()

    def work(i):
        k = Conn(port)
        done = 0
        while done < per:
            k.cmd("watch", "cas")
            v = int(k.cmd("get", "cas"))
            k.cmd("multi")
            k.cmd("set", "cas", str(v + 1))
            if k.cmd("exec") is None:
                with lock:
                    retries[0] += 1
            else:
                done += 1

    errs = run_threads(8, work)
    final = int(c.cmd("get", "cas"))
    return {"ok": not errs and final == 8 * per, "expected": 8 * per, "observed": final, "retries": retries[0], "errors": errs[:3]}


def t_expiry_race(port, scale):
    bad = []
    deadline = time.time() + 3 * scale

    def work(i):
        k = Conn(port)
        val = b"v" * (10 + i)
        while time.time() < deadline:
            key = "er%d" % (i % 4)
            k.cmd("set", key, val, "px", "1")
            r = k.cmd("get", key)
            if r is not None and r not in [b"v" * (10 + j) for j in range(8)]:
                bad.append(r)

    errs = run_threads(8, work)
    return {"ok": not errs and not bad, "bad_values": len(bad), "errors": errs[:3]}


def info_field(c, field):
    for line in c.cmd("info", "memory").decode().splitlines():
        if line.startswith(field + ":"):
            return int(line.split(":")[1])
    return 0


def t_memory_bound(port, scale):
    # Large values reach the limit with few keys; small values make many more
    # concurrent evictions per second, which exposed a check-then-act race.
    results = [memory_bound_once(port, scale, size) for size in (1000, 100)]
    out = {"ok": all(r["ok"] for r in results)}
    for r in results:
        out["v%d" % r["value_size"]] = {k: v for k, v in r.items() if k not in ("ok", "value_size")}
    return out


def memory_bound_once(port, scale, size):
    c = Conn(port)
    maxmem = info_field(c, "maxmemory")
    if maxmem == 0:
        return {"ok": True, "value_size": size, "skipped": "server has no maxmemory"}
    c.cmd("flushall")
    c.cmd("config", "resetstat")
    val = b"x" * size
    peak = [0]
    # Write about twice maxmemory (counting ~100 bytes of per-key overhead) so eviction must run.
    per = max(2000, int(2 * maxmem / (size + 100) / 8)) * scale

    def work(i):
        k = Conn(port)
        for j in range(per):
            k.send("set", "mb:%d:%d" % (i, j), val)
            if j % 64 == 63:
                for _ in range(64):
                    k.read()
        for _ in range(per % 64):
            k.read()

    def monitor(_):
        m = Conn(port)
        end = time.time() + 60
        while time.time() < end and any(t.is_alive() for t in workers):
            peak[0] = max(peak[0], info_field(m, "used_memory"))
            time.sleep(0.05)

    errors = []

    def guarded(i):
        try:
            work(i)
        except Exception as e:  # an OOM reply under allkeys-lru is a failure
            errors.append(repr(e))

    workers = [threading.Thread(target=guarded, args=(i,)) for i in range(8)]
    for w in workers:
        w.start()
    mon = threading.Thread(target=monitor, args=(0,))
    mon.start()
    for w in workers:
        w.join()
    mon.join()
    final = info_field(c, "used_memory")
    # Valkey allows transient overshoot while a command executes; allow 25%.
    evicted = 0
    for line in c.cmd("info", "stats").decode().splitlines():
        if line.startswith("evicted_keys:"):
            evicted = int(line.split(":")[1])
    ok = not errors and peak[0] <= maxmem * 1.25 and final <= maxmem * 1.10 and evicted > 0
    return {"ok": ok, "value_size": size, "maxmemory": maxmem, "peak_used": peak[0], "final_used": final,
            "evicted_keys": evicted, "errors": errors[:3], "error_count": len(errors)}


TESTS = [t_incr_atomic, t_mset_atomic, t_exec_isolation, t_watch_cas, t_expiry_race, t_memory_bound]


def main():
    port = int(sys.argv[1])
    label, out, scale = str(port), None, 1
    rest = sys.argv[2:]
    for i in range(0, len(rest), 2):
        if rest[i] == "--label":
            label = rest[i + 1]
        elif rest[i] == "--out":
            out = rest[i + 1]
        elif rest[i] == "--scale":
            scale = int(rest[i + 1])
    results = {}
    for t in TESTS:
        t0 = time.time()
        try:
            r = t(port, scale)
        except Exception as e:
            r = {"ok": False, "exception": repr(e)}
        r["seconds"] = round(time.time() - t0, 2)
        results[t.__name__[2:]] = r
        print(f"{label:<14} {t.__name__[2:]:<16} {'PASS' if r['ok'] else 'FAIL'}  {json.dumps({k: v for k, v in r.items() if k != 'ok'})[:160]}")
    if out:
        with open(out, "a") as f:
            f.write(json.dumps({"label": label, "results": results}) + "\n")
    sys.exit(0 if all(r["ok"] for r in results.values()) else 1)


if __name__ == "__main__":
    main()
