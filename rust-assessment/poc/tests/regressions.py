#!/usr/bin/env python3
"""Regression scenarios for defects found by the multi-agent review of rvk-server.

Usage: regressions.py [--out FILE] [--rvk PATH] [--valkey PATH]

Each scenario starts a fresh Valkey and a fresh rvk-server with the same
options, runs the same steps against both, and compares what a client
observes. A scenario passes when rvk-server behaves like Valkey (or, where
noted, meets the stated requirement). Exit status 0 if all pass. Stdlib only.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
VALKEY = os.path.join(REPO, "src", "valkey-server")
RVK = os.path.join(HERE, "..", "target", "release", "rvk-server")
VPORT, RPORT = 6480, 6481


class Err(str):
    pass


class Conn:
    def __init__(self, port, timeout=5):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.buf = b""

    def close(self):
        self.s.close()

    def send(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.s.sendall(b"".join(out))

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError("connection closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _n(self, n):
        while len(self.buf) < n:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError("connection closed")
            self.buf += d
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Err(rest.decode(errors="replace"))
        if t in (b":",):
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._n(n + 2)[:-2]
        if t in (b"*", b"~", b">"):
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"%":
            return {"map": [[self.read(), self.read()] for _ in range(int(rest))]}
        if t == b"_":
            return "RESP3-null"
        if t in (b"=",):
            return self._n(int(rest) + 2)[:-2]
        raise RuntimeError("bad reply %r" % line)

    def cmd(self, *args):
        self.send(*args)
        return self.read()


def start(kind, port, args, wd):
    if kind == "valkey":
        cmd = [VALKEY, "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
               "--dir", wd, "--logfile", os.path.join(wd, "v.log"), *args]
    else:
        cmd = [RVK, "--port", str(port), "--threads", "2", *args]
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.time()
    while time.time() - t0 < 10:
        try:
            Conn(port).close()
            return p
        except OSError:
            time.sleep(0.02)
    raise RuntimeError(f"{kind} did not start")


def stop(p, port):
    try:
        c = Conn(port, timeout=2)
        c.send("shutdown", "nosave")
        c.close()
    except OSError:
        pass
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def alive(port):
    try:
        c = Conn(port, timeout=2)
        r = c.cmd("ping")
        c.close()
        return r == "PONG"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# scenarios: each returns a JSON-able observation; rvk must equal Valkey's
# ---------------------------------------------------------------------------

def s_info_in_exec(port):
    c = Conn(port, timeout=3)
    c.cmd("multi")
    c.cmd("set", "foo", "bar")
    c.cmd("info", "keyspace")
    r = c.cmd("exec")
    return {"exec_len": len(r), "set": r[0], "info_is_text": isinstance(r[1], bytes), "alive": alive(port)}


def s_unwatch_in_exec(port):
    c = Conn(port, timeout=3)
    c.cmd("set", "k", "1")
    c.cmd("watch", "k")
    c.cmd("multi")
    c.cmd("unwatch")
    return {"exec": c.cmd("exec"), "alive": alive(port)}


def s_multi_oom_noeviction(port):
    c = Conn(port)
    c.cmd("config", "set", "maxmemory", "1")
    time.sleep(0.3)  # rvk refreshes its allocator snapshot every 100 ms
    plain = c.cmd("set", "x", "y")
    c.cmd("multi")
    queued = c.cmd("set", "big", "v" * 1000)
    ex = c.cmd("exec")
    c.cmd("config", "set", "maxmemory", "0")
    return {"plain": str(plain)[:3], "queued": str(queued)[:9], "exec": str(ex)[:9]}


def s_debug_sleep_inf(port):
    c = Conn(port)
    r = c.cmd("debug", "sleep", "inf")
    return {"ok_or_error": isinstance(r, (str, Err)), "alive": alive(port)}


def s_expiretime_rounding(port):
    c = Conn(port)
    c.cmd("set", "k", "v")
    c.cmd("pexpireat", "k", "4102444800600")
    return {"expiretime": c.cmd("expiretime", "k"), "pexpiretime": c.cmd("pexpiretime", "k")}


def s_watch_lazy_expired(port):
    # multi.tcl "EXEC fail on lazy expired WATCHed key"
    c = Conn(port)
    c.cmd("debug", "set-active-expire", "0")
    c.cmd("del", "key")
    c.cmd("set", "key", "1", "px", "100")
    c.cmd("watch", "key")
    time.sleep(0.15)
    c.cmd("multi")
    c.cmd("incr", "key")
    r = c.cmd("exec")
    c.cmd("debug", "set-active-expire", "1")
    return {"exec": r}


def s_watch_stale_keys(port):
    # multi.tcl "WATCH stale keys should not fail EXEC", "Delete WATCHed stale
    # keys should not fail EXEC", "FLUSHDB while watching stale keys..."
    out = {}
    c = Conn(port)
    c.cmd("debug", "set-active-expire", "0")
    for name, action in (("watch", None), ("delete", ("exists", "x")), ("flushdb", ("flushdb",))):
        c.cmd("del", "x")
        c.cmd("set", "x", "foo", "px", "1")
        time.sleep(0.005)
        c.cmd("watch", "x")
        if action:
            c.cmd(*action)
        c.cmd("multi")
        c.cmd("ping")
        out[name] = c.cmd("exec")
    c.cmd("debug", "set-active-expire", "1")
    return out


def s_flush_nonexisting_watched(port):
    # multi.tcl "FLUSHALL does not touch non affected keys"
    c = Conn(port)
    c.cmd("del", "x")
    c.cmd("watch", "x")
    c.cmd("flushall")
    c.cmd("multi")
    c.cmd("ping")
    return {"exec": c.cmd("exec")}


def s_half_close(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    s.sendall(b"PING\r\n")
    s.shutdown(socket.SHUT_WR)
    data = b""
    try:
        while True:
            d = s.recv(4096)
            if not d:
                break
            data += d
    except socket.timeout:
        return {"reply": data.decode(), "closed": False}
    return {"reply": data.decode(), "closed": True}


def s_exact_fill_then_close(port):
    # 16384 bytes of commands followed at once by FIN: all must execute.
    cmd = b"*3\r\n$3\r\nSET\r\n$7\r\nkfill%02d\r\n$3\r\nabc\r\n"
    one = cmd % 0
    n = 16384 // len(one)
    payload = b"".join(cmd % (i % 100) for i in range(n))
    payload += b"*1\r\n$4\r\nPING\r\n" * 0
    pad = 16384 - len(payload)
    payload += b"\r\n" * (pad // 2)  # blank inline lines are skipped
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    try:
        while s.recv(65536):
            pass
    except socket.timeout:
        pass
    s.close()
    time.sleep(0.1)
    c = Conn(port)
    return {"len": len(payload), "exists": c.cmd("exists", *[f"kfill{i:02d}" for i in range(min(n, 100))])}


def s_shutdown_args(port):
    c = Conn(port)
    out = {"abort": str(c.cmd("shutdown", "abort")), "typo": str(c.cmd("shutdown", "nosaev"))}
    c.cmd("multi")
    out["in_multi"] = str(c.cmd("shutdown"))
    out["exec"] = str(c.cmd("exec"))[:9]
    out["alive"] = alive(port)
    return out


def s_noauth_order(port):
    c = Conn(port)
    return {"unknown": str(c.cmd("foobar")), "arity": str(c.cmd("get")), "known": str(c.cmd("get", "k"))}


def s_subcommand_in_multi(port):
    c = Conn(port)
    c.cmd("del", "k")
    c.cmd("multi")
    bad = str(c.cmd("config", "gett", "x"))
    c.cmd("set", "k", "v")
    ex = str(c.cmd("exec"))[:9]
    return {"bad": bad, "exec": ex, "k": c.cmd("get", "k"), "arity": str(c.cmd("config", "get"))}


def s_config_set_atomic(port):
    c = Conn(port)
    c.cmd("config", "set", "maxmemory", "0")
    r = c.cmd("config", "set", "maxmemory", "100mb", "maxmemory-policy", "bogus")
    after = c.cmd("config", "get", "maxmemory")
    units = str(c.cmd("config", "set", "maxmemory", "100m"))
    got = c.cmd("config", "get", "maxmemory")
    c.cmd("config", "set", "maxmemory", "0")
    return {"error": isinstance(r, Err), "unchanged": after, "units": units, "100m": got}


def s_hello_auth_order(port):
    c = Conn(port)
    r = str(c.cmd("hello", "3", "auth", "default", "pw", "bogus"))
    return {"hello": r[:30], "still_noauth": str(c.cmd("get", "k"))[:6]}


def s_hello_setname(port):
    c = Conn(port)
    c.cmd("auth", "pw")
    return {"bad_name": str(c.cmd("hello", "2", "setname", "a b"))}


def s_hello_in_multi(port):
    c = Conn(port)
    c.cmd("del", "missing")
    c.cmd("multi")
    c.cmd("hello", "3")
    c.cmd("get", "missing")
    r = c.cmd("exec")
    return {"get_reply": r[1]}


def s_keys_unterminated_class(port):
    c = Conn(port)
    c.cmd("flushall")
    c.cmd("set", "a", "1")
    return {"keys": sorted(c.cmd("keys", "[abc"))}


def s_protocol_error_text(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    s.sendall(b"*1\r\n\n\r\n")
    data = b""
    try:
        while True:
            d = s.recv(4096)
            if not d:
                break
            data += d
    except socket.timeout:
        pass
    return {"reply": data.decode(errors="replace")}


def s_randomkey_mostly_expired(port):
    c = Conn(port)
    c.cmd("flushall")
    c.cmd("debug", "set-active-expire", "0")
    for i in range(0, 2000, 500):
        c.send(*(["mset"] + sum([[f"e{j}", "x"] for j in range(i, i + 500)], [])))
        c.read()
    for i in range(2000):
        c.send("pexpire", f"e{i}", "1")
    for _ in range(2000):
        c.read()
    c.cmd("set", "live", "1")
    time.sleep(0.01)
    got = [c.cmd("randomkey") for _ in range(5)]
    c.cmd("debug", "set-active-expire", "1")
    return {"randomkey": got}


def s_backpressure(port):
    # 200 pipelined GETs of a 1 MB value written before reading any reply:
    # every reply must eventually arrive and the connection stay open.
    c = Conn(port, timeout=20)
    c.cmd("set", "big", b"v" * (1 << 20))
    req = b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n" * 200
    c.s.sendall(req)
    time.sleep(0.5)
    n = 0
    for _ in range(200):
        r = c.read()
        n += r is not None and len(r) == (1 << 20)
    return {"replies": n, "alive": alive(port)}


def s_multi_queue_limit(port):
    # client-query-buffer-limit 1mb: a transaction queueing more than that
    # is disconnected (Valkey counts queued arguments as query buffer).
    c = Conn(port, timeout=5)
    c.cmd("multi")
    closed = False
    try:
        for _ in range(4):
            c.cmd("set", "k", b"x" * 400_000)
        c.cmd("exec")
    except (EOFError, ConnectionError, socket.timeout):
        closed = True
    return {"closed": closed, "alive": alive(port)}


SCENARIOS = [
    (s_info_in_exec, []),
    (s_unwatch_in_exec, []),
    (s_multi_oom_noeviction, ["--maxmemory-policy", "noeviction"]),
    (s_debug_sleep_inf, ["--enable-debug-command", "yes"]),
    (s_expiretime_rounding, []),
    (s_watch_lazy_expired, ["--enable-debug-command", "yes"]),
    (s_watch_stale_keys, ["--enable-debug-command", "yes"]),
    (s_flush_nonexisting_watched, []),
    (s_half_close, []),
    (s_exact_fill_then_close, []),
    (s_shutdown_args, []),
    (s_noauth_order, ["--requirepass", "pw"]),
    (s_subcommand_in_multi, []),
    (s_config_set_atomic, []),
    (s_hello_auth_order, ["--requirepass", "pw"]),
    (s_hello_setname, ["--requirepass", "pw"]),
    (s_hello_in_multi, []),
    (s_keys_unterminated_class, []),
    (s_protocol_error_text, []),
    (s_randomkey_mostly_expired, ["--enable-debug-command", "yes"]),
    (s_backpressure, []),
    (s_multi_queue_limit, ["--client-query-buffer-limit", "1mb"]),
]

# Where rvk legitimately differs from Valkey, the requirement checked instead.
EXPECT = {
    # rvk answers DEBUG SLEEP inf with OK at once; Valkey's behavior is
    # undefined (inf cast to an integer). The requirement is "no crash".
    "debug_sleep_inf": lambda v, r: r["alive"],
    # Any live key is a correct RANDOMKEY answer; nil is not.
    "randomkey_mostly_expired": lambda v, r: all(k == b"live" for k in r["randomkey"]),
}


def run(fn, args, kind, port):
    wd = tempfile.mkdtemp(prefix="rvkreg-")
    try:
        p = start(kind, port, args, wd)
    except RuntimeError as e:
        return {"exception": repr(e)}
    try:
        return fn(port)
    except Exception as e:
        return {"exception": repr(e)}
    finally:
        stop(p, port)


def main():
    global RVK, VALKEY
    out = None
    a = sys.argv[1:]
    for i in range(0, len(a), 2):
        if a[i] == "--out":
            out = a[i + 1]
        elif a[i] == "--rvk":
            RVK = a[i + 1]
        elif a[i] == "--valkey":
            VALKEY = a[i + 1]
    results = []
    for fn, args in SCENARIOS:
        name = fn.__name__[2:]
        v = run(fn, args, "valkey", VPORT)
        r = run(fn, args, "rvk", RPORT)
        check = EXPECT.get(name)
        ok = "exception" not in r and (check(v, r) if check else (v == r))
        results.append({"scenario": name, "ok": ok, "valkey": v, "rvk": r})
        print(f"{'PASS' if ok else 'FAIL'} {name:<28} valkey={json.dumps(v, default=repr)[:110]} "
              f"rvk={json.dumps(r, default=repr)[:110]}", flush=True)
    if out:
        with open(out, "w") as f:
            json.dump(results, f, indent=1, default=repr)
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()
