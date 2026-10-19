#!/usr/bin/env python3
"""Differential test: identical command streams against Valkey and rvk-server.

Usage: differential.py VALKEY_PORT RVK_PORT [--seed N] [--fuzz N] [--out FILE]

Both servers must be running on 127.0.0.1 and may be flushed by this script.
Replies are compared exactly (including error texts) after these documented
normalizations only:
  * TTL/PTTL/EXPIRETIME/PEXPIRETIME: equal within a small tolerance, because
    the two servers execute the command at slightly different times;
  * KEYS: compared as sorted lists (order is unspecified in Valkey too);
  * commands whose output is server identity or randomness (HELLO, INFO,
    CLIENT ID/INFO/LIST, TIME, RANDOMKEY, COMMAND, CONFIG GET *) are run but
    only their reply *type* is compared.
Exit status 0 when every in-scope case matches. Stdlib only.
"""

import json
import random
import socket
import sys
import time


class Err(str):
    """A RESP error reply."""


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = b""

    def close(self):
        self.s.close()

    def send_raw(self, data):
        self.s.sendall(data)

    @staticmethod
    def encode(args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            elif isinstance(a, int):
                a = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    def cmd(self, *args):
        self.s.sendall(self.encode(args))
        return self.read()

    def _fill(self):
        chunk = self.s.recv(65536)
        if not chunk:
            raise ConnectionError("closed")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Err(rest.decode())
        if t == b":":
            return int(rest)
        if t in (b"$", b"="):
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data[4:] if t == b"=" else data
        if t in (b"*", b">", b"~"):
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        if t == b"%":
            n = int(rest)
            return {"__map__": [(self.read(), self.read()) for _ in range(n)]}
        if t == b"_":
            return None
        if t == b",":
            return float(rest)
        if t == b"#":
            return rest == b"t"
        raise ValueError("bad reply type %r" % line)

    def read_raw(self, timeout=0.3):
        """Everything the server sends until it goes quiet or closes."""
        self.s.settimeout(timeout)
        data = self.buf
        self.buf = b""
        try:
            while True:
                chunk = self.s.recv(65536)
                if not chunk:
                    data += b"<closed>"
                    break
                data += chunk
        except socket.timeout:
            pass
        self.s.settimeout(5)
        return data


TOL = {"ttl": 1, "pttl": 60, "expiretime": 1, "pexpiretime": 60}
TYPE_ONLY = {"hello", "info", "time", "randomkey", "command"}


def kind(v):
    if v is None:
        return "null"
    if isinstance(v, Err):
        return "error"
    return type(v).__name__


def compare(args, a, b):
    """True if replies a (Valkey) and b (rvk) are equivalent for this command."""
    name = args[0].lower() if isinstance(args[0], str) else args[0].decode().lower()
    sub = (args[1].lower() if len(args) > 1 and isinstance(args[1], str) else "")
    if name in TYPE_ONLY or (name == "client" and sub in ("id", "info", "list")) or (name == "config" and sub == "get"):
        return kind(a) == kind(b)
    if name in TOL and isinstance(a, int) and isinstance(b, int) and a >= 0 and b >= 0:
        return abs(a - b) <= TOL[name]
    if name == "keys" and isinstance(a, list) and isinstance(b, list):
        return sorted(a) == sorted(b)
    return a == b and kind(a) == kind(b)


def show(v):
    if isinstance(v, bytes):
        return v.decode(errors="replace")
    if isinstance(v, Err):
        return "ERR<" + v + ">"
    if isinstance(v, list):
        return [show(x) for x in v]
    if isinstance(v, dict):
        return {"map": [(show(k), show(x)) for k, x in v["__map__"]]}
    return v


# ---------------------------------------------------------------------------
# Handwritten cases: lists of commands run in order on a fresh keyspace.
# ---------------------------------------------------------------------------

CASES = {
    "strings": [
        ["set", "k", "v"], ["get", "k"], ["get", "missing"], ["set", "k"], ["get"], ["get", "a", "b"],
        ["set", "k", "v2", "nx"], ["set", "k", "v3", "xx"], ["get", "k"], ["set", "new", "x", "xx"], ["exists", "new"],
        ["set", "k", "v", "nx", "xx"], ["set", "k", "v", "xx", "nx"], ["set", "k", "v", "foo"], ["set", "k", "v", "ex"],
        ["set", "k", "v", "ex", "10", "px", "100"], ["set", "k", "v", "ex", "0"], ["set", "k", "v", "ex", "-5"],
        ["set", "k", "v", "ex", "abc"], ["set", "k", "v", "px", "9223372036854775807"],
        ["set", "k", "v", "ex", "9223372036854775"], ["set", "k", "v", "keepttl", "ex", "10"],
        ["set", "k", "v", "get"], ["set", "k", "w", "get", "nx"], ["set", "k", "w", "get", "xx"], ["set", "nk", "w", "get", "xx"],
        ["set", "k", "v", "ex", "100"], ["ttl", "k"], ["set", "k", "v2", "keepttl"], ["ttl", "k"], ["set", "k", "v3"], ["ttl", "k"],
        ["set", "k", "v", "exat", "1"], ["exists", "k"], ["set", "k", "v", "pxat", "99999999999999"], ["pttl", "k"],
        ["set", "k", "a", "ifeq", "b"], ["set", "k", "a", "ifeq", "v"], ["get", "k"], ["set", "k", "b", "ifne", "a"],
        ["set", "k", "c", "ifne", "zzz"], ["get", "k"], ["set", "nokey", "c", "ifeq", "x"], ["set", "nokey2", "c", "ifne", "x"],
        ["get", "nokey2"], ["set", "k", "x", "ifeq", "c", "nx"],
        ["setnx", "sn", "1"], ["setnx", "sn", "2"], ["get", "sn"], ["setex", "se", "100", "v"], ["ttl", "se"],
        ["setex", "se", "0", "v"], ["setex", "se", "-1", "v"], ["setex", "se", "x", "v"], ["psetex", "pse", "100000", "v"],
        ["pttl", "pse"], ["psetex", "pse", "0", "v"],
        ["getset", "gs", "1"], ["getset", "gs", "2"], ["get", "gs"],
        ["getdel", "gs"], ["getdel", "gs"], ["exists", "gs"],
        ["set", "ge", "v"], ["getex", "ge"], ["getex", "ge", "ex", "100"], ["ttl", "ge"], ["getex", "ge", "persist"], ["ttl", "ge"],
        ["getex", "ge", "px", "100", "ex", "10"], ["getex", "ge", "persist", "ex", "10"], ["getex", "ge", "foo"],
        ["getex", "nokey", "ex", "10"], ["getex", "ge", "exat", "1"], ["exists", "ge"],
        ["mset", "a", "1", "b", "2"], ["mget", "a", "b", "c"], ["mset", "a"], ["mset", "a", "1", "b"],
        ["msetnx", "x1", "1", "a", "9"], ["mget", "x1", "a"], ["msetnx", "x1", "1", "x2", "2"], ["mget", "x1", "x2"],
        ["append", "ap", "hello"], ["append", "ap", " world"], ["get", "ap"], ["strlen", "ap"], ["strlen", "nokey"],
        ["getrange", "ap", "0", "4"], ["getrange", "ap", "-5", "-1"], ["getrange", "ap", "5", "2"], ["getrange", "ap", "-100", "100"],
        ["getrange", "ap", "0", "-100"], ["getrange", "nokey", "0", "1"], ["getrange", "ap", "x", "1"],
        ["setrange", "sr", "5", "abc"], ["get", "sr"], ["setrange", "sr", "0", "XY"], ["get", "sr"], ["setrange", "sr", "-1", "a"],
        ["setrange", "sr", "1", ""], ["setrange", "nokey3", "0", ""], ["exists", "nokey3"],
        ["setrange", "sr", "536870912", "a"],
    ],
    "integers": [
        ["incr", "i"], ["incr", "i"], ["decr", "i"], ["incrby", "i", "10"], ["decrby", "i", "3"], ["get", "i"],
        ["incrby", "i", "abc"], ["incrby", "i", "1.5"], ["decrby", "i", "-9223372036854775808"],
        ["set", "i", "9223372036854775806"], ["incr", "i"], ["incr", "i"], ["set", "i", "-9223372036854775808"], ["decr", "i"],
        ["set", "i", " 1"], ["incr", "i"], ["set", "i", "01"], ["incr", "i"], ["set", "i", "1.0"], ["incr", "i"],
        ["set", "i", "abc"], ["incr", "i"], ["set", "i", ""], ["incr", "i"], ["set", "i", "+1"], ["incr", "i"],
        ["set", "i", "-0"], ["incr", "i"], ["incrby", "j", "9223372036854775807"], ["incrby", "j", "1"],
        ["incrbyfloat", "f", "10.5"], ["incrbyfloat", "f", "0.1"], ["incrbyfloat", "f", "-5"], ["get", "f"],
        ["incrbyfloat", "f", "abc"], ["incrbyfloat", "f", "1e3"], ["set", "f", "abc"], ["incrbyfloat", "f", "1"],
        ["incrbyfloat", "g", "3.0e3"], ["incrbyfloat", "g", "inf"], ["set", "h", "1"], ["incrbyfloat", "h", "1.5"],
    ],
    "keyspace": [
        ["mset", "a", "1", "b", "2", "c", "3"], ["del", "a", "nokey"], ["del", "a"], ["exists", "a", "b", "b", "c"],
        ["unlink", "b"], ["type", "c"], ["type", "nokey"], ["keys", "*"], ["keys", "c*"], ["keys", "[ab]"], ["keys", "?"],
        ["dbsize"], ["rename", "c", "d"], ["get", "d"], ["exists", "c"], ["rename", "nokey", "x"], ["rename", "d", "d"],
        ["set", "e", "5"], ["renamenx", "d", "e"], ["renamenx", "d", "f"], ["get", "f"], ["renamenx", "nokey", "z"],
        ["set", "t", "v", "ex", "100"], ["rename", "t", "t2"], ["ttl", "t2"],
        ["expire", "t2", "50"], ["ttl", "t2"], ["expire", "t2", "500", "nx"], ["expire", "t2", "500", "xx"], ["ttl", "t2"],
        ["expire", "t2", "100", "gt"], ["expire", "t2", "1000", "gt"], ["ttl", "t2"], ["expire", "t2", "2000", "lt"],
        ["expire", "t2", "10", "lt"], ["ttl", "t2"], ["expire", "t2", "10", "nx", "xx"], ["expire", "t2", "10", "gt", "lt"],
        ["expire", "t2", "10", "foo"], ["expire", "t2", "abc"], ["expire", "nokey", "10"],
        ["expire", "t2", "9223372036854775807"], ["pexpire", "t2", "-9223372036854775808"], ["persist", "t2"], ["ttl", "t2"],
        ["persist", "t2"], ["persist", "nokey"], ["expire", "f", "100", "lt"], ["ttl", "f"], ["expire", "e", "100", "gt"],
        ["ttl", "e"], ["pexpire", "e", "100000"], ["pttl", "e"], ["expireat", "e", "1"], ["exists", "e"],
        ["set", "e", "1"], ["pexpireat", "e", "99999999999999"], ["pexpiretime", "e"], ["expiretime", "e"],
        ["expiretime", "nokey"], ["expiretime", "f"], ["ttl", "nokey"], ["pttl", "nokey"], ["expire", "f", "0"], ["exists", "f"],
        ["set", "g", "1"], ["expire", "g", "-1"], ["exists", "g"], ["flushdb", "async"], ["dbsize"], ["flushall", "foo"],
        ["flushall", "sync"], ["randomkey"],
    ],
    "transactions": [
        ["multi"], ["set", "a", "1"], ["incr", "a"], ["get", "a"], ["exec"],
        ["multi"], ["set", "a"], ["incr", "a"], ["exec"], ["get", "a"],
        ["multi"], ["unknowncmd", "x"], ["exec"],
        ["multi"], ["multi"], ["exec"],
        ["multi"], ["watch", "a"], ["exec"],
        ["exec"], ["discard"], ["multi"], ["set", "b", "1"], ["discard"], ["get", "b"],
        ["multi"], ["set", "c", "x"], ["incr", "c"], ["get", "c"], ["exec"],
        ["watch", "a"], ["multi"], ["incr", "a"], ["exec"],
        ["watch", "a"], ["set", "a", "100"], ["multi"], ["incr", "a"], ["exec"], ["get", "a"],
        ["watch", "a"], ["unwatch"], ["set", "a", "5"], ["multi"], ["incr", "a"], ["exec"],
        ["multi"], ["ping"], ["echo", "hi"], ["exec"],
    ],
    "connection": [
        ["ping"], ["ping", "hello"], ["ping", "a", "b"], ["echo", "x"], ["echo"], ["select", "0"], ["select", "x"],
        ["client", "getname"], ["client", "setname", "myname"], ["client", "getname"], ["client", "setname", "bad name"],
        ["client", "setname", ""], ["client", "getname"], ["client", "foo"], ["client", "id"],
        ["nonexistent"], ["nonexistent", "a", "b"], ["GET", "x"], ["gEt", "x"], ["config", "foo"], ["auth", "pass"],
        ["auth", "default", "pass"], ["hello", "4"], ["hello", "x"], ["time"], ["info"], ["command", "count"],
    ],
}


def run_cases(vport, rport):
    report = {}
    for group, cmds in CASES.items():
        cv, cr = Conn(vport), Conn(rport)
        cv.cmd("flushall")
        cr.cmd("flushall")
        mism = []
        for args in cmds:
            a, b = cv.cmd(*args), cr.cmd(*args)
            if not compare(args, a, b):
                mism.append({"cmd": args, "valkey": show(a), "rvk": show(b)})
        report[group] = {"total": len(cmds), "mismatches": mism}
        cv.close()
        cr.close()
    return report


def run_resp3(vport, rport):
    """Same command subset under HELLO 3: null and map encodings differ."""
    cmds = [["get", "nokey"], ["set", "k", "v"], ["get", "k"], ["mget", "k", "nokey"], ["getex", "nokey"],
            ["set", "k", "w", "nx"], ["multi"], ["get", "nokey"], ["exec"], ["watch", "k"], ["set", "k", "z"],
            ["multi"], ["get", "k"], ["exec"], ["incrbyfloat", "f", "1.5"], ["client", "getname"]]
    cv, cr = Conn(vport), Conn(rport)
    cv.cmd("flushall")
    cr.cmd("flushall")
    mism = []
    a, b = cv.cmd("hello", "3"), cr.cmd("hello", "3")
    if kind(a) != kind(b):
        mism.append({"cmd": ["hello", "3"], "valkey": show(a), "rvk": show(b)})
    for args in cmds:
        a, b = cv.cmd(*args), cr.cmd(*args)
        if not compare(args, a, b):
            mism.append({"cmd": args, "valkey": show(a), "rvk": show(b)})
    return {"total": len(cmds) + 1, "mismatches": mism}


def run_watch_two_clients(vport, rport):
    """WATCH invalidated by another connection, and by expiry-free deletes."""
    res = []
    for port in (vport, rport):
        a, b = Conn(port), Conn(port)
        a.cmd("flushall")
        out = [a.cmd("set", "w", "1"), a.cmd("watch", "w"), b.cmd("set", "w", "2"), a.cmd("multi"),
               a.cmd("incr", "w"), a.cmd("exec"), a.cmd("watch", "w"), b.cmd("get", "w"), a.cmd("multi"),
               a.cmd("incr", "w"), a.cmd("exec"), a.cmd("watch", "w", "other"), b.cmd("del", "other"),
               a.cmd("multi"), a.cmd("incr", "w"), a.cmd("exec"), a.cmd("watch", "other"), b.cmd("set", "other", "x"),
               a.cmd("multi"), a.cmd("incr", "w"), a.cmd("exec"), a.cmd("watch", "w"), b.cmd("flushall"),
               a.cmd("multi"), a.cmd("get", "w"), a.cmd("exec")]
        res.append([show(x) for x in out])
        a.close()
        b.close()
    mism = [] if res[0] == res[1] else [{"cmd": "watch-two-clients", "valkey": res[0], "rvk": res[1]}]
    return {"total": len(res[0]), "mismatches": mism}


RAW = [
    ("inline", b"PING\r\n"),
    ("inline-args", b"set inl \"a b\"\r\nget inl\r\n"),
    ("inline-quotes", b"echo 'it\\'s'\r\n"),
    ("inline-lf-only", b"PING\n"),
    ("inline-unbalanced", b"set \"a b\r\n"),
    ("empty-multibulk", b"*0\r\nPING\r\n"),
    ("negative-multibulk", b"*-1\r\nPING\r\n"),
    ("bad-multibulk-len", b"*abc\r\n"),
    ("bad-bulk-type", b"*1\r\n:1\r\n"),
    ("bad-bulk-len", b"*1\r\n$abc\r\n"),
    ("huge-multibulk", b"*1048577\r\n"),
    ("pipeline", b"*3\r\n$3\r\nset\r\n$1\r\np\r\n$1\r\n1\r\n*2\r\n$4\r\nincr\r\n$1\r\np\r\n*2\r\n$3\r\nget\r\n$1\r\np\r\n"),
    ("blank-lines", b"\r\n\r\nPING\r\n"),
    ("inline-mixed-quotes", b"set \"\"\"tes\"t-'k'e\"y\" \"test\"'-'value\r\nget test'-'key\r\n"),
    ("inline-unbalanced-single", b"set foo 'b'a'r\r\nping\r\n"),
    ("inline-unbalanced-double", b"set foo \"b\"a\"r\r\nping\r\n"),
    ("inline-nul", b"set foo bar\x00 ex 100\r\n"),
    ("inline-escapes", b"set e \"\\x41\\x4g\\q\\n\"\r\nget e\r\n"),
    ("inline-vtab-separator", b"echo a\x0bb\r\n"),
]


def run_raw(vport, rport):
    mism = []
    for name, data in RAW:
        outs = []
        for port in (vport, rport):
            c = Conn(port)
            c.cmd("flushall")
            c.send_raw(data)
            outs.append(c.read_raw())
            c.close()
        if outs[0] != outs[1]:
            mism.append({"cmd": name, "valkey": outs[0].decode(errors="replace"), "rvk": outs[1].decode(errors="replace")})
    return {"total": len(RAW), "mismatches": mism}


def fuzz(vport, rport, seed, n):
    """Random command sequences over a small keyspace; compares every reply."""
    rnd = random.Random(seed)
    keys = ["k%d" % i for i in range(8)]
    vals = ["0", "1", "-1", "42", "9223372036854775807", "abc", "", "3.5", "x" * 50, "10"]
    templates = [
        lambda: ["get", rnd.choice(keys)],
        lambda: ["set", rnd.choice(keys), rnd.choice(vals)] + rnd.choice([[], ["nx"], ["xx"], ["get"], ["ex", "100"], ["px", "50000"], ["keepttl"], ["xx", "get"]]),
        lambda: ["incr", rnd.choice(keys)],
        lambda: ["incrby", rnd.choice(keys), rnd.choice(["1", "-5", "100", "x"])],
        lambda: ["decr", rnd.choice(keys)],
        lambda: ["append", rnd.choice(keys), rnd.choice(vals)],
        lambda: ["strlen", rnd.choice(keys)],
        lambda: ["del"] + rnd.sample(keys, rnd.randint(1, 3)),
        lambda: ["exists"] + [rnd.choice(keys) for _ in range(rnd.randint(1, 3))],
        lambda: ["mget"] + rnd.sample(keys, rnd.randint(1, 4)),
        lambda: ["mset"] + sum([[k, rnd.choice(vals)] for k in rnd.sample(keys, rnd.randint(1, 3))], []),
        lambda: ["msetnx"] + sum([[k, rnd.choice(vals)] for k in rnd.sample(keys, rnd.randint(1, 3))], []),
        lambda: ["expire", rnd.choice(keys), rnd.choice(["100", "1000", "50"])] + rnd.choice([[], ["nx"], ["xx"], ["gt"], ["lt"]]),
        lambda: ["persist", rnd.choice(keys)],
        lambda: ["ttl", rnd.choice(keys)],
        lambda: ["type", rnd.choice(keys)],
        lambda: ["getdel", rnd.choice(keys)],
        lambda: ["getset", rnd.choice(keys), rnd.choice(vals)],
        lambda: ["getrange", rnd.choice(keys), str(rnd.randint(-5, 5)), str(rnd.randint(-5, 5))],
        lambda: ["setrange", rnd.choice(keys), str(rnd.randint(0, 8)), rnd.choice(vals)],
        lambda: ["rename", rnd.choice(keys), rnd.choice(keys)],
        lambda: ["renamenx", rnd.choice(keys), rnd.choice(keys)],
        lambda: ["getex", rnd.choice(keys)] + rnd.choice([[], ["persist"], ["ex", "100"]]),
        lambda: ["setnx", rnd.choice(keys), rnd.choice(vals)],
        lambda: ["dbsize"],
        lambda: ["keys", rnd.choice(["*", "k1", "k[0-3]", "?2"])],
    ]
    cv, cr = Conn(vport), Conn(rport)
    cv.cmd("flushall")
    cr.cmd("flushall")
    mism = []
    for _ in range(n):
        args = rnd.choice(templates)()
        a, b = cv.cmd(*args), cr.cmd(*args)
        if not compare(args, a, b):
            mism.append({"cmd": args, "valkey": show(a), "rvk": show(b)})
            if len(mism) > 50:
                break
    cv.close()
    cr.close()
    return {"total": n, "seed": seed, "mismatches": mism}


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    vport, rport = int(sys.argv[1]), int(sys.argv[2])
    seed, n, out = 1, 20000, None
    rest = sys.argv[3:]
    for i in range(0, len(rest), 2):
        if rest[i] == "--seed":
            seed = int(rest[i + 1])
        elif rest[i] == "--fuzz":
            n = int(rest[i + 1])
        elif rest[i] == "--out":
            out = rest[i + 1]
    started = time.time()
    report = {"cases": run_cases(vport, rport), "resp3": run_resp3(vport, rport),
              "watch_two_clients": run_watch_two_clients(vport, rport), "raw_protocol": run_raw(vport, rport),
              "fuzz": fuzz(vport, rport, seed, n)}
    total = mism = 0
    for k, v in report.items():
        groups = v.items() if k == "cases" else [(k, v)]
        for g, r in groups:
            total += r["total"]
            mism += len(r["mismatches"])
            print(f"{g:<20} {r['total']:>6} cases  {len(r['mismatches']):>4} mismatches")
    report["summary"] = {"total": total, "mismatches": mism, "seconds": round(time.time() - started, 1)}
    print(f"{'TOTAL':<20} {total:>6} cases  {mism:>4} mismatches")
    if out:
        with open(out, "w") as f:
            json.dump(report, f, indent=1)
    sys.exit(0 if mism == 0 else 1)


if __name__ == "__main__":
    main()
