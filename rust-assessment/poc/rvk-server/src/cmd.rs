//! Command table, dispatch and implementations for the declared subset.
//!
//! Semantics and error texts follow Valkey `unstable` (t_string.c, expire.c,
//! db.c, multi.c, server.c). Only the string type exists, so WRONGTYPE paths
//! are unreachable. FEATURE_MATRIX.md lists what is and is not covered.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, MutexGuard};

use crate::resp::{parse_i64, push_i64, Args, Reply};
use crate::server::{now_ms, CachePadded, Policy, Shared, Stats};
use crate::store::{Effects, Shard, Store, WatchFlag, NSHARDS};

// ---------------------------------------------------------------------------
// Per-thread and per-connection state
// ---------------------------------------------------------------------------

struct PoolEntry {
    idle: u32,
    shard: usize,
    key: Vec<u8>,
}

pub struct ThreadCtx {
    pub id: usize,
    pub shared: Arc<Shared>,
    stats: Arc<CachePadded<Stats>>,
    pub ranges: Vec<crate::resp::ArgRange>,
    pub scratch: Vec<u8>,
    rng: u64,
    pool: Vec<PoolEntry>,
    keybuf: Vec<(usize, u64)>,
    pub shutdown_requested: bool,
    active_expire: bool,
    /// jemalloc-allocated bytes and the dataset estimate at the last refresh.
    alloc_snapshot: (usize, usize),
}

impl ThreadCtx {
    pub fn new(id: usize, shared: Arc<Shared>) -> Self {
        let stats = shared.stats[id].clone();
        ThreadCtx {
            id,
            shared,
            stats,
            ranges: Vec::new(),
            scratch: Vec::new(),
            rng: 0x2545_F491_4F6C_DD1D ^ ((id as u64 + 1) * 0x9E37_79B9_7F4A_7C15),
            pool: Vec::new(),
            keybuf: Vec::new(),
            shutdown_requested: false,
            active_expire: true,
            alloc_snapshot: (0, 0),
        }
    }
    #[inline]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    fn next_rand(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }
}

struct Multi {
    queued: Vec<Vec<Vec<u8>>>,
    errored: bool,
}

pub struct ConnState {
    pub id: u64,
    resp3: bool,
    authed: bool,
    name: Option<Vec<u8>>,
    multi: Option<Multi>,
    watched: Vec<Vec<u8>>,
    watch_flag: WatchFlag,
    pub close_after_reply: bool,
}

impl ConnState {
    #[inline]
    pub fn authed(&self) -> bool {
        self.authed
    }
    pub fn new(id: u64, authed: bool) -> Self {
        ConnState {
            id,
            resp3: false,
            authed,
            name: None,
            multi: None,
            watched: Vec::new(),
            watch_flag: Arc::new(AtomicBool::new(false)),
            close_after_reply: false,
        }
    }
}

pub fn on_disconnect(tc: &mut ThreadCtx, conn: &mut ConnState) {
    unwatch_all(&tc.shared.clone().store, conn);
}

fn unwatch_all(store: &Store, conn: &mut ConnState) {
    for key in conn.watched.drain(..) {
        let h = store.hash(&key);
        store.lock(Store::shard_of(h)).unwatch(&key, &conn.watch_flag);
    }
    conn.watch_flag = Arc::new(AtomicBool::new(false));
}

// ---------------------------------------------------------------------------
// Command table
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Keys {
    None,
    /// first, last (negative counts from the end), step.
    Range(i32, i32, i32),
    /// Locks every shard (FLUSHALL, DBSIZE, KEYS, ...).
    All,
}

const WRITE: u8 = 1;
const DENYOOM: u8 = 2;
const NOAUTH: u8 = 4;
const NOMULTI: u8 = 8;

type Handler = fn(&mut Cx);

#[derive(Clone, Copy)]
struct Spec {
    name: &'static str,
    arity: i32,
    flags: u8,
    keys: Keys,
    run: Handler,
}

macro_rules! spec {
    ($name:expr, $arity:expr, $flags:expr, $keys:expr, $run:expr) => {
        Some(Spec { name: $name, arity: $arity, flags: $flags, keys: $keys, run: $run })
    };
}

const K1: Keys = Keys::Range(1, 1, 1);
const KALL1: Keys = Keys::Range(1, -1, 1);

fn lookup(name: &[u8]) -> Option<Spec> {
    let mut lower = [0u8; 24];
    if name.len() > lower.len() {
        return None;
    }
    for (d, s) in lower.iter_mut().zip(name) {
        *d = s.to_ascii_lowercase();
    }
    match &lower[..name.len()] {
        b"get" => spec!("get", 2, 0, K1, cmd_get),
        b"set" => spec!("set", -3, WRITE | DENYOOM, K1, cmd_set),
        b"setnx" => spec!("setnx", 3, WRITE | DENYOOM, K1, cmd_setnx),
        b"setex" => spec!("setex", 4, WRITE | DENYOOM, K1, cmd_setex),
        b"psetex" => spec!("psetex", 4, WRITE | DENYOOM, K1, cmd_psetex),
        b"getex" => spec!("getex", -2, WRITE, K1, cmd_getex),
        b"getdel" => spec!("getdel", 2, WRITE, K1, cmd_getdel),
        b"getset" => spec!("getset", 3, WRITE | DENYOOM, K1, cmd_getset),
        b"mget" => spec!("mget", -2, 0, KALL1, cmd_mget),
        b"mset" => spec!("mset", -3, WRITE | DENYOOM, Keys::Range(1, -1, 2), cmd_mset),
        b"msetnx" => spec!("msetnx", -3, WRITE | DENYOOM, Keys::Range(1, -1, 2), cmd_msetnx),
        b"append" => spec!("append", 3, WRITE | DENYOOM, K1, cmd_append),
        b"strlen" => spec!("strlen", 2, 0, K1, cmd_strlen),
        b"incr" => spec!("incr", 2, WRITE | DENYOOM, K1, cmd_incr),
        b"decr" => spec!("decr", 2, WRITE | DENYOOM, K1, cmd_decr),
        b"incrby" => spec!("incrby", 3, WRITE | DENYOOM, K1, cmd_incrby),
        b"decrby" => spec!("decrby", 3, WRITE | DENYOOM, K1, cmd_decrby),
        b"incrbyfloat" => spec!("incrbyfloat", 3, WRITE | DENYOOM, K1, cmd_incrbyfloat),
        b"getrange" => spec!("getrange", 4, 0, K1, cmd_getrange),
        b"setrange" => spec!("setrange", 4, WRITE | DENYOOM, K1, cmd_setrange),
        b"del" => spec!("del", -2, WRITE, KALL1, cmd_del),
        b"unlink" => spec!("unlink", -2, WRITE, KALL1, cmd_del),
        b"exists" => spec!("exists", -2, 0, KALL1, cmd_exists),
        b"type" => spec!("type", 2, 0, K1, cmd_type),
        b"expire" => spec!("expire", -3, WRITE, K1, cmd_expire),
        b"pexpire" => spec!("pexpire", -3, WRITE, K1, cmd_pexpire),
        b"expireat" => spec!("expireat", -3, WRITE, K1, cmd_expireat),
        b"pexpireat" => spec!("pexpireat", -3, WRITE, K1, cmd_pexpireat),
        b"ttl" => spec!("ttl", 2, 0, K1, cmd_ttl),
        b"pttl" => spec!("pttl", 2, 0, K1, cmd_pttl),
        b"expiretime" => spec!("expiretime", 2, 0, K1, cmd_expiretime),
        b"pexpiretime" => spec!("pexpiretime", 2, 0, K1, cmd_pexpiretime),
        b"persist" => spec!("persist", 2, WRITE, K1, cmd_persist),
        b"rename" => spec!("rename", 3, WRITE, Keys::Range(1, 2, 1), cmd_rename),
        b"renamenx" => spec!("renamenx", 3, WRITE, Keys::Range(1, 2, 1), cmd_renamenx),
        b"keys" => spec!("keys", 2, 0, Keys::All, cmd_keys),
        b"randomkey" => spec!("randomkey", 1, 0, Keys::All, cmd_randomkey),
        b"dbsize" => spec!("dbsize", 1, 0, Keys::All, cmd_dbsize),
        b"flushall" => spec!("flushall", -1, WRITE, Keys::All, cmd_flushall),
        b"flushdb" => spec!("flushdb", -1, WRITE, Keys::All, cmd_flushall),
        b"multi" => spec!("multi", 1, NOMULTI, Keys::None, cmd_multi),
        b"exec" => spec!("exec", 1, NOMULTI, Keys::None, cmd_exec),
        b"discard" => spec!("discard", 1, NOMULTI, Keys::None, cmd_discard),
        b"watch" => spec!("watch", -2, NOMULTI, KALL1, cmd_watch),
        b"unwatch" => spec!("unwatch", 1, 0, Keys::None, cmd_unwatch),
        b"ping" => spec!("ping", -1, 0, Keys::None, cmd_ping),
        b"echo" => spec!("echo", 2, 0, Keys::None, cmd_echo),
        b"select" => spec!("select", 2, 0, Keys::None, cmd_select),
        b"quit" => spec!("quit", -1, NOAUTH, Keys::None, cmd_quit),
        b"reset" => spec!("reset", 1, NOAUTH, Keys::None, cmd_reset),
        b"hello" => spec!("hello", -1, NOAUTH, Keys::None, cmd_hello),
        b"auth" => spec!("auth", -2, NOAUTH, Keys::None, cmd_auth),
        b"client" => spec!("client", -2, 0, Keys::None, cmd_client),
        b"command" => spec!("command", -1, 0, Keys::None, cmd_command),
        b"config" => spec!("config", -2, 0, Keys::None, cmd_config),
        b"info" => spec!("info", -1, 0, Keys::None, cmd_info),
        b"time" => spec!("time", 1, 0, Keys::None, cmd_time),
        b"debug" => spec!("debug", -2, 0, Keys::None, cmd_debug),
        b"shutdown" => spec!("shutdown", -1, 0, Keys::None, cmd_shutdown),
        b"function" => spec!("function", -2, 0, Keys::None, cmd_function_script),
        b"script" => spec!("script", -2, 0, Keys::None, cmd_function_script),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Execution context
// ---------------------------------------------------------------------------

enum Locks<'s> {
    None,
    One(usize, MutexGuard<'s, Shard>),
    Many(Vec<(usize, MutexGuard<'s, Shard>)>),
}

pub struct Cx<'a, 's> {
    sh: &'s Shared,
    tc: &'a mut ThreadCtx,
    conn: &'a mut ConnState,
    argv: Vec<&'a [u8]>,
    r: Reply<'a>,
    locks: Locks<'s>,
    now: u64,
    lru: u32,
    fx: Effects,
}

impl<'a, 's> Cx<'a, 's> {
    #[inline]
    fn arg(&self, i: usize) -> &'a [u8] {
        self.argv[i]
    }
    #[inline]
    fn argc(&self) -> usize {
        self.argv.len()
    }
    #[inline]
    fn store(&self) -> &'s Store {
        &self.sh.store
    }
    /// The locked shard holding `h`. The dispatcher locked it beforehand.
    #[inline]
    fn shard(&mut self, h: u64) -> &mut Shard {
        let want = Store::shard_of(h);
        match &mut self.locks {
            Locks::One(s, g) if *s == want => g,
            Locks::Many(v) => {
                let i = v.iter().position(|(s, _)| *s == want).expect("shard not locked");
                &mut v[i].1
            }
            _ => panic!("shard {want} not locked"),
        }
    }
    /// Looks a key up (lazy expiry applied), refreshing its LRU clock.
    #[inline]
    fn find(&mut self, key: &[u8]) -> (u64, Option<usize>) {
        let h = self.store().hash(key);
        let (now, lru) = (self.now, self.lru);
        let hasher = self.sh.store.hasher();
        let mut fx = self.fx;
        let s = self.shard(h);
        let i = s.touch_lookup(h, key, now, lru, &mut fx, hasher);
        self.fx = fx;
        (h, i)
    }
    fn value(&mut self, h: u64, i: usize) -> &[u8] {
        self.shard(h).slot(i).value()
    }
    /// Writes the value at (h, i) as a bulk reply straight from the slab.
    fn reply_value(&mut self, h: u64, i: usize) {
        let want = Store::shard_of(h);
        let Cx { locks, r, .. } = self;
        let shard: &Shard = match locks {
            Locks::One(s, g) if *s == want => g,
            Locks::Many(v) => &v.iter().find(|(s, _)| *s == want).expect("shard not locked").1,
            _ => panic!("shard {want} not locked"),
        };
        r.bulk(shard.slot(i).value());
    }
    fn set(&mut self, h: u64, key: &[u8], val: &[u8], expire: u64) -> bool {
        let lru = self.lru;
        let hasher = self.sh.store.hasher();
        self.shard(h).set(h, key, val, expire, lru, hasher)
    }
    fn remove(&mut self, h: u64, i: usize) {
        let hasher = self.sh.store.hasher();
        self.shard(h).remove_at(i, h, hasher);
    }
    fn hit(&mut self, found: bool) {
        if found {
            Stats::add(&self.tc.stats().hits, 1);
        } else {
            Stats::add(&self.tc.stats().misses, 1);
        }
    }
    fn err(&mut self, msg: &str) {
        self.r.error(msg);
        Stats::add(&self.tc.stats().errors, 1);
    }
    fn syntax_err(&mut self) {
        self.err("ERR syntax error");
    }
    fn not_int(&mut self) {
        self.err("ERR value is not an integer or out of range");
    }
    fn expire_time_err(&mut self) {
        let msg = format!("ERR invalid expire time in '{}' command", lower(self.arg(0)));
        self.err(&msg);
    }
    fn all_shards(&mut self) -> &mut Vec<(usize, MutexGuard<'s, Shard>)> {
        match &mut self.locks {
            Locks::Many(v) => v,
            _ => panic!("command requires all shards"),
        }
    }
}

fn lower(s: &[u8]) -> String {
    String::from_utf8_lossy(s).to_ascii_lowercase()
}

fn eq_ic(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Executes one command and appends its reply to `out`.
pub fn execute(tc: &mut ThreadCtx, conn: &mut ConnState, args: &Args, out: &mut Vec<u8>) {
    let shared = tc.shared.clone();
    let argv: Vec<&[u8]> = (0..args.len()).map(|i| args.get(i)).collect();
    run_command(&shared, tc, conn, argv, out, false);
}

fn unknown_command(argv: &[&[u8]]) -> String {
    let mut a = String::new();
    for x in &argv[1..] {
        if a.len() >= 128 {
            break;
        }
        let room = 128 - a.len();
        let s = String::from_utf8_lossy(x);
        let s: String = s.chars().take(room).collect();
        let _ = write!(a, "'{s}' ");
    }
    let name: String = String::from_utf8_lossy(argv[0]).chars().take(128).collect();
    format!("ERR unknown command '{name}', with args beginning with: {a}")
}

fn check_arity(spec: &Spec, argc: usize) -> bool {
    if spec.arity > 0 {
        argc == spec.arity as usize
    } else {
        argc >= (-spec.arity) as usize
    }
}

/// Validates, queues (inside MULTI) or runs a command. `in_exec` is true for
/// commands replayed by EXEC, whose shards are already locked by the caller
/// through `pre_locked`.
fn run_command<'s>(sh: &'s Shared, tc: &mut ThreadCtx, conn: &mut ConnState, argv: Vec<&[u8]>, out: &mut Vec<u8>, in_exec: bool) {
    Stats::add(&tc.stats().commands, 1);
    let mut r = Reply { out, resp3: conn.resp3 };
    let Some(spec) = lookup(argv[0]) else {
        r.error(&unknown_command(&argv));
        Stats::add(&tc.stats().errors, 1);
        if let Some(m) = conn.multi.as_mut() {
            m.errored = true;
        }
        return;
    };
    if !check_arity(&spec, argv.len()) {
        r.error(&format!("ERR wrong number of arguments for '{}' command", spec.name));
        Stats::add(&tc.stats().errors, 1);
        if let Some(m) = conn.multi.as_mut() {
            m.errored = true;
        }
        return;
    }
    if !conn.authed && spec.flags & NOAUTH == 0 {
        r.error("NOAUTH Authentication required.");
        Stats::add(&tc.stats().errors, 1);
        if let Some(m) = conn.multi.as_mut() {
            m.errored = true;
        }
        return;
    }
    if let Some(m) = conn.multi.as_mut() {
        if spec.flags & NOMULTI != 0 && spec.name != "exec" && spec.name != "discard" {
            m.errored = true;
            r.error(&format!("ERR Command '{}' not allowed inside a transaction", spec.name));
            Stats::add(&tc.stats().errors, 1);
            return;
        }
        if !matches!(spec.name, "exec" | "discard" | "quit" | "reset") {
            m.queued.push(argv.iter().map(|a| a.to_vec()).collect());
            r.simple(b"QUEUED");
            return;
        }
    }
    if spec.flags & DENYOOM != 0 && !in_exec && !make_room(sh, tc) {
        r.error("OOM command not allowed when used memory > 'maxmemory'.");
        Stats::add(&tc.stats().errors, 1);
        return;
    }
    if spec.name == "exec" {
        exec_transaction(sh, tc, conn, r);
        return;
    }
    let locks = lock_for(sh, tc, &spec, &argv);
    let now = now_ms();
    let mut cx = Cx { sh, tc, conn, argv, r, locks, now, lru: (now / 1000) as u32, fx: Effects::default() };
    (spec.run)(&mut cx);
    finish(&mut cx);
}

fn finish(cx: &mut Cx) {
    let fx = cx.fx;
    if fx.expired > 0 {
        Stats::add(&cx.tc.stats().expired, fx.expired);
    }
    let store = &cx.sh.store;
    match &cx.locks {
        Locks::None => {}
        Locks::One(s, g) => store.publish_mem(*s, g),
        Locks::Many(v) => {
            for (s, g) in v {
                store.publish_mem(*s, g);
            }
        }
    }
}

fn key_shards(sh: &Shared, keys: Keys, argv: &[&[u8]], out: &mut Vec<(usize, u64)>) {
    out.clear();
    if let Keys::Range(first, last, step) = keys {
        let argc = argv.len() as i32;
        let last = if last < 0 { argc + last } else { last.min(argc - 1) };
        let mut i = first;
        while i <= last && i < argc {
            let h = sh.store.hash(argv[i as usize]);
            out.push((Store::shard_of(h), h));
            i += step;
        }
    }
}

/// Locks the shards a command touches, in ascending order (deadlock-free).
fn lock_for<'s>(sh: &'s Shared, tc: &mut ThreadCtx, spec: &Spec, argv: &[&[u8]]) -> Locks<'s> {
    match spec.keys {
        Keys::None => Locks::None,
        Keys::All => Locks::Many((0..NSHARDS).map(|s| (s, sh.store.lock(s))).collect()),
        Keys::Range(..) => {
            let mut ks = std::mem::take(&mut tc.keybuf);
            key_shards(sh, spec.keys, argv, &mut ks);
            let locks = if ks.len() == 1 {
                Locks::One(ks[0].0, sh.store.lock(ks[0].0))
            } else {
                let mut ids: Vec<usize> = ks.iter().map(|k| k.0).collect();
                ids.sort_unstable();
                ids.dedup();
                if ids.len() == 1 {
                    Locks::One(ids[0], sh.store.lock(ids[0]))
                } else {
                    Locks::Many(ids.into_iter().map(|s| (s, sh.store.lock(s))).collect())
                }
            };
            tc.keybuf = ks;
            locks
        }
    }
}

// ---------------------------------------------------------------------------
// Memory limit and eviction
// ---------------------------------------------------------------------------

fn used_memory(sh: &Shared, tc: &ThreadCtx) -> usize {
    let (alloc, est_then) = tc.alloc_snapshot;
    let est_now = sh.store.used_memory();
    if alloc == 0 {
        est_now
    } else {
        (alloc + est_now).saturating_sub(est_then)
    }
}

/// Evicts keys until memory is below `maxmemory`. Returns false if the
/// command must be refused with OOM. Like Valkey's `performEvictions`, a
/// command proceeds once memory fits (EVICT_OK) or while eviction is making
/// progress (EVICT_RUNNING), and is refused only when nothing can be evicted
/// (EVICT_FAIL). Other threads change memory concurrently, so the decision
/// uses what this call observed rather than a final re-read; refusing after a
/// fixed number of evictions, or on a final re-read, both produced spurious
/// OOM errors under concurrent writers.
fn make_room(sh: &Shared, tc: &mut ThreadCtx) -> bool {
    let max = sh.maxmemory.load(Ordering::Relaxed);
    if max == 0 || used_memory(sh, tc) <= max {
        return true;
    }
    let policy = *sh.policy.read().unwrap();
    if policy == Policy::NoEviction {
        return false;
    }
    let now = now_ms();
    let lru_now = (now / 1000) as u32;
    let mut evicted = 0u64;
    let mut fits = false;
    // Bounded work per command, like Valkey's eviction time limit.
    for _ in 0..64 {
        if used_memory(sh, tc) <= max {
            fits = true;
            break;
        }
        if !evict_one(sh, tc, policy, lru_now) {
            break;
        }
        evicted += 1;
    }
    Stats::add(&tc.stats().evicted, evicted);
    fits || evicted > 0
}

fn evict_one(sh: &Shared, tc: &mut ThreadCtx, policy: Policy, lru_now: u32) -> bool {
    let samples = sh.cfg.maxmemory_samples.max(1);
    let hasher = sh.store.hasher();
    for _ in 0..NSHARDS {
        let s = (tc.next_rand() as usize) % NSHARDS;
        let mut g = sh.store.lock(s);
        if g.is_empty() {
            continue;
        }
        let mut rng = tc.next_rand();
        if policy == Policy::AllKeysRandom {
            let i = g.sample(1, &mut rng).next().unwrap();
            let key = g.slot(i).key().to_vec();
            let h = sh.store.hash(&key);
            g.remove_at(i, h, hasher);
            sh.store.publish_mem(s, &g);
            return true;
        }
        // Approximated LRU with a persistent candidate pool, as in evict.c.
        let picks: Vec<usize> = g.sample(samples, &mut rng).collect();
        for i in picks {
            let slot = g.slot(i);
            let idle = lru_now.wrapping_sub(slot.lru);
            if tc.pool.iter().any(|p| p.shard == s && p.key == slot.key()) {
                continue;
            }
            tc.pool.push(PoolEntry { idle, shard: s, key: slot.key().to_vec() });
        }
        drop(g);
        tc.pool.sort_by_key(|p| p.idle);
        if tc.pool.len() > 16 {
            let extra = tc.pool.len() - 16;
            tc.pool.drain(..extra);
        }
        while let Some(best) = tc.pool.pop() {
            let h = sh.store.hash(&best.key);
            let mut g = sh.store.lock(best.shard);
            let mut fx = Effects::default();
            if g.delete(h, &best.key, now_ms(), &mut fx, hasher) {
                sh.store.publish_mem(best.shard, &g);
                return true;
            }
        }
    }
    false
}

/// Periodic work: active expiry over this thread's shards and the memory
/// snapshot used by `maxmemory`.
pub fn cron(tc: &mut ThreadCtx) {
    let sh = tc.shared.clone();
    let start = std::time::Instant::now();
    // Valkey's slow cycle may use up to 25% of a tick (25 ms at hz 10).
    let limit = std::time::Duration::from_micros(250_000 / sh.cfg.hz.max(1) as u64);
    if tc.active_expire && sh.shutdown.load(Ordering::Relaxed) == false {
        let now = now_ms();
        let nthreads = sh.cfg.threads;
        let hasher = sh.store.hasher();
        let mut deleted = 0;
        let mut s = tc.id;
        while s < NSHARDS {
            let mut g = sh.store.lock(s);
            deleted += g.expire_due(now, 20_000, hasher);
            sh.store.publish_mem(s, &g);
            drop(g);
            if start.elapsed() > limit {
                break;
            }
            s += nthreads;
        }
        Stats::add(&tc.stats().expired, deleted);
    }
    if sh.maxmemory.load(Ordering::Relaxed) > 0 {
        let _ = tikv_jemalloc_ctl::epoch::advance();
        let alloc = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
        tc.alloc_snapshot = (alloc, sh.store.used_memory());
    }
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

fn cmd_get(cx: &mut Cx) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    cx.hit(i.is_some());
    match i {
        Some(i) => cx.reply_value(h, i),
        None => cx.r.null(),
    }
}

const NX: u32 = 1;
const XX: u32 = 2;
const GET: u32 = 4;
const KEEPTTL: u32 = 8;
const PERSIST: u32 = 16;
const EX: u32 = 32;
const PX: u32 = 64;
const EXAT: u32 = 128;
const PXAT: u32 = 256;
const IFEQ: u32 = 512;
const IFNE: u32 = 1024;
const EXPIRE_ANY: u32 = EX | PX | EXAT | PXAT;

#[derive(PartialEq, Eq)]
enum OptKind {
    Set,
    Get,
}

struct ExtOpts<'a> {
    flags: u32,
    expire: Option<&'a [u8]>,
    compare: Option<&'a [u8]>,
}

/// `parseExtendedCommandArgumentsOrReply` for SET and GETEX.
fn parse_ext<'a>(argv: &[&'a [u8]], start: usize, kind: OptKind) -> Option<ExtOpts<'a>> {
    let mut o = ExtOpts { flags: 0, expire: None, compare: None };
    let mut j = start;
    let set = kind == OptKind::Set;
    while j < argv.len() {
        let opt = argv[j];
        let next = argv.get(j + 1).copied();
        let f = o.flags;
        if eq_ic(opt, b"nx") && set && f & (XX | IFEQ | IFNE) == 0 {
            o.flags |= NX;
        } else if eq_ic(opt, b"xx") && set && f & (NX | IFEQ | IFNE) == 0 {
            o.flags |= XX;
        } else if eq_ic(opt, b"ifeq") && set && next.is_some() && f & (NX | XX | IFEQ | IFNE) == 0 {
            o.flags |= IFEQ;
            o.compare = next;
            j += 1;
        } else if eq_ic(opt, b"ifne") && set && next.is_some() && f & (NX | XX | IFEQ | IFNE) == 0 {
            o.flags |= IFNE;
            o.compare = next;
            j += 1;
        } else if eq_ic(opt, b"get") && set {
            o.flags |= GET;
        } else if eq_ic(opt, b"keepttl") && set && f & (PERSIST | EXPIRE_ANY) == 0 {
            o.flags |= KEEPTTL;
        } else if eq_ic(opt, b"persist") && !set && f & (EXPIRE_ANY | KEEPTTL) == 0 {
            o.flags |= PERSIST;
        } else if eq_ic(opt, b"ex") && next.is_some() && f & (KEEPTTL | PERSIST | EXAT | PX | PXAT) == 0 {
            o.flags |= EX;
            o.expire = next;
            j += 1;
        } else if eq_ic(opt, b"px") && next.is_some() && f & (KEEPTTL | PERSIST | EX | EXAT | PXAT) == 0 {
            o.flags |= PX;
            o.expire = next;
            j += 1;
        } else if eq_ic(opt, b"exat") && next.is_some() && f & (KEEPTTL | PERSIST | EX | PX | PXAT) == 0 {
            o.flags |= EXAT;
            o.expire = next;
            j += 1;
        } else if eq_ic(opt, b"pxat") && next.is_some() && f & (KEEPTTL | PERSIST | EX | EXAT | PX) == 0 {
            o.flags |= PXAT;
            o.expire = next;
            j += 1;
        } else {
            return None;
        }
        j += 1;
    }
    Some(o)
}

/// `getExpireMillisecondsOrReply`: absolute expiry in ms, or None after
/// replying with an error.
fn expire_ms(cx: &mut Cx, raw: &[u8], flags: u32) -> Option<u64> {
    let Some(v) = parse_i64(raw) else {
        cx.not_int();
        return None;
    };
    let seconds = flags & (EX | EXAT) != 0;
    if v <= 0 || (seconds && v > i64::MAX / 1000) {
        cx.expire_time_err();
        return None;
    }
    let mut ms = if seconds { v * 1000 } else { v };
    if flags & (EX | PX) != 0 {
        let base = cx.now as i64;
        if ms > i64::MAX - base {
            cx.expire_time_err();
            return None;
        }
        ms += base;
    }
    Some(ms as u64)
}

/// The shared core of SET, SETNX, SETEX, PSETEX and GETSET.
fn set_generic(cx: &mut Cx, flags: u32, val_idx: usize, expire: Option<&[u8]>, compare: Option<&[u8]>, ok: Option<i64>, abort: Option<i64>) {
    let mut when = 0u64;
    if let Some(raw) = expire {
        match expire_ms(cx, raw, flags) {
            Some(w) => when = w,
            None => return,
        }
    }
    let key = cx.arg(1);
    let val = cx.arg(val_idx);
    let (h, i) = cx.find(key);
    if flags & GET != 0 {
        match i {
            Some(i) => cx.reply_value(h, i),
            None => cx.r.null(),
        }
    }
    let found = i.is_some();
    let reply_abort = |cx: &mut Cx| {
        if flags & GET == 0 {
            match abort {
                Some(n) => cx.r.int(n),
                None => cx.r.null(),
            }
        }
    };
    if flags & (IFEQ | IFNE) != 0 {
        let Some(i) = i else {
            if flags & IFEQ != 0 {
                reply_abort(cx);
                return;
            }
            // IFNE on a missing key proceeds with the SET.
            return finish_set(cx, flags, key, val, h, None, when, expire.is_some(), ok);
        };
        let equal = cx.value(h, i) == compare.unwrap();
        if (flags & IFEQ != 0 && !equal) || (flags & IFNE != 0 && equal) {
            reply_abort(cx);
            return;
        }
    }
    if (flags & NX != 0 && found) || (flags & XX != 0 && !found) {
        reply_abort(cx);
        return;
    }
    finish_set(cx, flags, key, val, h, i, when, expire.is_some(), ok)
}

#[allow(clippy::too_many_arguments)]
fn finish_set(cx: &mut Cx, flags: u32, key: &[u8], val: &[u8], h: u64, existing: Option<usize>, when: u64, has_expire: bool, ok: Option<i64>) {
    if has_expire && when <= cx.now {
        // Already expired: do not store it; delete any existing key.
        if let Some(i) = existing {
            cx.remove(h, i);
        }
        if flags & GET == 0 {
            cx.r.ok();
        }
        return;
    }
    let expire = if has_expire {
        when
    } else if flags & KEEPTTL != 0 {
        existing.map(|i| cx.shard(h).slot(i).expire).unwrap_or(0)
    } else {
        0
    };
    cx.set(h, key, val, expire);
    if flags & GET == 0 {
        match ok {
            Some(n) => cx.r.int(n),
            None => cx.r.ok(),
        }
    }
}

fn cmd_set(cx: &mut Cx) {
    let argv = cx.argv.clone();
    let Some(o) = parse_ext(&argv, 3, OptKind::Set) else {
        return cx.syntax_err();
    };
    set_generic(cx, o.flags, 2, o.expire, o.compare, None, None);
}

fn cmd_setnx(cx: &mut Cx) {
    set_generic(cx, NX, 2, None, None, Some(1), Some(0));
}

fn cmd_setex(cx: &mut Cx) {
    let e = cx.arg(2);
    set_generic(cx, EX, 3, Some(e), None, None, None);
}

fn cmd_psetex(cx: &mut Cx) {
    let e = cx.arg(2);
    set_generic(cx, PX, 3, Some(e), None, None, None);
}

fn cmd_getset(cx: &mut Cx) {
    set_generic(cx, GET, 2, None, None, None, None);
}

fn cmd_getex(cx: &mut Cx) {
    let argv = cx.argv.clone();
    let Some(o) = parse_ext(&argv, 2, OptKind::Get) else {
        return cx.syntax_err();
    };
    let mut when = 0;
    if let Some(raw) = o.expire {
        match expire_ms(cx, raw, o.flags) {
            Some(w) => when = w,
            None => return,
        }
    }
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    cx.hit(i.is_some());
    let Some(i) = i else {
        return cx.r.null();
    };
    cx.reply_value(h, i);
    if o.expire.is_some() {
        if when <= cx.now {
            cx.remove(h, i);
        } else {
            let hasher = cx.sh.store.hasher();
            cx.shard(h).set_expire(i, h, when, hasher);
        }
    } else if o.flags & PERSIST != 0 && cx.shard(h).slot(i).expire != 0 {
        let hasher = cx.sh.store.hasher();
        cx.shard(h).set_expire(i, h, 0, hasher);
    }
}

fn cmd_getdel(cx: &mut Cx) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    cx.hit(i.is_some());
    match i {
        Some(i) => {
            cx.reply_value(h, i);
            cx.remove(h, i);
        }
        None => cx.r.null(),
    }
}

fn cmd_mget(cx: &mut Cx) {
    let n = cx.argc() - 1;
    cx.r.array(n);
    for j in 1..=n {
        let key = cx.arg(j);
        let (h, i) = cx.find(key);
        cx.hit(i.is_some());
        match i {
            Some(i) => cx.reply_value(h, i),
            None => cx.r.null(),
        }
    }
}

fn cmd_mset(cx: &mut Cx) {
    if cx.argc() % 2 == 0 {
        return cx.err("ERR wrong number of arguments for 'mset' command");
    }
    let mut j = 1;
    while j < cx.argc() {
        let (k, v) = (cx.arg(j), cx.arg(j + 1));
        let h = cx.store().hash(k);
        cx.set(h, k, v, 0);
        j += 2;
    }
    cx.r.ok();
}

fn cmd_msetnx(cx: &mut Cx) {
    if cx.argc() % 2 == 0 {
        return cx.err("ERR wrong number of arguments for 'msetnx' command");
    }
    let mut j = 1;
    while j < cx.argc() {
        let k = cx.arg(j);
        if cx.find(k).1.is_some() {
            return cx.r.int(0);
        }
        j += 2;
    }
    let mut j = 1;
    while j < cx.argc() {
        let (k, v) = (cx.arg(j), cx.arg(j + 1));
        let h = cx.store().hash(k);
        cx.set(h, k, v, 0);
        j += 2;
    }
    cx.r.int(1);
}

/// Valkey's `proto-max-bulk-len` check for commands that grow a string.
fn check_string_length(cx: &mut Cx, size: u64) -> bool {
    if size > cx.sh.cfg.proto_max_bulk_len as u64 {
        cx.err("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
        return false;
    }
    true
}

fn cmd_append(cx: &mut Cx) {
    let key = cx.arg(1);
    let add = cx.arg(2);
    let (h, i) = cx.find(key);
    match i {
        None => {
            cx.set(h, key, add, 0);
            cx.r.int(add.len() as i64);
        }
        Some(i) => {
            let cur_len = cx.value(h, i).len();
            if !check_string_length(cx, (cur_len + add.len()) as u64) {
                return;
            }
            let mut v = cx.value(h, i).to_vec();
            v.extend_from_slice(add);
            let lru = cx.lru;
            cx.shard(h).replace_value(i, &v, lru);
            cx.r.int(v.len() as i64);
        }
    }
}

fn cmd_strlen(cx: &mut Cx) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let n = i.map(|i| cx.value(h, i).len()).unwrap_or(0);
    cx.r.int(n as i64);
}

fn incr_generic(cx: &mut Cx, by: i64) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let cur = match i {
        None => 0,
        Some(i) => match parse_i64(cx.value(h, i)) {
            Some(v) => v,
            None => return cx.not_int(),
        },
    };
    let Some(newv) = cur.checked_add(by) else {
        return cx.err("ERR increment or decrement would overflow");
    };
    let mut buf = Vec::with_capacity(20);
    push_i64(&mut buf, newv);
    match i {
        Some(i) => {
            let lru = cx.lru;
            cx.shard(h).replace_value(i, &buf, lru);
        }
        None => {
            cx.set(h, key, &buf, 0);
        }
    }
    cx.r.int(newv);
}

fn cmd_incr(cx: &mut Cx) {
    incr_generic(cx, 1)
}
fn cmd_decr(cx: &mut Cx) {
    incr_generic(cx, -1)
}
fn cmd_incrby(cx: &mut Cx) {
    match parse_i64(cx.arg(2)) {
        Some(v) => incr_generic(cx, v),
        None => cx.not_int(),
    }
}
fn cmd_decrby(cx: &mut Cx) {
    match parse_i64(cx.arg(2)) {
        Some(i64::MIN) => cx.err("ERR decrement would overflow"),
        Some(v) => incr_generic(cx, -v),
        None => cx.not_int(),
    }
}

/// Parses a float the way `string2ld` accepts it: no spaces, no NaN.
fn parse_float(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s.len() > 5120 || s[0].is_ascii_whitespace() || s[s.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let t = std::str::from_utf8(s).ok()?;
    let v: f64 = t.parse().ok()?;
    if v.is_nan() {
        return None;
    }
    Some(v)
}

/// Approximates `ld2string(..., LD_STR_HUMAN)`. Valkey computes in 80-bit
/// long double and prints 17 fractional digits, which hides most binary
/// rounding noise; Rust has no long double, so this uses the shortest decimal
/// that round-trips the f64 result. Matches Valkey for common inputs but can
/// differ in the last digits (documented as partial support).
fn fmt_float(v: f64) -> String {
    if v.is_infinite() {
        return if v > 0.0 { "inf".into() } else { "-inf".into() };
    }
    let s = format!("{v}");
    if s == "-0" {
        "0".into()
    } else {
        s
    }
}

fn cmd_incrbyfloat(cx: &mut Cx) {
    let key = cx.arg(1);
    let Some(incr) = parse_float(cx.arg(2)) else {
        return cx.err("ERR value is not a valid float");
    };
    let (h, i) = cx.find(key);
    let cur = match i {
        None => 0.0,
        Some(i) => match parse_float(cx.value(h, i)) {
            Some(v) => v,
            None => return cx.err("ERR value is not a valid float"),
        },
    };
    let v = cur + incr;
    if v.is_nan() || v.is_infinite() {
        return cx.err("ERR increment would produce NaN or Infinity");
    }
    let s = fmt_float(v);
    match i {
        Some(i) => {
            let lru = cx.lru;
            cx.shard(h).replace_value(i, s.as_bytes(), lru);
        }
        None => {
            cx.set(h, key, s.as_bytes(), 0);
        }
    }
    cx.r.bulk(s.as_bytes());
}

fn cmd_getrange(cx: &mut Cx) {
    let (Some(mut start), Some(mut end)) = (parse_i64(cx.arg(2)), parse_i64(cx.arg(3))) else {
        return cx.not_int();
    };
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let Some(i) = i else {
        return cx.r.bulk(b"");
    };
    let v = cx.value(h, i).to_vec();
    let len = v.len() as i64;
    if start < 0 && end < 0 && start > end {
        return cx.r.bulk(b"");
    }
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.max(0);
    end = end.max(0);
    if end >= len {
        end = len - 1;
    }
    if len == 0 || start > end {
        return cx.r.bulk(b"");
    }
    cx.r.bulk(&v[start as usize..=end as usize]);
}

fn cmd_setrange(cx: &mut Cx) {
    let Some(offset) = parse_i64(cx.arg(2)) else {
        return cx.not_int();
    };
    if offset < 0 {
        return cx.err("ERR offset is out of range");
    }
    let key = cx.arg(1);
    let val = cx.arg(3);
    let (h, i) = cx.find(key);
    let cur: Vec<u8> = match i {
        Some(i) => cx.value(h, i).to_vec(),
        None => Vec::new(),
    };
    if val.is_empty() {
        return cx.r.int(cur.len() as i64);
    }
    if !check_string_length(cx, offset as u64 + val.len() as u64) {
        return;
    }
    let mut v = cur;
    let end = offset as usize + val.len();
    if v.len() < end {
        v.resize(end, 0);
    }
    v[offset as usize..end].copy_from_slice(val);
    match i {
        Some(i) => {
            let lru = cx.lru;
            cx.shard(h).replace_value(i, &v, lru);
        }
        None => {
            cx.set(h, key, &v, 0);
        }
    }
    cx.r.int(v.len() as i64);
}

// ---------------------------------------------------------------------------
// Keyspace commands
// ---------------------------------------------------------------------------

fn cmd_del(cx: &mut Cx) {
    let mut n = 0;
    for j in 1..cx.argc() {
        let key = cx.arg(j);
        let (h, i) = cx.find(key);
        if let Some(i) = i {
            cx.remove(h, i);
            n += 1;
        }
    }
    cx.r.int(n);
}

fn cmd_exists(cx: &mut Cx) {
    let mut n = 0;
    for j in 1..cx.argc() {
        let key = cx.arg(j);
        if cx.find(key).1.is_some() {
            n += 1;
        }
    }
    cx.r.int(n);
}

fn cmd_type(cx: &mut Cx) {
    let key = cx.arg(1);
    let found = cx.find(key).1.is_some();
    cx.r.simple(if found { b"string" } else { b"none" });
}

const E_NX: u32 = 1;
const E_XX: u32 = 2;
const E_GT: u32 = 4;
const E_LT: u32 = 8;

fn expire_generic(cx: &mut Cx, base: u64, seconds: bool) {
    let mut flags = 0;
    for j in 3..cx.argc() {
        let opt = cx.arg(j);
        flags |= if eq_ic(opt, b"nx") {
            E_NX
        } else if eq_ic(opt, b"xx") {
            E_XX
        } else if eq_ic(opt, b"gt") {
            E_GT
        } else if eq_ic(opt, b"lt") {
            E_LT
        } else {
            let msg = format!("ERR Unsupported option {}", String::from_utf8_lossy(opt));
            return cx.err(&msg);
        };
    }
    if flags & E_NX != 0 && flags & (E_XX | E_GT | E_LT) != 0 {
        return cx.err("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if flags & E_GT != 0 && flags & E_LT != 0 {
        return cx.err("ERR GT and LT options at the same time are not compatible");
    }
    let Some(mut when) = parse_i64(cx.arg(2)) else {
        return cx.not_int();
    };
    if seconds {
        if when > i64::MAX / 1000 || when < i64::MIN / 1000 {
            return cx.expire_time_err();
        }
        when *= 1000;
    }
    let base = base as i64;
    if when > i64::MAX - base {
        return cx.expire_time_err();
    }
    when += base;
    if when < 0 {
        when = 0;
    }
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let Some(i) = i else {
        return cx.r.int(0);
    };
    let cur = cx.shard(h).slot(i).expire;
    let cur = if cur == 0 { -1 } else { cur as i64 };
    if (flags & E_NX != 0 && cur != -1)
        || (flags & E_XX != 0 && cur == -1)
        || (flags & E_GT != 0 && (when <= cur || cur == -1))
        || (flags & E_LT != 0 && cur != -1 && when >= cur)
    {
        return cx.r.int(0);
    }
    if when as u64 <= cx.now {
        cx.remove(h, i);
    } else {
        let hasher = cx.sh.store.hasher();
        cx.shard(h).set_expire(i, h, when as u64, hasher);
    }
    cx.r.int(1);
}

fn cmd_expire(cx: &mut Cx) {
    let now = cx.now;
    expire_generic(cx, now, true)
}
fn cmd_pexpire(cx: &mut Cx) {
    let now = cx.now;
    expire_generic(cx, now, false)
}
fn cmd_expireat(cx: &mut Cx) {
    expire_generic(cx, 0, true)
}
fn cmd_pexpireat(cx: &mut Cx) {
    expire_generic(cx, 0, false)
}

fn ttl_generic(cx: &mut Cx, ms: bool, abs: bool) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let Some(i) = i else {
        return cx.r.int(-2);
    };
    let exp = cx.shard(h).slot(i).expire;
    if exp == 0 {
        return cx.r.int(-1);
    }
    let mut ttl = if abs { exp as i64 } else { exp as i64 - cx.now as i64 };
    if ttl < 0 {
        ttl = 0;
    }
    cx.r.int(if ms { ttl } else { (ttl + 500) / 1000 });
}

fn cmd_ttl(cx: &mut Cx) {
    ttl_generic(cx, false, false)
}
fn cmd_pttl(cx: &mut Cx) {
    ttl_generic(cx, true, false)
}
fn cmd_expiretime(cx: &mut Cx) {
    // EXPIRETIME reports whole seconds (ms / 1000), unlike TTL's rounding.
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    let Some(i) = i else {
        return cx.r.int(-2);
    };
    let exp = cx.shard(h).slot(i).expire;
    cx.r.int(if exp == 0 { -1 } else { exp as i64 / 1000 });
}
fn cmd_pexpiretime(cx: &mut Cx) {
    ttl_generic(cx, true, true)
}

fn cmd_persist(cx: &mut Cx) {
    let key = cx.arg(1);
    let (h, i) = cx.find(key);
    match i {
        Some(i) if cx.shard(h).slot(i).expire != 0 => {
            let hasher = cx.sh.store.hasher();
            cx.shard(h).set_expire(i, h, 0, hasher);
            cx.r.int(1);
        }
        _ => cx.r.int(0),
    }
}

fn rename_generic(cx: &mut Cx, nx: bool) {
    let (src, dst) = (cx.arg(1), cx.arg(2));
    let (hs, i) = cx.find(src);
    let Some(i) = i else {
        return cx.err("ERR no such key");
    };
    if src == dst {
        return if nx { cx.r.int(0) } else { cx.r.ok() };
    }
    let (val, exp) = {
        let s = cx.shard(hs).slot(i);
        (s.value().to_vec(), s.expire)
    };
    let (hd, j) = cx.find(dst);
    if nx && j.is_some() {
        return cx.r.int(0);
    }
    // Remove the source first: its slab position is still valid because the
    // destination lookup above can only have removed an expired destination.
    let (hs2, i2) = cx.find(src);
    let _ = hs2;
    cx.remove(hs, i2.expect("source vanished"));
    cx.set(hd, dst, &val, exp);
    if nx {
        cx.r.int(1)
    } else {
        cx.r.ok()
    }
}

fn cmd_rename(cx: &mut Cx) {
    rename_generic(cx, false)
}
fn cmd_renamenx(cx: &mut Cx) {
    rename_generic(cx, true)
}

/// `stringmatchlen` glob: * ? [set] [^set] [a-z] and backslash escapes.
pub fn glob_match(pat: &[u8], s: &[u8], nocase: bool) -> bool {
    fn eq(a: u8, b: u8, nocase: bool) -> bool {
        if nocase {
            a.eq_ignore_ascii_case(&b)
        } else {
            a == b
        }
    }
    let (mut p, mut i) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while i < s.len() {
        if p < pat.len() {
            match pat[p] {
                b'*' => {
                    while p + 1 < pat.len() && pat[p + 1] == b'*' {
                        p += 1;
                    }
                    star = Some((p, i));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    i += 1;
                    continue;
                }
                b'[' => {
                    let mut q = p + 1;
                    let not = q < pat.len() && pat[q] == b'^';
                    if not {
                        q += 1;
                    }
                    let mut matched = false;
                    while q < pat.len() && pat[q] != b']' {
                        if pat[q] == b'\\' && q + 1 < pat.len() {
                            q += 1;
                            if eq(pat[q], s[i], nocase) {
                                matched = true;
                            }
                        } else if q + 2 < pat.len() && pat[q + 1] == b'-' {
                            let (mut lo, mut hi) = (pat[q], pat[q + 2]);
                            if lo > hi {
                                std::mem::swap(&mut lo, &mut hi);
                            }
                            let c = if nocase { s[i].to_ascii_lowercase() } else { s[i] };
                            let (lo, hi) = if nocase { (lo.to_ascii_lowercase(), hi.to_ascii_lowercase()) } else { (lo, hi) };
                            if c >= lo && c <= hi {
                                matched = true;
                            }
                            q += 2;
                        } else if eq(pat[q], s[i], nocase) {
                            matched = true;
                        }
                        q += 1;
                    }
                    if not {
                        matched = !matched;
                    }
                    if matched {
                        p = q + 1;
                        i += 1;
                        continue;
                    }
                }
                b'\\' if p + 1 < pat.len() => {
                    if eq(pat[p + 1], s[i], nocase) {
                        p += 2;
                        i += 1;
                        continue;
                    }
                }
                c => {
                    if eq(c, s[i], nocase) {
                        p += 1;
                        i += 1;
                        continue;
                    }
                }
            }
        }
        match star {
            Some((sp, si)) => {
                p = sp + 1;
                i = si + 1;
                star = Some((sp, si + 1));
            }
            None => return false,
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

fn cmd_keys(cx: &mut Cx) {
    let pat = cx.arg(1);
    let now = cx.now;
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (_, g) in cx.all_shards().iter() {
        for i in 0..g.len() {
            let s = g.slot(i);
            if s.expire != 0 && s.expire <= now {
                continue;
            }
            if glob_match(pat, s.key(), false) {
                out.push(s.key().to_vec());
            }
        }
    }
    cx.r.array(out.len());
    for k in out {
        cx.r.bulk(&k);
    }
}

fn cmd_randomkey(cx: &mut Cx) {
    let now = cx.now;
    let mut rng = cx.tc.next_rand();
    let total: usize = cx.all_shards().iter().map(|(_, g)| g.len()).sum();
    if total == 0 {
        return cx.r.null();
    }
    for _ in 0..100 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let mut n = (rng % total as u64) as usize;
        let mut pick = None;
        for (_, g) in cx.all_shards().iter() {
            if n < g.len() {
                let s = g.slot(n);
                if s.expire == 0 || s.expire > now {
                    pick = Some(s.key().to_vec());
                }
                break;
            }
            n -= g.len();
        }
        if let Some(k) = pick {
            return cx.r.bulk(&k);
        }
    }
    cx.r.null();
}

fn cmd_dbsize(cx: &mut Cx) {
    let n: usize = cx.all_shards().iter().map(|(_, g)| g.len()).sum();
    cx.r.int(n as i64);
}

fn cmd_flushall(cx: &mut Cx) {
    if cx.argc() > 2 || (cx.argc() == 2 && !eq_ic(cx.arg(1), b"async") && !eq_ic(cx.arg(1), b"sync")) {
        return cx.syntax_err();
    }
    for (_, g) in cx.all_shards().iter_mut() {
        g.clear();
    }
    cx.r.ok();
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

fn cmd_multi(cx: &mut Cx) {
    cx.conn.multi = Some(Multi { queued: Vec::new(), errored: false });
    cx.r.ok();
}

fn cmd_discard(cx: &mut Cx) {
    if cx.conn.multi.take().is_none() {
        return cx.err("ERR DISCARD without MULTI");
    }
    unwatch_all(&cx.sh.store, cx.conn);
    cx.r.ok();
}

fn cmd_exec(_cx: &mut Cx) {
    unreachable!("EXEC is dispatched by run_command");
}

/// EXEC: locks every shard any queued command touches, in ascending order,
/// then runs the queue. Other threads cannot observe a partial transaction.
fn exec_transaction(sh: &Shared, tc: &mut ThreadCtx, conn: &mut ConnState, mut r: Reply) {
    let Some(m) = conn.multi.take() else {
        r.error("ERR EXEC without MULTI");
        Stats::add(&tc.stats().errors, 1);
        return;
    };
    if m.errored {
        unwatch_all(&sh.store, conn);
        r.error("EXECABORT Transaction discarded because of previous errors.");
        Stats::add(&tc.stats().errors, 1);
        return;
    }
    // Lock the shards of every queued command *and* of every watched key,
    // then check the WATCH flag while holding them. Writers set the flag
    // under the shard lock, so no modification can slip in between the check
    // and the execution. (Checking before locking lost updates; found by
    // tests/concurrency.py watch_cas.)
    let mut ids: Vec<usize> = Vec::new();
    for key in &conn.watched {
        ids.push(Store::shard_of(sh.store.hash(key)));
    }
    let mut all = false;
    let mut ks = Vec::new();
    for q in &m.queued {
        if let Some(spec) = lookup(&q[0]) {
            match spec.keys {
                Keys::All => all = true,
                Keys::None => {}
                Keys::Range(..) => {
                    let argv: Vec<&[u8]> = q.iter().map(|a| a.as_slice()).collect();
                    key_shards(sh, spec.keys, &argv, &mut ks);
                    ids.extend(ks.iter().map(|k| k.0));
                }
            }
        }
    }
    if all {
        ids = (0..NSHARDS).collect();
    }
    ids.sort_unstable();
    ids.dedup();
    let guards: Vec<(usize, MutexGuard<Shard>)> = ids.into_iter().map(|s| (s, sh.store.lock(s))).collect();
    if conn.watch_flag.load(Ordering::Relaxed) {
        drop(guards);
        unwatch_all(&sh.store, conn);
        r.null_array();
        return;
    }
    r.array(m.queued.len());
    let out = r.out;
    let resp3 = conn.resp3;
    let mut locks = Locks::Many(guards);
    for q in &m.queued {
        let argv: Vec<&[u8]> = q.iter().map(|a| a.as_slice()).collect();
        let spec = lookup(argv[0]).expect("validated at queue time");
        Stats::add(&tc.stats().commands, 1);
        let now = now_ms();
        let mut cx = Cx {
            sh,
            tc: &mut *tc,
            conn: &mut *conn,
            argv,
            r: Reply { out: &mut *out, resp3 },
            locks,
            now,
            lru: (now / 1000) as u32,
            fx: Effects::default(),
        };
        if spec.name == "exec" || spec.name == "multi" {
            cx.err("ERR Command not allowed inside a transaction");
        } else {
            (spec.run)(&mut cx);
        }
        finish(&mut cx);
        locks = std::mem::replace(&mut cx.locks, Locks::None);
        if cx.conn.resp3 != resp3 {
            // HELLO inside MULTI switches protocol for later replies.
        }
    }
    drop(locks);
    unwatch_all(&sh.store, conn);
}

fn cmd_watch(cx: &mut Cx) {
    if cx.conn.watch_flag.load(Ordering::Relaxed) {
        return cx.r.ok();
    }
    for j in 1..cx.argc() {
        let key = cx.arg(j);
        if cx.conn.watched.iter().any(|k| k == key) {
            continue;
        }
        let h = cx.store().hash(key);
        let flag = cx.conn.watch_flag.clone();
        cx.shard(h).watch(key, flag);
        cx.conn.watched.push(key.to_vec());
    }
    cx.r.ok();
}

fn cmd_unwatch(cx: &mut Cx) {
    unwatch_all(&cx.sh.store, cx.conn);
    cx.r.ok();
}

// ---------------------------------------------------------------------------
// Connection and server commands
// ---------------------------------------------------------------------------

fn cmd_ping(cx: &mut Cx) {
    match cx.argc() {
        1 => cx.r.simple(b"PONG"),
        2 => {
            let m = cx.arg(1);
            cx.r.bulk(m)
        }
        _ => cx.err("ERR wrong number of arguments for 'ping' command"),
    }
}

fn cmd_echo(cx: &mut Cx) {
    let m = cx.arg(1);
    cx.r.bulk(m);
}

fn cmd_select(cx: &mut Cx) {
    match parse_i64(cx.arg(1)) {
        None => cx.not_int(),
        Some(0) => cx.r.ok(),
        Some(_) => cx.err("ERR DB index is out of range"),
    }
}

fn cmd_quit(cx: &mut Cx) {
    cx.conn.close_after_reply = true;
    cx.r.ok();
}

fn cmd_reset(cx: &mut Cx) {
    cx.conn.multi = None;
    unwatch_all(&cx.sh.store, cx.conn);
    cx.conn.resp3 = false;
    cx.r.resp3 = false;
    cx.conn.name = None;
    if cx.sh.cfg.requirepass.is_some() {
        cx.conn.authed = false;
    }
    cx.r.simple(b"RESET");
}

fn check_password(cx: &Cx, user: Option<&[u8]>, pass: &[u8]) -> bool {
    match &cx.sh.cfg.requirepass {
        Some(p) => user.map(|u| u == b"default").unwrap_or(true) && p.as_slice() == pass,
        None => user.map(|u| u == b"default").unwrap_or(true),
    }
}

fn cmd_auth(cx: &mut Cx) {
    if cx.argc() > 3 {
        return cx.syntax_err();
    }
    if cx.argc() == 2 && cx.sh.cfg.requirepass.is_none() {
        return cx.err("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
    }
    let (user, pass) = if cx.argc() == 3 { (Some(cx.arg(1)), cx.arg(2)) } else { (None, cx.arg(1)) };
    if check_password(cx, user, pass) {
        cx.conn.authed = true;
        cx.r.ok();
    } else {
        cx.err("WRONGPASS invalid username-password pair or user is disabled.");
    }
}

fn cmd_hello(cx: &mut Cx) {
    let mut ver = if cx.conn.resp3 { 3 } else { 2 };
    let mut j = 1;
    if cx.argc() >= 2 {
        match parse_i64(cx.arg(1)) {
            Some(v) => ver = v,
            None => return cx.err("ERR Protocol version is not an integer or out of range"),
        }
        j = 2;
        if !(2..=3).contains(&ver) {
            return cx.err("NOPROTO unsupported protocol version");
        }
    }
    let mut setname = None;
    while j < cx.argc() {
        let opt = cx.arg(j);
        if eq_ic(opt, b"auth") && j + 2 < cx.argc() {
            let (u, p) = (cx.arg(j + 1), cx.arg(j + 2));
            if !check_password(cx, Some(u), p) {
                return cx.err("WRONGPASS invalid username-password pair or user is disabled.");
            }
            cx.conn.authed = true;
            j += 3;
        } else if eq_ic(opt, b"setname") && j + 1 < cx.argc() {
            setname = Some(cx.arg(j + 1).to_vec());
            j += 2;
        } else {
            let msg = format!("ERR Syntax error in HELLO option '{}'", String::from_utf8_lossy(opt));
            return cx.err(&msg);
        }
    }
    if !cx.conn.authed {
        return cx.err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    if let Some(n) = setname {
        cx.conn.name = Some(n);
    }
    cx.conn.resp3 = ver == 3;
    cx.r.resp3 = ver == 3;
    let id = cx.conn.id as i64;
    cx.r.map(7);
    cx.r.bulk(b"server");
    cx.r.bulk(b"rvk");
    cx.r.bulk(b"version");
    cx.r.bulk(env!("CARGO_PKG_VERSION").as_bytes());
    cx.r.bulk(b"proto");
    cx.r.int(ver);
    cx.r.bulk(b"id");
    cx.r.int(id);
    cx.r.bulk(b"mode");
    cx.r.bulk(b"standalone");
    cx.r.bulk(b"role");
    cx.r.bulk(b"master");
    cx.r.bulk(b"modules");
    cx.r.array(0);
}

fn cmd_client(cx: &mut Cx) {
    let sub = lower(cx.arg(1));
    match (sub.as_str(), cx.argc()) {
        ("id", 2) => {
            let id = cx.conn.id as i64;
            cx.r.int(id)
        }
        ("getname", 2) => match cx.conn.name.clone() {
            Some(n) => cx.r.bulk(&n),
            None => cx.r.null(),
        },
        ("setname", 3) => {
            let n = cx.arg(2);
            if n.iter().any(|&b| b <= b' ' || b > b'~') {
                return cx.err("ERR Client names cannot contain spaces, newlines or special characters.");
            }
            cx.conn.name = if n.is_empty() { None } else { Some(n.to_vec()) };
            cx.r.ok()
        }
        ("setinfo", 4) => cx.r.ok(),
        ("info", 2) | ("list", 2) => {
            let line = format!(
                "id={} addr=? name={} db=0 multi={} resp={} lib-name= lib-ver=\n",
                cx.conn.id,
                cx.conn.name.as_deref().map(|n| String::from_utf8_lossy(n).into_owned()).unwrap_or_default(),
                cx.conn.multi.as_ref().map(|m| m.queued.len() as i64).unwrap_or(-1),
                if cx.conn.resp3 { 3 } else { 2 }
            );
            let line = if sub == "info" { line } else { line };
            cx.r.verbatim(line.as_bytes())
        }
        _ => {
            let msg = format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(cx.arg(1)).chars().take(128).collect::<String>()
            );
            cx.err(&msg)
        }
    }
}

fn cmd_command(cx: &mut Cx) {
    if cx.argc() == 1 {
        return cx.r.array(0);
    }
    match lower(cx.arg(1)).as_str() {
        "count" => cx.r.int(SUPPORTED.len() as i64),
        "docs" => cx.r.map(0),
        "info" => {
            let n = cx.argc() - 2;
            cx.r.array(n);
            for _ in 0..n {
                cx.r.null_array();
            }
        }
        _ => {
            let msg = format!("ERR unknown subcommand '{}'. Try COMMAND HELP.", String::from_utf8_lossy(cx.arg(1)));
            cx.err(&msg)
        }
    }
}

/// Every command name the dispatcher accepts (COMMAND COUNT, INFO).
pub const SUPPORTED: &[&str] = &[
    "get", "set", "setnx", "setex", "psetex", "getex", "getdel", "getset", "mget", "mset", "msetnx", "append", "strlen",
    "incr", "decr", "incrby", "decrby", "incrbyfloat", "getrange", "setrange", "del", "unlink", "exists", "type", "expire",
    "pexpire", "expireat", "pexpireat", "ttl", "pttl", "expiretime", "pexpiretime", "persist", "rename", "renamenx", "keys",
    "randomkey", "dbsize", "flushall", "flushdb", "multi", "exec", "discard", "watch", "unwatch", "ping", "echo", "select",
    "quit", "reset", "hello", "auth", "client", "command", "config", "info", "time", "debug", "shutdown",
    "function", "script",
];

fn config_items(cx: &Cx) -> Vec<(&'static str, String)> {
    let policy = *cx.sh.policy.read().unwrap();
    vec![
        ("save", String::new()),
        ("appendonly", "no".into()),
        ("maxmemory", cx.sh.maxmemory.load(Ordering::Relaxed).to_string()),
        ("maxmemory-policy", policy.name().into()),
        ("maxmemory-samples", cx.sh.cfg.maxmemory_samples.to_string()),
        ("port", cx.sh.port.to_string()),
        ("bind", cx.sh.cfg.bind.clone()),
        ("databases", "1".into()),
        ("maxclients", cx.sh.cfg.maxclients.to_string()),
        ("hz", cx.sh.cfg.hz.to_string()),
        ("io-threads", cx.sh.cfg.threads.to_string()),
        ("proto-max-bulk-len", cx.sh.cfg.proto_max_bulk_len.to_string()),
        ("client-query-buffer-limit", cx.sh.cfg.client_query_buffer_limit.to_string()),
        ("timeout", "0".into()),
        ("protected-mode", "no".into()),
    ]
}

fn cmd_config(cx: &mut Cx) {
    let sub = lower(cx.arg(1));
    match sub.as_str() {
        "get" if cx.argc() >= 3 => {
            let items = config_items(cx);
            let mut hits: Vec<(&str, String)> = Vec::new();
            for j in 2..cx.argc() {
                let pat = cx.arg(j).to_vec();
                for (k, v) in &items {
                    if glob_match(&pat, k.as_bytes(), true) && !hits.iter().any(|(h, _)| h == k) {
                        hits.push((k, v.clone()));
                    }
                }
            }
            cx.r.map(hits.len());
            for (k, v) in hits {
                cx.r.bulk(k.as_bytes());
                cx.r.bulk(v.as_bytes());
            }
        }
        "set" if cx.argc() >= 4 && cx.argc() % 2 == 0 => {
            let mut j = 2;
            while j < cx.argc() {
                let (k, v) = (lower(cx.arg(j)), cx.arg(j + 1));
                let ok = match k.as_str() {
                    "maxmemory" => std::str::from_utf8(v)
                        .ok()
                        .and_then(crate_parse_memory)
                        .map(|m| cx.sh.maxmemory.store(m, Ordering::Relaxed))
                        .is_some(),
                    "maxmemory-policy" => Policy::parse(v).map(|p| *cx.sh.policy.write().unwrap() = p).is_some(),
                    "save" | "appendonly" => v.is_empty() || v == b"no",
                    _ => false,
                };
                if !ok {
                    let msg = format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - not supported by this proof of concept",
                        k
                    );
                    return cx.err(&msg);
                }
                j += 2;
            }
            cx.r.ok();
        }
        "resetstat" if cx.argc() == 2 => {
            cx.sh.reset_epoch.fetch_add(1, Ordering::Relaxed);
            // The calling thread's counters reset immediately; others at their
            // next loop iteration.
            cx.tc.stats().reset();
            cx.r.ok();
        }
        _ => {
            let msg = format!("ERR unknown subcommand '{}'. Try CONFIG HELP.", String::from_utf8_lossy(cx.arg(1)));
            cx.err(&msg);
        }
    }
}

fn crate_parse_memory(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    for (suf, mul) in [("gb", 1usize << 30), ("mb", 1 << 20), ("kb", 1 << 10)] {
        if let Some(n) = s.strip_suffix(suf) {
            return n.parse::<usize>().ok().map(|v| v * mul);
        }
    }
    s.parse().ok()
}

fn rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<usize>().ok()))
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

fn cmd_info(cx: &mut Cx) {
    let sh = cx.sh;
    let mut tot = [0u64; 10];
    for s in &sh.stats {
        for (k, c) in [
            &s.commands, &s.net_in, &s.net_out, &s.hits, &s.misses, &s.expired, &s.evicted, &s.errors, &s.conns_received,
            &s.rejected,
        ]
        .iter()
        .enumerate()
        {
            tot[k] += c.load(Ordering::Relaxed);
        }
    }
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let rss = rss_bytes();
    let (mut keys, mut volatile) = (0usize, 0usize);
    for i in 0..NSHARDS {
        let g = sh.store.lock(i);
        keys += g.len();
        volatile += g.volatile();
    }
    let up = sh.started.elapsed().as_secs();
    let wanted: Vec<String> = (1..cx.argc()).map(|j| lower(cx.arg(j))).collect();
    let all = wanted.is_empty() || wanted.iter().any(|w| matches!(w.as_str(), "all" | "everything" | "default"));
    let want = |name: &str| all || wanted.iter().any(|w| w == name);
    let mut t = String::with_capacity(2048);
    if want("server") {
    let _ = write!(
        t,
        "# Server\r\nrvk_version:{}\r\nserver_name:rvk\r\nrun_id:{}\r\nprocess_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\nio_threads_active:0\r\nworker_threads:{}\r\n\r\n",
        env!("CARGO_PKG_VERSION"),
        sh.run_id,
        std::process::id(),
        sh.port,
        up,
        sh.cfg.threads
    );
    }
    if want("clients") {
    let _ = write!(t, "# Clients\r\nconnected_clients:{}\r\nmaxclients:{}\r\n\r\n", sh.connected.load(Ordering::Relaxed), sh.cfg.maxclients);
    }
    let policy = *sh.policy.read().unwrap();
    if want("memory") {
    let _ = write!(
        t,
        "# Memory\r\nused_memory:{}\r\nused_memory_rss:{}\r\nused_memory_dataset_estimate:{}\r\nmem_fragmentation_ratio:{:.2}\r\nmaxmemory:{}\r\nmaxmemory_policy:{}\r\n\r\n",
        allocated,
        rss,
        sh.store.used_memory(),
        if allocated > 0 { rss as f64 / allocated as f64 } else { 0.0 },
        sh.maxmemory.load(Ordering::Relaxed),
        policy.name()
    );
    }
    if want("persistence") {
    let _ = write!(t, "# Persistence\r\nloading:0\r\naof_enabled:0\r\nrdb_changes_since_last_save:0\r\n\r\n");
    }
    if want("stats") {
    let _ = write!(
        t,
        "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\nrejected_connections:{}\r\nexpired_keys:{}\r\nevicted_keys:{}\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\ntotal_error_replies:{}\r\n\r\n",
        tot[8], tot[0], tot[1], tot[2], tot[9], tot[5], tot[6], tot[3], tot[4], tot[7]
    );
    }
    if want("keyspace") {
        let _ = write!(t, "# Keyspace\r\n");
        if keys > 0 {
            let _ = write!(t, "db0:keys={keys},expires={volatile},avg_ttl=0\r\n");
        }
    }
    cx.r.verbatim(t.as_bytes());
}

fn cmd_time(cx: &mut Cx) {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    cx.r.array(2);
    cx.r.bulk(d.as_secs().to_string().as_bytes());
    cx.r.bulk(d.subsec_micros().to_string().as_bytes());
}

fn cmd_debug(cx: &mut Cx) {
    let sub = lower(cx.arg(1));
    match sub.as_str() {
        "set-active-expire" if cx.argc() == 3 => {
            // Applies to the calling worker only; with --threads 1 this
            // matches Valkey. Documented as partial in FEATURE_MATRIX.md.
            cx.tc.active_expire = cx.arg(2) != b"0";
            cx.r.ok();
        }
        "sleep" if cx.argc() == 3 => {
            let secs: f64 = std::str::from_utf8(cx.arg(2)).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            std::thread::sleep(std::time::Duration::from_secs_f64(secs.max(0.0)));
            cx.r.ok();
        }
        _ => {
            let msg = format!("ERR DEBUG subcommand '{}' is not supported by this proof of concept", sub);
            cx.err(&msg)
        }
    }
}

/// Scripting and functions are out of scope. With none loaded, FLUSH is
/// trivially correct (the Valkey test harness calls FUNCTION FLUSH between
/// tests); every other subcommand is refused.
fn cmd_function_script(cx: &mut Cx) {
    let what = lower(cx.arg(0));
    let sub = lower(cx.arg(1));
    if sub == "flush" && (cx.argc() == 2 || (cx.argc() == 3 && matches!(lower(cx.arg(2)).as_str(), "async" | "sync"))) {
        return cx.r.ok();
    }
    let msg = format!("ERR {} {} is not supported by this proof of concept", what.to_uppercase(), sub.to_uppercase());
    cx.err(&msg)
}

fn cmd_shutdown(cx: &mut Cx) {
    cx.tc.shutdown_requested = true;
    cx.conn.close_after_reply = true;
    cx.r.ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob() {
        assert!(glob_match(b"*", b"anything", false));
        assert!(glob_match(b"h?llo", b"hello", false));
        assert!(glob_match(b"h*llo", b"heeello", false));
        assert!(glob_match(b"h[ae]llo", b"hallo", false));
        assert!(!glob_match(b"h[^e]llo", b"hello", false));
        assert!(glob_match(b"h[a-b]llo", b"hbllo", false));
        assert!(glob_match(b"h\\*llo", b"h*llo", false));
        assert!(!glob_match(b"h\\*llo", b"hello", false));
        assert!(glob_match(b"MAXMEMORY*", b"maxmemory-policy", true));
        assert!(!glob_match(b"a*b", b"acd", false));
        assert!(glob_match(b"*a*b*", b"xxaxxbxx", false));
    }

    #[test]
    fn floats() {
        assert_eq!(fmt_float(10.5), "10.5");
        assert_eq!(fmt_float(10.5 + 0.1), "10.6");
        assert_eq!(fmt_float(3.0), "3");
        assert_eq!(fmt_float(-0.0), "0");
        assert_eq!(parse_float(b" 1"), None);
        assert_eq!(parse_float(b"1e3"), Some(1000.0));
        assert_eq!(parse_float(b"nan"), None);
    }

    #[test]
    fn set_options() {
        let parse = |v: &[&[u8]]| parse_ext(v, 0, OptKind::Set).map(|o| o.flags);
        assert_eq!(parse(&[b"nx", b"nx"]), Some(NX));
        assert_eq!(parse(&[b"nx", b"xx"]), None);
        assert_eq!(parse(&[b"ex", b"10", b"px", b"5"]), None);
        assert_eq!(parse(&[b"ex", b"10", b"ex", b"5"]), Some(EX));
        assert_eq!(parse(&[b"keepttl", b"ex", b"5"]), None);
        assert_eq!(parse(&[b"ex"]), None);
        assert_eq!(parse(&[b"get", b"xx"]), Some(GET | XX));
        assert_eq!(parse(&[b"persist"]), None);
        assert_eq!(parse_ext(&[b"persist"], 0, OptKind::Get).map(|o| o.flags), Some(PERSIST));
    }
}
