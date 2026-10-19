//! Lock-sharded keyspace.
//!
//! The keyspace is split into `NSHARDS` shards, each behind its own mutex, so
//! any worker thread can execute any command after locking only the shards it
//! touches (memcached/KeyDB style). Multi-key commands lock their shards in
//! ascending order, which makes them atomic and deadlock-free.
//!
//! Per shard, entries live directly in a `hashbrown::HashTable<Slot>`, so a
//! lookup touches the control bytes, the entry, and the key/value allocation:
//! one dependent cache miss fewer than an index pointing into a separate slab
//! (the profile of the first version spent ~40% of pipelined CPU in lookups).
//! Entries are addressed by bucket index, which stays valid until the next
//! insertion. Random sampling for eviction picks random buckets. Key and value
//! share one allocation.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Keyed SipHash-1-3 with a random per-process key: like Valkey's keyed
/// SipHash, it keeps clients from choosing colliding keys (hash flooding).
use std::hash::RandomState;
use hashbrown::HashTable;
use std::hash::BuildHasher;

pub const NSHARDS: usize = 64;

/// Bookkeeping bytes charged per entry on top of key and value bytes: the
/// 32-byte bucket and its control byte at a typical load factor, plus allocator
/// rounding. Only used between jemalloc snapshots (see `cmd::used_memory`).
pub const ENTRY_OVERHEAD: usize = 56;

pub struct Slot {
    /// Key bytes followed by value bytes.
    data: Box<[u8]>,
    klen: u32,
    /// LRU clock (seconds, wrapping) of the last access.
    pub lru: u32,
    /// Absolute expiry in Unix milliseconds; 0 means no expiry.
    pub expire: u64,
}

impl Slot {
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.data[..self.klen as usize]
    }
    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.data[self.klen as usize..]
    }
    #[inline]
    fn cost(&self) -> usize {
        self.data.len() + ENTRY_OVERHEAD
    }
}

fn make_data(key: &[u8], val: &[u8]) -> Box<[u8]> {
    let mut v = Vec::with_capacity(key.len() + val.len());
    v.extend_from_slice(key);
    v.extend_from_slice(val);
    v.into_boxed_slice()
}

/// A connection's WATCH flag: set to true when any key it watches is touched.
pub type WatchFlag = Arc<AtomicBool>;

/// One connection watching one key (Valkey's `watchedKey`).
struct Watcher {
    flag: WatchFlag,
    /// The key was logically expired when WATCH ran (`wk->expired`). Deleting
    /// such a key later is no logical change, so it does not dirty the watch.
    expired: bool,
}

/// Lock-free mirrors of a shard's counters, one cache line per shard so that
/// threads publishing different shards do not contend.
#[derive(Default)]
#[repr(align(128))]
struct Published {
    mem: AtomicUsize,
    keys: AtomicUsize,
    volatile: AtomicUsize,
}

#[repr(align(128))]
struct PaddedShard(Mutex<Shard>);

pub struct Shard {
    index: HashTable<Slot>,
    /// Min-heap of (expire_ms, key hash) for active expiry; may hold stale
    /// items for keys that were overwritten or persisted.
    expires: BinaryHeap<Reverse<(u64, u64)>>,
    volatile: usize,
    mem: usize,
    watched: HashMap<Box<[u8]>, Vec<Watcher>>,
}

pub struct Store {
    shards: Box<[PaddedShard]>,
    /// Mirrors of each shard's counters, readable without the lock (INFO,
    /// `maxmemory` checks).
    published: Box<[Published]>,
    hasher: RandomState,
}

impl Store {
    pub fn new() -> Self {
        let shards = (0..NSHARDS)
            .map(|_| {
                PaddedShard(Mutex::new(Shard {
                    index: HashTable::new(),
                    expires: BinaryHeap::new(),
                    volatile: 0,
                    mem: 0,
                    watched: HashMap::new(),
                }))
            })
            .collect();
        let published = (0..NSHARDS).map(|_| Published::default()).collect();
        Store { shards, published, hasher: RandomState::default() }
    }

    #[inline]
    pub fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Shard index from hash bits that hashbrown uses for neither the bucket
    /// index (low bits) nor the control byte (top 7 bits).
    #[inline]
    pub fn shard_of(h: u64) -> usize {
        ((h >> 40) as usize) & (NSHARDS - 1)
    }

    #[inline]
    pub fn lock(&self, shard: usize) -> MutexGuard<'_, Shard> {
        self.shards[shard].0.lock().unwrap()
    }

    /// Publishes a shard's counters after an operation (lock held). Stores
    /// only what changed, so read-only commands do not write shared memory.
    #[inline]
    pub fn publish_mem(&self, shard: usize, s: &Shard) {
        let p = &self.published[shard];
        for (a, v) in [(&p.mem, s.mem), (&p.keys, s.index.len()), (&p.volatile, s.volatile)] {
            if a.load(Ordering::Relaxed) != v {
                a.store(v, Ordering::Relaxed);
            }
        }
    }

    /// Approximate dataset memory, readable without locks.
    pub fn used_memory(&self) -> usize {
        self.published.iter().map(|p| p.mem.load(Ordering::Relaxed)).sum()
    }

    /// (keys, keys with an expiry) as last published, readable without locks.
    pub fn key_counts(&self) -> (usize, usize) {
        self.published.iter().fold((0, 0), |(k, v), p| {
            (k + p.keys.load(Ordering::Relaxed), v + p.volatile.load(Ordering::Relaxed))
        })
    }

    pub fn hasher(&self) -> &RandomState {
        &self.hasher
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome counters a shard operation reports back for statistics.
#[derive(Default, Clone, Copy)]
pub struct Effects {
    pub expired: u64,
}

impl Shard {
    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
    /// The entry at bucket `i`, which must be occupied.
    #[inline]
    pub fn slot(&self, i: usize) -> &Slot {
        self.index.get_bucket(i).expect("empty bucket")
    }
    #[inline]
    fn slot_mut(&mut self, i: usize) -> &mut Slot {
        self.index.get_bucket_mut(i).expect("empty bucket")
    }
    pub fn iter(&self) -> impl Iterator<Item = &Slot> {
        self.index.iter()
    }

    #[inline]
    fn find(&self, h: u64, key: &[u8]) -> Option<usize> {
        self.index.find_bucket_index(h, |s| s.key() == key)
    }

    /// Looks up a key, deleting it first if it has expired (lazy expiry).
    #[inline]
    pub fn lookup(&mut self, h: u64, key: &[u8], now: u64, fx: &mut Effects, hasher: &RandomState) -> Option<usize> {
        let i = self.find(h, key)?;
        let exp = self.slot(i).expire;
        // Valkey's timestampIsExpired: expired once now > expire.
        if exp != 0 && exp < now {
            self.remove_at(i, h, hasher);
            fx.expired += 1;
            return None;
        }
        Some(i)
    }

    /// Like `lookup` but also refreshes the entry's LRU clock.
    #[inline]
    pub fn touch_lookup(&mut self, h: u64, key: &[u8], now: u64, lru: u32, fx: &mut Effects, hasher: &RandomState) -> Option<usize> {
        let i = self.lookup(h, key, now, fx, hasher)?;
        self.slot_mut(i).lru = lru;
        Some(i)
    }

    /// Inserts or overwrites `key` with `val`. `expire` is the new absolute
    /// expiry (0 = none). Returns true if the key already existed.
    pub fn set(&mut self, h: u64, key: &[u8], val: &[u8], expire: u64, lru: u32, hasher: &RandomState) -> bool {
        self.notify_watchers(key, true);
        if let Some(i) = self.find(h, key) {
            let old = self.slot_mut(i);
            let (old_cost, old_volatile) = (old.cost(), old.expire != 0);
            old.data = make_data(key, val);
            old.expire = expire;
            old.lru = lru;
            let new_cost = old.cost();
            self.mem = self.mem - old_cost + new_cost;
            if old_volatile {
                self.volatile -= 1;
            }
            if expire != 0 {
                self.volatile += 1;
                self.expires.push(Reverse((expire, h)));
                self.maybe_compact_heap(hasher);
            }
            return true;
        }
        let slot = Slot { data: make_data(key, val), klen: key.len() as u32, lru, expire };
        self.mem += slot.cost();
        self.index.insert_unique(h, slot, |s| hasher.hash_one(s.key()));
        if expire != 0 {
            self.volatile += 1;
            self.expires.push(Reverse((expire, h)));
        }
        self.maybe_compact_heap(hasher);
        false
    }

    /// Replaces the value of an existing entry, keeping key and expiry.
    pub fn replace_value(&mut self, i: usize, val: &[u8], lru: u32) {
        self.notify_watchers_at(i, true);
        let s = self.slot_mut(i);
        let key_len = s.klen as usize;
        let mut v = Vec::with_capacity(key_len + val.len());
        v.extend_from_slice(&s.data[..key_len]);
        v.extend_from_slice(val);
        let old_cost = s.cost();
        s.data = v.into_boxed_slice();
        s.lru = lru;
        let new_cost = s.cost();
        self.mem = self.mem - old_cost + new_cost;
    }

    /// Sets or clears the expiry of an existing entry.
    pub fn set_expire(&mut self, i: usize, h: u64, expire: u64, hasher: &RandomState) {
        self.notify_watchers_at(i, true);
        let s = self.slot_mut(i);
        let was_volatile = s.expire != 0;
        s.expire = expire;
        if was_volatile {
            self.volatile -= 1;
        }
        if expire != 0 {
            self.volatile += 1;
            self.expires.push(Reverse((expire, h)));
            self.maybe_compact_heap(hasher);
        }
    }

    /// Removes the entry in bucket `i`. Other entries keep their buckets.
    pub fn remove_at(&mut self, i: usize, _h: u64, _hasher: &RandomState) {
        self.notify_watchers_at(i, false);
        let removed = match self.index.get_bucket_entry(i) {
            Ok(e) => e.remove().0,
            Err(_) => unreachable!("removing an empty bucket"),
        };
        self.mem -= removed.cost();
        if removed.expire != 0 {
            self.volatile -= 1;
        }
    }

    /// Deletes a key if present (expired keys count as absent).
    pub fn delete(&mut self, h: u64, key: &[u8], now: u64, fx: &mut Effects, hasher: &RandomState) -> bool {
        match self.lookup(h, key, now, fx, hasher) {
            Some(i) => {
                self.remove_at(i, h, hasher);
                true
            }
            None => false,
        }
    }

    /// FLUSHALL/FLUSHDB for this shard. Like `touchAllWatchedKeysInDb`, only
    /// watches on keys that exist are touched.
    pub fn clear(&mut self, hasher: &RandomState) {
        if !self.watched.is_empty() {
            let present: Vec<Box<[u8]>> =
                self.watched.keys().filter(|k| self.find(hasher.hash_one(&k[..]), k).is_some()).cloned().collect();
            for k in present {
                self.notify_watchers(&k, false);
            }
        }
        self.index.clear();
        self.index.shrink_to_fit(|s| hasher.hash_one(s.key()));
        self.expires.clear();
        self.volatile = 0;
        self.mem = 0;
    }

    /// Active expiry: deletes keys whose expiry is due, oldest first, doing at
    /// most `budget` heap pops. Returns the number of keys deleted.
    pub fn expire_due(&mut self, now: u64, budget: usize, hasher: &RandomState) -> u64 {
        let mut deleted = 0;
        for _ in 0..budget {
            match self.expires.peek() {
                Some(Reverse((exp, _))) if *exp < now => {}
                _ => break,
            }
            let Reverse((exp, h)) = self.expires.pop().unwrap();
            // Stale heap items (overwritten or persisted keys) match nothing.
            let hit = self.index.find_bucket_index(h, |s| s.expire == exp && hasher.hash_one(s.key()) == h);
            if let Some(i) = hit {
                self.remove_at(i, h, hasher);
                deleted += 1;
            }
        }
        deleted
    }

    /// Rebuilds the expiry heap when stale items dominate it. Stale items come
    /// from keys whose expiry was overwritten, cleared or deleted; the rebuild
    /// is amortized over the heap growth that triggered it.
    fn maybe_compact_heap(&mut self, hasher: &RandomState) {
        if self.expires.len() > 2 * self.volatile + 4096 {
            let items: Vec<Reverse<(u64, u64)>> = self
                .index
                .iter()
                .filter(|s| s.expire != 0)
                .map(|s| Reverse((s.expire, hasher.hash_one(s.key()))))
                .collect();
            self.expires = BinaryHeap::from(items);
        }
    }

    // ---- WATCH support -------------------------------------------------

    /// Registers a watch. `expired` records whether the key was logically
    /// expired at WATCH time.
    pub fn watch(&mut self, key: &[u8], flag: WatchFlag, expired: bool) {
        self.watched.entry(key.into()).or_default().push(Watcher { flag, expired });
    }

    /// Valkey's `isWatchedKeyExpired` for one key: the key was live when
    /// watched and has logically expired since (it may still be in the slab).
    pub fn watched_key_expired(&self, h: u64, key: &[u8], flag: &WatchFlag, now: u64) -> bool {
        let Some(w) = self.watched.get(key).and_then(|v| v.iter().find(|w| Arc::ptr_eq(&w.flag, flag))) else {
            return false;
        };
        if w.expired {
            return false;
        }
        match self.find(h, key) {
            Some(i) => {
                let exp = self.slot(i).expire;
                exp != 0 && exp < now
            }
            None => false,
        }
    }

    /// Whether the key exists and is logically expired (`keyIsExpired`).
    pub fn is_expired(&self, h: u64, key: &[u8], now: u64) -> bool {
        self.find(h, key).map(|i| self.slot(i).expire).map(|e| e != 0 && e < now).unwrap_or(false)
    }

    pub fn unwatch(&mut self, key: &[u8], flag: &WatchFlag) {
        if let Some(v) = self.watched.get_mut(key) {
            v.retain(|w| !Arc::ptr_eq(&w.flag, flag));
            if v.is_empty() {
                self.watched.remove(key);
            }
        }
    }

    /// Valkey's `touchWatchedKey`. `exists_after` says whether the key exists
    /// once the operation completes; deleting a key that was already expired
    /// at WATCH time is not a logical change.
    #[inline]
    fn notify_watchers(&mut self, key: &[u8], exists_after: bool) {
        if self.watched.is_empty() {
            return;
        }
        if let Some(v) = self.watched.get_mut(key) {
            for w in v.iter_mut() {
                if w.expired && !exists_after {
                    w.expired = false;
                    continue;
                }
                w.flag.store(true, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    fn notify_watchers_at(&mut self, i: usize, exists_after: bool) {
        if self.watched.is_empty() {
            return;
        }
        let key: Box<[u8]> = self.slot(i).key().into();
        self.notify_watchers(&key, exists_after);
    }

    // ---- Eviction sampling --------------------------------------------

    /// Returns up to `n` random occupied bucket indices (with replacement).
    /// Buckets are at least ~44% full after growth, so a few probes suffice;
    /// the probe count is bounded for nearly empty tables.
    pub fn sample(&self, n: usize, rng: &mut u64) -> Vec<usize> {
        let mut out = Vec::with_capacity(n);
        let buckets = self.index.num_buckets();
        if self.index.is_empty() || buckets == 0 {
            return out;
        }
        let mut state = *rng | 1;
        *rng = rng.wrapping_add(0x9E37_79B9_7F4A_7C15).rotate_left(17);
        for _ in 0..n * 64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let i = (state % buckets as u64) as usize;
            if self.index.get_bucket(i).is_some() {
                out.push(i);
                if out.len() == n {
                    break;
                }
            }
        }
        out
    }
}
