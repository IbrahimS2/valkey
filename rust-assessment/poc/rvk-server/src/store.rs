//! Lock-sharded keyspace.
//!
//! The keyspace is split into `NSHARDS` shards, each behind its own mutex, so
//! any worker thread can execute any command after locking only the shards it
//! touches (memcached/KeyDB style). Multi-key commands lock their shards in
//! ascending order, which makes them atomic and deadlock-free.
//!
//! Per shard, entries live in a dense slab (`Vec<Slot>`); a `HashTable<u32>`
//! maps keys to slab positions. The dense slab makes random sampling for
//! eviction O(1). Key and value share one allocation.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use foldhash::fast::RandomState;
use hashbrown::HashTable;
use std::hash::BuildHasher;

pub const NSHARDS: usize = 64;

/// Bookkeeping bytes charged per entry on top of key and value bytes: the slab
/// slot (32), index entry with control byte (5) and allocator rounding. It is
/// calibrated against jemalloc's `stats.allocated` (see FEATURE_MATRIX.md).
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

/// A WATCH registration: set to true when the watched key is modified.
pub type WatchFlag = Arc<AtomicBool>;

pub struct Shard {
    index: HashTable<u32>,
    slots: Vec<Slot>,
    /// Min-heap of (expire_ms, key hash) for active expiry; may hold stale
    /// items for keys that were overwritten or persisted.
    expires: BinaryHeap<Reverse<(u64, u64)>>,
    volatile: usize,
    mem: usize,
    watched: HashMap<Box<[u8]>, Vec<WatchFlag>>,
}

pub struct Store {
    shards: Box<[Mutex<Shard>]>,
    /// Mirrors of each shard's `mem`, readable without the lock.
    mem: Box<[AtomicUsize]>,
    hasher: RandomState,
}

impl Store {
    pub fn new() -> Self {
        let shards = (0..NSHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    index: HashTable::new(),
                    slots: Vec::new(),
                    expires: BinaryHeap::new(),
                    volatile: 0,
                    mem: 0,
                    watched: HashMap::new(),
                })
            })
            .collect();
        let mem = (0..NSHARDS).map(|_| AtomicUsize::new(0)).collect();
        Store { shards, mem, hasher: RandomState::default() }
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
        self.shards[shard].lock().unwrap()
    }

    /// Publishes a shard's memory counter after modification (lock held).
    #[inline]
    pub fn publish_mem(&self, shard: usize, s: &Shard) {
        self.mem[shard].store(s.mem, Ordering::Relaxed);
    }

    /// Approximate dataset memory, readable without locks.
    pub fn used_memory(&self) -> usize {
        self.mem.iter().map(|m| m.load(Ordering::Relaxed)).sum()
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
        self.slots.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    #[inline]
    pub fn volatile(&self) -> usize {
        self.volatile
    }
    #[inline]
    pub fn slot(&self, i: usize) -> &Slot {
        &self.slots[i]
    }

    #[inline]
    fn find(&self, h: u64, key: &[u8]) -> Option<usize> {
        let slots = &self.slots;
        self.index.find(h, |&i| slots[i as usize].key() == key).map(|&i| i as usize)
    }

    /// Looks up a key, deleting it first if it has expired (lazy expiry).
    #[inline]
    pub fn lookup(&mut self, h: u64, key: &[u8], now: u64, fx: &mut Effects, hasher: &RandomState) -> Option<usize> {
        let i = self.find(h, key)?;
        let exp = self.slots[i].expire;
        if exp != 0 && exp <= now {
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
        self.slots[i].lru = lru;
        Some(i)
    }

    /// Inserts or overwrites `key` with `val`. `expire` is the new absolute
    /// expiry (0 = none). Returns true if the key already existed.
    pub fn set(&mut self, h: u64, key: &[u8], val: &[u8], expire: u64, lru: u32, hasher: &RandomState) -> bool {
        self.notify_watchers(key);
        if let Some(i) = self.find(h, key) {
            let old = &mut self.slots[i];
            self.mem -= old.cost();
            if old.expire != 0 {
                self.volatile -= 1;
            }
            old.data = make_data(key, val);
            old.expire = expire;
            old.lru = lru;
            self.mem += self.slots[i].cost();
            if expire != 0 {
                self.volatile += 1;
                self.expires.push(Reverse((expire, h)));
                self.maybe_compact_heap(hasher);
            }
            return true;
        }
        let idx = self.slots.len() as u32;
        self.slots.push(Slot { data: make_data(key, val), klen: key.len() as u32, lru, expire });
        let slots = &self.slots;
        self.index.insert_unique(h, idx, |&i| hasher.hash_one(slots[i as usize].key()));
        self.mem += self.slots[idx as usize].cost();
        if expire != 0 {
            self.volatile += 1;
            self.expires.push(Reverse((expire, h)));
        }
        self.maybe_compact_heap(hasher);
        false
    }

    /// Replaces the value of an existing entry, keeping key and expiry.
    pub fn replace_value(&mut self, i: usize, val: &[u8], lru: u32) {
        let key_len = self.slots[i].klen as usize;
        let mut v = Vec::with_capacity(key_len + val.len());
        v.extend_from_slice(&self.slots[i].data[..key_len]);
        v.extend_from_slice(val);
        self.notify_watchers_at(i);
        let s = &mut self.slots[i];
        self.mem -= s.cost();
        s.data = v.into_boxed_slice();
        s.lru = lru;
        self.mem += s.cost();
    }

    /// Sets or clears the expiry of an existing entry.
    pub fn set_expire(&mut self, i: usize, h: u64, expire: u64, hasher: &RandomState) {
        self.notify_watchers_at(i);
        let s = &mut self.slots[i];
        if s.expire != 0 {
            self.volatile -= 1;
        }
        s.expire = expire;
        if expire != 0 {
            self.volatile += 1;
            self.expires.push(Reverse((expire, h)));
            self.maybe_compact_heap(hasher);
        }
    }

    /// Removes the entry at slab position `i` whose key hashes to `h`.
    pub fn remove_at(&mut self, i: usize, h: u64, hasher: &RandomState) {
        self.notify_watchers_at(i);
        let idx = i as u32;
        match self.index.find_entry(h, |&v| v == idx) {
            Ok(e) => {
                e.remove();
            }
            Err(_) => unreachable!("slab entry missing from index"),
        }
        let last = self.slots.len() - 1;
        let removed = self.slots.swap_remove(i);
        self.mem -= removed.cost();
        if removed.expire != 0 {
            self.volatile -= 1;
        }
        if i != last {
            // The former last slot moved to `i`; repoint its index entry.
            let moved_h = hasher.hash_one(self.slots[i].key());
            let from = last as u32;
            match self.index.find_mut(moved_h, |&v| v == from) {
                Some(v) => *v = idx,
                None => unreachable!("moved slab entry missing from index"),
            }
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

    pub fn clear(&mut self) {
        let keys: Vec<Box<[u8]>> = self.watched.keys().cloned().collect();
        for k in keys {
            self.notify_watchers(&k);
        }
        self.index.clear();
        self.slots.clear();
        self.slots.shrink_to_fit();
        self.index.shrink_to_fit(|_| 0);
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
                Some(Reverse((exp, _))) if *exp <= now => {}
                _ => break,
            }
            let Reverse((exp, h)) = self.expires.pop().unwrap();
            // Stale heap items (overwritten or persisted keys) match nothing.
            let slots = &self.slots;
            let hit = self
                .index
                .find(h, |&i| {
                    let s = &slots[i as usize];
                    s.expire == exp && hasher.hash_one(s.key()) == h
                })
                .map(|&i| i as usize);
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
                .slots
                .iter()
                .filter(|s| s.expire != 0)
                .map(|s| Reverse((s.expire, hasher.hash_one(s.key()))))
                .collect();
            self.expires = BinaryHeap::from(items);
        }
    }

    // ---- WATCH support -------------------------------------------------

    pub fn watch(&mut self, key: &[u8], flag: WatchFlag) {
        self.watched.entry(key.into()).or_default().push(flag);
    }

    pub fn unwatch(&mut self, key: &[u8], flag: &WatchFlag) {
        if let Some(v) = self.watched.get_mut(key) {
            v.retain(|f| !Arc::ptr_eq(f, flag));
            if v.is_empty() {
                self.watched.remove(key);
            }
        }
    }

    #[inline]
    fn notify_watchers(&mut self, key: &[u8]) {
        if self.watched.is_empty() {
            return;
        }
        if let Some(v) = self.watched.get(key) {
            for f in v {
                f.store(true, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    fn notify_watchers_at(&mut self, i: usize) {
        if self.watched.is_empty() {
            return;
        }
        let key: Box<[u8]> = self.slots[i].key().into();
        self.notify_watchers(&key);
    }

    // ---- Eviction sampling --------------------------------------------

    /// Returns up to `n` random slab positions (with replacement).
    pub fn sample(&self, n: usize, rng: &mut u64) -> impl Iterator<Item = usize> + '_ {
        let len = self.slots.len();
        let mut state = *rng;
        *rng = rng.wrapping_add(0x9E37_79B9_7F4A_7C15).rotate_left(17);
        (0..if len == 0 { 0 } else { n }).map(move |_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % len as u64) as usize
        })
    }
}
