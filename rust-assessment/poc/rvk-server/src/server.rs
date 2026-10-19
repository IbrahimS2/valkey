//! Worker threads, connections and the event loop.
//!
//! Every worker owns a mio `Poll` (epoll, edge-triggered) and its own
//! `SO_REUSEPORT` listener, so the kernel spreads connections across workers
//! without a dispatcher thread. A worker executes commands for its own
//! connections directly against the shared, lock-sharded `Store`.
//!
//! I/O discipline (the measured bottleneck is per-request system calls):
//! * each chunk read is executed before the next read, and reading stops after
//!   a short read: the socket is known to be drained, and edge-triggered epoll
//!   reports any later data, so no EAGAIN read is needed;
//! * replies are written after every ready connection of an `epoll_wait`
//!   batch has been served, like Valkey's `beforeSleep`: a client then gets
//!   its replies back-to-back and is woken once, instead of being woken for
//!   each reply as it is produced (the first version did that, and the profile
//!   showed the extra cross-CPU wakeups as kernel time);
//! * a connection with more than `OUTPUT_PAUSE` bytes of pending output is not
//!   read until it drains, so TCP flow control pushes back on the client.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use crate::cmd::{self, ConnState, ThreadCtx};
use crate::resp::{self, ArgRange, Args, Parsed};
use crate::store::{Store, NSHARDS};

const LISTENER: Token = Token(usize::MAX);
const READ_CHUNK: usize = 16 * 1024;
/// Stop processing a connection's input while this much output is pending.
const OUTPUT_PAUSE: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    NoEviction,
    AllKeysLru,
    AllKeysRandom,
}

impl Policy {
    pub fn parse(s: &[u8]) -> Option<Policy> {
        match s.to_ascii_lowercase().as_slice() {
            b"noeviction" => Some(Policy::NoEviction),
            b"allkeys-lru" => Some(Policy::AllKeysLru),
            b"allkeys-random" => Some(Policy::AllKeysRandom),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Policy::NoEviction => "noeviction",
            Policy::AllKeysLru => "allkeys-lru",
            Policy::AllKeysRandom => "allkeys-random",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub threads: usize,
    pub maxclients: usize,
    pub requirepass: Option<Vec<u8>>,
    pub hz: u32,
    pub proto_max_bulk_len: usize,
    pub client_query_buffer_limit: usize,
    pub maxmemory_samples: usize,
    /// `enable-debug-command`: DEBUG is refused unless this is set.
    pub enable_debug: bool,
}

/// Per-thread counters; written by one thread, summed by INFO.
#[derive(Default)]
pub struct Stats {
    pub commands: AtomicU64,
    pub net_in: AtomicU64,
    pub net_out: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub expired: AtomicU64,
    pub evicted: AtomicU64,
    pub errors: AtomicU64,
    pub conns_received: AtomicU64,
    pub rejected: AtomicU64,
}

impl Stats {
    #[inline]
    pub fn add(c: &AtomicU64, n: u64) {
        // Single writer per counter: a relaxed load+store avoids a locked RMW.
        c.store(c.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
    }
    pub fn values(&self) -> [u64; 10] {
        [
            &self.commands, &self.net_in, &self.net_out, &self.hits, &self.misses, &self.expired,
            &self.evicted, &self.errors, &self.conns_received, &self.rejected,
        ]
        .map(|c| c.load(Ordering::Relaxed))
    }
}

pub struct Shared {
    pub store: Store,
    pub cfg: Config,
    pub maxmemory: AtomicUsize,
    pub policy: RwLock<Policy>,
    pub stats: Vec<Arc<CachePadded<Stats>>>,
    /// Counter totals at the last CONFIG RESETSTAT. Counters only grow, so
    /// INFO reports totals minus this baseline; no thread's counter is ever
    /// written by another thread.
    pub stats_base: std::sync::Mutex<[u64; 10]>,
    /// `DEBUG SET-ACTIVE-EXPIRE`, for every worker.
    pub active_expire: AtomicBool,
    pub connected: AtomicUsize,
    pub next_client_id: AtomicU64,
    pub shutdown: AtomicBool,
    pub started: Instant,
    pub run_id: String,
    pub port: u16,
}

#[repr(align(128))]
#[derive(Default)]
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

struct Conn {
    stream: TcpStream,
    rbuf: Vec<u8>,
    rlen: usize,
    wbuf: Vec<u8>,
    wpos: usize,
    writable_registered: bool,
    /// A multibulk request that has partly arrived (see `resp::Partial`).
    partial: Option<resp::Partial>,
    /// Reading stopped because too much output is pending.
    read_paused: bool,
    /// The peer shut down its sending side (EPOLLRDHUP); read to EOF.
    peer_closed: bool,
    /// In this pass's list of connections with replies to write.
    queued: bool,
    state: ConnState,
}

impl Conn {
    #[inline]
    fn pending_output(&self) -> usize {
        self.wbuf.len() - self.wpos
    }
}

fn make_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sock = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(511)?;
    Ok(sock.into())
}

pub fn run(cfg: Config, maxmemory: usize, policy: Policy) -> io::Result<()> {
    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // SO_REUSEPORT would let a second server bind the same port and silently
    // split connections with this one. A plain bind fails if anything is
    // already listening, as Valkey's does.
    drop(std::net::TcpListener::bind(addr)?);
    // Bind every listener before starting workers so start-up errors surface here.
    let listeners: Vec<std::net::TcpListener> = (0..cfg.threads).map(|_| make_listener(addr)).collect::<io::Result<_>>()?;
    let run_id: String = {
        let seed = now_ms() ^ (std::process::id() as u64).rotate_left(32);
        let mut x = seed | 1;
        (0..40)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                b"0123456789abcdef"[(x % 16) as usize] as char
            })
            .collect()
    };
    let shared = Arc::new(Shared {
        store: Store::new(),
        stats: (0..cfg.threads).map(|_| Arc::new(CachePadded(Stats::default()))).collect(),
        maxmemory: AtomicUsize::new(maxmemory),
        policy: RwLock::new(policy),
        stats_base: std::sync::Mutex::new([0; 10]),
        active_expire: AtomicBool::new(true),
        connected: AtomicUsize::new(0),
        next_client_id: AtomicU64::new(1),
        shutdown: AtomicBool::new(false),
        started: Instant::now(),
        run_id,
        port: cfg.port,
        cfg,
    });
    eprintln!(
        "rvk-server listening on {} with {} worker thread(s), {} shards, run_id {}",
        addr, shared.cfg.threads, NSHARDS, shared.run_id
    );
    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let sh = shared.clone();
            std::thread::Builder::new().name(format!("rvk-worker-{i}")).spawn(move || worker(i, sh, l)).expect("spawn worker")
        })
        .collect();
    for h in handles {
        if let Err(e) = h.join().expect("worker panicked") {
            eprintln!("rvk-server: worker error: {e}");
        }
    }
    Ok(())
}

fn worker(id: usize, shared: Arc<Shared>, listener: std::net::TcpListener) -> io::Result<()> {
    let mut poll = Poll::new()?;
    let mut listener = TcpListener::from_std(listener);
    poll.registry().register(&mut listener, LISTENER, Interest::READABLE)?;
    let mut events = Events::with_capacity(1024);
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut tc = ThreadCtx::new(id, shared.clone());
    let cron_every = Duration::from_millis((1000 / shared.cfg.hz.max(1)) as u64);
    let mut next_cron = Instant::now() + cron_every;
    // Set when accept() failed for a reason other than an empty queue (for
    // example EMFILE): the edge-triggered listener raises no new event for
    // connections already queued, so accepting is retried every tick.
    let mut accept_retry = false;
    // Connections with replies to write once this pass's events are served.
    let mut pending: Vec<usize> = Vec::new();
    loop {
        let mut timeout = next_cron.saturating_duration_since(Instant::now());
        if accept_retry {
            timeout = timeout.min(Duration::from_millis(10));
        }
        if let Err(e) = poll.poll(&mut events, Some(timeout)) {
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
        for ev in events.iter() {
            match ev.token() {
                LISTENER => accept_retry = !accept_all(&mut listener, &poll, &mut conns, &mut free, &shared, &tc),
                Token(t) => {
                    let mut close = false;
                    if let Some(Some(conn)) = conns.get_mut(t) {
                        if ev.is_readable() || ev.is_read_closed() {
                            close |= on_readable(conn, &mut tc, &shared, ev.is_read_closed());
                        }
                        if !close && ev.is_writable() {
                            close |= write_out(conn, &mut tc, &shared);
                        }
                        if !close && (ev.is_error() || (ev.is_write_closed() && conn.pending_output() > 0)) {
                            close = true;
                        }
                        if !close {
                            if conn.pending_output() > 0 || conn.state.close_after_reply {
                                if !conn.queued {
                                    conn.queued = true;
                                    pending.push(t);
                                }
                            } else {
                                close |= update_interest(conn, &poll, t);
                            }
                        }
                    }
                    if close {
                        close_conn(&mut conns, &mut free, t, &poll, &shared);
                    }
                }
            }
            if tc.shutdown_requested {
                shared.shutdown.store(true, Ordering::SeqCst);
            }
        }
        // Valkey's handleClientsWithPendingWrites.
        let mut batch = std::mem::take(&mut pending);
        for &t in &batch {
            let mut close = false;
            if let Some(Some(conn)) = conns.get_mut(t) {
                if !conn.queued {
                    continue; // the slot now belongs to a new connection
                }
                conn.queued = false;
                close = write_out(conn, &mut tc, &shared) || update_interest(conn, &poll, t);
            }
            if close {
                close_conn(&mut conns, &mut free, t, &poll, &shared);
            }
        }
        batch.clear();
        pending = batch;
        if accept_retry {
            accept_retry = !accept_all(&mut listener, &poll, &mut conns, &mut free, &shared, &tc);
        }
        if shared.shutdown.load(Ordering::Relaxed) {
            // SHUTDOWN: flush what we can and exit the whole process.
            for c in conns.iter_mut().flatten() {
                let _ = c.stream.write_all(&c.wbuf[c.wpos..]);
            }
            std::process::exit(0);
        }
        let now = Instant::now();
        if now >= next_cron {
            next_cron = now + cron_every;
            cmd::cron(&mut tc);
        }
    }
}

/// Valkey's `anetKeepAlive(fd, 300)`: probe idle peers so dead ones are freed.
fn set_keepalive(stream: &TcpStream) {
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(300))
        .with_interval(Duration::from_secs(100))
        .with_retries(3);
    let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&ka);
}

/// Accepts every queued connection. Returns false if accept() failed with an
/// error that leaves connections queued (so the caller retries later).
fn accept_all(listener: &mut TcpListener, poll: &Poll, conns: &mut Vec<Option<Conn>>, free: &mut Vec<usize>, shared: &Shared, tc: &ThreadCtx) -> bool {
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let _ = stream.set_nodelay(true);
                set_keepalive(&stream);
                Stats::add(&tc.stats().conns_received, 1);
                // Reserve the slot first so concurrent accepts on other workers
                // cannot all pass the check.
                if shared.connected.fetch_add(1, Ordering::Relaxed) >= shared.cfg.maxclients {
                    shared.connected.fetch_sub(1, Ordering::Relaxed);
                    let _ = stream.write(b"-ERR max number of clients reached\r\n");
                    Stats::add(&tc.stats().rejected, 1);
                    continue;
                }
                let t = free.pop().unwrap_or_else(|| {
                    conns.push(None);
                    conns.len() - 1
                });
                if poll.registry().register(&mut stream, Token(t), Interest::READABLE).is_err() {
                    shared.connected.fetch_sub(1, Ordering::Relaxed);
                    free.push(t);
                    continue;
                }
                let cid = shared.next_client_id.fetch_add(1, Ordering::Relaxed);
                conns[t] = Some(Conn {
                    stream,
                    rbuf: vec![0u8; READ_CHUNK],
                    rlen: 0,
                    wbuf: Vec::with_capacity(READ_CHUNK),
                    wpos: 0,
                    writable_registered: false,
                    partial: None,
                    read_paused: false,
                    peer_closed: false,
                    queued: false,
                    state: ConnState::new(cid, shared.cfg.requirepass.is_none()),
                });
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                if e.kind() != io::ErrorKind::ConnectionAborted {
                    eprintln!("rvk-server: accept: {e}");
                    return false;
                }
            }
        }
    }
}

fn close_conn(conns: &mut [Option<Conn>], free: &mut Vec<usize>, t: usize, poll: &Poll, shared: &Shared) {
    if let Some(mut c) = conns[t].take() {
        let _ = poll.registry().deregister(&mut c.stream);
        cmd::on_disconnect(shared, &mut c.state);
        shared.connected.fetch_sub(1, Ordering::Relaxed);
        free.push(t);
    }
}

/// Valkey's 16 KB static reply buffer plus REPLY_BUFFER_SIZE_UNAUTHENTICATED_CLIENT.
const UNAUTH_OUTPUT_LIMIT: usize = 16 * 1024 + 1024;

/// Like Valkey's querybuf limit, this applies to input not yet executed;
/// clients that never authenticated are held to 1 MB.
fn query_limit(conn: &Conn, sh: &Shared) -> usize {
    if conn.state.never_authed() {
        sh.cfg.client_query_buffer_limit.min(1 << 20)
    } else {
        sh.cfg.client_query_buffer_limit
    }
}

/// Outcome of executing buffered input.
enum Step {
    Close,
    /// Execution stopped for output backpressure; reading is paused.
    Paused,
    Done,
}

/// Executes buffered input; replies are written later by `write_out`.
fn run_buffered(conn: &mut Conn, tc: &mut ThreadCtx, sh: &Shared) -> Step {
    let paused = process(conn, tc, sh);
    if conn.state.kill || conn.rlen > query_limit(conn, sh) {
        return Step::Close;
    }
    if conn.state.never_authed() && conn.pending_output() > UNAUTH_OUTPUT_LIMIT {
        return Step::Close;
    }
    if paused {
        conn.read_paused = true;
        return Step::Paused;
    }
    Step::Done
}

/// Reads and executes input one chunk at a time, until a short read (the
/// socket is drained), EOF, or output backpressure. Returns true if the
/// connection must be closed.
fn on_readable(conn: &mut Conn, tc: &mut ThreadCtx, sh: &Shared, read_closed: bool) -> bool {
    if read_closed {
        // The FIN may have arrived with the last data, in which case no
        // further edge is raised: read until EOF instead of stopping early.
        conn.peer_closed = true;
    }
    loop {
        if conn.read_paused || conn.state.close_after_reply {
            // write_out resumes reading once the output drains.
            return false;
        }
        if conn.rlen == conn.rbuf.len() {
            // Buffer full of an incomplete request: grow it, but never far past
            // the limit (the next read then closes the connection).
            let new_len = (conn.rbuf.len() * 2).max(READ_CHUNK).min(query_limit(conn, sh) + READ_CHUNK);
            if new_len <= conn.rbuf.len() {
                return true;
            }
            conn.rbuf.resize(new_len, 0);
        }
        let avail = conn.rbuf.len() - conn.rlen;
        match conn.stream.read(&mut conn.rbuf[conn.rlen..]) {
            Ok(0) => {
                // EOF: execute what arrived before the FIN, as Valkey does.
                process(conn, tc, sh);
                let _ = flush(conn, tc);
                return true;
            }
            Ok(n) => {
                conn.rlen += n;
                Stats::add(&tc.stats().net_in, n as u64);
                match run_buffered(conn, tc, sh) {
                    Step::Close => return true,
                    Step::Paused => return false,
                    Step::Done => {}
                }
                if n < avail && !conn.peer_closed {
                    return false;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return false,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return true,
        }
    }
}

/// Writes a connection's pending replies (Valkey's `beforeSleep` write
/// phase) and, once output paused for backpressure has drained, executes the
/// input left waiting and reads on: no new epoll edge would report it.
/// Returns true if the connection must be closed.
fn write_out(conn: &mut Conn, tc: &mut ThreadCtx, sh: &Shared) -> bool {
    for _ in 0..8 {
        if flush(conn, tc) {
            return true;
        }
        if !(conn.read_paused && conn.pending_output() <= OUTPUT_PAUSE / 2) {
            return false;
        }
        conn.read_paused = false;
        match run_buffered(conn, tc, sh) {
            Step::Close => return true,
            Step::Paused => continue,
            Step::Done => {}
        }
        if on_readable(conn, tc, sh, false) {
            return true;
        }
        if conn.pending_output() == 0 {
            return false;
        }
    }
    false
}

/// Executes every complete command in the read buffer. Returns true if it
/// stopped early because too much output is pending.
fn process(conn: &mut Conn, tc: &mut ThreadCtx, sh: &Shared) -> bool {
    let Conn { rbuf, rlen, wbuf, wpos, state, partial, .. } = conn;
    let buf = &rbuf[..*rlen];
    let mut pos = 0;
    let mut ranges: Vec<ArgRange> = std::mem::take(&mut tc.ranges);
    let mut scratch: Vec<u8> = std::mem::take(&mut tc.scratch);
    let mut paused = false;
    while pos < buf.len() && !state.close_after_reply && !state.kill {
        if wbuf.len() - *wpos > OUTPUT_PAUSE {
            paused = true;
            break;
        }
        match resp::parse(buf, pos, sh.cfg.proto_max_bulk_len, state.authed(), &mut ranges, &mut scratch, partial) {
            Parsed::Incomplete => break,
            Parsed::Skip { consumed } => pos += consumed,
            Parsed::Command { consumed } => {
                let args = Args { buf, scratch: &scratch, ranges: &ranges };
                cmd::execute(sh, tc, state, &args, wbuf);
                pos += consumed;
            }
            Parsed::Error(msg) => {
                wbuf.extend_from_slice(b"-ERR ");
                wbuf.extend_from_slice(&msg);
                wbuf.extend_from_slice(b"\r\n");
                Stats::add(&tc.stats().errors, 1);
                state.close_after_reply = true;
            }
        }
    }
    tc.ranges = ranges;
    tc.scratch = scratch;
    if pos > 0 {
        rbuf.copy_within(pos..*rlen, 0);
        *rlen -= pos;
    }
    if rbuf.len() > 4 * READ_CHUNK && *rlen < READ_CHUNK {
        rbuf.truncate(READ_CHUNK);
        rbuf.shrink_to_fit();
    }
    paused
}

/// Writes pending output. Returns true if the connection must be closed.
fn flush(conn: &mut Conn, tc: &mut ThreadCtx) -> bool {
    while conn.wpos < conn.wbuf.len() {
        match conn.stream.write(&conn.wbuf[conn.wpos..]) {
            Ok(0) => return true,
            Ok(n) => {
                conn.wpos += n;
                Stats::add(&tc.stats().net_out, n as u64);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return false,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return true,
        }
    }
    conn.wbuf.clear();
    conn.wpos = 0;
    if conn.wbuf.capacity() > 1 << 20 {
        conn.wbuf = Vec::with_capacity(READ_CHUNK);
    }
    conn.state.close_after_reply
}

/// Registers write interest only while output is pending.
fn update_interest(conn: &mut Conn, poll: &Poll, t: usize) -> bool {
    let want_write = conn.wpos < conn.wbuf.len();
    if want_write != conn.writable_registered {
        let interest = if want_write { Interest::READABLE | Interest::WRITABLE } else { Interest::READABLE };
        if poll.registry().reregister(&mut conn.stream, Token(t), interest).is_err() {
            return true;
        }
        conn.writable_registered = want_write;
    }
    false
}
