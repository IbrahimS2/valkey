//! RESP request parsing and reply encoding.
//!
//! Parsing is allocation-free for multibulk requests: arguments are recorded as
//! byte ranges into the connection's read buffer. Inline requests (a single
//! text line) are tokenized into a scratch buffer, mirroring `sdssplitargs`.
//! Limits and error texts follow Valkey's `networking.c`.

/// Valkey `PROTO_INLINE_MAX_SIZE`: longest inline request or count line.
pub const INLINE_MAX: usize = 64 * 1024;
/// Valkey's multibulk length limit (`ll > INT_MAX` is invalid).
pub const MAX_MULTIBULK: i64 = i32::MAX as i64;
/// Limits for clients that still need to AUTH (networking.c).
pub const UNAUTH_MAX_MULTIBULK: i64 = 10;
pub const UNAUTH_MAX_BULK: usize = 16384;

/// Where an argument's bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Src {
    Buf,
    Scratch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArgRange {
    pub start: u32,
    pub end: u32,
    pub src: Src,
}

/// Arguments of one parsed command, borrowed from the read buffer and scratch.
pub struct Args<'a> {
    pub buf: &'a [u8],
    pub scratch: &'a [u8],
    pub ranges: &'a [ArgRange],
}

impl<'a> Args<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }
    #[inline]
    pub fn get(&self, i: usize) -> &'a [u8] {
        let r = self.ranges[i];
        let s = match r.src {
            Src::Buf => self.buf,
            Src::Scratch => self.scratch,
        };
        &s[r.start as usize..r.end as usize]
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// More bytes are needed.
    Incomplete,
    /// A command was parsed into the range vector; `consumed` bytes used.
    Command { consumed: usize },
    /// Bytes that produce no command (empty multibulk, blank inline line).
    Skip { consumed: usize },
    /// Protocol error: reply with `-ERR <msg>` and close the connection. The
    /// message is raw bytes because it may quote an offending input byte.
    Error(Vec<u8>),
}

fn perr(msg: &str) -> Parsed {
    Parsed::Error(msg.as_bytes().to_vec())
}

/// A multibulk request whose header and some arguments have arrived but not
/// the rest. Keeping it per connection means every byte is parsed once, as
/// Valkey does with `multibulklen`/`argv` on the client. Offsets are relative
/// to the start of the request, which stays valid when the read buffer is
/// compacted.
#[derive(Debug, Default)]
pub struct Partial {
    count: i64,
    /// Offset of the next argument's `$` header.
    next: usize,
    ranges: Vec<ArgRange>,
}

/// Parses a signed decimal the way Valkey's `string2ll` does for protocol
/// lengths (no leading '+', no spaces).
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    let (neg, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    // string2ll: "0" alone is valid; otherwise the first digit is 1-9, so
    // "-0" and leading zeros are rejected.
    if digits.is_empty() || (digits[0] == b'0' && (neg || digits.len() > 1)) {
        return None;
    }
    let mut v: i64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        let d = (d - b'0') as i64;
        v = if neg { v.checked_mul(10)?.checked_sub(d)? } else { v.checked_mul(10)?.checked_add(d)? };
    }
    Some(v)
}

/// Parses one request at `buf[pos..]`. Ranges are absolute offsets into `buf`.
///
/// Follows `processMultibulkBuffer`: each header line ends at the first '\r',
/// which must be followed by '\n'; every bulk payload must be followed by
/// "\r\n" (otherwise "invalid CRLF in request").
pub fn parse(
    buf: &[u8],
    pos: usize,
    max_bulk: usize,
    authed: bool,
    ranges: &mut Vec<ArgRange>,
    scratch: &mut Vec<u8>,
    partial: &mut Option<Partial>,
) -> Parsed {
    ranges.clear();
    if pos >= buf.len() {
        return Parsed::Incomplete;
    }
    let (count, mut p) = match partial.take() {
        Some(pt) => {
            // Resume: earlier arguments were validated on a previous call.
            ranges.extend(pt.ranges.iter().map(|r| ArgRange { start: r.start + pos as u32, end: r.end + pos as u32, src: Src::Buf }));
            (pt.count, pos + pt.next)
        }
        None => {
            if buf[pos] != b'*' {
                return parse_inline(buf, pos, ranges, scratch);
            }
            let Some(cr) = memchr_cr(buf, pos) else {
                if buf.len() - pos > INLINE_MAX {
                    return perr("Protocol error: too big mbulk count string");
                }
                return Parsed::Incomplete;
            };
            if cr + 1 >= buf.len() {
                return Parsed::Incomplete;
            }
            if buf[cr + 1] != b'\n' {
                return perr("Protocol error: invalid CRLF in request");
            }
            let count = match parse_i64(&buf[pos + 1..cr]) {
                Some(n) if n <= MAX_MULTIBULK => n,
                _ => return perr("Protocol error: invalid multibulk length"),
            };
            if count > UNAUTH_MAX_MULTIBULK && !authed {
                return perr("Protocol error: unauthenticated multibulk length");
            }
            if count <= 0 {
                return Parsed::Skip { consumed: cr + 2 - pos };
            }
            (count, cr + 2)
        }
    };
    let save = |ranges: &mut Vec<ArgRange>, partial: &mut Option<Partial>, p: usize| {
        let rel = ranges.iter().map(|r| ArgRange { start: r.start - pos as u32, end: r.end - pos as u32, src: Src::Buf }).collect();
        *partial = Some(Partial { count, next: p - pos, ranges: rel });
        Parsed::Incomplete
    };
    while (ranges.len() as i64) < count {
        let Some(cr) = memchr_cr(buf, p) else {
            if buf.len() - p > INLINE_MAX {
                return perr("Protocol error: too big bulk count string");
            }
            return save(ranges, partial, p);
        };
        if cr + 1 >= buf.len() {
            return save(ranges, partial, p);
        }
        if buf[p] != b'$' {
            // addReplyErrorFormat maps CR and LF to spaces and copies other bytes as they are.
            let c = match buf[p] {
                b'\r' | b'\n' => b' ',
                c => c,
            };
            let mut msg = b"Protocol error: expected '$', got '".to_vec();
            msg.push(c);
            msg.push(b'\'');
            return Parsed::Error(msg);
        }
        if buf[cr + 1] != b'\n' {
            return perr("Protocol error: invalid CRLF in request");
        }
        let len = match parse_i64(&buf[p + 1..cr]) {
            Some(n) if n >= 0 && (n as u64) <= max_bulk as u64 => n as usize,
            _ => return perr("Protocol error: invalid bulk length"),
        };
        if len > UNAUTH_MAX_BULK && !authed {
            return perr("Protocol error: unauthenticated bulk length");
        }
        let start = cr + 2;
        let end = start + len;
        if buf.len() < end + 2 {
            return save(ranges, partial, p);
        }
        if buf[end] != b'\r' || buf[end + 1] != b'\n' {
            return perr("Protocol error: invalid CRLF in request");
        }
        ranges.push(ArgRange { start: start as u32, end: end as u32, src: Src::Buf });
        p = end + 2;
    }
    Parsed::Command { consumed: p - pos }
}

#[inline]
fn memchr_cr(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].iter().position(|&b| b == b'\r').map(|o| from + o)
}

fn parse_inline(buf: &[u8], pos: usize, ranges: &mut Vec<ArgRange>, scratch: &mut Vec<u8>) -> Parsed {
    let Some(nl) = buf[pos..].iter().position(|&b| b == b'\n') else {
        if buf.len() - pos > INLINE_MAX {
            return perr("Protocol error: too big inline request");
        }
        return Parsed::Incomplete;
    };
    if nl > INLINE_MAX {
        return perr("Protocol error: too big inline request");
    }
    let mut line = &buf[pos..pos + nl];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    if line.contains(&0) {
        return perr("Protocol error: embedded NUL byte in inline request");
    }
    scratch.clear();
    if !split_args(line, scratch, ranges) {
        return perr("Protocol error: unbalanced quotes in request");
    }
    if ranges.is_empty() {
        return Parsed::Skip { consumed: nl + 1 };
    }
    Parsed::Command { consumed: nl + 1 }
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// C `isspace` in the "C" locale.
fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Mirrors Valkey's `sdsnsplitargs`/`sdsparsearg`: blanks (C isspace) are
/// skipped between arguments; inside an argument, quoted and unquoted
/// segments concatenate shell-style (`"a"b'c'` is `abc`). "Double" quotes
/// understand \xHH and \n \r \t \b \a escapes (any other escaped byte is
/// literal); 'single' quotes understand only \'. An argument ends at an
/// unquoted space, \t, \r or \n. Unterminated quotes are an error.
fn split_args(line: &[u8], scratch: &mut Vec<u8>, ranges: &mut Vec<ArgRange>) -> bool {
    let n = line.len();
    let mut i = 0;
    loop {
        while i < n && is_c_space(line[i]) {
            i += 1;
        }
        if i >= n {
            return true;
        }
        let start = scratch.len();
        let (mut inq, mut insq) = (false, false);
        loop {
            if inq {
                if i >= n {
                    return false;
                }
                if line[i] == b'\\' && i + 3 < n && line[i + 1] == b'x' {
                    if let (Some(h), Some(l)) = (hex_val(line[i + 2]), hex_val(line[i + 3])) {
                        scratch.push(h * 16 + l);
                        i += 4;
                        continue;
                    }
                }
                if line[i] == b'\\' && i + 1 < n {
                    scratch.push(match line[i + 1] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        other => other,
                    });
                    i += 2;
                } else if line[i] == b'"' {
                    inq = false;
                    i += 1;
                } else {
                    scratch.push(line[i]);
                    i += 1;
                }
            } else if insq {
                if i >= n {
                    return false;
                }
                if line[i] == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    scratch.push(b'\'');
                    i += 2;
                } else if line[i] == b'\'' {
                    insq = false;
                    i += 1;
                } else {
                    scratch.push(line[i]);
                    i += 1;
                }
            } else {
                if i >= n {
                    break;
                }
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' => {
                        i += 1;
                        break;
                    }
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    c => scratch.push(c),
                }
                i += 1;
            }
        }
        ranges.push(ArgRange { start: start as u32, end: scratch.len() as u32, src: Src::Scratch });
    }
}

// ---------------------------------------------------------------------------
// Reply encoding
// ---------------------------------------------------------------------------

/// Appends the decimal form of `v`.
pub fn push_i64(out: &mut Vec<u8>, v: i64) {
    let mut tmp = [0u8; 20];
    let mut n = v.unsigned_abs();
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if v < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(&tmp[i..]);
}

/// Reply writer aware of the connection's protocol version (2 or 3).
pub struct Reply<'a> {
    pub out: &'a mut Vec<u8>,
    pub resp3: bool,
}

impl Reply<'_> {
    #[inline]
    pub fn ok(&mut self) {
        self.out.extend_from_slice(b"+OK\r\n");
    }
    #[inline]
    pub fn simple(&mut self, s: &[u8]) {
        self.out.push(b'+');
        self.out.extend_from_slice(s);
        self.out.extend_from_slice(b"\r\n");
    }
    /// `msg` must already include the error code (e.g. "ERR ...").
    #[inline]
    pub fn error(&mut self, msg: &str) {
        self.out.push(b'-');
        // Valkey replaces newlines in error strings to keep the protocol valid.
        for &b in msg.as_bytes() {
            self.out.push(if b == b'\r' || b == b'\n' { b' ' } else { b });
        }
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    pub fn int(&mut self, v: i64) {
        self.out.push(b':');
        push_i64(self.out, v);
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    pub fn bulk(&mut self, v: &[u8]) {
        self.out.push(b'$');
        push_i64(self.out, v.len() as i64);
        self.out.extend_from_slice(b"\r\n");
        self.out.extend_from_slice(v);
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    pub fn null(&mut self) {
        self.out.extend_from_slice(if self.resp3 { b"_\r\n" } else { b"$-1\r\n" });
    }
    /// Null reply for aggregate contexts (e.g. aborted EXEC).
    #[inline]
    pub fn null_array(&mut self) {
        self.out.extend_from_slice(if self.resp3 { b"_\r\n" } else { b"*-1\r\n" });
    }
    #[inline]
    pub fn array(&mut self, n: usize) {
        self.out.push(b'*');
        push_i64(self.out, n as i64);
        self.out.extend_from_slice(b"\r\n");
    }
    /// A map of `n` pairs: `%n` in RESP3, a flat array of 2n in RESP2.
    #[inline]
    pub fn map(&mut self, n: usize) {
        if self.resp3 {
            self.out.push(b'%');
            push_i64(self.out, n as i64);
            self.out.extend_from_slice(b"\r\n");
        } else {
            self.array(n * 2);
        }
    }
    /// A verbatim/plain text block (INFO): bulk in RESP2, verbatim `=` in RESP3.
    pub fn verbatim(&mut self, text: &[u8]) {
        if self.resp3 {
            self.out.push(b'=');
            push_i64(self.out, text.len() as i64 + 4);
            self.out.extend_from_slice(b"\r\ntxt:");
            self.out.extend_from_slice(text);
            self.out.extend_from_slice(b"\r\n");
        } else {
            self.bulk(text);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One-shot parse with no carried-over state.
    fn parse(buf: &[u8], pos: usize, max: usize, authed: bool, r: &mut Vec<ArgRange>, s: &mut Vec<u8>) -> Parsed {
        super::parse(buf, pos, max, authed, r, s, &mut None)
    }

    fn has(p: &Parsed, needle: &str) -> bool {
        match p {
            Parsed::Error(e) => e.windows(needle.len()).any(|w| w == needle.as_bytes()),
            _ => false,
        }
    }

    fn args_of(buf: &[u8], ranges: &[ArgRange], scratch: &[u8]) -> Vec<Vec<u8>> {
        let a = Args { buf, scratch, ranges };
        (0..a.len()).map(|i| a.get(i).to_vec()).collect()
    }

    #[test]
    fn multibulk_complete_and_partial() {
        let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
        let (mut r, mut s) = (Vec::new(), Vec::new());
        assert_eq!(parse(req, 0, 512 << 20, true, &mut r, &mut s), Parsed::Command { consumed: req.len() });
        assert_eq!(args_of(req, &r, &s), vec![b"SET".to_vec(), b"k".to_vec(), b"value".to_vec()]);
        for cut in 0..req.len() {
            assert_eq!(parse(&req[..cut], 0, 512 << 20, true, &mut r, &mut s), Parsed::Incomplete, "cut {cut}");
        }
    }

    #[test]
    fn multibulk_errors_and_skip() {
        let (mut r, mut s) = (Vec::new(), Vec::new());
        assert_eq!(parse(b"*0\r\n", 0, 100, true, &mut r, &mut s), Parsed::Skip { consumed: 4 });
        assert_eq!(parse(b"*-1\r\n", 0, 100, true, &mut r, &mut s), Parsed::Skip { consumed: 5 });
        assert!(has(&parse(b"*x\r\n", 0, 100, true, &mut r, &mut s), "multibulk length"));
        assert_eq!(parse(b"*2000000\r\n", 0, 100, true, &mut r, &mut s), Parsed::Incomplete);
        assert!(matches!(parse(b"*2147483648\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(_)));
        assert!(has(&parse(b"*11\r\n", 0, 100, false, &mut r, &mut s), "unauthenticated multibulk"));
        assert!(has(&parse(b"*1\r\n$16385\r\n", 0, 1 << 20, false, &mut r, &mut s), "unauthenticated bulk"));
        assert_eq!(parse(b"*1\r\n:1\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(b"Protocol error: expected '$', got ':'".to_vec()));
        // CR/LF in the quoted byte become spaces; other bytes are copied raw.
        assert_eq!(parse(b"*1\r\n\n\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(b"Protocol error: expected '$', got ' '".to_vec()));
        assert_eq!(parse(b"*1\r\n\xff\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(b"Protocol error: expected '$', got '\xff'".to_vec()));
        assert!(has(&parse(b"*1\r\n$101\r\n", 0, 100, true, &mut r, &mut s), "bulk length"));
        assert!(matches!(parse(b"*1\r\n$-1\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(_)));
        // Strict CRLF checks (protocol.tcl "Check CRLF when parsing the querybuf").
        let crlf = |b: &[u8]| has(&parse(b, 0, 100, true, &mut Vec::new(), &mut Vec::new()), "invalid CRLF");
        assert!(crlf(b"*3\rx$3\r\nSET\r\n"));
        assert!(crlf(b"*1\r\n$3\rxSET\r\n"));
        assert!(crlf(b"*1\r\n$3\r\nSETxx"));
        assert!(has(&parse(b"*3x\n$3\r\nSET\r\n", 0, 100, true, &mut r, &mut s), "multibulk length"));
        assert!(has(&parse(b"*1\r\n$3x\nSET\r\n", 0, 100, true, &mut r, &mut s), "bulk length"));
    }

    #[test]
    fn inline_tokenizer() {
        let (mut r, mut s) = (Vec::new(), Vec::new());
        let line = b"set  \"a b\" 'c\\'d' \"\\x41\\n\"\r\n";
        assert_eq!(parse(line, 0, 100, true, &mut r, &mut s), Parsed::Command { consumed: line.len() });
        assert_eq!(args_of(line, &r, &s), vec![b"set".to_vec(), b"a b".to_vec(), b"c'd".to_vec(), b"A\n".to_vec()]);
        assert_eq!(parse(b"\r\n", 0, 100, true, &mut r, &mut s), Parsed::Skip { consumed: 2 });
        assert!(matches!(parse(b"set \"a\r\n", 0, 100, true, &mut r, &mut s), Parsed::Error(_)));
        // Adjacent quoted and unquoted segments concatenate (protocol.tcl).
        let mixed = b"set \"\"\"tes\"t-'k'e\"y\" \"test\"'-'value\r\n";
        assert_eq!(parse(mixed, 0, 100, true, &mut r, &mut s), Parsed::Command { consumed: mixed.len() });
        assert_eq!(args_of(mixed, &r, &s), vec![b"set".to_vec(), b"test-key".to_vec(), b"test-value".to_vec()]);
        assert!(has(&parse(b"set foo 'b'a'r\r\n", 0, 100, true, &mut r, &mut s), "unbalanced"));
        assert!(has(&parse(b"set foo bar\x00 ex\r\n", 0, 100, true, &mut r, &mut s), "NUL"));
        assert_eq!(parse(b"PING", 0, 100, true, &mut r, &mut s), Parsed::Incomplete);
    }

    #[test]
    fn integers() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-12"), Some(-12));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775808"), None);
        assert_eq!(parse_i64(b"01"), None);
        assert_eq!(parse_i64(b"+1"), None);
        assert_eq!(parse_i64(b" 1"), None);
        assert_eq!(parse_i64(b"-"), None);
        assert_eq!(parse_i64(b"-0"), None);
        let mut v = Vec::new();
        for x in [0i64, 7, -7, i64::MAX, i64::MIN, 1234567890] {
            v.clear();
            push_i64(&mut v, x);
            assert_eq!(v, x.to_string().as_bytes());
        }
    }

    proptest::proptest! {
        // Any split point of any sequence of encoded commands yields the same
        // commands once all bytes are present, and never a protocol error.
        #[test]
        fn split_points_never_change_parse(args in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..40), 1..6), cut in 0usize..400) {
            let mut req = Vec::new();
            req.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in &args {
                req.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                req.extend_from_slice(a);
                req.extend_from_slice(b"\r\n");
            }
            let (mut r, mut s) = (Vec::new(), Vec::new());
            let cut = cut.min(req.len());
            match parse(&req[..cut], 0, 1 << 20, true, &mut r, &mut s) {
                Parsed::Incomplete => proptest::prop_assert!(cut < req.len()),
                Parsed::Command { consumed } => proptest::prop_assert_eq!(consumed, req.len()),
                other => proptest::prop_assert!(false, "unexpected {:?}", other),
            }
            proptest::prop_assert_eq!(parse(&req, 0, 1 << 20, true, &mut r, &mut s), Parsed::Command { consumed: req.len() });
            proptest::prop_assert_eq!(args_of(&req, &r, &s), args);
        }

        // Feeding a request in pieces with the resume state carried between
        // calls (as the server does) yields exactly the one-shot result, even
        // when the request does not start at offset 0 and the buffer is
        // compacted between calls.
        #[test]
        fn resumed_parse_matches_one_shot(args in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..40), 1..8), cuts in proptest::collection::vec(0usize..600, 0..6), lead in 0usize..3) {
            let mut req = Vec::new();
            req.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in &args {
                req.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                req.extend_from_slice(a);
                req.extend_from_slice(b"\r\n");
            }
            let mut cuts: Vec<usize> = cuts.into_iter().map(|c| c.min(req.len())).collect();
            cuts.sort_unstable();
            cuts.push(req.len());
            // `lead` bytes of an already-consumed earlier command precede the request.
            let mut buf = vec![b'x'; lead];
            let (mut r, mut s, mut partial) = (Vec::new(), Vec::new(), None);
            let mut fed = 0;
            let mut pos = lead;
            let mut done = false;
            for c in cuts {
                buf.extend_from_slice(&req[fed..c]);
                fed = c;
                match super::parse(&buf, pos, 1 << 20, true, &mut r, &mut s, &mut partial) {
                    Parsed::Incomplete => {
                        proptest::prop_assert!(fed < req.len());
                        // Compact like server::process does.
                        buf.drain(..pos);
                        pos = 0;
                    }
                    Parsed::Command { consumed } => {
                        proptest::prop_assert_eq!(consumed, req.len());
                        proptest::prop_assert_eq!(args_of(&buf, &r, &s), args.clone());
                        done = true;
                        break;
                    }
                    other => proptest::prop_assert!(false, "unexpected {:?}", other),
                }
            }
            proptest::prop_assert!(done);
        }
    }
}
