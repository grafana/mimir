// SPDX-License-Identifier: AGPL-3.0-only

//! Remote write 2.0 records read in place: positions into the content only, since Go owns the content and passes it
//! again with every call that needs it.

use crate::wire::Result;

#[derive(Clone, Copy, Default)]
pub struct Span {
    pub at: u32,
    pub len: u32,
}

impl Span {
    #[inline]
    pub fn of<'a>(&self, content: &'a [u8]) -> &'a [u8] {
        &content[self.at as usize..(self.at + self.len) as usize]
    }
}

#[derive(Clone, Copy, Default)]
pub struct Series {
    pub refs_at: u32,
    pub refs_to: u32,
    pub samples_at: u32,
    pub samples_to: u32,
    /// The whole field 5 entry of the series, tag included, to copy it into a subset.
    pub entry: Span,
    /// Whether the series has more than floats: histograms, exemplars, a created timestamp or unknown fields.
    pub other: bool,
}

pub struct Metadata {
    pub series: u32,
    pub kind: u8,
    pub help: u32,
    pub unit: u32,
}

#[derive(Default)]
pub struct Record {
    pub symbols: Vec<Span>,
    pub refs: Vec<u32>,
    pub samples: Vec<(i64, f64)>,
    pub series: Vec<Series>,
    pub metadata: Vec<Metadata>,
    /// The top-level fields that aren't series, to copy into a subset.
    pub rest: Vec<Span>,
    pub source: u8,
    pub partial: bool,
    pub min_ts: i64,
    pub content_len: usize,
}

const MALFORMED: &str = "malformed remote write 2.0 record";

/// A varint: one or two bytes inline, the rare longer ones out of line.
#[inline(always)]
fn uvarint(data: &[u8]) -> Option<(u64, usize)> {
    let b0 = *data.first()?;
    if b0 < 0x80 {
        return Some((b0 as u64, 1));
    }
    let b1 = *data.get(1)?;
    if b1 < 0x80 {
        return Some(((b0 & 0x7f) as u64 | (b1 as u64) << 7, 2));
    }
    uvarint_slow(data)
}

#[inline(never)]
fn uvarint_slow(data: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &b) in data.iter().enumerate() {
        if i == 10 {
            return None;
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return None;
            }
            return Some((x | (b as u64) << s, i + 1));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    None
}

/// One protobuf field: its number, wire type, value (a varint's encoding, a fixed's bytes, or a length-delimited
/// field's bytes) and the length of the whole field.
#[inline]
fn field(data: &[u8]) -> Option<(u64, u8, &[u8], usize)> {
    let (tag, n) = uvarint(data)?;
    let rest = &data[n..];
    let (number, wire) = (tag >> 3, (tag & 7) as u8);
    match wire {
        0 => {
            let (_, m) = uvarint(rest)?;
            Some((number, wire, &rest[..m], n + m))
        }
        1 => (rest.len() >= 8).then(|| (number, wire, &rest[..8], n + 8)),
        2 => {
            let (len, m) = uvarint(rest)?;
            let len = usize::try_from(len).ok()?;
            if rest.len() - m < len {
                return None;
            }
            Some((number, wire, &rest[m..m + len], n + m + len))
        }
        5 => (rest.len() >= 4).then(|| (number, wire, &rest[..4], n + 4)),
        _ => None,
    }
}

fn varint_value(v: &[u8]) -> u64 {
    uvarint(v).map_or(0, |(x, _)| x)
}

impl Record {
    pub fn clear(&mut self) {
        self.symbols.clear();
        self.refs.clear();
        self.samples.clear();
        self.series.clear();
        self.metadata.clear();
        self.rest.clear();
        self.source = 0;
        self.partial = false;
        self.min_ts = i64::MAX;
        self.content_len = 0;
    }

    /// Reads the record. References are checked against the common symbols, whose count is `common`, and the
    /// record's own after `offset`.
    pub fn read(&mut self, content: &[u8], common: u32, offset: u32) -> Result<()> {
        self.clear();
        if content.len() > u32::MAX as usize {
            return Err(MALFORMED.into());
        }
        self.content_len = content.len();
        let base = content.as_ptr() as usize;
        let span = |s: &[u8]| Span {
            at: (s.as_ptr() as usize - base) as u32,
            len: s.len() as u32,
        };
        let mut bodies: Vec<(Span, Span)> = Vec::with_capacity(self.series.capacity().max(64));
        let mut at = 0;
        while at < content.len() {
            let (number, wire, value, len) = field(&content[at..]).ok_or(MALFORMED)?;
            let whole = Span {
                at: at as u32,
                len: len as u32,
            };
            match (number, wire) {
                (4, 2) => {
                    self.symbols.push(span(value));
                    self.rest.push(whole);
                }
                (5, 2) => bodies.push((span(value), whole)),
                (2, 0) => {
                    self.source = varint_value(value) as u8;
                    self.rest.push(whole);
                }
                // Series of a version 1 request.
                (1, _) => return Err(MALFORMED.into()),
                // Validation the distributor did.
                (1000, _) | (1001, _) => self.rest.push(whole),
                _ => {
                    self.partial = true;
                    self.rest.push(whole);
                }
            }
            at += len;
        }
        let limit = offset as u64 + self.symbols.len() as u64;
        let valid = |r: u64| r < common as u64 || (r >= offset as u64 && r < limit);
        self.series.reserve(bodies.len());
        self.samples.reserve(bodies.len());
        self.refs.reserve(bodies.len() * 16);
        for (body, entry) in bodies {
            self.read_series(body.of(content), entry, &valid)?;
        }
        Ok(())
    }

    fn read_series(
        &mut self,
        mut body: &[u8],
        entry: Span,
        valid: &impl Fn(u64) -> bool,
    ) -> Result<()> {
        let mut s = Series {
            refs_at: self.refs.len() as u32,
            samples_at: self.samples.len() as u32,
            entry,
            ..Default::default()
        };
        while !body.is_empty() {
            let (number, wire, mut value, len) = field(body).ok_or(MALFORMED)?;
            body = &body[len..];
            match (number, wire) {
                (1, 2) => {
                    self.refs.reserve(value.len());
                    while !value.is_empty() {
                        let (r, n) = uvarint(value).ok_or(MALFORMED)?;
                        value = &value[n..];
                        if !valid(r) {
                            return Err(MALFORMED.into());
                        }
                        self.refs.push(r as u32);
                    }
                }
                (2, 2) => {
                    // What writers send: the value as fixed64 field 1, then the time as varint field 2.
                    if value.len() >= 10 && value[0] == 0x09 && value[9] == 0x10 {
                        if let Some((t, n)) = uvarint(&value[10..]) {
                            if 10 + n == value.len() {
                                let v = f64::from_bits(u64::from_le_bytes(
                                    value[1..9].try_into().unwrap(),
                                ));
                                self.samples.push((t as i64, v));
                                self.min_ts = self.min_ts.min(t as i64);
                                continue;
                            }
                        }
                    }
                    let (mut v, mut t) = (0f64, 0i64);
                    while !value.is_empty() {
                        let (n2, w2, v2, l2) = field(value).ok_or(MALFORMED)?;
                        value = &value[l2..];
                        match (n2, w2) {
                            (1, 1) => {
                                v = f64::from_bits(u64::from_le_bytes(v2.try_into().unwrap()))
                            }
                            (2, 0) => t = varint_value(v2) as i64,
                            _ => {}
                        }
                    }
                    self.samples.push((t, v));
                    self.min_ts = self.min_ts.min(t);
                }
                (5, 2) => {
                    if value.is_empty() {
                        // The empty metadata writers put in every series.
                        continue;
                    }
                    let mut m = Metadata {
                        series: self.series.len() as u32,
                        kind: 0,
                        help: 0,
                        unit: 0,
                    };
                    while !value.is_empty() {
                        let (n2, w2, v2, l2) = field(value).ok_or(MALFORMED)?;
                        value = &value[l2..];
                        if w2 != 0 {
                            continue;
                        }
                        let x = varint_value(v2);
                        match n2 {
                            1 => m.kind = x as u8,
                            3 | 4 => {
                                if !valid(x) {
                                    return Err(MALFORMED.into());
                                }
                                if n2 == 3 {
                                    m.help = x as u32;
                                } else {
                                    m.unit = x as u32;
                                }
                            }
                            _ => {}
                        }
                    }
                    self.metadata.push(m);
                }
                (3, 2) => {
                    s.other = true;
                    self.observe(value, 15)?;
                }
                (4, 2) => {
                    s.other = true;
                    self.observe(value, 3)?;
                }
                _ => s.other = true,
            }
        }
        s.refs_to = self.refs.len() as u32;
        s.samples_to = self.samples.len() as u32;
        if (s.refs_to - s.refs_at) % 2 != 0 {
            return Err(MALFORMED.into());
        }
        self.series.push(s);
        Ok(())
    }

    fn observe(&mut self, mut message: &[u8], ts_field: u64) -> Result<()> {
        while !message.is_empty() {
            let (n, w, v, l) = field(message).ok_or(MALFORMED)?;
            message = &message[l..];
            if n == ts_field && w == 0 {
                self.min_ts = self.min_ts.min(varint_value(v) as i64);
            }
        }
        Ok(())
    }

    /// The bytes of symbol `r`: a common one, or the record's.
    #[inline]
    pub fn symbol<'a>(
        &self,
        r: u32,
        content: &'a [u8],
        common: &'a [Box<str>],
        offset: u32,
    ) -> &'a [u8] {
        if r < offset {
            common[r as usize].as_bytes()
        } else {
            self.symbols[(r - offset) as usize].of(content)
        }
    }

    #[inline]
    pub fn series_refs(&self, s: &Series) -> &[u32] {
        &self.refs[s.refs_at as usize..s.refs_to as usize]
    }

    #[inline]
    pub fn series_samples(&self, s: &Series) -> &[(i64, f64)] {
        &self.samples[s.samples_at as usize..s.samples_to as usize]
    }

    /// The content of a record with only the series at indices, which are in order.
    pub fn subset(&self, content: &[u8], indices: &[u32], out: &mut Vec<u8>) {
        for r in &self.rest {
            out.extend_from_slice(r.of(content));
        }
        for &i in indices {
            if let Some(s) = self.series.get(i as usize) {
                out.extend_from_slice(s.entry.of(content));
            }
        }
    }
}
