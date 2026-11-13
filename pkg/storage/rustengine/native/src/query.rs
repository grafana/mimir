// SPDX-License-Identifier: AGPL-3.0-only

//! Selecting series by their labels, and the snapshots queries read their chunks from.

use crate::cold::ColdFile;
use crate::engine::{State, decode_hist, encode_chunk, encode_hist};
use crate::head::{self, Chunk, HIST_ENCODING, NO_T, Series};
use crate::symbols::Sym;
use crate::wire::{Reader, Result, Writer};
use crate::xor;
use std::cmp::Ordering;
use std::sync::Arc;

/// A matcher Go resolved to the values it matches: a series matches when it has the label with one of the values,
/// or with exclude one that isn't, or lacks the label and match_missing is set.
pub struct Filter<'a> {
    pub name: &'a str,
    pub values: Vec<&'a str>,
    pub exclude: bool,
    pub match_missing: bool,
}

pub fn read_filters<'a>(r: &mut Reader<'a>) -> Result<Vec<Filter<'a>>> {
    let n = r.count(6)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        let m = r.count(4)?;
        let mut values = Vec::with_capacity(m);
        for _ in 0..m {
            values.push(r.str()?);
        }
        let exclude = r.bool()?;
        let match_missing = r.bool()?;
        out.push(Filter {
            name,
            values,
            exclude,
            match_missing,
        });
    }
    Ok(out)
}

/// A filter with its strings resolved to symbols.
struct Resolved<'p> {
    name: Option<Sym>,
    values: Vec<Sym>,
    exclude: bool,
    match_missing: bool,
    /// The postings of each of the values, for positive filters.
    lists: Vec<&'p [u32]>,
}

impl Resolved<'_> {
    #[inline]
    fn matches(&self, labels: &[Sym]) -> bool {
        let Some(name) = self.name else {
            return self.match_missing;
        };
        for p in labels.chunks_exact(2) {
            if p[0] == name {
                return self.values.contains(&p[1]) != self.exclude;
            }
        }
        self.match_missing
    }

    fn positive(&self) -> bool {
        !self.exclude && !self.match_missing
    }

    fn size(&self) -> usize {
        self.lists.iter().map(|l| l.len()).sum()
    }
}

/// The union of sorted lists.
fn union(lists: &[&[u32]]) -> Vec<u32> {
    match lists.len() {
        0 => Vec::new(),
        1 => lists[0].to_vec(),
        _ => {
            let mut all: Vec<u32> = Vec::with_capacity(lists.iter().map(|l| l.len()).sum());
            for l in lists {
                all.extend_from_slice(l);
            }
            all.sort_unstable();
            all.dedup();
            all
        }
    }
}

/// Keeps the slots of `into` that are in the sorted list, galloping through the longer one.
fn intersect(into: &mut Vec<u32>, list: &[u32]) {
    let mut at = 0;
    into.retain(|&s| {
        if at >= list.len() {
            return false;
        }
        if list[at] < s {
            // Gallop.
            let mut step = 1;
            let mut hi = at + 1;
            while hi < list.len() && list[hi] < s {
                at = hi;
                hi += step;
                step *= 2;
            }
            let hi = hi.min(list.len());
            at += list[at..hi].partition_point(|&x| x < s);
        }
        at < list.len() && list[at] == s
    });
}

/// The slots of the series matching all filters, in increasing order.
pub fn select(st: &State, filters: &[Filter]) -> Vec<u32> {
    let head = &st.head;
    let mut resolved: Vec<Resolved> = Vec::with_capacity(filters.len());
    for f in filters {
        let name = head.symbols.lookup(f.name.as_bytes());
        let values: Vec<Sym> = f
            .values
            .iter()
            .filter_map(|v| head.symbols.lookup(v.as_bytes()))
            .collect();
        let mut lists = Vec::new();
        let by_value = name.and_then(|n| head.postings.by_name.get(&n));
        if !f.exclude && !f.match_missing {
            if let Some(by_value) = by_value {
                for v in &values {
                    if let Some(l) = by_value.get(v) {
                        lists.push(l.as_slice());
                    }
                }
            }
            if lists.is_empty() {
                return Vec::new();
            }
        }
        resolved.push(Resolved {
            name,
            values,
            exclude: f.exclude,
            match_missing: f.match_missing,
            lists,
        });
    }
    // The positive filters intersect their postings, smallest first; the others check each candidate's labels.
    let mut positive: Vec<&Resolved> = resolved.iter().filter(|r| r.positive()).collect();
    positive.sort_by_key(|r| r.size());
    let rest: Vec<&Resolved> = resolved.iter().filter(|r| !r.positive()).collect();
    let mut out: Vec<u32> = match positive.first() {
        Some(seed) => union(&seed.lists),
        None => head.iter().map(|(slot, _)| slot).collect(),
    };
    for p in positive.iter().skip(1) {
        if out.is_empty() {
            break;
        }
        if p.lists.len() == 1 {
            intersect(&mut out, p.lists[0]);
        } else if p.size() <= out.len() * 4 {
            intersect(&mut out, &union(&p.lists));
        } else {
            // Many values over many series: checking the candidates' labels is cheaper than merging postings.
            out.retain(|&s| head.get(s).is_some_and(|x| p.matches(&x.labels)));
        }
    }
    if !rest.is_empty() {
        out.retain(|&s| {
            head.get(s)
                .is_some_and(|x| rest.iter().all(|r| r.matches(&x.labels)))
        });
    }
    out
}

#[inline]
pub fn overlaps(s: &Series, mint: i64, maxt: i64) -> bool {
    (s.mem_mint != NO_T && s.mem_mint <= maxt && s.mem_maxt >= mint)
        || (s.cold_mint != NO_T && s.cold_mint <= maxt && s.cold_maxt >= mint)
}

/// Orders label sets as Prometheus does, by their pairs, through the symbols' ranks rather than their strings.
#[inline]
pub fn compare_labels(st: &State, a: &[Sym], b: &[Sym]) -> Ordering {
    let symbols = &st.head.symbols;
    for (x, y) in a.iter().zip(b) {
        if x != y {
            return symbols.rank(*x).cmp(&symbols.rank(*y));
        }
    }
    a.len().cmp(&b.len())
}

/// What keeps a cursor's chunk bytes alive: the pages and files they are in, and the buffer of the open chunks.
#[allow(dead_code)] // Only held, for what they keep alive.
enum Keep {
    Page(crate::head::Bytes),
    File(Arc<ColdFile>),
}

pub struct QChunk {
    pub mint: i64,
    pub maxt: i64,
    pub encoding: u8,
    ptr: *const u8,
    len: u32,
}

impl QChunk {
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

#[derive(Clone, Copy)]
pub struct QSeries {
    pub slot: u32,
    pub first: u32,
    pub count: u32,
}

/// What a query reads, taken when it starts: later appends, flushes and retention don't change it. Its chunks point
/// into pages and files it keeps, once each.
pub struct Cursor {
    pub series: Vec<QSeries>,
    pub chunks: Vec<QChunk>,
    keep: Vec<Keep>,
    /// Chunks made for the query: copies of open chunks, and merged or encoded ones.
    owned: Vec<Box<[u8]>>,
    page: Vec<u8>,
}

// The chunks point into what the cursor keeps, which nothing changes.
unsafe impl Send for Cursor {}
unsafe impl Sync for Cursor {}

impl ColdFile {
    #[inline]
    pub fn bytes_at(&self, at: usize, len: usize) -> &[u8] {
        self.bytes(&crate::cold::ChunkRef {
            mint: 0,
            maxt: 0,
            at,
            len,
            encoding: 0,
        })
    }
}

impl Cursor {
    fn push_page(&mut self, mint: i64, maxt: i64, encoding: u8, b: &crate::head::Bytes) {
        if !matches!(self.keep.last(), Some(Keep::Page(p)) if p.same_page(b)) {
            self.keep.push(Keep::Page(b.clone()));
        }
        self.chunks.push(QChunk {
            mint,
            maxt,
            encoding,
            ptr: b.as_ptr(),
            len: b.len() as u32,
        });
    }

    fn push_owned(&mut self, mint: i64, maxt: i64, encoding: u8, b: Box<[u8]>) {
        self.push_copy(mint, maxt, encoding, &b);
    }

    /// Copies the bytes into the cursor's pages, which never grow past their capacity, so what points into them
    /// stays valid.
    fn push_copy(&mut self, mint: i64, maxt: i64, encoding: u8, b: &[u8]) {
        const PAGE: usize = 64 << 10;
        if b.len() > PAGE / 4 {
            let own: Box<[u8]> = b.into();
            self.chunks.push(QChunk {
                mint,
                maxt,
                encoding,
                ptr: own.as_ptr(),
                len: own.len() as u32,
            });
            self.owned.push(own);
            return;
        }
        if self.page.capacity() - self.page.len() < b.len() {
            let full = std::mem::replace(&mut self.page, Vec::with_capacity(PAGE));
            if !full.is_empty() {
                self.owned.push(full.into_boxed_slice());
            }
        }
        let at = self.page.len();
        self.page.extend_from_slice(b);
        self.chunks.push(QChunk {
            mint,
            maxt,
            encoding,
            ptr: unsafe { self.page.as_ptr().add(at) },
            len: b.len() as u32,
        });
    }

    /// The chunks of the series selected in [mint, maxt]: with ordered, those of a series don't overlap.
    pub fn open(
        st: &State,
        slots: &[u32],
        mint: i64,
        maxt: i64,
        sorted: bool,
        ordered: bool,
        labels_only: bool,
    ) -> Cursor {
        let files: Vec<&Arc<ColdFile>> = st
            .cold
            .iter()
            .filter(|c| c.mint <= maxt && c.maxt >= mint)
            .collect();
        let mut cur = Cursor {
            series: Vec::with_capacity(slots.len()),
            chunks: Vec::with_capacity(slots.len()),
            keep: Vec::new(),
            owned: Vec::new(),
            page: Vec::new(),
        };
        for f in &files {
            cur.keep.push(Keep::File(Arc::clone(f)));
        }
        for &slot in slots {
            let Some(s) = st.head.get(slot) else { continue };
            if !overlaps(s, mint, maxt) {
                continue;
            }
            let first = cur.chunks.len();
            if labels_only {
                cur.series.push(QSeries {
                    slot,
                    first: first as u32,
                    count: 0,
                });
                continue;
            }
            if s.cold_mint != NO_T && s.cold_mint <= maxt && s.cold_maxt >= mint {
                for f in &files {
                    for c in f.chunks_of(slot) {
                        if c.mint <= maxt && c.maxt >= mint {
                            let b = f.bytes(&c);
                            cur.chunks.push(QChunk {
                                mint: c.mint,
                                maxt: c.maxt,
                                encoding: c.encoding,
                                ptr: b.as_ptr(),
                                len: b.len() as u32,
                            });
                        }
                    }
                }
            }
            if s.mem_mint != NO_T && s.mem_mint <= maxt && s.mem_maxt >= mint {
                for c in &s.sealed {
                    if c.mint <= maxt && c.maxt >= mint {
                        cur.push_page(c.mint, c.maxt, c.encoding, &c.data);
                    }
                }
                if let Some(h) = &s.head {
                    if let (Some(a), Some(b)) = (h.first_timestamp(), h.last_timestamp()) {
                        if a <= maxt && b >= mint {
                            cur.push_copy(a, b, 1, h.bytes());
                        }
                    }
                }
                let hist: Vec<_> = s
                    .hist()
                    .iter()
                    .filter(|h| h.t >= mint && h.t <= maxt)
                    .cloned()
                    .collect();
                if !hist.is_empty() {
                    let c = encode_hist(&hist);
                    cur.push_owned(c.mint, c.maxt, c.encoding, (&*c.data).into());
                }
                let ooo: Vec<(i64, f64)> = s
                    .ooo()
                    .iter()
                    .filter(|x| x.0 >= mint && x.0 <= maxt)
                    .copied()
                    .collect();
                for part in ooo.chunks(head::CHUNK_SAMPLES as usize) {
                    let c = encode_chunk(part);
                    cur.push_owned(c.mint, c.maxt, 1, (&*c.data).into());
                }
            }
            if cur.chunks.len() == first {
                continue;
            }
            if ordered {
                cur.normalize(first);
            }
            let count = (cur.chunks.len() - first) as u32;
            cur.series.push(QSeries {
                slot,
                first: first as u32,
                count,
            });
        }
        if sorted {
            // The series' label ranks, side by side, so sorting compares numbers in one array rather than following
            // each series to its labels.
            let mut keys: Vec<u64> = Vec::with_capacity(cur.series.len() * 12);
            let mut spans: Vec<(u32, u32)> = Vec::with_capacity(cur.series.len());
            for q in &cur.series {
                let labels = &st.head.get(q.slot).unwrap().labels;
                spans.push((keys.len() as u32, labels.len() as u32));
                keys.extend(labels.iter().map(|&sym| st.head.symbols.rank(sym)));
            }
            let mut order: Vec<u32> = (0..cur.series.len() as u32).collect();
            let key = |i: u32| {
                let (at, len) = spans[i as usize];
                &keys[at as usize..(at + len) as usize]
            };
            order.sort_unstable_by(|&a, &b| key(a).cmp(key(b)));
            let series = std::mem::take(&mut cur.series);
            cur.series = order
                .into_iter()
                .map(|i| QSeries {
                    ..series[i as usize]
                })
                .collect();
        }
        cur
    }

    /// Makes the float chunks from `first` on non-overlapping and in order, merging the overlapping ones.
    fn normalize(&mut self, first: usize) {
        let part = &mut self.chunks[first..];
        part.sort_by_key(|c| (c.mint, c.maxt));
        if !part.windows(2).any(|w| w[1].mint <= w[0].maxt) {
            return;
        }
        let taken: Vec<QChunk> = self.chunks.drain(first..).collect();
        let mut floats: Vec<(i64, f64)> = Vec::new();
        let mut hists = Vec::new();
        for c in &taken {
            if c.encoding == HIST_ENCODING {
                if let Ok(h) = decode_hist(c.bytes()) {
                    hists.extend(h);
                }
            } else {
                floats.extend(xor::decode(c.bytes()));
            }
        }
        // In-order samples come first in the chunks, so a stable sort keeps them over out-of-order duplicates.
        floats.sort_by_key(|x| x.0);
        floats.dedup_by_key(|x| x.0);
        for part in floats.chunks(head::CHUNK_SAMPLES as usize) {
            let c: Chunk = encode_chunk(part);
            self.push_owned(c.mint, c.maxt, 1, (&*c.data).into());
        }
        if !hists.is_empty() {
            hists.sort_by_key(|h| h.t);
            let c = encode_hist(&hists);
            self.push_owned(c.mint, c.maxt, c.encoding, (&*c.data).into());
        }
        self.chunks[first..].sort_by_key(|c| c.mint);
    }
}

pub fn write_labels(st: &State, w: &mut Writer, labels: &[Sym]) {
    w.u32(labels.len() as u32 / 2);
    for p in labels.chunks_exact(2) {
        w.bytes(st.head.symbols.get(p[0]).as_bytes());
        w.bytes(st.head.symbols.get(p[1]).as_bytes());
    }
}

/// Which series label queries over [mint, maxt] see, as Prometheus' blocks do: the head's whole index when the head
/// overlaps the range, and the series of the cold files that overlap it.
pub struct Visibility<'a> {
    head: bool,
    files: Vec<&'a Arc<ColdFile>>,
    all: bool,
    /// The oldest data Prometheus' head would still have.
    floor: i64,
}

impl<'a> Visibility<'a> {
    pub fn new(st: &'a State, mint: i64, maxt: i64) -> Self {
        // As Prometheus' head index reader: the head's whole index when the range overlaps its in-order or
        // out-of-order samples.
        let in_head = |lo: i64, hi: i64| lo != NO_T && lo <= maxt && hi >= mint;
        let head = in_head(st.head_mint, st.head_maxt) || in_head(st.ooo_mint, st.ooo_maxt);
        let files: Vec<_> = st
            .cold
            .iter()
            .filter(|c| c.mint <= maxt && c.maxt >= mint)
            .collect();
        let all = head && files.len() == st.cold.len() && covers_all(st, mint, maxt);
        Visibility {
            head,
            files,
            all,
            floor: st.min_valid,
        }
    }

    #[inline]
    pub fn sees(&self, st: &State, slot: u32) -> bool {
        if self.all {
            return true;
        }
        let Some(s) = st.head.get(slot) else {
            return false;
        };
        if self.head && (s.in_head || s.idle && s.cold_maxt >= self.floor) {
            return true;
        }
        s.cold_mint != NO_T && self.files.iter().any(|f| f.contains(slot))
    }

    fn any(&self, st: &State, list: &[u32]) -> bool {
        if self.all {
            return !list.is_empty();
        }
        list.iter().any(|&s| self.sees(st, s))
    }
}

/// The label names of the series selected by the filters, or of all, with data in [mint, maxt].
pub fn label_names(
    st: &State,
    filters: &[Filter],
    mint: i64,
    maxt: i64,
    limit: usize,
) -> Vec<String> {
    let vis = Visibility::new(st, mint, maxt);
    let mut names: Vec<&str> = if filters.is_empty() {
        st.head
            .postings
            .by_name
            .iter()
            .filter(|(_, values)| values.values().any(|l| vis.any(st, l)))
            .map(|(&n, _)| st.head.symbols.get(n))
            .collect()
    } else {
        let mut seen: rustc_hash::FxHashSet<Sym> = Default::default();
        for slot in select(st, filters) {
            if vis.sees(st, slot) {
                for p in st.head.get(slot).unwrap().labels.chunks_exact(2) {
                    seen.insert(p[0]);
                }
            }
        }
        seen.into_iter().map(|n| st.head.symbols.get(n)).collect()
    };
    names.sort_unstable();
    if limit > 0 {
        names.truncate(limit);
    }
    names.into_iter().map(str::to_owned).collect()
}

pub fn label_values(
    st: &State,
    name: &str,
    filters: &[Filter],
    mint: i64,
    maxt: i64,
    limit: usize,
) -> Vec<String> {
    let Some(n) = st.head.symbols.lookup(name.as_bytes()) else {
        return Vec::new();
    };
    let Some(by_value) = st.head.postings.by_name.get(&n) else {
        return Vec::new();
    };
    let vis = Visibility::new(st, mint, maxt);
    let mut values: Vec<&str> = if filters.is_empty() {
        by_value
            .iter()
            .filter(|(_, l)| vis.any(st, l))
            .map(|(&v, _)| st.head.symbols.get(v))
            .collect()
    } else {
        let selected = select(st, filters);
        if selected.len() > by_value.len() * 8 {
            // Fewer values than series: intersect each value's postings with the selection.
            by_value
                .iter()
                .filter(|(_, l)| {
                    let mut x: Vec<u32> = l.to_vec();
                    intersect(&mut x, &selected);
                    vis.any(st, &x)
                })
                .map(|(&v, _)| st.head.symbols.get(v))
                .collect()
        } else {
            let mut seen: rustc_hash::FxHashSet<Sym> = Default::default();
            for slot in selected {
                if vis.sees(st, slot) {
                    for p in st.head.get(slot).unwrap().labels.chunks_exact(2) {
                        if p[0] == n {
                            seen.insert(p[1]);
                        }
                    }
                }
            }
            seen.into_iter().map(|v| st.head.symbols.get(v)).collect()
        }
    };
    values.sort_unstable();
    if limit > 0 {
        values.truncate(limit);
    }
    values.into_iter().map(str::to_owned).collect()
}

/// Whether [mint, maxt] covers all the data, so every series counts without checking its times.
fn covers_all(st: &State, mint: i64, maxt: i64) -> bool {
    let lo = st
        .cold
        .iter()
        .map(|c| c.mint)
        .min()
        .unwrap_or(i64::MAX)
        .min(if st.head_mint == NO_T {
            i64::MAX
        } else {
            st.head_mint
        })
        .min(if st.ooo_mint == NO_T {
            i64::MAX
        } else {
            st.ooo_mint
        });
    let hi = st
        .cold
        .iter()
        .map(|c| c.maxt)
        .max()
        .unwrap_or(i64::MIN)
        .max(st.head_maxt)
        .max(st.ooo_maxt);
    mint <= lo && maxt >= hi
}
