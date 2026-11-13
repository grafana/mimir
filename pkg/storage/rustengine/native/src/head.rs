// SPDX-License-Identifier: AGPL-3.0-only

//! The series of a tenant and what is in memory of them.

use crate::symbols::{Sym, Symbols};
use crate::xor;
use hashbrown::HashTable;
use rustc_hash::FxHashMap;
use std::hash::Hasher;
use std::sync::Arc;

pub const NO_T: i64 = i64::MIN;
/// Samples per in-order chunk, as Prometheus targets.
pub const CHUNK_SAMPLES: u16 = 120;

pub const KIND_FLOAT: u8 = 1;
pub const KIND_HISTOGRAM: u8 = 2;
pub const KIND_FLOAT_HISTOGRAM: u8 = 3;

/// Why a sample wasn't appended. The codes are the wire's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Reject {
    Duplicate = 1,
    OutOfOrder = 2,
    TooOld = 3,
    OutOfBounds = 4,
    TooFarPast = 5,
    TooFarFuture = 6,
    NotFound = 7,
    ExemplarsDisabled = 8,
    ExemplarOutOfOrder = 9,
    DuplicateHistogramToFloat = 10,
}

/// An immutable chunk: XOR for floats, or `HIST_ENCODING` for histogram samples.
#[derive(Clone)]
pub struct Chunk {
    pub mint: i64,
    pub maxt: i64,
    pub encoding: u8,
    pub data: Arc<[u8]>,
}

/// Histogram samples, kept as their protobuf, since the querier decodes them.
pub const HIST_ENCODING: u8 = 0x80;

#[derive(Clone)]
pub struct Hist {
    pub t: i64,
    pub kind: u8,
    pub data: Box<[u8]>,
}

#[derive(Clone)]
pub struct Exemplar {
    pub t: i64,
    pub v: f64,
    pub labels: Box<[(Sym, Sym)]>,
}

/// What few series have: out-of-order floats, histograms, exemplars.
#[derive(Default, Clone)]
pub struct Extra {
    /// Out-of-order floats not yet flushed, by time.
    pub ooo: Vec<(i64, f64)>,
    /// Histograms not yet flushed, by time.
    pub hist: Vec<Hist>,
    pub exemplars: Vec<Exemplar>,
}

pub struct Series {
    /// A second hash of the labels: with the table's, finding a series never compares its labels.
    pub check: u64,
    /// The newest in-order sample in memory, or NO_T.
    pub max_t: i64,
    pub last_v: u64,
    /// The kind of the newest in-order sample, for staleness markers of histogram series.
    pub last_kind: u8,
    pub head: Option<Box<xor::Appender>>,
    pub sealed: Vec<Chunk>,
    pub extra: Option<Box<Extra>>,
    /// When the series last got a sample, in wall-clock milliseconds, for the active series.
    pub active_ms: i64,
    pub labels: Box<[Sym]>,
    pub hash: u64,
    /// The span of the series' data on disk, NO_T without any.
    pub cold_mint: i64,
    pub cold_maxt: i64,
    /// The span of its data in memory, in order or not, NO_T without any.
    pub mem_mint: i64,
    pub mem_maxt: i64,
    /// Custom tracker matches, set by Go when the series is created.
    pub trackers: u64,
    pub otlp: bool,
    /// Whether the series counts as in the head for Go: created, and not yet left it at a flush or eviction.
    pub in_head: bool,
    /// Whether it left the head for being idle, which Prometheus' head would have kept until its data is compacted:
    /// label queries still see it until then.
    pub idle: bool,
}

impl Series {
    #[inline]
    pub fn in_head(&self) -> bool {
        self.in_head
    }

    pub fn has_mem(&self) -> bool {
        self.mem_mint != NO_T
    }

    pub fn ooo(&self) -> &[(i64, f64)] {
        self.extra.as_ref().map_or(&[], |e| &e.ooo)
    }

    pub fn hist(&self) -> &[Hist] {
        self.extra.as_ref().map_or(&[], |e| &e.hist)
    }

    pub fn extra_mut(&mut self) -> &mut Extra {
        self.extra.get_or_insert_with(Default::default)
    }

    fn widen_mem(&mut self, t: i64) {
        if self.mem_mint == NO_T {
            self.mem_mint = t;
            self.mem_maxt = t;
        } else {
            self.mem_mint = self.mem_mint.min(t);
            self.mem_maxt = self.mem_maxt.max(t);
        }
    }

    /// Seals the open chunk, if any.
    pub fn seal(&mut self) {
        if let Some(head) = self.head.take() {
            if let (Some(mint), Some(maxt)) = (head.first_timestamp(), head.last_timestamp()) {
                let mut bytes = head.into_bytes();
                bytes.shrink_to_fit();
                self.sealed.push(Chunk {
                    mint,
                    maxt,
                    encoding: 1,
                    data: bytes.into(),
                });
            }
        }
    }

    /// Appends an in-order float, cutting the open chunk at its size or the block's end.
    #[inline]
    pub fn append_in_order(&mut self, t: i64, v: f64, block_range: i64) {
        if let Some(head) = &self.head {
            let block_end = (head.first_timestamp().unwrap_or(t)).div_euclid(block_range)
                * block_range
                + block_range;
            if head.len() >= CHUNK_SAMPLES as usize
                || head.byte_len() >= xor::MAX_BYTES_BEFORE_APPEND
                || t >= block_end
            {
                self.seal();
            }
        }
        self.head.get_or_insert_with(Default::default).append(t, v);
        self.max_t = t;
        self.last_v = v.to_bits();
        self.last_kind = KIND_FLOAT;
        self.widen_mem(t);
    }

    pub fn insert_ooo(&mut self, t: i64, v: f64) {
        let extra = self.extra_mut();
        match extra.ooo.binary_search_by_key(&t, |s| s.0) {
            // Prometheus drops an out-of-order duplicate.
            Ok(_) => return,
            Err(at) => extra.ooo.insert(at, (t, v)),
        }
        self.widen_mem(t);
    }

    pub fn insert_hist(&mut self, h: Hist) {
        let t = h.t;
        let kind = h.kind;
        let extra = self.extra_mut();
        match extra.hist.binary_search_by_key(&t, |s| s.t) {
            Ok(at) => extra.hist[at] = h,
            Err(at) => extra.hist.insert(at, h),
        }
        if self.max_t == NO_T || t >= self.max_t {
            self.max_t = t;
            self.last_kind = kind;
        }
        self.widen_mem(t);
    }

    /// The in-memory floats in order, for flushing or merging: sealed chunks, the open chunk, and out-of-order ones.
    pub fn mem_chunks(&self) -> impl Iterator<Item = Chunk> + '_ {
        self.sealed
            .iter()
            .cloned()
            .chain(self.head.iter().filter_map(|h| {
                Some(Chunk {
                    mint: h.first_timestamp()?,
                    maxt: h.last_timestamp()?,
                    encoding: 1,
                    data: h.bytes().into(),
                })
            }))
    }

    pub fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Series>()
            + self.labels.len() * 4
            + self.head.as_ref().map_or(0, |h| {
                std::mem::size_of::<xor::Appender>() + h.byte_capacity()
            })
            + self.sealed.capacity() * std::mem::size_of::<Chunk>()
            + self.sealed.iter().map(|c| c.data.len() + 16).sum::<usize>()
            + self.extra.as_ref().map_or(0, |e| {
                std::mem::size_of::<Extra>()
                    + e.ooo.capacity() * 16
                    + e.hist.iter().map(|h| h.data.len() + 32).sum::<usize>()
            })
    }
}

#[inline]
pub fn hash_pairs(pairs: &[Sym]) -> (u64, u64) {
    let mut a = rustc_hash::FxHasher::default();
    let mut b: u64 = 0x9E37_79B9_7F4A_7C15;
    for &p in pairs {
        a.write_u32(p);
        b = (b ^ p as u64)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9)
            .rotate_left(29);
    }
    let a = a.finish();
    // A different mix, so the two together are as good as a 128-bit hash.
    b ^= b >> 31;
    b = b.wrapping_mul(0x94D0_49BB_1331_11EB);
    b ^= b >> 29;
    (a, b | 1)
}

/// The label index: postings by name and value, of slots in increasing order.
#[derive(Default)]
pub struct Postings {
    pub by_name: FxHashMap<Sym, FxHashMap<Sym, Vec<u32>>>,
    /// Changes when a label gets a value or loses one, so Go can keep what it resolved from its values.
    pub generations: FxHashMap<Sym, u64>,
    next_generation: u64,
}

impl Postings {
    pub fn generation(&self, name: Sym) -> u64 {
        self.generations.get(&name).copied().unwrap_or(0)
    }

    pub fn add(&mut self, labels: &[Sym], slot: u32) {
        for pair in labels.chunks_exact(2) {
            let values = self.by_name.entry(pair[0]).or_default();
            let list = match values.get_mut(&pair[1]) {
                Some(list) => list,
                None => {
                    values.insert(pair[1], Vec::new());
                    self.next_generation += 1;
                    self.generations.insert(pair[0], self.next_generation);
                    self.by_name
                        .get_mut(&pair[0])
                        .unwrap()
                        .get_mut(&pair[1])
                        .unwrap()
                }
            };
            // Slots only grow, so appending keeps the lists sorted.
            debug_assert!(list.last().is_none_or(|&l| l < slot));
            list.push(slot);
        }
    }

    /// Removes the slots, whose labels are given, in one pass over each list they are in.
    pub fn remove(&mut self, removed: &[(u32, Box<[Sym]>)]) {
        let mut touched: FxHashMap<(Sym, Sym), Vec<u32>> = FxHashMap::default();
        for (slot, labels) in removed {
            for pair in labels.chunks_exact(2) {
                touched.entry((pair[0], pair[1])).or_default().push(*slot);
            }
        }
        for ((name, value), mut slots) in touched {
            slots.sort_unstable();
            let Some(values) = self.by_name.get_mut(&name) else {
                continue;
            };
            if let Some(list) = values.get_mut(&value) {
                let mut i = 0;
                list.retain(|s| {
                    while i < slots.len() && slots[i] < *s {
                        i += 1;
                    }
                    !(i < slots.len() && slots[i] == *s)
                });
                if list.is_empty() {
                    values.remove(&value);
                    self.next_generation += 1;
                    self.generations.insert(name, self.next_generation);
                } else if list.capacity() > 2 * list.len() + 8 {
                    list.shrink_to_fit();
                }
            }
            if values.is_empty() {
                self.by_name.remove(&name);
            }
        }
    }

    pub fn heap_bytes(&self) -> usize {
        self.by_name
            .values()
            .map(|v| v.capacity() * 40 + v.values().map(|l| l.capacity() * 4).sum::<usize>())
            .sum()
    }
}

/// The series of a tenant, by slot. A slot is never reused while the engine runs, so postings stay sorted by
/// appending; a series' ref is its slot plus one.
/// A series in the lookup table, with both hashes, so finding it doesn't read the series.
#[derive(Clone, Copy)]
pub struct Entry {
    pub hash: u64,
    pub check: u64,
    pub slot: u32,
}

#[derive(Default)]
pub struct Head {
    pub symbols: Symbols,
    pub slots: Vec<Option<Box<Series>>>,
    pub table: HashTable<Entry>,
    pub postings: Postings,
    pub live: usize,
    /// How many series are in the head, as Go counts them.
    pub in_head: usize,
}

#[inline]
pub fn ref_of(slot: u32) -> u64 {
    slot as u64 + 1
}

#[inline]
pub fn slot_of(r: u64) -> Option<u32> {
    r.checked_sub(1).and_then(|s| u32::try_from(s).ok())
}

impl Head {
    #[inline]
    pub fn get(&self, slot: u32) -> Option<&Series> {
        self.slots.get(slot as usize)?.as_deref()
    }

    #[inline]
    pub fn get_mut(&mut self, slot: u32) -> Option<&mut Series> {
        self.slots.get_mut(slot as usize)?.as_deref_mut()
    }

    pub fn set_in_head(&mut self, slot: u32, v: bool) {
        if let Some(s) = self.get_mut(slot) {
            if v {
                s.idle = false;
            }
            if s.in_head != v {
                s.in_head = v;
                if v {
                    self.in_head += 1;
                } else {
                    self.in_head -= 1;
                }
            }
        }
    }

    #[inline]
    pub fn by_ref(&self, r: u64) -> Option<&Series> {
        self.get(slot_of(r)?)
    }

    /// The slot of the series with these label pairs, sorted by name.
    #[inline]
    pub fn find(&self, pairs: &[Sym]) -> Option<u32> {
        let (hash, check) = hash_pairs(pairs);
        self.find_hashed(hash, check)
    }

    #[inline]
    pub fn find_hashed(&self, hash: u64, check: u64) -> Option<u32> {
        self.table
            .find(hash, |e| e.hash == hash && e.check == check)
            .map(|e| e.slot)
    }

    /// Interns the labels, sorted by name, and returns their pairs.
    pub fn intern(&mut self, labels: &[(&str, &str)]) -> Box<[Sym]> {
        let mut pairs = Vec::with_capacity(labels.len() * 2);
        for (n, v) in labels {
            pairs.push(self.symbols.intern(n));
            pairs.push(self.symbols.intern(v));
        }
        pairs.into_boxed_slice()
    }

    /// Creates the series of the pairs, which must not exist.
    pub fn create(&mut self, pairs: Box<[Sym]>) -> u32 {
        let (hash, check) = hash_pairs(&pairs);
        let slot = u32::try_from(self.slots.len()).expect("series slots exhausted");
        self.postings.add(&pairs, slot);
        self.slots.push(Some(Box::new(Series {
            check,
            max_t: NO_T,
            last_v: 0,
            last_kind: 0,
            head: None,
            sealed: Vec::new(),
            extra: None,
            active_ms: i64::MIN,
            labels: pairs,
            hash,
            cold_mint: NO_T,
            cold_maxt: NO_T,
            mem_mint: NO_T,
            mem_maxt: NO_T,
            trackers: 0,
            otlp: false,
            in_head: false,
            idle: false,
        })));
        self.table
            .insert_unique(hash, Entry { hash, check, slot }, |e| e.hash);
        self.live += 1;
        slot
    }

    /// Removes series, which must have no data left.
    pub fn remove(&mut self, slots: &[u32]) -> Vec<(u32, Box<[Sym]>)> {
        let mut removed = Vec::with_capacity(slots.len());
        for &slot in slots {
            let Some(series) = self.slots.get_mut(slot as usize).and_then(Option::take) else {
                continue;
            };
            let hash = series.hash;
            if let Ok(entry) = self.table.find_entry(hash, |e| e.slot == slot) {
                entry.remove();
            }
            self.live -= 1;
            if series.in_head {
                self.in_head -= 1;
            }
            removed.push((slot, series.labels));
        }
        self.postings.remove(&removed);
        removed
    }

    pub fn label_pairs<'a>(
        &'a self,
        s: &'a Series,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        s.labels
            .chunks_exact(2)
            .map(|p| (self.symbols.get(p[0]), self.symbols.get(p[1])))
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &Series)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| Some((i as u32, s.as_deref()?)))
    }
}
