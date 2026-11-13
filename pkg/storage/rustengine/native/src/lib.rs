// SPDX-License-Identifier: AGPL-3.0-only

mod disk;
mod query;
mod wire;
pub mod xor;

// Keep Rust allocation and reclamation independent of Go and other CGO libraries.
#[cfg(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos")))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use query::{QueryChunk, QueryCursor};
use wire::Wire;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Label {
    name: Arc<str>,
    value: Arc<str>,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub struct Sample {
    t: i64,
    kind: u8,
    // Float bits preserve NaNs (including Prometheus stale markers) through JSON.
    #[serde(default)]
    bits: u64,
    #[serde(default)]
    data: String,
    #[serde(default, skip_serializing_if = "is_false")]
    out_of_order: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

// Append-order bookkeeping does not change a sample's value for duplicate checks.
impl PartialEq for Sample {
    fn eq(&self, other: &Self) -> bool {
        self.t == other.t
            && self.kind == other.kind
            && self.bits == other.bits
            && self.data == other.data
    }
}
impl Eq for Sample {}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub struct Exemplar {
    t: i64,
    bits: u64,
    labels: Vec<Label>,
}

enum ChunkOwner {
    Heap(Vec<u8>),
    Mapped(memmap2::Mmap),
}
impl std::ops::Deref for ChunkOwner {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::Mapped(map) => map,
        }
    }
}
const MAX_COLD_MAPPINGS: usize = 64;

struct ColdMapping {
    owner: Arc<ChunkOwner>,
    device: u64,
    inode: u64,
}
#[derive(Clone, Default)]
struct ChunkBytes {
    owner: Option<Arc<ChunkOwner>>,
    start: usize,
    end: usize,
}
impl std::ops::Deref for ChunkBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.owner
            .as_ref()
            .map_or(&[], |bytes| &bytes[self.start..self.end])
    }
}
impl From<Vec<u8>> for ChunkBytes {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::default();
        }
        let end = bytes.len();
        Self {
            owner: Some(Arc::new(ChunkOwner::Heap(bytes))),
            start: 0,
            end,
        }
    }
}
impl From<&[u8]> for ChunkBytes {
    fn from(bytes: &[u8]) -> Self {
        bytes.to_vec().into()
    }
}
impl Serialize for ChunkBytes {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self[..].serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ChunkBytes {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Ok(Vec::<u8>::deserialize(deserializer)?.into())
    }
}
#[derive(Clone, Deserialize, Serialize)]
struct FloatChunk {
    mint: i64,
    maxt: i64,
    data: ChunkBytes,
    #[serde(default, skip_serializing_if = "is_false")]
    out_of_order: bool,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Series {
    #[serde(rename = "ref")]
    reference: u64,
    labels: Arc<Vec<Label>>,
    #[serde(default)]
    samples: BTreeMap<i64, Sample>,
    #[serde(default)]
    pending_floats: usize,
    #[serde(default)]
    uncompacted_ooo: bool,
    #[serde(default)]
    float_chunks: Vec<FloatChunk>,
    #[serde(default, with = "open_chunk")]
    open_float: Option<xor::Appender>,
    #[serde(default)]
    exemplars: Vec<Exemplar>,
    #[serde(default)]
    last: Option<Sample>,
}

mod open_chunk {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &Option<xor::Appender>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(xor::Appender::bytes)
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<xor::Appender>, D::Error> {
        let bytes = Option::<Vec<u8>>::deserialize(deserializer)?;
        Ok(bytes.map(|bytes| {
            let mut chunk = xor::Appender::default();
            for (t, v) in xor::decode(&bytes) {
                chunk.append(t, v);
            }
            chunk
        }))
    }
}

impl Series {
    fn label_block_starts(&self, range: i64) -> BTreeSet<i64> {
        let start = |t: i64| t.div_euclid(range).saturating_mul(range);
        let mut blocks: BTreeSet<_> = self.samples.keys().map(|t| start(*t)).collect();
        let mut add = |mint: i64, maxt: i64, bytes: &[u8]| {
            let first = start(mint);
            if first == start(maxt) {
                blocks.insert(first);
            } else {
                // A chunk spanning ranges may have gaps with no series in them.
                blocks.extend(xor::decode(bytes).into_iter().map(|(t, _)| start(t)));
            }
        };
        for chunk in &self.float_chunks {
            add(chunk.mint, chunk.maxt, &chunk.data);
        }
        if let Some(chunk) = &self.open_float {
            let first = chunk.first_timestamp().unwrap();
            let last = chunk.last_timestamp().unwrap();
            if start(first) == start(last) {
                add(first, last, &[]);
            } else {
                add(first, last, chunk.bytes());
            }
        }
        blocks
    }

    fn has_samples(&self) -> bool {
        !self.samples.is_empty()
            || !self.float_chunks.is_empty()
            || self.open_float.as_ref().is_some_and(|c| !c.is_empty())
    }
    fn samples_in(&self, mint: i64, maxt: i64) -> BTreeMap<i64, Sample> {
        if mint > maxt {
            return BTreeMap::new();
        }
        let mut samples = BTreeMap::new();
        for chunk in &self.float_chunks {
            if chunk.maxt < mint || chunk.mint > maxt {
                continue;
            }
            for (t, v) in xor::decode(&chunk.data) {
                if t >= mint && t <= maxt {
                    samples.insert(
                        t,
                        Sample {
                            t,
                            kind: 1,
                            bits: v.to_bits(),
                            data: String::new(),
                            out_of_order: chunk.out_of_order,
                        },
                    );
                }
            }
        }
        if let Some(chunk) = &self.open_float
            && chunk.first_timestamp().is_some_and(|t| t <= maxt)
            && chunk.last_timestamp().is_some_and(|t| t >= mint)
        {
            for (t, v) in xor::decode(chunk.bytes()) {
                if t >= mint && t <= maxt {
                    samples.insert(
                        t,
                        Sample {
                            t,
                            kind: 1,
                            bits: v.to_bits(),
                            data: String::new(),
                            out_of_order: false,
                        },
                    );
                }
            }
        }
        samples.extend(
            self.samples
                .range(mint..=maxt)
                .map(|(&t, s)| (t, s.clone())),
        );
        samples
    }
    fn lookup(&self, t: i64) -> Option<Sample> {
        if self.last.as_ref().is_some_and(|last| t > last.t) {
            return None;
        }
        if let Some(last) = self.last.as_ref().filter(|last| last.t == t) {
            return Some(last.clone());
        }
        self.samples
            .get(&t)
            .cloned()
            .or_else(|| self.samples_in(t, t).remove(&t))
    }
    fn bounds(&self) -> Option<(i64, i64)> {
        let mint = self
            .samples
            .first_key_value()
            .map(|(&t, _)| t)
            .into_iter()
            .chain(self.float_chunks.iter().map(|c| c.mint))
            .chain(self.open_float.as_ref().and_then(|c| c.first_timestamp()))
            .min()?;
        let maxt = self
            .samples
            .last_key_value()
            .map(|(&t, _)| t)
            .into_iter()
            .chain(self.float_chunks.iter().map(|c| c.maxt))
            .chain(self.open_float.as_ref().and_then(|c| c.last_timestamp()))
            .max()?;
        Some((mint, maxt))
    }
    fn ordered_bounds(&self, out_of_order: bool) -> Option<(i64, i64)> {
        if out_of_order && !self.uncompacted_ooo {
            return None;
        }
        let first = self
            .samples
            .iter()
            .find(|(_, sample)| sample.out_of_order == out_of_order)
            .map(|(&t, _)| t)
            .into_iter()
            .chain(
                self.float_chunks
                    .iter()
                    .filter(|chunk| chunk.out_of_order == out_of_order)
                    .map(|chunk| chunk.mint),
            )
            .chain(
                self.open_float
                    .as_ref()
                    .filter(|_| !out_of_order)
                    .and_then(|chunk| chunk.first_timestamp()),
            )
            .min()?;
        let last = self
            .samples
            .iter()
            .rev()
            .find(|(_, sample)| sample.out_of_order == out_of_order)
            .map(|(&t, _)| t)
            .into_iter()
            .chain(
                self.float_chunks
                    .iter()
                    .filter(|chunk| chunk.out_of_order == out_of_order)
                    .map(|chunk| chunk.maxt),
            )
            .chain(
                self.open_float
                    .as_ref()
                    .filter(|_| !out_of_order)
                    .and_then(|chunk| chunk.last_timestamp()),
            )
            .max()?;
        Some((first, last))
    }

    fn insert(&mut self, mut sample: Sample) {
        if self.last.as_ref().is_some_and(|last| sample.t < last.t) {
            sample.out_of_order = true;
        }
        if sample.out_of_order {
            self.uncompacted_ooo = true;
        }
        if sample.kind == 1
            && !sample.out_of_order
            && self.last.as_ref().is_none_or(|last| sample.t > last.t)
        {
            if self.open_float.as_ref().is_some_and(|chunk| {
                chunk.len() >= 120 || chunk.byte_len() >= xor::MAX_BYTES_BEFORE_APPEND
            }) {
                let chunk = self.open_float.take().unwrap();
                self.float_chunks.push(FloatChunk {
                    mint: chunk.first_timestamp().unwrap(),
                    maxt: chunk.last_timestamp().unwrap(),
                    data: chunk.into_bytes().into(),
                    out_of_order: false,
                });
            }
            self.open_float
                .get_or_insert_with(xor::Appender::default)
                .append(sample.t, f64::from_bits(sample.bits));
        } else {
            let previous = self.samples.insert(sample.t, sample.clone());
            if sample.kind == 1 && previous.as_ref().is_none_or(|s| s.kind != 1) {
                self.pending_floats += 1;
            } else if sample.kind != 1 && previous.as_ref().is_some_and(|s| s.kind == 1) {
                self.pending_floats -= 1;
            }
        }
        if !sample.out_of_order && self.last.as_ref().is_none_or(|last| sample.t >= last.t) {
            self.last = Some(sample);
        }
        self.compress();
    }
    fn repack(&mut self) {
        self.last = None;
        let samples = std::mem::take(&mut self.samples);
        self.pending_floats = 0;
        self.uncompacted_ooo = false;
        self.float_chunks.clear();
        self.open_float = None;
        for (_, sample) in samples {
            self.insert(sample);
        }
    }
    fn compress(&mut self) {
        // Keep a bounded open tail, so steady-state writes do not re-encode full chunks.
        // Histogram growth cannot make the float queue ready to compress.
        while self.pending_floats >= 240 {
            let floats: Vec<_> = self
                .samples
                .iter()
                .filter(|(_, s)| s.kind == 1)
                .take(120)
                .map(|(&t, s)| (t, f64::from_bits(s.bits)))
                .collect();
            if floats.len() < 120 {
                self.pending_floats = floats.len();
                break;
            }
            self.pending_floats -= floats.len();
            let data = ChunkBytes::from(xor::encode(&floats));
            self.float_chunks.push(FloatChunk {
                mint: floats[0].0,
                maxt: floats.last().unwrap().0,
                data,
                out_of_order: true,
            });
            for (t, _) in floats {
                self.samples.remove(&t);
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Cold {
    created_at: i64,
    file: String,
    mint: i64,
    maxt: i64,
    refs: Arc<BTreeSet<u64>>,
    ranges: Arc<BTreeMap<u64, (i64, i64)>>,
    #[serde(default)]
    locations: Arc<BTreeMap<u64, (u64, u64)>>,
    #[serde(default)]
    label_blocks: Arc<Vec<ColdLabelBlock>>,
    #[serde(default)]
    evicted: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct ColdLabelBlock {
    mint: i64,
    maxt: i64,
    refs: Vec<u64>,
}

#[derive(Default)]
struct LabelValues {
    present: usize,
    values: BTreeMap<Arc<str>, BTreeSet<u64>>,
}
impl std::ops::Deref for LabelValues {
    type Target = BTreeMap<Arc<str>, BTreeSet<u64>>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl std::ops::DerefMut for LabelValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}
#[derive(Default)]
struct LabelIndex {
    values: BTreeMap<Arc<str>, LabelValues>,
    all: BTreeSet<u64>,
    generation: u64,
}
impl LabelIndex {
    fn select_refs<'a>(&'a self, state: &State, filters: &[LabelFilter]) -> Cow<'a, BTreeSet<u64>> {
        if filters.len() == 1 {
            return self.select_single(&filters[0]);
        }
        let filters: Vec<_> = filters
            .iter()
            .filter(|filter| {
                if !filter.match_missing {
                    if !filter.exclude {
                        return true;
                    }
                    return self.values.get(filter.name.as_str()).is_none_or(|values| {
                        values.present != self.all.len()
                            || filter
                                .values
                                .iter()
                                .any(|value| values.contains_key(value.as_str()))
                    });
                }
                let Some(values) = self.values.get(filter.name.as_str()) else {
                    return false;
                };
                if filter.exclude {
                    filter
                        .values
                        .iter()
                        .any(|value| values.contains_key(value.as_str()))
                } else {
                    values.keys().any(|value| {
                        filter
                            .values
                            .binary_search_by(|candidate| candidate.as_str().cmp(value.as_ref()))
                            .is_err()
                    })
                }
            })
            .collect();
        if filters.is_empty() {
            return Cow::Borrowed(&self.all);
        }
        if filters.len() == 1 {
            return self.select_single(filters[0]);
        }
        let seed = filters
            .iter()
            .enumerate()
            .filter(|(_, filter)| !filter.exclude && !filter.match_missing)
            .min_by_key(|(_, filter)| {
                self.values.get(filter.name.as_str()).map_or(0, |values| {
                    filter
                        .values
                        .iter()
                        .filter_map(|value| values.get(value.as_str()))
                        .map(BTreeSet::len)
                        .sum::<usize>()
                })
            })
            .map(|(index, _)| index);
        let selected = if let Some(seed) = seed {
            let filter = filters[seed];
            if filter.values.len() == 1 {
                match self
                    .values
                    .get(filter.name.as_str())
                    .and_then(|values| values.get(filter.values[0].as_str()))
                {
                    Some(refs) => Cow::Borrowed(refs),
                    None => Cow::Owned(BTreeSet::new()),
                }
            } else {
                let mut refs = BTreeSet::new();
                if let Some(values) = self.values.get(filter.name.as_str()) {
                    for value in &filter.values {
                        if let Some(postings) = values.get(value.as_str()) {
                            refs.extend(postings);
                        }
                    }
                }
                Cow::Owned(refs)
            }
        } else {
            Cow::Borrowed(&self.all)
        };
        if filters.len() == 1 && seed.is_some() {
            return selected;
        }
        let direct: Vec<_> = filters
            .iter()
            .map(|filter| {
                (filter.values.len() == 1 && filter.exclude == filter.match_missing).then(|| {
                    self.values
                        .get(filter.name.as_str())
                        .and_then(|values| values.get(filter.values[0].as_str()))
                })
            })
            .collect();
        Cow::Owned(
            selected
                .iter()
                .filter(|reference| {
                    let mut row = None;
                    filters.iter().enumerate().all(|(index, filter)| {
                        Some(index) == seed || {
                            // Literal filters can use postings without loading the series' labels.
                            if let Some(postings) = direct[index] {
                                return postings.is_some_and(|refs| refs.contains(*reference))
                                    != filter.exclude;
                            }
                            let series = match row {
                                Some(series) => series,
                                None => {
                                    let Some(series) = state.series.get(*reference) else {
                                        return false;
                                    };
                                    row = Some(series);
                                    series
                                }
                            };
                            match series.labels.binary_search_by(|label| {
                                label.name.as_ref().cmp(filter.name.as_str())
                            }) {
                                Ok(index) => {
                                    filter
                                        .values
                                        .binary_search_by(|candidate| {
                                            candidate
                                                .as_str()
                                                .cmp(series.labels[index].value.as_ref())
                                        })
                                        .is_ok()
                                        != filter.exclude
                                }
                                Err(_) => filter.match_missing,
                            }
                        }
                    })
                })
                .copied()
                .collect(),
        )
    }
    fn select_single<'a>(&'a self, filter: &LabelFilter) -> Cow<'a, BTreeSet<u64>> {
        let column = self.values.get(filter.name.as_str());
        if filter.exclude
            && !filter.match_missing
            && column.is_some_and(|values| {
                values.present == self.all.len()
                    && filter
                        .values
                        .iter()
                        .all(|value| !values.contains_key(value.as_str()))
            })
        {
            return Cow::Borrowed(&self.all);
        }

        if filter.values.len() == 1 && filter.exclude == filter.match_missing {
            return match column.and_then(|values| values.get(filter.values[0].as_str())) {
                Some(refs) if !filter.match_missing => Cow::Borrowed(refs),
                Some(refs) => Cow::Owned(self.all.difference(refs).copied().collect()),
                None if filter.match_missing => Cow::Borrowed(&self.all),
                None => Cow::Owned(BTreeSet::new()),
            };
        }
        let postings: Vec<_> = if filter.exclude == filter.match_missing {
            filter
                .values
                .iter()
                .filter_map(|value| column?.get(value.as_str()))
                .collect()
        } else {
            self.values
                .get(filter.name.as_str())
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(value, _)| {
                    filter
                        .values
                        .binary_search_by(|candidate| candidate.as_str().cmp(value.as_ref()))
                        .is_err()
                })
                .map(|(_, refs)| refs)
                .collect()
        };
        // Values of one label have disjoint postings, so their lengths give the union size.
        let count = postings.iter().map(|refs| refs.len()).sum::<usize>();
        if count == 0 {
            return if filter.match_missing {
                Cow::Borrowed(&self.all)
            } else {
                Cow::Owned(BTreeSet::new())
            };
        }
        if count == self.all.len() {
            return if filter.match_missing {
                Cow::Owned(BTreeSet::new())
            } else {
                Cow::Borrowed(&self.all)
            };
        }
        let union = if postings.len() == 1 {
            Cow::Borrowed(postings[0])
        } else {
            let mut refs = Vec::with_capacity(count);
            for postings in postings {
                refs.extend(postings.iter().copied());
            }
            refs.sort_unstable();
            Cow::Owned(refs.into_iter().collect())
        };
        if filter.match_missing {
            Cow::Owned(self.all.difference(&union).copied().collect())
        } else {
            union
        }
    }
    fn equal_refs(&self, labels: &[Label]) -> Result<Cow<'_, BTreeSet<u64>>> {
        if labels.is_empty() || labels.iter().any(|label| label.value.is_empty()) {
            return Err("nonempty equality selectors required".into());
        }
        let Some(postings): Option<Vec<_>> = labels
            .iter()
            .map(|label| self.values.get(&label.name)?.get(&label.value))
            .collect()
        else {
            return Ok(Cow::Owned(BTreeSet::new()));
        };
        let smallest = *postings.iter().min_by_key(|refs| refs.len()).unwrap();
        if postings.iter().all(|refs| ptr::eq(*refs, smallest)) {
            return Ok(Cow::Borrowed(smallest));
        }
        Ok(Cow::Owned(
            smallest
                .iter()
                .filter(|reference| {
                    postings
                        .iter()
                        .all(|refs| ptr::eq(*refs, smallest) || refs.contains(*reference))
                })
                .copied()
                .collect(),
        ))
    }
    fn intern_labels(&self, labels: &mut Arc<Vec<Label>>) {
        for label in Arc::make_mut(labels) {
            if let Some((name, values)) = self.values.get_key_value(label.name.as_ref()) {
                label.name = name.clone();
                if let Some((value, _)) = values.get_key_value(label.value.as_ref()) {
                    label.value = value.clone();
                }
            }
        }
    }
    fn insert(&mut self, series: &Series) {
        if !self.all.insert(series.reference) {
            return;
        }
        self.generation = self.generation.wrapping_add(1);
        for l in series.labels.iter() {
            let values = self.values.entry(l.name.clone()).or_default();
            values.present += 1;
            values
                .entry(l.value.clone())
                .or_default()
                .insert(series.reference);
        }
    }
    fn remove(&mut self, series: &Series) {
        if !self.all.remove(&series.reference) {
            return;
        }
        self.generation = self.generation.wrapping_add(1);
        for l in series.labels.iter() {
            if let Some(values) = self.values.get_mut(&l.name) {
                values.present -= 1;
                if let Some(refs) = values.get_mut(&l.value) {
                    refs.remove(&series.reference);
                    if refs.is_empty() {
                        values.remove(&l.value);
                    }
                }
                if values.is_empty() {
                    self.values.remove(&l.name);
                }
            }
        }
    }
}
fn labels_key(labels: &[Label]) -> Vec<u8> {
    // Identity keys live with retained series, so geometric spare capacity wastes resident memory.
    let mut key = Vec::with_capacity(4 + labels.iter().map(Wire::size).sum::<usize>());
    (labels.len() as u32).put(&mut key);
    for label in labels {
        label.name.put(&mut key);
        label.value.put(&mut key);
    }
    key
}
fn lowest() -> i64 {
    i64::MIN
}
#[derive(Clone, Deserialize, Serialize)]
struct State {
    seq: u64,
    next_ref: u64,
    series: BTreeMap<u64, Series>,
    cold: Vec<Cold>,
    #[serde(default = "lowest")]
    min_valid: i64,
    exemplar_count: usize,
    // One entry per nonempty series keeps capped commits independent of retained series count.
    #[serde(skip)]
    exemplar_oldest: Option<BTreeSet<(i64, u64)>>,
    #[serde(default)]
    label_head_mint: Option<i64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            seq: 0,
            next_ref: 0,
            series: BTreeMap::new(),
            cold: Vec::new(),
            min_valid: i64::MIN,
            exemplar_count: 0,
            exemplar_oldest: None,
            label_head_mint: None,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Commit {
    max_exemplars: usize,
    seq: u64,
    next_ref: u64,
    series: BTreeMap<u64, Series>,
}

impl State {
    fn head_bounds(&self, out_of_order: bool) -> (i64, i64) {
        self.series
            .values()
            .filter_map(|series| series.ordered_bounds(out_of_order))
            .fold((i64::MAX, i64::MIN), |(min, max), (first, last)| {
                (min.min(first), max.max(last))
            })
    }
    fn apply(&mut self, commit: Commit) {
        let oldest = self.exemplar_oldest.get_or_insert_with(|| {
            self.series
                .values()
                .filter_map(|s| s.exemplars.first().map(|e| (e.t, s.reference)))
                .collect()
        });
        self.seq = commit.seq;
        self.next_ref = commit.next_ref;
        for (r, delta) in commit.series {
            let series = self.series.entry(r).or_insert_with(|| Series {
                reference: r,
                labels: delta.labels.clone(),
                ..Default::default()
            });
            series.uncompacted_ooo |= delta.uncompacted_ooo;
            for (_, sample) in delta.samples {
                series.insert(sample);
            }
            self.exemplar_count += delta.exemplars.len();
            if series.exemplars.is_empty()
                && let Some(first) = delta.exemplars.first()
            {
                oldest.insert((first.t, r));
            }
            series.exemplars.extend(delta.exemplars);
        }
        let mut excess = self.exemplar_count.saturating_sub(commit.max_exemplars);
        while excess > 0 {
            let (_, reference) = oldest
                .pop_first()
                .expect("exemplar count matches retained series");
            let series = self.series.get_mut(&reference).unwrap();
            // Drain consecutive victims together so shrinking the cap does not repeatedly shift a long vector.
            let count = oldest
                .first()
                .map_or(series.exemplars.len(), |next| {
                    series
                        .exemplars
                        .partition_point(|e| (e.t, reference) <= *next)
                })
                .min(excess);
            series.exemplars.drain(..count);
            if let Some(first) = series.exemplars.first() {
                oldest.insert((first.t, reference));
            }
            self.exemplar_count -= count;
            excess -= count;
        }
    }
}

#[derive(Default, Deserialize)]
struct FloatSeries<'a> {
    index: usize,
    // Keys are consumed before the synchronous call returns; retained identities stay owned.
    #[serde(borrow)]
    key: Cow<'a, [u8]>,
    samples: Vec<Sample>,
}
#[derive(Default, Serialize)]
struct FloatResult {
    index: usize,
    #[serde(rename = "ref")]
    reference: u64,
    accepted: usize,
    errors: Vec<SampleError>,
}
#[derive(Default, Serialize)]
struct SampleError {
    timestamp: i64,
    error: String,
}

#[derive(Default, Deserialize)]
struct LabelFilter {
    name: String,
    values: Vec<String>,
    exclude: bool,
    match_missing: bool,
}

#[derive(Default, Deserialize)]
struct Request<'a> {
    #[serde(default)]
    unordered: bool,
    #[serde(default)]
    cursor: u64,
    #[serde(default)]
    sorted: bool,
    op: String,
    #[serde(default, rename = "ref")]
    reference: u64,
    #[serde(default)]
    labels: Vec<Label>,
    sample: Option<Sample>,
    exemplar: Option<Exemplar>,
    #[serde(default)]
    reject_ooo: bool,
    #[serde(default)]
    window: i64,
    #[serde(default)]
    max_exemplars: usize,
    #[serde(default)]
    refs: Vec<u64>,
    #[serde(default)]
    up_to: i64,
    #[serde(default)]
    mint: i64,
    #[serde(default)]
    maxt: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    include_cold: bool,
    #[serde(default)]
    filter: bool,
    #[serde(default)]
    #[serde(borrow)]
    batch: Vec<FloatSeries<'a>>,
    #[serde(default)]
    limit: usize,
    #[serde(default)]
    filters: Vec<LabelFilter>,
}

#[derive(Default, Serialize)]
struct Response {
    cursor: u64,
    done: bool,
    chunks: Vec<QueryChunk>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(rename = "ref")]
    reference: u64,
    series: Vec<Series>,
    active: bool,
    refs: Vec<u64>,
    values: Vec<String>,
    results: Vec<FloatResult>,
    leftovers: Vec<usize>,
    sample: Option<Sample>,
    count: usize,
    mint: i64,
    maxt: i64,
    min_valid: i64,
    bytes: BTreeMap<String, usize>,
    label_counts: BTreeMap<String, BTreeMap<String, u64>>,
}

struct Engine {
    kafka_replay: bool,
    restored_clean: bool,
    sealed: bool,
    cursors: HashMap<u64, QueryCursor>,
    cold_mappings: RefCell<VecDeque<(String, Arc<ColdMapping>)>>,
    next_cursor: u64,
    dir: PathBuf,
    state: State,
    keys: HashMap<Vec<u8>, u64>,
    head_index: LabelIndex,
    index: LabelIndex,
    pending: Option<BTreeMap<u64, Series>>,
    pending_keys: HashMap<Vec<u8>, u64>,
    wal: Option<BufWriter<File>>,
    buffered_wal: bool,
    wal_dirty: bool,
    _lock: File,
    window: i64,
    max_exemplars: usize,
    block_range: i64,
    poisoned: bool,
    head_mint: i64,
    head_maxt: i64,
    head_ooo_mint: i64,
    head_ooo_maxt: i64,
}

type Result<T> = std::result::Result<T, String>;
fn io(err: impl std::fmt::Display) -> String {
    err.to_string()
}

impl Engine {
    fn open(dir: &Path) -> Result<Self> {
        Self::open_mode(dir, false)
    }
    fn open_mode(dir: &Path, kafka_replay: bool) -> Result<Self> {
        fs::create_dir_all(dir).map_err(io)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))
            .map_err(io)?;
        lock.try_lock().map_err(io)?;
        let stored_kafka_mode = dir.join("kafka.mode").try_exists().map_err(io)?;
        if stored_kafka_mode && !kafka_replay {
            return Err(
                "Kafka replay storage requires Kafka-only ingestion with file-stored offsets"
                    .into(),
            );
        }
        let mut state: State = match fs::read(dir.join("head.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(io(e)),
        };
        if dir.join("wal.jsonl").try_exists().map_err(io)? {
            return Err("legacy JSON WAL requires a fresh Rust engine directory".into());
        }
        for series in state.series.values_mut() {
            if let Some(labels) = Arc::get_mut(&mut series.labels) {
                labels.shrink_to_fit();
            }
            series.pending_floats = series.samples.values().filter(|s| s.kind == 1).count();
            series.compress();
        }
        let path = dir.join("wal.bin");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(io(e)),
        };
        let valid = disk::replay(&bytes, &mut state)?;
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(io)?;
        if valid != bytes.len() {
            wal.set_len(valid as u64).map_err(io)?;
        }
        wal.sync_all().map_err(io)?;
        File::open(dir).and_then(|d| d.sync_all()).map_err(io)?;
        for series in state.series.values_mut() {
            if !series.has_samples() {
                series.last = None;
            }
        }
        let keys = state
            .series
            .values()
            .map(|s| (labels_key(&s.labels), s.reference))
            .collect();
        let mut head_index = LabelIndex::default();
        let mut index = LabelIndex::default();
        for series in state.series.values_mut() {
            index.intern_labels(&mut series.labels);
            index.insert(series);
            if series.has_samples() {
                head_index.insert(series);
            }
        }
        let (head_mint, head_maxt) = state.head_bounds(false);
        let (head_ooo_mint, head_ooo_maxt) = state.head_bounds(true);
        let clean = match fs::read(dir.join("kafka.clean")) {
            Ok(bytes) => bytes == state.seq.to_le_bytes(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(io(error)),
        };
        let mut engine = Self {
            kafka_replay: false,
            // A WAL-backed directory is already durable through its committed Kafka offset.
            restored_clean: !stored_kafka_mode || clean,
            sealed: false,
            cursors: HashMap::new(),
            cold_mappings: Default::default(),
            next_cursor: 0,
            dir: dir.into(),
            state,
            keys,
            head_index,
            index,
            pending: None,
            pending_keys: HashMap::new(),
            wal: Some(BufWriter::with_capacity(64 * 1024, wal)),
            buffered_wal: false,
            wal_dirty: false,
            _lock: lock,
            window: 0,
            max_exemplars: 0,
            block_range: 2 * 60 * 60 * 1000,
            poisoned: false,
            head_mint,
            head_maxt,
            head_ooo_mint,
            head_ooo_maxt,
        };
        if kafka_replay {
            // Preserve a recovered WAL before switching its durability source to Kafka.
            if !stored_kafka_mode {
                engine
                    .atomic_write("head.json", &serde_json::to_vec(&engine.state).map_err(io)?)?;
            }
            engine.atomic_write("kafka.mode", b"1")?;
            match fs::remove_file(dir.join("kafka.clean")) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io(error)),
            }
            File::open(dir).and_then(|d| d.sync_all()).map_err(io)?;
            engine.reset_wal()?;
            engine.wal = None;
            engine.kafka_replay = true;
        }
        Ok(engine)
    }

    fn call(&mut self, mut req: Request) -> Result<Response> {
        if self.sealed {
            return Err("engine is closed".into());
        }
        if self.poisoned {
            return Err("engine requires reopen after WAL failure".into());
        }
        let mut response = Response::default();
        for filter in &mut req.filters {
            filter.values.sort_unstable();
            filter.values.dedup();
        }
        match req.op.as_str() {
            "recovery_state" => {
                response.active = self.kafka_replay;
                response.done = self.restored_clean;
            }
            "close_checkpoint" => {
                if !self.kafka_replay || self.pending.is_some() {
                    return Err("clean checkpoint requires an idle Kafka replay engine".into());
                }
                self.atomic_write("head.json", &serde_json::to_vec(&self.state).map_err(io)?)?;
                // Only a durable complete head can justify keeping the committed Kafka offset.
                self.atomic_write("kafka.clean", &self.state.seq.to_le_bytes())?;
                self.sealed = true;
            }
            "ingest_floats" | "begin_floats" | "finish_floats" => {
                let finish = req.op != "begin_floats";
                if req.op != "finish_floats" {
                    self.call(Request {
                        op: "begin".into(),
                        ..Default::default()
                    })?;
                }
                req.op = "append_floats".into();
                let mut result = self.call(req)?;
                // A Go callback can reject soft-error classification or series creation.
                // Only a fully accepted batch may publish before returning to Go.
                if finish
                    && result.leftovers.is_empty()
                    && result.results.iter().all(|r| r.errors.is_empty())
                {
                    self.call(Request {
                        op: "commit".into(),
                        ..Default::default()
                    })?;
                    result.done = true;
                }
                return Ok(result);
            }

            "cursor_open"
            | "cursor_open_labels"
            | "cursor_open_all"
            | "cursor_open_labels_all"
            | "cursor_open_equal"
            | "cursor_open_labels_equal"
            | "cursor_open_select"
            | "cursor_open_labels_select" => {
                let selected = if req.op.ends_with("_select") {
                    self.index.select_refs(&self.state, &req.filters)
                } else if req.op.ends_with("_equal") {
                    self.index.equal_refs(&req.labels)?
                } else if req.op.ends_with("_all") {
                    Cow::Borrowed(&self.index.all)
                } else {
                    Cow::Owned(req.refs.iter().copied().collect())
                };
                let mut cursor = if req.op.starts_with("cursor_open_labels") {
                    QueryCursor::labels(self, &req, &selected)?
                } else {
                    QueryCursor::open(self, &req, &selected)?
                };
                self.next_cursor = self
                    .next_cursor
                    .checked_add(1)
                    .ok_or("query cursor ID overflow")?;
                if req.limit > 0 {
                    response = cursor.call(&Request {
                        op: "cursor_series_page".into(),
                        limit: req.limit,
                        ..Default::default()
                    })?;
                }
                response.cursor = self.next_cursor;
                response.count = cursor.len();
                self.cursors.insert(response.cursor, cursor);
            }
            "cursor_series" | "cursor_chunks" | "cursor_series_page" | "cursor_chunks_batch" => {
                return self
                    .cursors
                    .get_mut(&req.cursor)
                    .ok_or("query cursor is closed")?
                    .call(&req);
            }
            "cursor_close" => {
                self.cursors.remove(&req.cursor);
            }

            "stats" => {
                response.count = self.head_index.all.len();
                response.mint = self.head_mint;
                response.maxt = self.head_maxt;
                response.min_valid = self.state.min_valid;
            }
            "ooo_stats" => {
                response.mint = self.head_ooo_mint;
                response.maxt = self.head_ooo_maxt;
            }
            "bytes" => {
                for (name, values) in &self.head_index.values {
                    response.bytes.insert(
                        name.to_string(),
                        values
                            .keys()
                            .filter(|v| v.len() > 128)
                            .map(|v| v.len())
                            .sum(),
                    );
                }
            }
            "begin" => {
                if self.pending.is_some() {
                    return Err("transaction already active".into());
                }
                self.pending = Some(BTreeMap::new());
                self.pending_keys.clear();
            }
            "append_floats" => {
                if self.pending.is_none() {
                    return Err("no transaction".into());
                }
                for row in req.batch {
                    let reference = self.keys.get(row.key.as_ref()).copied().unwrap_or(0);
                    if reference == 0
                        || !self.head_index.all.contains(&reference)
                        || row.samples.iter().any(|s| s.bits == 0x7ff0000000000002)
                    {
                        response.leftovers.push(row.index);
                        continue;
                    }
                    let mut result = FloatResult {
                        index: row.index,
                        reference,
                        ..Default::default()
                    };
                    for sample in row.samples {
                        let timestamp = sample.t;
                        let error = if timestamp < req.mint {
                            Some("too_far_past".into())
                        } else if timestamp > req.maxt {
                            Some("too_far_future".into())
                        } else {
                            self.call(Request {
                                op: "append".into(),
                                reference,
                                sample: Some(sample),
                                ..Default::default()
                            })
                            .err()
                        };
                        if let Some(error) = error {
                            result.errors.push(SampleError { timestamp, error });
                        } else {
                            result.accepted += 1;
                        }
                    }
                    response.results.push(result);
                }
            }
            "lookup" => {
                let key = labels_key(&req.labels);
                response.reference = self
                    .pending_keys
                    .get(&key)
                    .or_else(|| self.keys.get(&key))
                    .copied()
                    .unwrap_or(0);
                response.active = self.head_index.all.contains(&response.reference)
                    || self.pending.as_ref().is_some_and(|p| {
                        p.get(&response.reference).is_some_and(|s| s.has_samples())
                    });
            }
            "postings" | "values" | "names" => {
                let index = if req.include_cold {
                    &self.index
                } else {
                    &self.head_index
                };
                if req.op == "names" {
                    response.values = index.values.keys().map(ToString::to_string).collect();
                } else if req.op == "values" {
                    response.values = index
                        .values
                        .get(req.name.as_str())
                        .map(|v| v.keys().map(ToString::to_string).collect())
                        .unwrap_or_default();
                } else if req.name.is_empty() {
                    response.refs = index.all.iter().copied().collect();
                } else if let Some(values) = index.values.get(req.name.as_str()) {
                    if req.values.len() == 1 {
                        response.refs = values
                            .get(req.values[0].as_str())
                            .map(|refs| refs.iter().copied().collect())
                            .unwrap_or_default();
                    } else {
                        let mut refs = Vec::new();
                        for value in req.values {
                            if let Some(postings) = values.get(value.as_str()) {
                                refs.extend(postings.iter().copied());
                            }
                        }
                        refs.sort_unstable();
                        refs.dedup();
                        response.refs = refs;
                    }
                }
            }
            "postings_select" => {
                let index = if req.include_cold {
                    &self.index
                } else {
                    &self.head_index
                };
                response.refs = index
                    .select_refs(&self.state, &req.filters)
                    .iter()
                    .copied()
                    .collect();
            }
            "metadata_names" | "metadata_values" | "metadata_counts" | "metadata_all"
            | "metadata_sets" => {
                let selected = if req.filter {
                    Cow::Owned(req.refs.into_iter().collect::<BTreeSet<_>>())
                } else if !req.filters.is_empty() {
                    self.index.select_refs(&self.state, &req.filters)
                } else {
                    Cow::Borrowed(&self.index.all)
                };
                let visible = if req.include_cold {
                    selected
                } else {
                    let mut visible = BTreeSet::new();
                    visible.extend(selected.intersection(&self.head_index.all).copied());
                    let floor = self.state.label_head_mint.unwrap_or(self.head_mint);
                    for block in &self.state.cold {
                        if block.evicted && block.maxt >= floor {
                            visible.extend(block.ranges.iter().filter_map(|(&r, &(_, maxt))| {
                                (maxt >= floor && selected.contains(&r)).then_some(r)
                            }));
                        }
                    }
                    Cow::Owned(visible)
                };
                let all_visible = ptr::eq(visible.as_ref(), &self.index.all)
                    || visible.as_ref() == &self.index.all;
                // Shared labels make posting scans cheaper once the selected series outnumber values.
                let dictionary_scan = all_visible
                    || match req.op.as_str() {
                        "metadata_names" => visible.len() >= self.index.values.len(),
                        "metadata_values" => self
                            .index
                            .values
                            .get(req.name.as_str())
                            .is_some_and(|values| visible.len() >= values.len()),
                        _ => {
                            visible.len().saturating_mul(self.index.values.len())
                                >= self.index.values.values().map(|values| values.len()).sum()
                        }
                    };
                if dictionary_scan {
                    let limit = if req.limit == 0 {
                        usize::MAX
                    } else {
                        req.limit
                    };
                    if req.op == "metadata_names" {
                        response.values = self
                            .index
                            .values
                            .iter()
                            .filter(|(_, values)| {
                                all_visible
                                    || values.values().any(|refs| !refs.is_disjoint(&visible))
                            })
                            .map(|(name, _)| name.to_string())
                            .take(limit)
                            .collect();
                    } else if req.op == "metadata_values" {
                        if let Some(values) = self.index.values.get(req.name.as_str()) {
                            response.values = values
                                .iter()
                                .filter(|(_, refs)| all_visible || !refs.is_disjoint(&visible))
                                .map(|(value, _)| value.to_string())
                                .take(limit)
                                .collect();
                        }
                    } else {
                        for (name, values) in &self.index.values {
                            if req.op == "metadata_counts"
                                && !req
                                    .values
                                    .iter()
                                    .any(|value| value.as_str() == name.as_ref())
                            {
                                continue;
                            }
                            let counts: BTreeMap<_, _> = values
                                .iter()
                                .filter_map(|(value, refs)| {
                                    let count = if req.op == "metadata_sets" {
                                        usize::from(all_visible || !refs.is_disjoint(&visible))
                                    } else if all_visible {
                                        refs.len()
                                    } else {
                                        refs.intersection(&visible).count()
                                    };
                                    (count > 0).then(|| (value.to_string(), count as u64))
                                })
                                .collect();
                            if !counts.is_empty() {
                                response.label_counts.insert(name.to_string(), counts);
                            }
                        }
                    }
                    return Ok(response);
                }
                let mut values = BTreeSet::new();
                let mut counts: BTreeMap<&str, BTreeMap<&str, u64>> = BTreeMap::new();
                let wanted: BTreeSet<_> = req.values.iter().map(String::as_str).collect();
                for reference in visible.iter() {
                    let Some(series) = self.state.series.get(reference) else {
                        continue;
                    };
                    for label in series.labels.iter() {
                        match req.op.as_str() {
                            "metadata_names" => {
                                values.insert(label.name.as_ref());
                            }
                            "metadata_values" if label.name.as_ref() == req.name.as_str() => {
                                values.insert(label.value.as_ref());
                                break;
                            }
                            "metadata_all" | "metadata_counts" | "metadata_sets"
                                if req.op == "metadata_all"
                                    || req.op == "metadata_sets"
                                    || wanted.contains(label.name.as_ref()) =>
                            {
                                let count = counts
                                    .entry(label.name.as_ref())
                                    .or_default()
                                    .entry(label.value.as_ref())
                                    .or_default();
                                if req.op == "metadata_sets" {
                                    *count = 1;
                                } else {
                                    *count += 1;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                response.values = values
                    .into_iter()
                    .take(if req.limit == 0 {
                        usize::MAX
                    } else {
                        req.limit
                    })
                    .map(str::to_owned)
                    .collect();
                response.label_counts = counts
                    .into_iter()
                    .map(|(name, values)| {
                        (
                            name.to_owned(),
                            values
                                .into_iter()
                                .map(|(value, count)| (value.to_owned(), count))
                                .collect(),
                        )
                    })
                    .collect();
            }
            "label_names_for" | "index_label_values" => {
                let mut values = BTreeSet::new();
                for reference in req.refs {
                    let series = self.state.series.get(&reference).ok_or("not_found")?;
                    for l in series.labels.iter() {
                        if req.op == "label_names_for" {
                            values.insert(l.name.as_ref());
                        } else if l.name.as_ref() == req.name.as_str() {
                            values.insert(l.value.as_ref());
                            break;
                        }
                    }
                }
                let limit = if req.limit == 0 {
                    usize::MAX
                } else {
                    req.limit
                };
                response.values = values.into_iter().take(limit).map(str::to_owned).collect();
            }
            "label_values" | "label_names" => {
                let is_filtered = req.filter || !req.filters.is_empty();
                let selection = if req.filter {
                    Cow::Owned(req.refs.iter().copied().collect::<BTreeSet<_>>())
                } else if !req.filters.is_empty() {
                    self.index.select_refs(&self.state, &req.filters)
                } else {
                    Cow::Borrowed(&self.index.all)
                };
                let selected = selection.as_ref();
                let mut active = BTreeSet::new();
                if req.mint <= req.maxt {
                    // Prometheus exposes the entire head index when its time
                    // range overlaps, including series with no sample in the query.
                    let label_head_mint = self.state.label_head_mint.unwrap_or(self.head_mint);
                    let head_overlaps = label_head_mint <= req.maxt && self.head_maxt >= req.mint;
                    if head_overlaps {
                        active.extend(selected.intersection(&self.head_index.all).copied());
                    }
                    for block in &self.state.cold {
                        if is_filtered && block.refs.is_disjoint(selected) {
                            continue;
                        }
                        if head_overlaps && block.evicted && block.maxt >= label_head_mint {
                            active.extend(block.ranges.iter().filter_map(|(&r, &(_, maxt))| {
                                (maxt >= label_head_mint && selected.contains(&r)).then_some(r)
                            }));
                        }
                        if block.label_blocks.is_empty() {
                            for (&r, &(mint, maxt)) in block.ranges.iter() {
                                let start = mint
                                    .div_euclid(self.block_range)
                                    .saturating_mul(self.block_range);
                                let end = maxt
                                    .div_euclid(self.block_range)
                                    .saturating_mul(self.block_range)
                                    .saturating_add(self.block_range - 1);
                                if start <= req.maxt && end >= req.mint && selected.contains(&r) {
                                    active.insert(r);
                                }
                            }
                        } else {
                            for labels in block.label_blocks.iter() {
                                if labels.mint <= req.maxt && labels.maxt >= req.mint {
                                    if !is_filtered {
                                        active.extend(labels.refs.iter().copied());
                                    } else if selected.len() <= labels.refs.len() {
                                        active.extend(
                                            selected
                                                .iter()
                                                .filter(|r| labels.refs.binary_search(r).is_ok())
                                                .copied(),
                                        );
                                    } else {
                                        active.extend(
                                            labels
                                                .refs
                                                .iter()
                                                .filter(|r| selected.contains(*r))
                                                .copied(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                if active.is_empty() {
                    return Ok(response);
                }
                // Posting intersections avoid revisiting shared labels for broad selections.
                let limit = if req.limit == 0 {
                    usize::MAX
                } else {
                    req.limit
                };
                if req.op == "label_names" && active.len() >= self.index.values.len() {
                    response.values = self
                        .index
                        .values
                        .iter()
                        .filter(|(_, values)| {
                            values.values().any(|refs| !refs.is_disjoint(&active))
                        })
                        .map(|(name, _)| name.to_string())
                        .take(limit)
                        .collect();
                    return Ok(response);
                }
                if req.op == "label_values" {
                    let Some(values) = self.index.values.get(req.name.as_str()) else {
                        return Ok(response);
                    };
                    if active.len() >= values.len() {
                        response.values = values
                            .iter()
                            .filter(|(_, refs)| !refs.is_disjoint(&active))
                            .map(|(value, _)| value.to_string())
                            .take(limit)
                            .collect();
                        return Ok(response);
                    }
                }
                // Selective reads should only visit their series' labels, rather
                // than every value in the tenant's retained dictionary.
                let mut values = BTreeSet::new();
                for reference in active {
                    if let Some(series) = self.state.series.get(&reference) {
                        for label in series.labels.iter() {
                            if req.op == "label_names" {
                                values.insert(label.name.as_ref());
                            } else if label.name.as_ref() == req.name.as_str() {
                                values.insert(label.value.as_ref());
                                break;
                            }
                        }
                    }
                }
                response.values = values.into_iter().take(limit).map(str::to_owned).collect();
            }
            "last" => {
                let staged = self
                    .pending
                    .as_ref()
                    .and_then(|p| p.get(&req.reference))
                    .and_then(|s| s.samples.last_key_value())
                    .map(|(_, s)| s);
                let committed = self
                    .state
                    .series
                    .get(&req.reference)
                    .and_then(|s| s.last.as_ref());
                response.sample = staged
                    .into_iter()
                    .chain(committed)
                    .max_by_key(|s| s.t)
                    .cloned();
            }
            "series" | "series_refs" => {
                let refs = if req.op == "series" {
                    std::slice::from_ref(&req.reference)
                } else {
                    &req.refs
                };
                if refs.len() > 64 {
                    return Err("series label page too large".into());
                }
                response.series = Vec::with_capacity(refs.len());
                for reference in refs {
                    if let Some(series) = self
                        .pending
                        .as_ref()
                        .and_then(|p| p.get(reference))
                        .or_else(|| self.state.series.get(reference))
                    {
                        response.series.push(Series {
                            reference: series.reference,
                            labels: series.labels.clone(),
                            ..Default::default()
                        });
                    }
                }
            }
            "append" => {
                let mut sample = req.sample.ok_or("missing sample")?;
                if self.pending.is_none() {
                    return Err("no transaction".into());
                }
                let previous_time = self
                    .state
                    .series
                    .get(&req.reference)
                    .and_then(|s| s.last.as_ref())
                    .map(|s| s.t);
                let global_ooo = sample.t
                    < self
                        .state
                        .min_valid
                        .max(self.head_maxt.saturating_sub(self.block_range / 2));
                if global_ooo {
                    let latest = self.head_maxt.max(
                        self.state
                            .series
                            .get(&req.reference)
                            .and_then(|s| s.last.as_ref())
                            .map_or(i64::MIN, |s| s.t),
                    );
                    if req.reject_ooo
                        || self.window == 0
                        || latest.saturating_sub(sample.t) > self.window
                    {
                        return Err("out_of_bounds".into());
                    }
                }
                if global_ooo || previous_time.is_some_and(|t| sample.t < t) {
                    // Idle-evicted data can be newer than the compaction cutoff.
                    let selected = BTreeSet::from([req.reference]);
                    for block in &self.state.cold {
                        if block.mint > sample.t
                            || block.maxt < sample.t
                            || !block.refs.contains(&req.reference)
                        {
                            continue;
                        }
                        for s in disk::read_cold(self, block, &selected, sample.t, sample.t)? {
                            if let Some(old) = s.samples_in(sample.t, sample.t).get(&sample.t)
                                && *old != sample
                            {
                                return Err("duplicate".into());
                            }
                        }
                    }
                }
                let pending = self.pending.as_mut().ok_or("no transaction")?;
                let reference = if req.reference == 0 {
                    if req.labels.is_empty()
                        || req.labels.windows(2).any(|w| w[0].name >= w[1].name)
                    {
                        return Err("invalid_labels".into());
                    }
                    self.state.next_ref += 1;
                    self.state.next_ref
                } else {
                    req.reference
                };
                let existing = self.state.series.get(&reference);
                if req.reference != 0 && existing.is_none() && !pending.contains_key(&reference) {
                    return Err("not_found".into());
                }
                let new_pending = existing.is_none() && !pending.contains_key(&reference);
                let series = pending.entry(reference).or_insert_with(|| Series {
                    reference,
                    labels: existing
                        .map(|s| s.labels.clone())
                        .unwrap_or_else(|| Arc::new(req.labels)),
                    ..Default::default()
                });
                let old = series
                    .samples
                    .get(&sample.t)
                    .cloned()
                    .or_else(|| existing.and_then(|s| s.lookup(sample.t)));
                if let Some(old) = old {
                    if old != sample {
                        return Err("duplicate".into());
                    }
                } else {
                    let latest = series
                        .samples
                        .last_key_value()
                        .map(|(&t, _)| t)
                        .into_iter()
                        .chain(existing.and_then(|s| s.last.as_ref()).map(|s| s.t))
                        .max();
                    if let Some(latest) = latest
                        && sample.t < latest
                        && (req.reject_ooo || latest.saturating_sub(sample.t) > self.window)
                    {
                        return Err(if self.window == 0 || req.reject_ooo {
                            "out_of_order"
                        } else {
                            "too_old"
                        }
                        .into());
                    }
                    sample.out_of_order =
                        global_ooo || latest.is_some_and(|latest| sample.t < latest);
                    series.uncompacted_ooo |= sample.out_of_order;
                    series.samples.insert(sample.t, sample);
                }
                if new_pending {
                    self.pending_keys
                        .insert(labels_key(&series.labels), reference);
                }
                response.reference = reference;
            }
            "exemplar" => {
                if self.max_exemplars == 0 {
                    return Err("exemplars_disabled".into());
                }
                let pending = self.pending.as_mut().ok_or("no transaction")?;
                let existing = self.state.series.get(&req.reference);
                if existing.is_none() && !pending.contains_key(&req.reference) {
                    return Err("not_found".into());
                }
                let series = pending.entry(req.reference).or_insert_with(|| Series {
                    reference: req.reference,
                    labels: existing.unwrap().labels.clone(),
                    ..Default::default()
                });
                let exemplar = req.exemplar.ok_or("missing exemplar")?;
                let previous = series
                    .exemplars
                    .last()
                    .or_else(|| existing.and_then(|s| s.exemplars.last()));
                if let Some(previous) = previous {
                    if previous.t > exemplar.t {
                        return Err("exemplar_out_of_order".into());
                    }
                    if previous.t == exemplar.t
                        && previous.bits == exemplar.bits
                        && previous.labels == exemplar.labels
                    {
                        return Ok(response);
                    }
                }
                series.exemplars.push(exemplar);
                // A tenant-wide cap is applied at commit, once all series are visible together.
            }
            "commit" => {
                let pending = self.pending.take().ok_or("no transaction")?;
                let mut commit = Commit {
                    max_exemplars: self.max_exemplars,
                    seq: self.state.seq + 1,
                    next_ref: self.state.next_ref,
                    series: pending,
                };
                if !self.kafka_replay {
                    let bytes = disk::wal_record(&commit, &self.state);
                    let result = self
                        .wal
                        .as_mut()
                        .ok_or("WAL closed")?
                        .write_all(&bytes)
                        .and_then(|_| {
                            if self.buffered_wal {
                                Ok(())
                            } else {
                                self.wal.as_mut().unwrap().flush()
                            }
                        });
                    if let Err(err) = result {
                        self.poison_wal();
                        return Err(io(err));
                    }
                    self.wal_dirty = true;
                }
                self.state.seq = commit.seq;
                for (&r, s) in &mut commit.series {
                    if let Some((mint, maxt)) = s.ordered_bounds(false) {
                        self.head_mint = self.head_mint.min(mint);
                        self.head_maxt = self.head_maxt.max(maxt);
                    }
                    if let Some((mint, maxt)) = s.ordered_bounds(true) {
                        self.head_ooo_mint = self.head_ooo_mint.min(mint);
                        self.head_ooo_maxt = self.head_ooo_maxt.max(maxt);
                    }
                    if !self.index.all.contains(&r) {
                        self.index.intern_labels(&mut s.labels);
                        self.keys.insert(labels_key(&s.labels), r);
                        self.index.insert(s);
                    }
                    if s.has_samples() && !self.head_index.all.contains(&r) {
                        self.head_index.insert(s);
                    }
                }
                self.state.apply(commit);
                self.pending_keys.clear();
            }
            "rollback" => {
                self.pending = None;
                self.pending_keys.clear();
            }
            "configure" => {
                if req.window < 0 {
                    return Err("negative out-of-order window".into());
                }
                self.window = req.window;
                self.max_exemplars = req.max_exemplars;
                if req.up_to > 0 {
                    self.block_range = req.up_to;
                }
            }
            "head" => {
                response.series = self
                    .head_index
                    .all
                    .iter()
                    .filter_map(|r| self.state.series.get(r))
                    .map(|s| Series {
                        reference: s.reference,
                        labels: s.labels.clone(),
                        ..Default::default()
                    })
                    .collect();
            }
            "exemplars" => {
                let selected: BTreeSet<_> = req.refs.into_iter().collect();
                for s in self.state.series.values() {
                    if req.filter && !selected.contains(&s.reference) {
                        continue;
                    }
                    let exemplars: Vec<_> = s
                        .exemplars
                        .iter()
                        .filter(|e| e.t >= req.mint && e.t <= req.maxt)
                        .cloned()
                        .collect();
                    if !exemplars.is_empty() {
                        response.series.push(Series {
                            reference: s.reference,
                            labels: s.labels.clone(),
                            exemplars,
                            ..Default::default()
                        });
                    }
                }
            }
            "read" => {
                let mut all: BTreeMap<u64, Series> = BTreeMap::new();
                let selected: BTreeSet<u64> = req.refs.iter().copied().collect();
                for block in &self.state.cold {
                    if block.maxt < req.mint
                        || block.mint > req.maxt
                        || (req.filter && block.refs.is_disjoint(&selected))
                    {
                        continue;
                    }
                    let wanted = if req.filter {
                        &selected
                    } else {
                        block.refs.as_ref()
                    };
                    let cold = disk::read_cold(self, block, wanted, req.mint, req.maxt)?;
                    for s in cold {
                        if req.filter && !selected.contains(&s.reference) {
                            continue;
                        }
                        let entry = all.entry(s.reference).or_insert_with(|| Series {
                            reference: s.reference,
                            labels: s.labels.clone(),
                            ..Default::default()
                        });
                        entry.samples.extend(s.samples_in(req.mint, req.maxt));
                    }
                }
                for s in self.state.series.values() {
                    if req.filter && !selected.contains(&s.reference) {
                        continue;
                    }
                    let entry = all.entry(s.reference).or_insert_with(|| Series {
                        reference: s.reference,
                        labels: s.labels.clone(),
                        ..Default::default()
                    });
                    entry.samples.extend(s.samples_in(req.mint, req.maxt));
                    entry.exemplars = s.exemplars.clone();
                }
                response.series = all
                    .into_values()
                    .map(|mut s| {
                        s.samples.retain(|t, _| *t >= req.mint && *t <= req.maxt);
                        s
                    })
                    .collect();
            }
            "bounds" => {
                response.series = self
                    .state
                    .series
                    .values()
                    .filter_map(|s| {
                        s.bounds().map(|(mint, maxt)| {
                            let mut out = Series {
                                reference: s.reference,
                                ..Default::default()
                            };
                            for t in [mint, maxt] {
                                out.samples.insert(
                                    t,
                                    Sample {
                                        t,
                                        kind: 1,
                                        bits: 0,
                                        data: String::new(),
                                        ..Default::default()
                                    },
                                );
                            }
                            out
                        })
                    })
                    .collect();
            }
            "retain" => {
                if self.pending.is_some() {
                    return Err("transaction active".into());
                }
                if !self.state.cold.iter().any(|b| b.created_at < req.up_to) {
                    return Ok(response);
                }
                let mut next = self.state.clone();
                let mut removed = Vec::new();
                next.cold.retain(|block| {
                    if block.created_at < req.up_to {
                        removed.push(block.file.clone());
                        false
                    } else {
                        true
                    }
                });
                if removed.is_empty() {
                    return Ok(response);
                }
                let cold_refs: BTreeSet<_> = next
                    .cold
                    .iter()
                    .flat_map(|b| b.refs.iter().copied())
                    .collect();
                next.series.retain(|r, s| {
                    s.has_samples() || !s.exemplars.is_empty() || cold_refs.contains(r)
                });
                next.seq += 1;
                if let Err(error) =
                    self.atomic_write("head.json", &serde_json::to_vec(&next).map_err(io)?)
                {
                    // A failed directory sync can leave a newer checkpoint on disk.
                    self.poisoned = true;
                    return Err(error);
                }
                for (&r, s) in &self.state.series {
                    if !next.series.contains_key(&r) {
                        self.index.remove(s);
                        self.keys.remove(&labels_key(&s.labels));
                    }
                }
                self.state = next;
                self.reset_wal()?;
                // Unreferenced files after a crash are safe; referenced files must survive the checkpoint.
                for file in removed {
                    self.cold_mappings
                        .borrow_mut()
                        .retain(|(name, _)| name != &file);
                    fs::remove_file(self.dir.join(file)).map_err(io)?;
                }
            }
            "enable_buffered_wal" => {
                if self.pending.is_some() {
                    return Err("transaction active".into());
                }
                self.buffered_wal = true;
            }
            "sync" => {
                if let Some(file) = self.prepare_sync()?
                    && let Err(err) = file.sync_all()
                {
                    self.poison_wal();
                    return Err(io(err));
                }
            }
            "evict_idle" => {
                if req.up_to <= 0 {
                    return Ok(response);
                }
                let cutoff = self.head_maxt.saturating_sub(req.up_to);
                req.refs = self
                    .head_index
                    .all
                    .iter()
                    .filter_map(|r| {
                        let series = self.state.series.get(r)?;
                        (!series.uncompacted_ooo
                            && series.bounds().is_some_and(|(_, max)| max < cutoff))
                        .then_some(*r)
                    })
                    .collect();
                if req.refs.is_empty() {
                    return Ok(response);
                }
                req.op = "evict".into();
                req.filter = true;
                return self.call(req);
            }
            "flush" | "evict" | "compact_ooo" => {
                if self.pending.is_some() {
                    return Err("transaction active".into());
                }
                if req.op == "flush" {
                    let data_maxt = self
                        .state
                        .series
                        .values()
                        .filter_map(|series| series.bounds().map(|bounds| bounds.1))
                        .max()
                        .unwrap_or(i64::MIN);
                    if data_maxt == i64::MIN {
                        return Ok(response);
                    }
                    // A forced flush only truncates data that exists; future writes must remain appendable.
                    req.up_to = req.up_to.min(data_maxt);
                }
                let mut next = self.state.clone();
                next.seq += 1;
                let mut cold = Vec::new();
                let mut first_in_range = BTreeMap::<i64, i64>::new();
                let selected: BTreeSet<_> = req.refs.iter().copied().collect();
                for s in next.series.values_mut() {
                    if req.op == "evict" && !selected.contains(&s.reference) {
                        continue;
                    }
                    // Unaffected series can keep their compressed bytes and open appender.
                    if (req.op == "compact_ooo" && !s.uncompacted_ooo)
                        || (req.op == "flush"
                            && s.bounds().is_none_or(|(mint, _)| mint > req.up_to))
                    {
                        continue;
                    }
                    let mut moved = Series {
                        reference: s.reference,
                        labels: s.labels.clone(),
                        ..Default::default()
                    };
                    s.samples = s.samples_in(i64::MIN, i64::MAX);
                    s.float_chunks.clear();
                    s.open_float = None;
                    s.samples.retain(|t, v| {
                        let compact = match req.op.as_str() {
                            "evict" => true,
                            "compact_ooo" => v.out_of_order,
                            _ => *t <= req.up_to,
                        };
                        if compact {
                            if req.op == "flush" {
                                let start = t
                                    .div_euclid(self.block_range)
                                    .saturating_mul(self.block_range);
                                first_in_range
                                    .entry(start)
                                    .and_modify(|first| *first = (*first).min(*t))
                                    .or_insert(*t);
                            }
                            moved.samples.insert(*t, v.clone());
                            false
                        } else {
                            true
                        }
                    });
                    s.repack();
                    moved.repack();
                    if moved.has_samples() {
                        cold.push(moved);
                    }
                }
                if cold.is_empty() && (req.op != "flush" || req.up_to < next.min_valid) {
                    return Ok(response);
                }
                if !cold.is_empty() {
                    let file = format!("cold-{}.bin", next.seq);
                    let (bytes, locations) = disk::cold_file(&cold);
                    self.atomic_write(&file, &bytes)?;
                    let mut label_blocks = BTreeMap::<i64, BTreeSet<u64>>::new();
                    for series in &cold {
                        for start in series.label_block_starts(self.block_range) {
                            label_blocks
                                .entry(start)
                                .or_default()
                                .insert(series.reference);
                        }
                    }
                    let block = Cold {
                        evicted: req.op == "evict" && req.filter,
                        label_blocks: label_blocks
                            .into_iter()
                            .map(|(start, refs)| {
                                let end = start.saturating_add(self.block_range - 1);
                                let (mint, maxt) = if req.op == "flush" {
                                    let previous =
                                        self.state.label_head_mint.unwrap_or(self.head_mint);
                                    let appendable = self
                                        .state
                                        .min_valid
                                        .max(self.head_maxt.saturating_sub(self.block_range / 2));
                                    let mint = if start <= previous {
                                        start.max(previous)
                                    } else {
                                        first_in_range[&start].min(start.max(appendable))
                                    };
                                    (mint, end.min(req.up_to))
                                } else {
                                    (start, end)
                                };
                                ColdLabelBlock {
                                    mint,
                                    maxt,
                                    refs: refs.into_iter().collect(),
                                }
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        locations: locations.into(),
                        created_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(io)?
                            .as_millis() as i64,
                        file,
                        mint: cold
                            .iter()
                            .filter_map(|s| s.bounds().map(|b| b.0))
                            .min()
                            .unwrap(),
                        maxt: cold
                            .iter()
                            .filter_map(|s| s.bounds().map(|b| b.1))
                            .max()
                            .unwrap(),
                        refs: cold
                            .iter()
                            .map(|s| s.reference)
                            .collect::<BTreeSet<_>>()
                            .into(),
                        ranges: cold
                            .iter()
                            .filter_map(|s| s.bounds().map(|range| (s.reference, range)))
                            .collect::<BTreeMap<_, _>>()
                            .into(),
                    };
                    next.cold.push(block);
                }
                if req.op == "flush" {
                    next.min_valid = next.min_valid.max(req.up_to.saturating_add(1));
                }
                let previous = if req.op == "flush" {
                    req.up_to.saturating_add(1)
                } else {
                    self.state.label_head_mint.unwrap_or(self.head_mint)
                };
                let actual = next
                    .series
                    .values()
                    .filter_map(|s| s.ordered_bounds(false).map(|b| b.0))
                    .min()
                    .unwrap_or(previous);
                // The Go head index spans its logical appendable window, even
                // when idle eviction leaves only newer samples in memory.
                next.label_head_mint = Some(if actual > previous {
                    actual.min(
                        next.min_valid
                            .max(self.head_maxt.saturating_sub(self.block_range / 2)),
                    )
                } else {
                    previous
                });
                if let Err(error) =
                    self.atomic_write("head.json", &serde_json::to_vec(&next).map_err(io)?)
                {
                    // A failed directory sync can leave a newer checkpoint on disk.
                    self.poisoned = true;
                    return Err(error);
                }
                for series in next.series.values() {
                    if !series.has_samples() && self.head_index.all.contains(&series.reference) {
                        response.series.push(Series {
                            reference: series.reference,
                            labels: series.labels.clone(),
                            ..Default::default()
                        });
                        self.head_index.remove(series);
                    }
                }
                self.state = next;
                (self.head_mint, self.head_maxt) = self.state.head_bounds(false);
                (self.head_ooo_mint, self.head_ooo_maxt) = self.state.head_bounds(true);
                // Replay ignores records covered by the checkpoint sequence, even if truncation fails.
                self.reset_wal()?;
            }
            _ => return Err("unknown operation".into()),
        }
        Ok(response)
    }

    fn prepare_sync(&mut self) -> Result<Option<File>> {
        if self.sealed {
            return Err("engine is closed".into());
        }
        if self.poisoned {
            return Err("engine requires reopen after WAL failure".into());
        }
        if !self.wal_dirty {
            return Ok(None);
        }
        let wal = self.wal.as_mut().ok_or("WAL closed")?;
        let result = wal.flush().and_then(|_| wal.get_ref().try_clone());
        match result {
            Ok(file) => {
                // New commits must remain dirty while this descriptor is syncing.
                self.wal_dirty = false;
                Ok(Some(file))
            }
            Err(err) => {
                self.poison_wal();
                Err(io(err))
            }
        }
    }

    fn poison_wal(&mut self) {
        self.poisoned = true;
        if let Some(wal) = self.wal.take() {
            // Never let Drop publish buffered bytes from a failed transaction.
            let (file, _) = wal.into_parts();
            self.wal = Some(BufWriter::with_capacity(64 * 1024, file));
        }
    }
    fn reset_wal(&mut self) -> Result<()> {
        if self.kafka_replay {
            return Ok(());
        }
        let (file, _) = self.wal.take().ok_or("WAL closed")?.into_parts();
        // The durable checkpoint covers the buffered records too.
        let result = file.set_len(0).and_then(|_| file.sync_all());
        self.wal = Some(BufWriter::with_capacity(64 * 1024, file));
        if let Err(err) = result {
            self.poisoned = true;
            return Err(io(err));
        }
        self.wal_dirty = false;
        Ok(())
    }

    fn atomic_write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let temp = self.dir.join(format!("{name}.tmp"));
        let mut f = File::create(&temp).map_err(io)?;
        f.write_all(bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
        fs::rename(temp, self.dir.join(name)).map_err(io)?;
        File::open(&self.dir).and_then(|d| d.sync_all()).map_err(io)
    }
}

#[repr(C)]
pub struct Buffer {
    data: *mut u8,
    len: usize,
}
fn buffer(bytes: Vec<u8>) -> Buffer {
    let mut bytes = bytes.into_boxed_slice();
    let out = Buffer {
        data: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    std::mem::forget(bytes);
    out
}

// Panics cannot unwind across the C ABI. A poisoned instance refuses further calls.
/// # Safety
/// `path` must be readable for `len` bytes and `error` must be writable.
/// The returned handle must be closed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_open(
    path: *const u8,
    len: usize,
    kafka_replay: u8,
    error: *mut Buffer,
) -> *mut std::ffi::c_void {
    let result = std::panic::catch_unwind(|| {
        let path =
            std::str::from_utf8(unsafe { std::slice::from_raw_parts(path, len) }).map_err(io)?;
        match kafka_replay {
            0 => Engine::open(Path::new(path)),
            1 => Engine::open_mode(Path::new(path), true),
            _ => Err("invalid engine recovery mode".into()),
        }
    });
    match result {
        Ok(Ok(engine)) => Box::into_raw(Box::new(engine)).cast(),
        other => {
            let message = match other {
                Ok(Err(e)) => e,
                _ => "Rust engine panic opening storage".into(),
            };
            unsafe {
                *error = buffer(message.into_bytes());
            }
            ptr::null_mut()
        }
    }
}

/// # Safety
/// `engine` must be a live handle with exclusive access during the call.
/// `request` must be readable for `len` bytes. Free the returned buffer once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_call(
    engine: *mut std::ffi::c_void,
    request: *const u8,
    len: usize,
) -> Buffer {
    let engine = unsafe { &mut *engine.cast::<Engine>() };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let req = wire::request(unsafe { std::slice::from_raw_parts(request, len) })?;
        engine.call(req)
    }));
    let response = match result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => Response {
            error,
            ..Default::default()
        },
        Err(_) => {
            engine.poisoned = true;
            Response {
                error: "Rust engine panic; reopen required".into(),
                ..Default::default()
            }
        }
    };
    buffer(wire::response(&response))
}

/// # Safety
/// `buf` must be an unmodified buffer returned by this library, freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_free(buf: Buffer) {
    if !buf.data.is_null() {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(buf.data, buf.len)) });
    }
}

/// # Safety
/// `engine` must be live and exclusively held; `error` must be writable.
/// Serialize sync operations until the returned file has been consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_prepare_sync(
    engine: *mut std::ffi::c_void,
    error: *mut Buffer,
) -> *mut std::ffi::c_void {
    let engine = unsafe { &mut *engine.cast::<Engine>() };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| engine.prepare_sync())) {
        Ok(Ok(Some(file))) => Box::into_raw(Box::new(file)).cast(),
        Ok(Ok(None)) => ptr::null_mut(),
        other => {
            engine.poison_wal();
            let message = match other {
                Ok(Err(err)) => err,
                _ => "Rust engine panic preparing WAL sync".into(),
            };
            unsafe { *error = buffer(message.into_bytes()) };
            ptr::null_mut()
        }
    }
}

/// # Safety
/// `file` must be a non-null handle returned by prepare_sync, consumed once.
/// This call owns only the file and may run concurrently with engine calls.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_sync_file(file: *mut std::ffi::c_void) -> Buffer {
    let file = unsafe { Box::from_raw(file.cast::<File>()) };
    match std::panic::catch_unwind(|| file.sync_all()) {
        Ok(Ok(())) => buffer(Vec::new()),
        Ok(Err(err)) => buffer(io(err).into_bytes()),
        Err(_) => buffer(b"Rust panic syncing WAL".to_vec()),
    }
}

/// # Safety
/// `engine` must be a live handle with exclusive access.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_fail_sync(engine: *mut std::ffi::c_void) {
    unsafe { &mut *engine.cast::<Engine>() }.poison_wal();
}

/// # Safety
/// `engine` must be live with exclusive access; `generation` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_index_epoch(
    engine: *mut std::ffi::c_void,
    include_cold: u8,
    generation: *mut u64,
) -> u8 {
    let engine = unsafe { &*engine.cast::<Engine>() };
    if engine.poisoned {
        return 0;
    }
    let index = if include_cold != 0 {
        &engine.index
    } else {
        &engine.head_index
    };
    unsafe { *generation = index.generation };
    1
}

/// # Safety
/// `engine` must be null or a live handle returned by open, closed once,
/// with no concurrent calls or outstanding uses.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_engine_close(engine: *mut std::ffi::c_void) {
    if !engine.is_null() {
        drop(unsafe { Box::from_raw(engine.cast::<Engine>()) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sample_order_survives_compression_checkpoint_and_repack() {
        let mut series = Series::default();
        for t in 1000..1300 {
            series.insert(Sample {
                t,
                kind: 1,
                bits: (t as f64).to_bits(),
                ..Default::default()
            });
        }
        for t in 0..300 {
            series.insert(Sample {
                t,
                kind: 1,
                bits: (t as f64).to_bits(),
                out_of_order: true,
                ..Default::default()
            });
        }
        series.insert(Sample {
            t: -1,
            kind: 2,
            data: "histogram".into(),
            out_of_order: true,
            ..Default::default()
        });
        assert!(series.float_chunks.iter().any(|chunk| chunk.out_of_order));
        let bytes = serde_json::to_vec(&series).unwrap();
        let mut restored: Series = serde_json::from_slice(&bytes).unwrap();
        for series in [&mut series, &mut restored] {
            for _ in 0..2 {
                assert_eq!(series.ordered_bounds(false), Some((1000, 1299)));
                assert_eq!(series.ordered_bounds(true), Some((-1, 299)));
                assert_eq!(series.last.as_ref().unwrap().t, 1299);
                let samples = series.samples_in(i64::MIN, i64::MAX);
                assert_eq!(samples.len(), 601);
                for (&t, sample) in &samples {
                    assert_eq!(sample.out_of_order, t < 1000);
                }
                series.samples = samples;
                series.repack();
            }
        }
    }
    #[test]
    fn mixed_samples_survive_float_queue_compression_and_restore() {
        let mut series = Series::default();
        let mut expected = BTreeMap::new();
        for n in 0..600 {
            let sample = Sample {
                t: n * 2,
                kind: 2,
                bits: 0,
                data: "opaque histogram".into(),
                ..Default::default()
            };
            expected.insert(sample.t, sample.clone());
            series.insert(sample);
        }
        for n in 0..260 {
            let sample = Sample {
                t: n * 2 + 1,
                kind: 1,
                bits: (n as f64).to_bits(),
                data: String::new(),
                ..Default::default()
            };
            expected.insert(sample.t, sample.clone());
            series.insert(sample);
        }
        assert_eq!(series.samples_in(i64::MIN, i64::MAX), expected);
        let bytes = serde_json::to_vec(&series).unwrap();
        let mut restored: Series = serde_json::from_slice(&bytes).unwrap();
        for n in 260..360 {
            let sample = Sample {
                t: n * 2 + 1,
                kind: 1,
                bits: (n as f64).to_bits(),
                data: String::new(),
                ..Default::default()
            };
            expected.insert(sample.t, sample.clone());
            restored.insert(sample);
        }
        assert_eq!(restored.samples_in(i64::MIN, i64::MAX), expected);
    }
    #[test]
    fn floats_keep_a_bounded_compressed_tail() {
        let mut series = Series::default();
        for i in 0..3120 {
            series.insert(Sample {
                t: i * 15000,
                kind: 1,
                bits: (i as f64).to_bits(),
                data: String::new(),
                ..Default::default()
            });
        }
        assert!(series.samples.is_empty());
        assert!(series.open_float.as_ref().unwrap().len() <= 120);
        let bytes: usize = series
            .float_chunks
            .iter()
            .map(|c| c.data.len())
            .sum::<usize>()
            + series.open_float.as_ref().unwrap().byte_capacity();
        assert!(bytes < 3120 * 3, "compressed bytes: {bytes}");
        let checkpoint = serde_json::to_vec(&series).unwrap();
        let restored: Series = serde_json::from_slice(&checkpoint).unwrap();
        let samples = restored.samples_in(i64::MIN, i64::MAX);
        assert_eq!(samples.len(), 3120);
        assert_eq!(samples[&(100 * 15000)].bits, 100f64.to_bits());
    }
}

#[cfg(test)]
mod metadata_sets_regression {
    use super::*;
    #[test]
    fn value_sets_keep_counts_separate_and_filter_head_and_cold() {
        let path = std::env::temp_dir().join(format!("mimir-meta-sets-{}", std::process::id()));
        let mut e = Engine::open(&path).unwrap();
        for reference in 1..=4 {
            let mut labels = vec![Label {
                name: "__name__".into(),
                value: "metric".into(),
            }];
            if reference < 4 {
                labels.push(Label {
                    name: "group".into(),
                    value: if reference < 3 { "a" } else { "b" }.into(),
                });
            }
            let mut series = Series {
                reference,
                labels: Arc::new(labels),
                ..Default::default()
            };
            series.insert(Sample {
                t: 10,
                kind: 1,
                bits: (reference as f64).to_bits(),
                ..Default::default()
            });
            e.index.insert(&series);
            e.head_index.insert(&series);
            e.state.series.insert(reference, series);
        }
        e.head_mint = 10;
        e.head_maxt = 10;
        for stage in 0..2 {
            if stage == 1 {
                e.call(Request {
                    op: "flush".into(),
                    up_to: 10,
                    ..Default::default()
                })
                .unwrap();
            }
            for op in ["metadata_all", "metadata_sets"] {
                let out = e
                    .call(Request {
                        op: op.into(),
                        include_cold: true,
                        ..Default::default()
                    })
                    .unwrap();
                assert_eq!(
                    out.label_counts["group"]["a"],
                    if op == "metadata_all" { 2 } else { 1 }
                );
                let out = e
                    .call(Request {
                        op: op.into(),
                        include_cold: true,
                        filters: vec![LabelFilter {
                            name: "group".into(),
                            values: vec!["a".into(), "a".into()],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .unwrap();
                assert_eq!(out.label_counts["group"].len(), 1);
                assert_eq!(
                    out.label_counts["group"]["a"],
                    if op == "metadata_all" { 2 } else { 1 }
                );
            }
            let out = e
                .call(Request {
                    op: "label_values".into(),
                    name: "group".into(),
                    mint: 0,
                    maxt: 20,
                    filters: vec![LabelFilter {
                        name: "group".into(),
                        values: vec!["a".into()],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(out.values, ["a"]);
        }
        drop(e);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod posting_selector_tests {
    use super::*;
    fn fixture(size: u64) -> (State, LabelIndex, LabelIndex) {
        let mut state = State::default();
        let mut all = LabelIndex::default();
        let mut head = LabelIndex::default();
        for reference in 1..=size {
            let mut labels = vec![Label {
                name: "__name__".into(),
                value: "metric".into(),
            }];
            if reference % 5 != 0 {
                labels.push(Label {
                    name: "group".into(),
                    value: ["", "a", "b", "λ\0"][reference as usize % 4].into(),
                });
            }
            let series = Series {
                reference,
                labels: Arc::new(labels),
                ..Default::default()
            };
            all.insert(&series);
            if reference % 2 == 0 {
                head.insert(&series);
            }
            state.series.insert(reference, series);
        }
        (state, all, head)
    }
    fn oracle_refs(index: &LabelIndex, state: &State, filters: &[LabelFilter]) -> BTreeSet<u64> {
        index
            .all
            .iter()
            .filter(|reference| {
                let row = &state.series[reference];
                filters.iter().all(|filter| {
                    row.labels
                        .iter()
                        .find(|label| label.name.as_ref() == filter.name.as_str())
                        .map_or(filter.match_missing, |label| {
                            filter
                                .values
                                .iter()
                                .any(|value| value.as_str() == label.value.as_ref())
                                != filter.exclude
                        })
                })
            })
            .copied()
            .collect()
    }
    #[test]
    fn posting_selection_matches_row_selection_for_missing_empty_and_negated_values() {
        let (state, all, head) = fixture(37);
        let values = ["", "a", "b", "λ\0", "unknown"];
        for index in [&all, &head] {
            let mut filters = Vec::new();
            for name in ["group", "missing", "__name__"] {
                for mask in 0..(1 << values.len()) {
                    for exclude in [false, true] {
                        for match_missing in [false, true] {
                            let mut included: Vec<_> = values
                                .iter()
                                .enumerate()
                                .filter(|(n, _)| mask & (1 << n) != 0)
                                .map(|(_, value)| value.to_string())
                                .collect();
                            included.sort();
                            let filter = LabelFilter {
                                name: name.into(),
                                values: included,
                                exclude,
                                match_missing,
                            };
                            assert_eq!(
                                index
                                    .select_refs(&state, std::slice::from_ref(&filter))
                                    .as_ref(),
                                &oracle_refs(index, &state, std::slice::from_ref(&filter)),
                                "name={name} mask={mask} exclude={exclude} missing={match_missing}"
                            );
                            filters.push(filter);
                        }
                    }
                }
            }
            for a in filters.iter().step_by(17) {
                for b in filters.iter().step_by(19) {
                    let clone = |f: &LabelFilter| LabelFilter {
                        name: f.name.clone(),
                        values: f.values.clone(),
                        exclude: f.exclude,
                        match_missing: f.match_missing,
                    };
                    let pair = [clone(a), clone(b)];
                    assert_eq!(
                        index.select_refs(&state, &pair).as_ref(),
                        &oracle_refs(index, &state, &pair)
                    );
                }
            }
        }
    }
    #[test]
    fn presence_counts_follow_insert_remove_and_reinsertion() {
        let (state, mut index, _) = fixture(37);
        for reference in 1..=37 {
            let series = &state.series[&reference];
            index.remove(series);
            for values in index.values.values() {
                assert_eq!(
                    values.present,
                    values.values().map(BTreeSet::len).sum::<usize>()
                );
            }
            let filter = LabelFilter {
                name: "__name__".into(),
                values: vec!["".into()],
                exclude: true,
                match_missing: false,
            };
            assert_eq!(
                index
                    .select_refs(&state, std::slice::from_ref(&filter))
                    .as_ref(),
                &oracle_refs(&index, &state, std::slice::from_ref(&filter))
            );
            if reference % 2 == 0 {
                index.insert(series);
                index.insert(series);
                for values in index.values.values() {
                    assert_eq!(
                        values.present,
                        values.values().map(BTreeSet::len).sum::<usize>()
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod exemplar_index_tests {
    use super::*;
    #[test]
    fn indexed_eviction_matches_sorted_retention_after_restore_and_cap_changes() {
        let mut state = State::default();
        let mut expected: BTreeMap<u64, Vec<Exemplar>> = BTreeMap::new();
        for n in 0..1000_u64 {
            let cap = [0, 1, 7, 23, 100][(n / 17) as usize % 5];
            let reference = n.wrapping_mul(17) % 31;
            let additions = (0..n % 4)
                .map(|v| Exemplar {
                    t: (n / 4) as i64,
                    bits: v,
                    labels: Vec::new(),
                })
                .collect::<Vec<_>>();
            expected
                .entry(reference)
                .or_default()
                .extend(additions.clone());
            state.apply(Commit {
                max_exemplars: cap,
                seq: n,
                next_ref: 31,
                series: BTreeMap::from([(
                    reference,
                    Series {
                        reference,
                        exemplars: additions,
                        ..Default::default()
                    },
                )]),
            });
            let mut all = expected
                .iter()
                .flat_map(|(&r, es)| es.iter().map(move |e| (e.t, r)))
                .collect::<Vec<_>>();
            all.sort_unstable();
            let excess = all.len().saturating_sub(cap);
            let mut remove: BTreeMap<u64, usize> = BTreeMap::new();
            for (_, r) in all.into_iter().take(excess) {
                *remove.entry(r).or_default() += 1;
            }
            for (r, count) in remove {
                expected.get_mut(&r).unwrap().drain(..count);
            }
            assert_eq!(
                state.exemplar_count,
                expected.values().map(Vec::len).sum::<usize>()
            );
            for (&r, es) in &expected {
                assert_eq!(
                    state.series[&r]
                        .exemplars
                        .iter()
                        .map(|e| (e.t, e.bits))
                        .collect::<Vec<_>>(),
                    es.iter().map(|e| (e.t, e.bits)).collect::<Vec<_>>()
                );
            }
            if n % 43 == 0 {
                state = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
            } else if n % 19 == 0 {
                state = state.clone();
            }
        }
    }
}

#[cfg(test)]
mod shared_labels_tests {
    use super::*;
    #[test]
    fn commits_and_recovery_share_owned_labels_without_changing_identity() {
        let path = std::env::temp_dir().join(format!("mimir-shared-labels-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        let mut engine = Engine::open(&path).unwrap();
        engine
            .call(Request {
                op: "begin".into(),
                ..Default::default()
            })
            .unwrap();
        let mut refs = Vec::new();
        let mut keys = Vec::new();
        for instance in ["first", "second", "third"] {
            let labels = vec![
                Label {
                    name: "group".into(),
                    value: "same".into(),
                },
                Label {
                    name: "instance".into(),
                    value: instance.into(),
                },
            ];
            keys.push(labels_key(&labels));
            refs.push(
                engine
                    .call(Request {
                        op: "append".into(),
                        labels,
                        sample: Some(Sample {
                            t: 1,
                            kind: 1,
                            bits: 1.0_f64.to_bits(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .unwrap()
                    .reference,
            );
        }
        engine
            .call(Request {
                op: "commit".into(),
                ..Default::default()
            })
            .unwrap();
        for stage in 0..3 {
            if stage > 0 {
                if stage == 2 {
                    fs::write(
                        path.join("head.json"),
                        serde_json::to_vec(&engine.state).unwrap(),
                    )
                    .unwrap();
                }
                drop(engine);
                engine = Engine::open(&path).unwrap();
            }
            let first = &engine.state.series[&refs[0]].labels;
            for (&reference, key) in refs.iter().zip(&keys) {
                let labels = &engine.state.series[&reference].labels;
                assert!(Arc::ptr_eq(&first[0].name, &labels[0].name));
                assert!(Arc::ptr_eq(&first[0].value, &labels[0].value));
                assert!(Arc::ptr_eq(&first[1].name, &labels[1].name));
                assert_eq!(labels_key(labels), *key);
                assert_eq!(engine.keys[key], reference);
                let (name, values) = engine.index.values.get_key_value("group").unwrap();
                assert!(Arc::ptr_eq(name, &labels[0].name));
                assert!(Arc::ptr_eq(
                    values.get_key_value("same").unwrap().0,
                    &labels[0].value
                ));
            }
            assert!(!Arc::ptr_eq(
                &first[1].value,
                &engine.state.series[&refs[1]].labels[1].value
            ));
        }
        drop(engine);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod compaction_selection_tests {
    use super::*;
    #[test]
    fn compaction_keeps_unaffected_sealed_chunks_and_samples() {
        let path =
            std::env::temp_dir().join(format!("mimir-compact-selected-{}", std::process::id()));
        let mut engine = Engine::open(&path).unwrap();
        for reference in 1..=2 {
            let mut series = Series {
                reference,
                labels: Arc::new(vec![Label {
                    name: "instance".into(),
                    value: reference.to_string().into(),
                }]),
                ..Default::default()
            };
            for t in 1000..1300 {
                series.insert(Sample {
                    t,
                    kind: 1,
                    bits: (t as f64).to_bits(),
                    ..Default::default()
                });
            }
            if reference == 2 {
                series.insert(Sample {
                    t: 10,
                    kind: 1,
                    bits: 10.0_f64.to_bits(),
                    out_of_order: true,
                    ..Default::default()
                });
            }
            engine.index.insert(&series);
            engine.head_index.insert(&series);
            engine.state.series.insert(reference, series);
        }
        let owner = engine.state.series[&1].float_chunks[0]
            .data
            .owner
            .clone()
            .unwrap();
        let expected = engine.state.series[&1].samples_in(i64::MIN, i64::MAX);
        for (op, up_to) in [("compact_ooo", 0), ("flush", 500)] {
            engine
                .call(Request {
                    op: op.into(),
                    up_to,
                    ..Default::default()
                })
                .unwrap();
            let series = &engine.state.series[&1];
            assert!(Arc::ptr_eq(
                &owner,
                series.float_chunks[0].data.owner.as_ref().unwrap()
            ));
            assert_eq!(expected, series.samples_in(i64::MIN, i64::MAX));
            assert_eq!(
                engine.state.series[&2].samples_in(i64::MIN, i64::MAX),
                expected
            );
        }
        assert_eq!(engine.state.cold.len(), 1);
        drop(engine);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod shared_cold_tests {
    use super::*;
    #[test]
    fn state_clones_share_cold_indexes_and_keep_checkpoint_bytes() {
        let mut state = State::default();
        state.cold.push(Cold {
            created_at: 0,
            file: "cold.bin".into(),
            mint: 1,
            maxt: 2,
            refs: BTreeSet::from([1, 2]).into(),
            ranges: BTreeMap::from([(1, (1, 2)), (2, (1, 2))]).into(),
            locations: BTreeMap::from([(1, (0, 20)), (2, (20, 40))]).into(),
            label_blocks: vec![ColdLabelBlock {
                mint: 1,
                maxt: 2,
                refs: vec![1, 2],
            }]
            .into(),
            evicted: false,
        });
        let copy = state.clone();
        let a = &state.cold[0];
        let b = &copy.cold[0];
        assert!(Arc::ptr_eq(&a.refs, &b.refs));
        assert!(Arc::ptr_eq(&a.ranges, &b.ranges));
        assert!(Arc::ptr_eq(&a.locations, &b.locations));
        assert!(Arc::ptr_eq(&a.label_blocks, &b.label_blocks));
        let bytes = serde_json::to_vec(&state).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&copy).unwrap());
        let restored: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&restored).unwrap());
    }
}
