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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Label {
    name: String,
    value: String,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Sample {
    t: i64,
    kind: u8,
    // Float bits preserve NaNs (including Prometheus stale markers) through JSON.
    #[serde(default)]
    bits: u64,
    #[serde(default)]
    data: String,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub struct Exemplar {
    t: i64,
    bits: u64,
    labels: Vec<Label>,
}

#[derive(Clone, Deserialize, Serialize)]
struct FloatChunk {
    mint: i64,
    maxt: i64,
    data: Box<[u8]>,
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
            .map(|chunk| chunk.clone().into_bytes())
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
                        },
                    );
                }
            }
        }
        if let Some(chunk) = &self.open_float
            && chunk.first_timestamp().is_some_and(|t| t <= maxt)
            && chunk.last_timestamp().is_some_and(|t| t >= mint)
        {
            for (t, v) in xor::decode(&chunk.clone().into_bytes()) {
                if t >= mint && t <= maxt {
                    samples.insert(
                        t,
                        Sample {
                            t,
                            kind: 1,
                            bits: v.to_bits(),
                            data: String::new(),
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
    fn insert(&mut self, sample: Sample) {
        if sample.kind == 1 && self.last.as_ref().is_none_or(|last| sample.t > last.t) {
            if self.open_float.as_ref().is_some_and(|chunk| {
                chunk.len() >= 120 || chunk.byte_len() >= xor::MAX_BYTES_BEFORE_APPEND
            }) {
                let chunk = self.open_float.take().unwrap();
                self.float_chunks.push(FloatChunk {
                    mint: chunk.first_timestamp().unwrap(),
                    maxt: chunk.last_timestamp().unwrap(),
                    data: chunk.into_bytes().into_boxed_slice(),
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
        if self.last.as_ref().is_none_or(|last| sample.t >= last.t) {
            self.last = Some(sample);
        }
        self.compress();
    }
    fn repack(&mut self) {
        let last = self.last.take();
        let samples = std::mem::take(&mut self.samples);
        self.pending_floats = 0;
        self.float_chunks.clear();
        self.open_float = None;
        for (_, sample) in samples {
            self.insert(sample);
        }
        if last
            .as_ref()
            .is_some_and(|last| self.last.as_ref().is_none_or(|s| last.t > s.t))
        {
            self.last = last;
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
            let data = xor::encode(&floats).into_boxed_slice();
            self.float_chunks.push(FloatChunk {
                mint: floats[0].0,
                maxt: floats.last().unwrap().0,
                data,
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
    refs: BTreeSet<u64>,
    ranges: BTreeMap<u64, (i64, i64)>,
    #[serde(default)]
    locations: BTreeMap<u64, (u64, u64)>,
}

#[derive(Default)]
struct LabelIndex {
    values: BTreeMap<String, BTreeMap<String, BTreeSet<u64>>>,
    all: BTreeSet<u64>,
}
impl LabelIndex {
    fn insert(&mut self, series: &Series) {
        self.all.insert(series.reference);
        for l in series.labels.iter() {
            self.values
                .entry(l.name.clone())
                .or_default()
                .entry(l.value.clone())
                .or_default()
                .insert(series.reference);
        }
    }
    fn remove(&mut self, series: &Series) {
        self.all.remove(&series.reference);
        for l in series.labels.iter() {
            if let Some(values) = self.values.get_mut(&l.name) {
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
    let mut key = Vec::new();
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
    fn apply(&mut self, commit: Commit) {
        self.seq = commit.seq;
        self.next_ref = commit.next_ref;
        for (r, delta) in commit.series {
            let series = self.series.entry(r).or_insert_with(|| Series {
                reference: r,
                labels: delta.labels.clone(),
                ..Default::default()
            });
            for (_, sample) in delta.samples {
                series.insert(sample);
            }
            self.exemplar_count += delta.exemplars.len();
            series.exemplars.extend(delta.exemplars);
        }
        let excess = self.exemplar_count.saturating_sub(commit.max_exemplars);
        if excess > 0 {
            let mut oldest: Vec<_> = self
                .series
                .values()
                .flat_map(|s| s.exemplars.iter().map(move |e| (e.t, s.reference)))
                .collect();
            oldest.sort_unstable();
            let mut remove: HashMap<u64, usize> = HashMap::new();
            for (_, r) in oldest.into_iter().take(excess) {
                *remove.entry(r).or_default() += 1;
            }
            for (r, n) in remove {
                self.series.get_mut(&r).unwrap().exemplars.drain(..n);
            }
            self.exemplar_count -= excess;
        }
    }
}

#[derive(Default, Deserialize)]
struct FloatSeries {
    index: usize,
    key: Vec<u8>,
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
struct Request {
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
    batch: Vec<FloatSeries>,
    #[serde(default)]
    limit: usize,
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
}

struct Engine {
    cursors: HashMap<u64, QueryCursor>,
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
    poisoned: bool,
    head_mint: i64,
    head_maxt: i64,
}

type Result<T> = std::result::Result<T, String>;
fn io(err: impl std::fmt::Display) -> String {
    err.to_string()
}

impl Engine {
    fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).map_err(io)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))
            .map_err(io)?;
        lock.try_lock().map_err(io)?;
        let mut state: State = match fs::read(dir.join("head.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(io(e)),
        };
        if dir.join("wal.jsonl").try_exists().map_err(io)? {
            return Err("legacy JSON WAL requires a fresh Rust engine directory".into());
        }
        for series in state.series.values_mut() {
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
        let keys = state
            .series
            .values()
            .map(|s| (labels_key(&s.labels), s.reference))
            .collect();
        let mut head_index = LabelIndex::default();
        let mut index = LabelIndex::default();
        for series in state.series.values() {
            index.insert(series);
            if series.has_samples() {
                head_index.insert(series);
            }
        }
        let head_mint = state
            .series
            .values()
            .filter_map(|s| s.bounds().map(|b| b.0))
            .min()
            .unwrap_or(i64::MAX);
        let head_maxt = state
            .series
            .values()
            .filter_map(|s| s.bounds().map(|b| b.1))
            .max()
            .unwrap_or(i64::MIN);
        Ok(Self {
            cursors: HashMap::new(),
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
            poisoned: false,
            head_mint,
            head_maxt,
        })
    }

    fn call(&mut self, mut req: Request) -> Result<Response> {
        if self.poisoned {
            return Err("engine requires reopen after WAL failure".into());
        }
        let mut response = Response::default();
        match req.op.as_str() {
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

            "cursor_open" => {
                let cursor = QueryCursor::open(self, &req)?;
                self.next_cursor = self
                    .next_cursor
                    .checked_add(1)
                    .ok_or("query cursor ID overflow")?;
                response.cursor = self.next_cursor;
                response.count = cursor.len();
                self.cursors.insert(response.cursor, cursor);
            }
            "cursor_series" | "cursor_chunks" => {
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
            "bytes" => {
                for (name, values) in &self.head_index.values {
                    response.bytes.insert(
                        name.clone(),
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
                    let reference = self.keys.get(&row.key).copied().unwrap_or(0);
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
                    response.values = index.values.keys().cloned().collect();
                } else if req.op == "values" {
                    response.values = index
                        .values
                        .get(&req.name)
                        .map(|v| v.keys().cloned().collect())
                        .unwrap_or_default();
                } else if req.name.is_empty() {
                    response.refs = index.all.iter().copied().collect();
                } else if let Some(values) = index.values.get(&req.name) {
                    if req.values.len() == 1 {
                        response.refs = values
                            .get(&req.values[0])
                            .map(|refs| refs.iter().copied().collect())
                            .unwrap_or_default();
                    } else {
                        let mut refs = Vec::new();
                        for value in req.values {
                            if let Some(postings) = values.get(&value) {
                                refs.extend(postings.iter().copied());
                            }
                        }
                        refs.sort_unstable();
                        refs.dedup();
                        response.refs = refs;
                    }
                }
            }
            "label_names_for" | "index_label_values" => {
                let mut values = BTreeSet::new();
                for reference in req.refs {
                    let series = self.state.series.get(&reference).ok_or("not_found")?;
                    for l in series.labels.iter() {
                        if req.op == "label_names_for" {
                            values.insert(l.name.as_str());
                        } else if l.name == req.name {
                            values.insert(l.value.as_str());
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
                let selected: BTreeSet<_> = req.refs.iter().copied().collect();
                let mut active = BTreeSet::new();
                if req.mint <= req.maxt {
                    for r in &selected {
                        if let Some(s) = self.state.series.get(r)
                            && (s.samples.range(req.mint..=req.maxt).next().is_some()
                                || s.float_chunks
                                    .iter()
                                    .any(|c| c.mint <= req.maxt && c.maxt >= req.mint)
                                || s.open_float.as_ref().is_some_and(|c| {
                                    c.first_timestamp().unwrap() <= req.maxt
                                        && c.last_timestamp().unwrap() >= req.mint
                                }))
                        {
                            active.insert(*r);
                        }
                    }
                    for block in &self.state.cold {
                        if block.maxt < req.mint
                            || block.mint > req.maxt
                            || block.refs.is_disjoint(&selected)
                        {
                            continue;
                        }
                        for (&r, &(mint, maxt)) in &block.ranges {
                            if mint <= req.maxt && maxt >= req.mint && selected.contains(&r) {
                                active.insert(r);
                            }
                        }
                    }
                }
                if req.op == "label_names" {
                    for (name, values) in &self.index.values {
                        if values.values().any(|refs| !refs.is_disjoint(&active)) {
                            response.values.push(name.clone());
                        }
                        if req.limit > 0 && response.values.len() >= req.limit {
                            break;
                        }
                    }
                } else if let Some(values) = self.index.values.get(&req.name) {
                    for (value, refs) in values {
                        if !refs.is_disjoint(&active) {
                            response.values.push(value.clone());
                        }
                        if req.limit > 0 && response.values.len() >= req.limit {
                            break;
                        }
                    }
                }
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
            "series" => {
                if let Some(series) = self
                    .pending
                    .as_ref()
                    .and_then(|p| p.get(&req.reference))
                    .or_else(|| self.state.series.get(&req.reference))
                {
                    response.series.push(Series {
                        reference: series.reference,
                        labels: series.labels.clone(),
                        ..Default::default()
                    });
                }
            }
            "append" => {
                let sample = req.sample.ok_or("missing sample")?;
                if self.pending.is_none() {
                    return Err("no transaction".into());
                }
                if sample.t < self.state.min_valid {
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
                    // Compaction does not close the OOO window. Cold duplicates still need validation.
                    let selected = BTreeSet::from([req.reference]);
                    for block in &self.state.cold {
                        if block.mint > sample.t
                            || block.maxt < sample.t
                            || !block.refs.contains(&req.reference)
                        {
                            continue;
                        }
                        for s in disk::read_cold(self, block, &selected)? {
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
                let commit = Commit {
                    max_exemplars: self.max_exemplars,
                    seq: self.state.seq + 1,
                    next_ref: self.state.next_ref,
                    series: pending,
                };
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
                self.state.seq = commit.seq;
                for (&r, s) in &commit.series {
                    if let Some((mint, maxt)) = s.bounds() {
                        self.head_mint = self.head_mint.min(mint);
                        self.head_maxt = self.head_maxt.max(maxt);
                    }
                    if !self.index.all.contains(&r) {
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
                        selected.clone()
                    } else {
                        block.refs.clone()
                    };
                    let cold = disk::read_cold(self, block, &wanted)?;
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
                if self.wal_dirty {
                    let result = self
                        .wal
                        .as_mut()
                        .ok_or("WAL closed")?
                        .flush()
                        .and_then(|_| self.wal.as_ref().unwrap().get_ref().sync_all());
                    if let Err(err) = result {
                        self.poison_wal();
                        return Err(io(err));
                    }
                    self.wal_dirty = false;
                }
            }
            "flush" | "evict" => {
                if self.pending.is_some() {
                    return Err("transaction active".into());
                }
                let mut next = self.state.clone();
                next.seq += 1;
                let mut cold = Vec::new();
                let selected: BTreeSet<_> = req.refs.iter().copied().collect();
                for s in next.series.values_mut() {
                    if req.op == "evict" && !selected.contains(&s.reference) {
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
                        if req.op == "evict" || *t <= req.up_to {
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
                if cold.is_empty() {
                    return Ok(response);
                }
                let file = format!("cold-{}.bin", next.seq);
                let (bytes, locations) = disk::cold_file(&cold);
                self.atomic_write(&file, &bytes)?;
                let block = Cold {
                    locations,
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
                    refs: cold.iter().map(|s| s.reference).collect(),
                    ranges: cold
                        .iter()
                        .filter_map(|s| s.bounds().map(|range| (s.reference, range)))
                        .collect(),
                };
                next.cold.push(block);
                if req.op == "flush" {
                    next.min_valid = next.min_valid.max(req.up_to.saturating_add(1));
                }
                if let Err(error) =
                    self.atomic_write("head.json", &serde_json::to_vec(&next).map_err(io)?)
                {
                    // A failed directory sync can leave a newer checkpoint on disk.
                    self.poisoned = true;
                    return Err(error);
                }
                for series in next.series.values() {
                    if !series.has_samples() {
                        self.head_index.remove(series);
                    }
                }
                self.state = next;
                self.head_mint = self
                    .state
                    .series
                    .values()
                    .filter_map(|s| s.bounds().map(|b| b.0))
                    .min()
                    .unwrap_or(i64::MAX);
                self.head_maxt = self
                    .state
                    .series
                    .values()
                    .filter_map(|s| s.bounds().map(|b| b.1))
                    .max()
                    .unwrap_or(i64::MIN);
                // Replay ignores records covered by the checkpoint sequence, even if truncation fails.
                self.reset_wal()?;
            }
            _ => return Err("unknown operation".into()),
        }
        Ok(response)
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
    error: *mut Buffer,
) -> *mut std::ffi::c_void {
    let result = std::panic::catch_unwind(|| {
        let path =
            std::str::from_utf8(unsafe { std::slice::from_raw_parts(path, len) }).map_err(io)?;
        Engine::open(Path::new(path))
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
    fn mixed_samples_survive_float_queue_compression_and_restore() {
        let mut series = Series::default();
        let mut expected = BTreeMap::new();
        for n in 0..600 {
            let sample = Sample {
                t: n * 2,
                kind: 2,
                bits: 0,
                data: "opaque histogram".into(),
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
