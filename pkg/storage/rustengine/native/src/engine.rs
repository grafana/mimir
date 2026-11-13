// SPDX-License-Identifier: AGPL-3.0-only

//! A tenant's engine: its series, the files their old chunks moved to, and the operations Go calls.

use crate::cold::ColdFile;
use crate::head::{
    self, Chunk, Exemplar, HIST_ENCODING, Head, Hist, KIND_FLOAT, NO_T, Reject, Series,
};
use crate::rw2;
use crate::symbols::Sym;
use crate::wire::{Reader, Result, Writer};
use crate::xor;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn io(e: impl std::fmt::Display) -> String {
    e.to_string()
}

pub struct Options {
    pub common: Vec<Box<str>>,
    pub offset: u32,
    pub block_range: i64,
}

pub struct State {
    pub head: Head,
    pub cold: Vec<Arc<ColdFile>>,
    /// The newest and oldest in-order samples in memory.
    pub head_mint: i64,
    pub head_maxt: i64,
    pub ooo_mint: i64,
    pub ooo_maxt: i64,
    /// Older in-order samples than this are out of bounds: their range was flushed.
    pub min_valid: i64,
    pub window: i64,
    pub max_exemplars: usize,
    /// Exemplars by age, of the series they belong to, to evict the oldest past the limit.
    pub exemplar_order: VecDeque<(u32, i64)>,
    pub exemplar_count: usize,
    /// Changes when series come or go, which caches of selections depend on.
    pub generation: u64,
    pub cold_seq: u64,
}

pub const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;
/// A sample of zero, unless the series already has one at that time.
pub const QUIET_ZERO_NAN: u64 = 0x7ff0_0000_0000_0003;

/// What a request is checked against, which Prometheus' appender fixes when it starts.
#[derive(Clone, Copy)]
pub struct Bounds {
    pub head_maxt: i64,
    pub min_valid: i64,
    pub window: i64,
}

/// Where a sample goes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    InOrder,
    OutOfOrder,
    /// Accepted, but nothing to store: the same as the newest sample, or a quiet zero where there is one.
    Drop,
}

/// What the series' newest in-order sample is: time, value bits, kind.
pub type Newest = (i64, u64, u8);

/// Checks a float against the series, as Prometheus' memSeries.appendable does.
#[inline]
pub fn appendable(
    b: &Bounds,
    (max_t, last_v, last_kind): Newest,
    t: i64,
    v: f64,
) -> std::result::Result<Verdict, Reject> {
    let bits = v.to_bits();
    if b.window == 0 && t < b.min_valid {
        return Err(Reject::OutOfBounds);
    }
    if t >= b.min_valid {
        if max_t == NO_T || t > max_t {
            return Ok(Verdict::InOrder);
        }
        if t == max_t {
            if last_kind != KIND_FLOAT && last_kind != 0 {
                return Err(Reject::DuplicateHistogramToFloat);
            }
            if last_v != bits && bits != QUIET_ZERO_NAN {
                return Err(Reject::Duplicate);
            }
            return Ok(Verdict::Drop);
        }
    }
    if bits == QUIET_ZERO_NAN {
        return Ok(Verdict::Drop);
    }
    // An empty head takes its time from the first sample.
    let head_maxt = if b.head_maxt == NO_T { t } else { b.head_maxt };
    if b.window > 0 && t >= head_maxt - b.window {
        return Ok(Verdict::OutOfOrder);
    }
    if b.window > 0 {
        return Err(Reject::TooOld);
    }
    if t < b.min_valid {
        return Err(Reject::OutOfBounds);
    }
    Err(Reject::OutOfOrder)
}

/// Checks a histogram against the series, as Prometheus' memSeries.appendableHistogram does; equal is whether it is
/// the series' newest sample, when it has the same time.
#[inline]
pub fn appendable_histogram(
    b: &Bounds,
    max_t: i64,
    t: i64,
    equal: impl FnOnce() -> bool,
) -> std::result::Result<Verdict, Reject> {
    if b.window == 0 && t < b.min_valid {
        return Err(Reject::OutOfBounds);
    }
    if t >= b.min_valid {
        if max_t == NO_T || t > max_t {
            return Ok(Verdict::InOrder);
        }
        if t == max_t {
            return if equal() {
                Ok(Verdict::Drop)
            } else {
                Err(Reject::Duplicate)
            };
        }
    }
    let head_maxt = if b.head_maxt == NO_T { t } else { b.head_maxt };
    if b.window > 0 && t >= head_maxt - b.window {
        return Ok(Verdict::OutOfOrder);
    }
    if b.window > 0 {
        return Err(Reject::TooOld);
    }
    if t < b.min_valid {
        return Err(Reject::OutOfBounds);
    }
    Err(Reject::OutOfOrder)
}

impl State {
    fn new() -> Self {
        Self {
            head: Head::default(),
            cold: Vec::new(),
            head_mint: NO_T,
            head_maxt: NO_T,
            ooo_mint: NO_T,
            ooo_maxt: NO_T,
            min_valid: i64::MIN,
            window: 0,
            max_exemplars: 0,
            exemplar_order: VecDeque::new(),
            exemplar_count: 0,
            generation: 0,
            cold_seq: 0,
        }
    }

    /// Prometheus' appendable minimum: a block can't get samples once it may be compacting.
    #[inline]
    pub fn appendable_min(&self, block_range: i64) -> i64 {
        let cw = if self.head_maxt == NO_T {
            i64::MIN
        } else {
            self.head_maxt - block_range / 2
        };
        self.min_valid.max(cw)
    }

    #[inline]
    fn note_in_order(&mut self, t: i64) {
        if self.head_maxt == NO_T {
            self.head_mint = t;
            self.head_maxt = t;
        } else {
            self.head_mint = self.head_mint.min(t);
            self.head_maxt = self.head_maxt.max(t);
        }
    }

    #[inline]
    fn note_ooo(&mut self, t: i64) {
        if self.ooo_maxt == NO_T {
            self.ooo_mint = t;
            self.ooo_maxt = t;
        } else {
            self.ooo_mint = self.ooo_mint.min(t);
            self.ooo_maxt = self.ooo_maxt.max(t);
        }
    }

    /// The bounds of a request, fixed when it starts, as Prometheus' appender fixes them.
    pub fn bounds(&self, block_range: i64) -> Bounds {
        Bounds {
            head_maxt: self.head_maxt,
            min_valid: self.appendable_min(block_range),
            window: self.window,
        }
    }

    /// Appends a float the series was checked to take.
    #[inline]
    pub fn apply(&mut self, slot: u32, t: i64, v: f64, ooo: bool, block_range: i64) {
        let v = if v.to_bits() == QUIET_ZERO_NAN {
            0.0
        } else {
            v
        };
        let s = self.head.get_mut(slot).unwrap();
        if ooo {
            s.insert_ooo(t, v);
            self.note_ooo(t);
        } else {
            s.append_in_order(t, v, block_range);
            self.note_in_order(t);
        }
    }

    fn add_exemplar(&mut self, slot: u32, e: Exemplar) {
        let t = e.t;
        let s = self.head.get_mut(slot).unwrap();
        let extra = s.extra_mut();
        extra.exemplars.push(e);
        self.exemplar_order.push_back((slot, t));
        self.exemplar_count += 1;
        while self.exemplar_count > self.max_exemplars {
            let Some((victim, _)) = self.exemplar_order.pop_front() else {
                break;
            };
            if let Some(extra) = self.head.get_mut(victim).and_then(|s| s.extra.as_mut()) {
                if !extra.exemplars.is_empty() {
                    extra.exemplars.remove(0);
                    self.exemplar_count -= 1;
                }
            }
        }
    }

    pub fn recompute_bounds_pub(&mut self) {
        self.recompute_bounds()
    }

    fn recompute_bounds(&mut self) {
        let (mut mint, mut maxt, mut omint, mut omaxt) = (i64::MAX, i64::MIN, i64::MAX, i64::MIN);
        for (_, s) in self.head.iter() {
            for c in &s.sealed {
                mint = mint.min(c.mint);
                maxt = maxt.max(c.maxt);
            }
            if let Some(h) = &s.head {
                if let (Some(a), Some(b)) = (h.first_timestamp(), h.last_timestamp()) {
                    mint = mint.min(a);
                    maxt = maxt.max(b);
                }
            }
            for h in s.hist() {
                mint = mint.min(h.t);
                maxt = maxt.max(h.t);
            }
            if let (Some(a), Some(b)) = (s.ooo().first(), s.ooo().last()) {
                omint = omint.min(a.0);
                omaxt = omaxt.max(b.0);
            }
        }
        if maxt == i64::MIN {
            // Prometheus keeps the head's max time once it has had samples: appends stay bounded by it.
            self.head_mint = NO_T;
        } else {
            self.head_mint = mint;
            self.head_maxt = self.head_maxt.max(maxt);
        }
        (self.ooo_mint, self.ooo_maxt) = if omaxt == i64::MIN {
            (NO_T, NO_T)
        } else {
            (omint, omaxt)
        };
    }
}

/// Series of the transaction Go builds for what the record path left: new series, histograms, exemplars.
#[derive(Default)]
struct Txn {
    /// The request's bounds, from when its first sample was checked.
    bounds: Option<Bounds>,
    /// Floats and histograms in the order they were appended.
    samples: Vec<(u32, i64, u8, f64, Option<Box<[u8]>>)>,
    exemplars: Vec<(u32, Exemplar)>,
    created: Vec<u32>,
}

pub struct Engine {
    pub dir: PathBuf,
    pub opts: Options,
    pub state: RwLock<State>,
    records: Mutex<FxHashMap<u32, rw2::Record>>,
    /// Records released, to reuse their buffers.
    spare: Mutex<Vec<rw2::Record>>,
    next_record: AtomicU64,
    txn: Mutex<Option<Txn>>,
    pub cursors: Mutex<FxHashMap<u64, Arc<crate::query::Cursor>>>,
    pub next_cursor: AtomicU64,
    pub restored_clean: bool,
    _lock: File,
    closed: std::sync::atomic::AtomicBool,
}

const SNAPSHOT: &str = "head.snap";
const CLEAN: &str = "clean";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Results of ingesting a record, written to Go.
pub struct IngestResult {
    pub accepted: u64,
    /// The series that got samples, with their refs, when Go tracks the active series.
    pub ingested: Vec<(u32, u64)>,
    pub leftovers: Vec<u32>,
    pub skipped: Vec<u32>,
    pub errors: Vec<(u32, i64, u8)>,
}

impl Engine {
    pub fn open(dir: &Path, opts: Options) -> Result<Engine> {
        fs::create_dir_all(dir).map_err(io)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))
            .map_err(io)?;
        lock.try_lock().map_err(io)?;
        let clean = dir.join(CLEAN).exists();
        let mut state = State::new();
        // A new directory has nothing to replay either.
        let mut restored_clean = fs::read_dir(dir)
            .map_err(io)?
            .filter_map(|e| e.ok())
            .all(|e| e.file_name() == "lock");
        if clean {
            crate::snapshot::read(&dir.join(SNAPSHOT), &mut state)?;
            for entry in fs::read_dir(dir).map_err(io)? {
                let path = entry.map_err(io)?.path();
                if path.extension().is_some_and(|e| e == "cold") {
                    state.cold.push(Arc::new(ColdFile::open(&path)?));
                }
            }
            state.cold.sort_by_key(|c| (c.mint, c.created_ms));
            state.cold_seq = state.cold.len() as u64 + 1;
            restored_clean = true;
        } else {
            // Without a clean head, Kafka replays the tenant from its retention: what is on disk would duplicate it.
            for entry in fs::read_dir(dir).map_err(io)? {
                let path = entry.map_err(io)?.path();
                if path.file_name().is_some_and(|n| n != "lock") {
                    fs::remove_file(&path).map_err(io)?;
                }
            }
        }
        // Opening consumes the marker: a crash from now on must replay.
        if clean {
            fs::remove_file(dir.join(CLEAN)).map_err(io)?;
            File::open(dir).and_then(|d| d.sync_all()).map_err(io)?;
        }
        Ok(Engine {
            dir: dir.into(),
            opts,
            state: RwLock::new(state),
            records: Mutex::new(FxHashMap::default()),
            spare: Mutex::new(Vec::new()),
            next_record: AtomicU64::new(1),
            txn: Mutex::new(None),
            cursors: Mutex::new(FxHashMap::default()),
            next_cursor: AtomicU64::new(1),
            restored_clean,
            _lock: lock,
            closed: Default::default(),
        })
    }

    pub fn close_checkpoint(&self) -> Result<()> {
        let st = self.state.read();
        crate::snapshot::write(&self.dir.join(SNAPSHOT), &st)?;
        fs::write(self.dir.join(CLEAN), b"1").map_err(io)?;
        File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(io)?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err("engine is closed".into());
        }
        Ok(())
    }

    // ---- records ----

    pub fn parse(&self, content: &[u8], w: &mut Writer) -> Result<()> {
        let mut rec = self.spare.lock().pop().unwrap_or_default();
        if let Err(e) = rec.read(content, self.opts.common.len() as u32, self.opts.offset) {
            self.spare.lock().push(rec);
            return Err(e);
        }
        let handle = self.next_record.fetch_add(1, Ordering::Relaxed) as u32;
        w.u32(handle);
        w.u32(rec.series.len() as u32);
        w.u8(rec.source);
        w.bool(!rec.partial);
        w.i64(rec.min_ts);
        let at = w.placeholder();
        let mut n = 0;
        for m in &rec.metadata {
            let s = &rec.series[m.series as usize];
            // The metric family is the series' name.
            let mut name: &[u8] = b"";
            for p in rec.series_refs(s).chunks_exact(2) {
                if rec.symbol(p[0], content, &self.opts.common, self.opts.offset) == b"__name__" {
                    name = rec.symbol(p[1], content, &self.opts.common, self.opts.offset);
                }
            }
            let help = rec.symbol(m.help, content, &self.opts.common, self.opts.offset);
            let unit = rec.symbol(m.unit, content, &self.opts.common, self.opts.offset);
            if name.is_empty() || help.is_empty() && unit.is_empty() && m.kind == 0 {
                continue;
            }
            w.bytes(name);
            w.u8(m.kind);
            w.bytes(help);
            w.bytes(unit);
            n += 1;
        }
        w.patch_u32(at, n);
        self.records.lock().insert(handle, rec);
        Ok(())
    }

    pub fn release(&self, handle: u32) {
        if let Some(rec) = self.records.lock().remove(&handle) {
            let mut spare = self.spare.lock();
            if spare.len() < 16 && rec.refs.capacity() < 1 << 22 {
                spare.push(rec);
            }
        }
    }

    fn with_record<T>(
        &self,
        handle: u32,
        content: &[u8],
        f: impl FnOnce(&rw2::Record) -> Result<T>,
    ) -> Result<T> {
        let records = self.records.lock();
        let rec = records.get(&handle).ok_or("unknown record")?;
        if rec.content_len != content.len() {
            return Err("record content changed".into());
        }
        f(rec)
    }

    pub fn series_of(&self, handle: u32, index: u32, content: &[u8], w: &mut Writer) -> Result<()> {
        self.with_record(handle, content, |rec| {
            let s = rec
                .series
                .get(index as usize)
                .ok_or("series out of range")?;
            let refs = rec.series_refs(s);
            w.u32(refs.len() as u32 / 2);
            for p in refs.chunks_exact(2) {
                w.bytes(rec.symbol(p[0], content, &self.opts.common, self.opts.offset));
                w.bytes(rec.symbol(p[1], content, &self.opts.common, self.opts.offset));
            }
            let samples = rec.series_samples(s);
            w.u32(samples.len() as u32);
            for &(t, v) in samples {
                w.i64(t);
                w.u64(v.to_bits());
            }
            Ok(())
        })
    }

    pub fn subset(
        &self,
        handle: u32,
        content: &[u8],
        indices: &[u32],
        w: &mut Writer,
    ) -> Result<()> {
        self.with_record(handle, content, |rec| {
            rec.subset(content, indices, &mut w.buf);
            Ok(())
        })
    }

    /// Ingests the floats of the record's series the engine has in its head. The others are left to Go.
    #[allow(clippy::too_many_arguments)]
    pub fn ingest(
        &self,
        handle: u32,
        content: &[u8],
        min_ts: i64,
        max_ts: i64,
        ingested_ms: i64,
        skip_before: Option<i64>,
        track: bool,
    ) -> Result<IngestResult> {
        let records = self.records.lock();
        let rec = records.get(&handle).ok_or("unknown record")?;
        if rec.content_len != content.len() {
            return Err("record content changed".into());
        }
        let common = &self.opts.common;
        let offset = self.opts.offset;
        let block_range = self.opts.block_range;
        let mut out = IngestResult {
            accepted: 0,
            ingested: Vec::new(),
            leftovers: Vec::new(),
            skipped: Vec::new(),
            errors: Vec::new(),
        };
        const UNSEEN: u32 = u32::MAX;
        const ABSENT: u32 = u32::MAX - 1;
        let mut syms = vec![UNSEEN; offset as usize + rec.symbols.len()];
        let mut pairs: Vec<Sym> = Vec::with_capacity(32);
        let mut st = self.state.write();
        let bounds = st.bounds(block_range);
        // The Go appender's samples of the same request are checked against the same bounds.
        *self.txn.lock() = Some(Txn {
            bounds: Some(bounds),
            ..Default::default()
        });
        for (i, s) in rec.series.iter().enumerate() {
            let i = i as u32;
            let samples = rec.series_samples(s);
            if s.other {
                out.leftovers.push(i);
                continue;
            }
            if samples.is_empty() {
                // Nothing to append, as Go's engine: neither ingested nor left over.
                continue;
            }
            if let Some(before) = skip_before {
                if samples.iter().all(|&(t, _)| t < before) {
                    out.skipped.push(i);
                    continue;
                }
            }
            pairs.clear();
            let mut absent = false;
            for &r in rec.series_refs(s) {
                let mut sym = syms[r as usize];
                if sym == UNSEEN {
                    sym = st
                        .head
                        .symbols
                        .lookup(rec.symbol(r, content, common, offset))
                        .unwrap_or(ABSENT);
                    syms[r as usize] = sym;
                }
                if sym == ABSENT {
                    absent = true;
                    break;
                }
                pairs.push(sym);
            }
            let slot = if absent { None } else { st.head.find(&pairs) };
            let Some(slot) = slot.filter(|&slot| st.head.get(slot).is_some_and(|s| s.in_head()))
            else {
                out.leftovers.push(i);
                continue;
            };
            let series = st.head.get(slot).unwrap();
            let pre: Newest = (series.max_t, series.last_v, series.last_kind);
            // A staleness marker of a histogram series is a histogram, which Go makes.
            if pre.2 != KIND_FLOAT
                && pre.2 != 0
                && samples.iter().any(|s| s.1.to_bits() == STALE_NAN)
            {
                out.leftovers.push(i);
                continue;
            }
            let before = out.accepted;
            for &(t, v) in samples {
                if t < min_ts {
                    out.errors.push((i, t, Reject::TooFarPast as u8));
                    continue;
                }
                if t > max_ts {
                    out.errors.push((i, t, Reject::TooFarFuture as u8));
                    continue;
                }
                // Checked against the series as it was before the request, then, as Prometheus' commit does, against
                // what the request appended: what conflicts with that is dropped.
                match appendable(&bounds, pre, t, v) {
                    Ok(_) => {
                        out.accepted += 1;
                        let s = st.head.get(slot).unwrap();
                        match appendable(&bounds, (s.max_t, s.last_v, s.last_kind), t, v) {
                            Ok(Verdict::InOrder) => st.apply(slot, t, v, false, block_range),
                            Ok(Verdict::OutOfOrder) => st.apply(slot, t, v, true, block_range),
                            Ok(Verdict::Drop) | Err(_) => {}
                        }
                    }
                    Err(e) => out.errors.push((i, t, e as u8)),
                }
            }
            st.head.get_mut(slot).unwrap().active_ms = ingested_ms;
            if track && out.accepted > before {
                out.ingested.push((i, head::ref_of(slot)));
            }
        }
        if out.leftovers.is_empty() && out.skipped.is_empty() {
            // Nothing for Go to append in this request.
            self.txn.lock().take();
        }
        Ok(out)
    }

    // ---- transactions, for what records leave to Go ----

    pub fn lookup(&self, pairs: &[(&str, &str)]) -> (u64, bool) {
        let st = self.state.read();
        let mut syms = Vec::with_capacity(pairs.len() * 2);
        for (n, v) in pairs {
            match (
                st.head.symbols.lookup(n.as_bytes()),
                st.head.symbols.lookup(v.as_bytes()),
            ) {
                (Some(a), Some(b)) => {
                    syms.push(a);
                    syms.push(b);
                }
                _ => return (0, false),
            }
        }
        match st.head.find(&syms) {
            Some(slot) => (head::ref_of(slot), st.head.get(slot).unwrap().in_head()),
            None => (0, false),
        }
    }

    /// Creates the series, or brings back into the head one that left it, within the transaction.
    pub fn create(&self, pairs: &[(&str, &str)], trackers: u64, otlp: bool) -> Result<u64> {
        let mut st = self.state.write();
        let syms = st.head.intern(pairs);
        let slot = match st.head.find(&syms) {
            Some(slot) => slot,
            None => {
                st.generation += 1;
                st.head.create(syms)
            }
        };
        let revived = !st.head.get(slot).unwrap().in_head();
        st.head.set_in_head(slot, true);
        let s = st.head.get_mut(slot).unwrap();
        s.trackers = trackers;
        s.otlp = otlp;
        drop(st);
        if revived {
            self.txn
                .lock()
                .get_or_insert_with(Default::default)
                .created
                .push(slot);
        }
        Ok(head::ref_of(slot))
    }

    /// Checks and stages a sample of the transaction, against the series as it was before the request.
    pub fn append(
        &self,
        r: u64,
        t: i64,
        kind: u8,
        v: f64,
        data: Option<Box<[u8]>>,
        reject_ooo: bool,
    ) -> std::result::Result<(), Reject> {
        let slot = head::slot_of(r).ok_or(Reject::NotFound)?;
        let st = self.state.read();
        let s = st.head.get(slot).ok_or(Reject::NotFound)?;
        let mut txn = self.txn.lock();
        let txn = txn.get_or_insert_with(Default::default);
        let bounds = *txn
            .bounds
            .get_or_insert_with(|| st.bounds(self.opts.block_range));
        let verdict = if kind == KIND_FLOAT {
            appendable(&bounds, (s.max_t, s.last_v, s.last_kind), t, v)?
        } else {
            appendable_histogram(&bounds, s.max_t, t, || {
                s.hist()
                    .iter()
                    .rev()
                    .find(|h| h.t == t)
                    .is_some_and(|h| Some(&h.data) == data.as_ref())
            })?
        };
        if verdict == Verdict::OutOfOrder && reject_ooo {
            return Err(Reject::OutOfOrder);
        }
        if verdict != Verdict::Drop {
            txn.samples.push((slot, t, kind, v, data));
        }
        Ok(())
    }

    pub fn append_exemplar(&self, r: u64, e: Exemplar) -> std::result::Result<(), Reject> {
        let slot = head::slot_of(r).ok_or(Reject::NotFound)?;
        let st = self.state.read();
        if st.max_exemplars == 0 {
            return Err(Reject::ExemplarsDisabled);
        }
        let s = st.head.get(slot).ok_or(Reject::NotFound)?;
        let mut txn = self.txn.lock();
        let txn = txn.get_or_insert_with(Default::default);
        let newest = txn
            .exemplars
            .iter()
            .rev()
            .find(|(s, _)| *s == slot)
            .map(|(_, e)| e.t)
            .or_else(|| {
                s.extra
                    .as_ref()
                    .and_then(|x| x.exemplars.last())
                    .map(|e| e.t)
            });
        if let Some(newest) = newest {
            if e.t < newest {
                return Err(Reject::ExemplarOutOfOrder);
            }
            // A duplicate of the newest is ignored.
            if e.t == newest {
                return Ok(());
            }
        }
        txn.exemplars.push((slot, e));
        Ok(())
    }

    pub fn commit(&self, ingested_ms: i64) -> u64 {
        let Some(txn) = self.txn.lock().take() else {
            return 0;
        };
        let mut st = self.state.write();
        let block_range = self.opts.block_range;
        let bounds = txn.bounds.unwrap_or_else(|| st.bounds(block_range));
        let mut applied = 0;
        for (slot, t, kind, v, data) in txn.samples {
            let Some(s) = st.head.get(slot) else { continue };
            // Checked again, as Prometheus' commit does: what conflicts with the request's own samples is dropped.
            if kind == KIND_FLOAT {
                match appendable(&bounds, (s.max_t, s.last_v, s.last_kind), t, v) {
                    Ok(Verdict::InOrder) => st.apply(slot, t, v, false, block_range),
                    Ok(Verdict::OutOfOrder) => st.apply(slot, t, v, true, block_range),
                    Ok(Verdict::Drop) | Err(_) => continue,
                }
            } else {
                let data = data.unwrap_or_default();
                match appendable_histogram(&bounds, s.max_t, t, || {
                    s.hist()
                        .iter()
                        .rev()
                        .find(|h| h.t == t)
                        .is_some_and(|h| h.data == data)
                }) {
                    Ok(Verdict::InOrder) | Ok(Verdict::OutOfOrder) => {}
                    Ok(Verdict::Drop) | Err(_) => continue,
                }
                let s = st.head.get_mut(slot).unwrap();
                s.seal();
                s.insert_hist(Hist { t, kind, data });
                st.note_in_order(t);
            }
            st.head.get_mut(slot).unwrap().active_ms = ingested_ms;
            applied += 1;
        }
        for (slot, e) in txn.exemplars {
            st.add_exemplar(slot, e);
        }
        applied
    }

    /// Drops the transaction, and the series it created that got nothing.
    pub fn rollback(&self) -> Vec<(u64, Vec<(String, String)>)> {
        let Some(txn) = self.txn.lock().take() else {
            return Vec::new();
        };
        let mut st = self.state.write();
        let mut out = Vec::new();
        let mut gone = Vec::new();
        for slot in txn.created {
            let Some(s) = st.head.get(slot) else { continue };
            if s.has_mem() {
                continue;
            }
            st.head.set_in_head(slot, false);
            let labels = st
                .head
                .label_pairs(st.head.get(slot).unwrap())
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
                .collect();
            out.push((head::ref_of(slot), labels));
            if st.head.get(slot).unwrap().cold_maxt == NO_T {
                gone.push(slot);
            }
        }
        if !gone.is_empty() {
            st.head.remove(&gone);
            st.generation += 1;
        }
        out
    }

    pub fn last(&self, r: u64) -> Option<(i64, u8)> {
        let st = self.state.read();
        let s = st.head.by_ref(r)?;
        (s.max_t != NO_T).then_some((s.max_t, s.last_kind))
    }

    // ---- leaving memory ----

    /// Moves what is at or before up_to out of memory, of all series or of these, into cold files. Returns the series
    /// that left the head: they have nothing in memory any more.
    pub fn flush(
        &self,
        up_to: i64,
        only: Option<&[u32]>,
    ) -> Result<Vec<(u64, Vec<(String, String)>)>> {
        let mut st = self.state.write();
        let block_range = self.opts.block_range;
        let mut by_block: std::collections::BTreeMap<i64, Vec<(u32, Vec<Chunk>)>> =
            Default::default();
        let candidates: Vec<u32> = match only {
            Some(slots) => slots.to_vec(),
            None => st
                .head
                .iter()
                .filter(|(_, s)| s.has_mem() || s.in_head())
                .map(|(i, _)| i)
                .collect(),
        };
        let mut left = Vec::new();
        for slot in candidates {
            let Some(s) = st.head.get_mut(slot) else {
                continue;
            };
            if !s.has_mem() && !s.in_head() {
                continue;
            }
            let moved = take_until(s, up_to, block_range);
            for (block, chunks) in moved {
                by_block.entry(block).or_default().push((slot, chunks));
            }
            if !s.has_mem() && s.in_head() {
                left.push(slot);
            }
        }
        for &slot in &left {
            st.head.set_in_head(slot, false);
        }
        self.write_cold(&mut st, by_block)?;
        if only.is_none() && up_to != i64::MAX {
            st.min_valid = st.min_valid.max(up_to + 1);
        }
        st.recompute_bounds();
        if !left.is_empty() {
            st.generation += 1;
        }
        Ok(left
            .into_iter()
            .map(|slot| {
                let s = st.head.get(slot).unwrap();
                (
                    head::ref_of(slot),
                    st.head
                        .label_pairs(s)
                        .map(|(a, b)| (a.to_owned(), b.to_owned()))
                        .collect(),
                )
            })
            .collect())
    }

    /// Drops the files older than the cutoff, and the series left with nothing.
    pub fn retain(&self, cutoff: i64) -> Result<()> {
        let mut st = self.state.write();
        let (old, keep): (Vec<_>, Vec<_>) = st.cold.drain(..).partition(|c| c.maxt < cutoff);
        st.cold = keep;
        for c in &old {
            fs::remove_file(&c.path).map_err(io)?;
        }
        if old.is_empty() {
            return Ok(());
        }
        let oldest = st.cold.iter().map(|c| c.mint).min().unwrap_or(i64::MAX);
        let mut gone = Vec::new();
        for (slot, s) in st.head.iter() {
            if s.cold_maxt != NO_T
                && s.cold_maxt < oldest.min(cutoff)
                && !s.has_mem()
                && !s.in_head()
            {
                gone.push(slot);
            } else if s.cold_mint != NO_T && s.cold_mint < oldest {
                // Partly retained: what is left starts later.
            }
        }
        for (_, s) in st
            .head
            .slots
            .iter_mut()
            .enumerate()
            .filter_map(|(i, s)| Some((i, s.as_deref_mut()?)))
        {
            if s.cold_mint != NO_T && s.cold_mint < oldest {
                if s.cold_maxt < oldest {
                    s.cold_mint = NO_T;
                    s.cold_maxt = NO_T;
                } else {
                    s.cold_mint = oldest;
                }
            }
        }
        if !gone.is_empty() {
            st.head.remove(&gone);
        }
        st.generation += 1;
        Ok(())
    }

    /// Moves every out-of-order float to cold files, where queries merge them with the in-order chunks.
    pub fn compact_ooo(&self) -> Result<()> {
        let mut st = self.state.write();
        let block_range = self.opts.block_range;
        let mut by_block: std::collections::BTreeMap<i64, Vec<(u32, Vec<Chunk>)>> =
            Default::default();
        let slots: Vec<u32> = st
            .head
            .iter()
            .filter(|(_, s)| !s.ooo().is_empty())
            .map(|(i, _)| i)
            .collect();
        for &slot in &slots {
            let s = st.head.get_mut(slot).unwrap();
            let ooo = std::mem::take(&mut s.extra.as_mut().unwrap().ooo);
            reset_mem_span(s);
            let mut groups: std::collections::BTreeMap<i64, Vec<(i64, f64)>> = Default::default();
            for x in ooo {
                groups
                    .entry(x.0.div_euclid(block_range))
                    .or_default()
                    .push(x);
            }
            for (block, samples) in groups {
                let chunks = samples
                    .chunks(head::CHUNK_SAMPLES as usize)
                    .map(encode_chunk)
                    .collect();
                by_block
                    .entry(block * block_range)
                    .or_default()
                    .push((slot, chunks));
            }
        }
        self.write_cold(&mut st, by_block)?;
        st.recompute_bounds();
        Ok(())
    }

    fn write_cold(
        &self,
        st: &mut State,
        by_block: std::collections::BTreeMap<i64, Vec<(u32, Vec<Chunk>)>>,
    ) -> Result<()> {
        let created = now_ms();
        for (block, mut series) in by_block {
            series.sort_unstable_by_key(|(slot, _)| *slot);
            st.cold_seq += 1;
            let name = format!("{block:020}-{:08}.cold", st.cold_seq);
            if let Some(file) = ColdFile::write(&self.dir, &name, created, &series)? {
                for (slot, chunks) in &series {
                    let s = st.head.get_mut(*slot).unwrap();
                    let (a, b) = (
                        chunks.iter().map(|c| c.mint).min().unwrap(),
                        chunks.iter().map(|c| c.maxt).max().unwrap(),
                    );
                    s.cold_mint = if s.cold_mint == NO_T {
                        a
                    } else {
                        s.cold_mint.min(a)
                    };
                    s.cold_maxt = if s.cold_maxt == NO_T {
                        b
                    } else {
                        s.cold_maxt.max(b)
                    };
                }
                st.cold.push(Arc::new(file));
            }
        }
        st.cold.sort_by_key(|c| (c.mint, c.created_ms));
        Ok(())
    }

    pub fn evict_idle(&self, age: i64) -> Result<Vec<(u64, Vec<(String, String)>)>> {
        let idle: Vec<u32> = {
            let st = self.state.read();
            if st.head_maxt == NO_T {
                return Ok(Vec::new());
            }
            let cutoff = st.head_maxt - age;
            st.head
                .iter()
                .filter(|(_, s)| {
                    s.in_head() && s.ooo().is_empty() && (!s.has_mem() || s.mem_maxt < cutoff)
                })
                .map(|(i, _)| i)
                .collect()
        };
        if idle.is_empty() {
            return Ok(Vec::new());
        }
        let left = self.flush(i64::MAX, Some(&idle))?;
        let mut st = self.state.write();
        for (r, _) in &left {
            if let Some(s) = head::slot_of(*r).and_then(|slot| st.head.get_mut(slot)) {
                s.idle = true;
            }
        }
        Ok(left)
    }
}

/// Takes what the series has in memory at or before up_to, by block, merged in order.
fn take_until(s: &mut Series, up_to: i64, block_range: i64) -> Vec<(i64, Vec<Chunk>)> {
    if !s.has_mem() || s.mem_mint > up_to {
        return Vec::new();
    }
    // The open chunk moves too when all of it is old enough.
    if s.head
        .as_ref()
        .and_then(|h| h.last_timestamp())
        .is_some_and(|t| t <= up_to)
    {
        s.seal();
    }
    let mut out: Vec<Chunk> = Vec::new();
    let mut keep = Vec::new();
    for c in s.sealed.drain(..) {
        if c.maxt <= up_to {
            out.push(c);
        } else if c.mint <= up_to {
            // Straddles: split it.
            let samples = xor::decode(&c.data);
            let (old, new): (Vec<_>, Vec<_>) = samples.into_iter().partition(|(t, _)| *t <= up_to);
            out.push(encode_chunk(&old));
            keep.push(encode_chunk(&new));
        } else {
            keep.push(c);
        }
    }
    s.sealed = keep;
    if let Some(h) = &s.head {
        if h.first_timestamp().is_some_and(|t| t <= up_to) {
            let samples = xor::decode(h.bytes());
            let (old, new): (Vec<_>, Vec<_>) = samples.into_iter().partition(|(t, _)| *t <= up_to);
            out.push(encode_chunk(&old));
            let mut app = xor::Appender::default();
            for (t, v) in new {
                app.append(t, v);
            }
            s.head = (!app.is_empty()).then(|| Box::new(app));
        }
    }
    let mut ooo = Vec::new();
    let mut hist = Vec::new();
    if let Some(extra) = s.extra.as_mut() {
        let at = extra.ooo.partition_point(|x| x.0 <= up_to);
        ooo = extra.ooo.drain(..at).collect();
        let at = extra.hist.partition_point(|x| x.t <= up_to);
        hist = extra.hist.drain(..at).collect();
        if extra.ooo.is_empty() && extra.hist.is_empty() && extra.exemplars.is_empty() {
            s.extra = None;
        }
    }
    reset_mem_span(s);
    if s.head.is_none() && s.sealed.is_empty() && s.hist().is_empty() {
        // Nothing in order in memory: the next sample starts a chunk, whatever its time past min_valid.
        s.max_t = NO_T;
    }
    // By block, merged with the out-of-order floats when there are any.
    let mut by_block: std::collections::BTreeMap<i64, Vec<Chunk>> = Default::default();
    for c in out {
        by_block
            .entry(c.mint.div_euclid(block_range))
            .or_default()
            .push(c);
    }
    let mut ooo_by_block: std::collections::BTreeMap<i64, Vec<(i64, f64)>> = Default::default();
    for (t, v) in ooo {
        ooo_by_block
            .entry(t.div_euclid(block_range))
            .or_default()
            .push((t, v));
    }
    for (block, samples) in ooo_by_block {
        let chunks = by_block.entry(block).or_default();
        let mut all: Vec<(i64, f64)> = chunks.iter().flat_map(|c| xor::decode(&c.data)).collect();
        all.extend(samples);
        all.sort_by_key(|x| x.0);
        all.dedup_by_key(|x| x.0);
        *chunks = all
            .chunks(head::CHUNK_SAMPLES as usize)
            .map(encode_chunk)
            .collect();
    }
    let mut hist_by_block: std::collections::BTreeMap<i64, Vec<Hist>> = Default::default();
    for h in hist {
        hist_by_block
            .entry(h.t.div_euclid(block_range))
            .or_default()
            .push(h);
    }
    for (block, hs) in hist_by_block {
        by_block.entry(block).or_default().push(encode_hist(&hs));
    }
    by_block
        .into_iter()
        .map(|(b, mut cs)| {
            cs.sort_by_key(|c| c.mint);
            (b * block_range, cs)
        })
        .collect()
}

/// Sets the span of what the series has in memory from what it has.
fn reset_mem_span(s: &mut Series) {
    let (mut mint, mut maxt) = (i64::MAX, i64::MIN);
    for c in &s.sealed {
        mint = mint.min(c.mint);
        maxt = maxt.max(c.maxt);
    }
    if let Some(h) = &s.head {
        if let (Some(a), Some(b)) = (h.first_timestamp(), h.last_timestamp()) {
            mint = mint.min(a);
            maxt = maxt.max(b);
        }
    }
    for &(t, _) in s.ooo() {
        mint = mint.min(t);
        maxt = maxt.max(t);
    }
    for h in s.hist() {
        mint = mint.min(h.t);
        maxt = maxt.max(h.t);
    }
    (s.mem_mint, s.mem_maxt) = if maxt == i64::MIN {
        (NO_T, NO_T)
    } else {
        (mint, maxt)
    };
}

pub fn encode_chunk(samples: &[(i64, f64)]) -> Chunk {
    let mut bytes = xor::encode(samples);
    bytes.shrink_to_fit();
    Chunk {
        mint: samples.first().map_or(0, |s| s.0),
        maxt: samples.last().map_or(0, |s| s.0),
        encoding: 1,
        data: bytes.into(),
    }
}

pub fn encode_hist(hs: &[Hist]) -> Chunk {
    let mut w = Writer::default();
    w.u32(hs.len() as u32);
    for h in hs {
        w.i64(h.t);
        w.u8(h.kind);
        w.bytes(&h.data);
    }
    Chunk {
        mint: hs.first().map_or(0, |h| h.t),
        maxt: hs.last().map_or(0, |h| h.t),
        encoding: HIST_ENCODING,
        data: w.buf.into(),
    }
}

pub fn decode_hist(data: &[u8]) -> Result<Vec<Hist>> {
    let mut r = Reader::new(data);
    let n = r.count(13)?;
    (0..n)
        .map(|_| {
            Ok(Hist {
                t: r.i64()?,
                kind: r.u8()?,
                data: r.bytes()?.into(),
            })
        })
        .collect()
}
