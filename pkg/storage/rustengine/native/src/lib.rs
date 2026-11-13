// SPDX-License-Identifier: AGPL-3.0-only

//! A tenant storage engine for the Go ingester, called through C: each call takes an operation and a frame, and
//! returns a frame Go frees. Engines synchronize themselves, so Go can query while it ingests.

pub mod cold;
pub mod engine;
pub mod head;
pub mod query;
pub mod rw2;
pub mod snapshot;
pub mod symbols;
pub mod wire;
pub mod xor;

#[cfg(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use engine::{Engine, Options};
use head::Exemplar;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use wire::{Reader, Result, Writer};

#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

fn buffer(mut v: Vec<u8>) -> Buffer {
    let b = Buffer {
        data: v.as_mut_ptr(),
        len: v.len(),
        cap: v.capacity(),
    };
    std::mem::forget(v);
    b
}

/// # Safety
/// The buffer must come from this library and be freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_free(b: Buffer) {
    if !b.data.is_null() {
        drop(unsafe { Vec::from_raw_parts(b.data, b.len, b.cap) });
    }
}

unsafe fn slice<'a>(p: *const u8, n: usize) -> &'a [u8] {
    if p.is_null() || n == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(p, n) }
    }
}

/// The engine, or null with the error in `err`.
///
/// # Safety
/// `opts` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_open(
    opts: *const u8,
    len: usize,
    err: *mut Buffer,
) -> *const Engine {
    let opts = unsafe { slice(opts, len) };
    let result = catch_unwind(|| -> Result<Engine> {
        let mut r = Reader::new(opts);
        let dir = r.str()?.to_owned();
        let block_range = r.i64()?;
        let offset = r.u32()?;
        let n = r.count(4)?;
        let mut common = Vec::with_capacity(n);
        for _ in 0..n {
            common.push(r.str()?.into());
        }
        Engine::open(
            Path::new(&dir),
            Options {
                common,
                offset,
                block_range,
            },
        )
    });
    match result {
        Ok(Ok(e)) => Arc::into_raw(Arc::new(e)),
        Ok(Err(e)) => {
            unsafe { *err = buffer(e.into_bytes()) };
            std::ptr::null()
        }
        Err(_) => {
            unsafe { *err = buffer(b"Rust engine panicked while opening".to_vec()) };
            std::ptr::null()
        }
    }
}

/// # Safety
/// The engine must come from `mimir_open`, and not be used after.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_close(e: *const Engine) {
    if !e.is_null() {
        drop(unsafe { Arc::from_raw(e) });
    }
}

/// Runs the operation. The response starts with 0 and the operation's frame, or 1 and an error.
///
/// # Safety
/// The engine must be open; `input` and `data` must point to readable bytes, borrowed for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mimir_call(
    e: *const Engine,
    op: u32,
    input: *const u8,
    len: usize,
    data: *const u8,
    data_len: usize,
) -> Buffer {
    let engine = unsafe { &*e };
    let input = unsafe { slice(input, len) };
    let data = unsafe { slice(data, data_len) };
    let mut w = Writer::with_capacity(64);
    w.u8(0);
    let result = catch_unwind(AssertUnwindSafe(|| {
        engine.check_open()?;
        call(engine, op, &mut Reader::new(input), data, &mut w)
    }));
    match result {
        Ok(Ok(())) => buffer(w.buf),
        Ok(Err(msg)) => {
            let mut v = vec![1];
            v.extend_from_slice(msg.as_bytes());
            buffer(v)
        }
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            let mut v = vec![1];
            v.extend_from_slice(format!("Rust engine panicked: {msg}").as_bytes());
            buffer(v)
        }
    }
}

pub const OP_PARSE: u32 = 1;
pub const OP_INGEST: u32 = 2;
pub const OP_SERIES_OF: u32 = 3;
pub const OP_SUBSET: u32 = 4;
pub const OP_RELEASE: u32 = 5;
pub const OP_LOOKUP: u32 = 10;
pub const OP_CREATE: u32 = 11;
pub const OP_APPEND: u32 = 12;
pub const OP_EXEMPLAR: u32 = 13;
pub const OP_COMMIT: u32 = 14;
pub const OP_ROLLBACK: u32 = 15;
pub const OP_LAST: u32 = 16;
pub const OP_STATS: u32 = 20;
pub const OP_SELECT: u32 = 21;
pub const OP_CURSOR_SERIES: u32 = 22;
pub const OP_CURSOR_CHUNKS: u32 = 23;
pub const OP_CURSOR_CLOSE: u32 = 24;
pub const OP_LABEL_NAMES: u32 = 30;
pub const OP_LABEL_VALUES: u32 = 31;
pub const OP_POSTINGS: u32 = 32;
pub const OP_SERIES_LABELS: u32 = 33;
pub const OP_VALUES: u32 = 34;
pub const OP_VALUES_IF_CHANGED: u32 = 37;
pub const OP_HEAD_SERIES: u32 = 35;
pub const OP_LABEL_BYTES: u32 = 36;
pub const OP_FLUSH: u32 = 40;
pub const OP_EVICT: u32 = 41;
pub const OP_EVICT_IDLE: u32 = 42;
pub const OP_RETAIN: u32 = 43;
pub const OP_CONFIGURE: u32 = 44;
pub const OP_CLOSE_CHECKPOINT: u32 = 45;
pub const OP_RECOVERY: u32 = 46;
pub const OP_COMPACT_OOO: u32 = 47;
pub const OP_EXEMPLARS: u32 = 50;
pub const OP_ACTIVE: u32 = 60;
pub const OP_DEACTIVATE: u32 = 61;
pub const OP_SET_TRACKERS: u32 = 62;
pub const OP_IS_ACTIVE: u32 = 63;
pub const OP_MEMORY: u32 = 70;
pub const OP_PURGE: u32 = 71;

fn read_labels<'a>(r: &mut Reader<'a>) -> Result<Vec<(&'a str, &'a str)>> {
    let n = r.count(8)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push((r.str()?, r.str()?));
    }
    Ok(out)
}

fn write_removed(w: &mut Writer, removed: &[(u64, Vec<(String, String)>)]) {
    w.u32(removed.len() as u32);
    for (r, labels) in removed {
        w.u64(*r);
        w.u32(labels.len() as u32);
        for (n, v) in labels {
            w.bytes(n.as_bytes());
            w.bytes(v.as_bytes());
        }
    }
}

fn call(e: &Engine, op: u32, r: &mut Reader, data: &[u8], w: &mut Writer) -> Result<()> {
    match op {
        OP_PARSE => e.parse(data, w)?,
        OP_INGEST => {
            let handle = r.u32()?;
            let (min_ts, max_ts, ingested) = (r.i64()?, r.i64()?, r.i64()?);
            let otlp = r.bool()?;
            let skip = r.bool()?;
            let skip_before = r.i64()?;
            let track = r.bool()?;
            let out = e.ingest(
                handle,
                data,
                min_ts,
                max_ts,
                ingested,
                otlp,
                skip.then_some(skip_before),
                track,
            )?;
            w.u64(out.accepted);
            w.u32(out.ingested.len() as u32);
            for (i, rf) in out.ingested {
                w.u32(i);
                w.u64(rf);
            }
            w.u32(out.leftovers.len() as u32);
            for i in out.leftovers {
                w.u32(i);
            }
            w.u32(out.skipped.len() as u32);
            for i in out.skipped {
                w.u32(i);
            }
            w.u32(out.errors.len() as u32);
            for (i, t, code) in out.errors {
                w.u32(i);
                w.i64(t);
                w.u8(code);
            }
        }
        OP_SERIES_OF => {
            let handle = r.u32()?;
            let index = r.u32()?;
            e.series_of(handle, index, data, w)?;
        }
        OP_SUBSET => {
            let handle = r.u32()?;
            let n = r.count(4)?;
            let indices = (0..n).map(|_| r.u32()).collect::<Result<Vec<_>>>()?;
            w.buf.clear();
            w.u8(0);
            e.subset(handle, data, &indices, w)?;
        }
        OP_RELEASE => e.release(r.u32()?),
        OP_LOOKUP => {
            let labels = read_labels(r)?;
            let (rf, in_head) = e.lookup(&labels);
            w.u64(rf);
            w.bool(in_head);
        }
        OP_CREATE => {
            let labels = read_labels(r)?;
            let trackers = r.u64()?;
            let otlp = r.bool()?;
            w.u64(e.create(&labels, trackers, otlp)?);
        }
        OP_APPEND => {
            let rf = r.u64()?;
            let t = r.i64()?;
            let kind = r.u8()?;
            let v = f64::from_bits(r.u64()?);
            let payload = r.bytes()?;
            let buckets = r.u32()? as i32;
            let reject_ooo = r.bool()?;
            let data = (!payload.is_empty()).then(|| payload.into());
            w.u8(match e.append(rf, t, kind, v, data, buckets, reject_ooo) {
                Ok(()) => 0,
                Err(code) => code as u8,
            });
        }
        OP_EXEMPLAR => {
            let rf = r.u64()?;
            let t = r.i64()?;
            let v = f64::from_bits(r.u64()?);
            let labels = read_labels(r)?;
            let mut st = e.state.write();
            let pairs = labels
                .iter()
                .map(|(n, v)| (st.head.symbols.intern(n), st.head.symbols.intern(v)))
                .collect();
            drop(st);
            w.u8(
                match e.append_exemplar(
                    rf,
                    Exemplar {
                        t,
                        v,
                        labels: pairs,
                    },
                ) {
                    Ok(()) => 0,
                    Err(code) => code as u8,
                },
            );
        }
        OP_COMMIT => {
            let ingested = r.i64()?;
            w.u64(e.commit(ingested, r.bool()?));
        }
        OP_ROLLBACK => write_removed(w, &e.rollback()),
        OP_LAST => match e.last(r.u64()?) {
            Some((t, kind)) => {
                w.bool(true);
                w.i64(t);
                w.u8(kind);
            }
            None => w.bool(false),
        },
        OP_STATS => {
            let st = e.state.read();
            w.u64(st.head.in_head as u64);
            w.i64(st.head_mint);
            w.i64(st.head_maxt);
            w.i64(st.min_valid);
            w.i64(st.ooo_mint);
            w.i64(st.ooo_maxt);
            w.u64(st.head.live as u64);
            w.u64(st.generation);
        }
        OP_SELECT => {
            let (mint, maxt) = (r.i64()?, r.i64()?);
            let (sorted, ordered, labels_only) = (r.bool()?, r.bool()?, r.bool()?);
            let mode = r.u8()?;
            let st = e.state.read();
            let slots: Vec<u32> = match mode {
                0 => st.head.iter().map(|(s, _)| s).collect(),
                1 => query::select(&st, &query::read_filters(r)?),
                _ => {
                    let mut v: Vec<u32> = r.u64s()?.into_iter().filter_map(head::slot_of).collect();
                    v.sort_unstable();
                    v.dedup();
                    v
                }
            };
            // The first page comes with the cursor, which is all a small query reads.
            let (inline_series, inline_series_max, inline_bytes) =
                (r.u32()? as usize, r.u32()? as usize, r.u32()? as usize);
            let cursor = query::Cursor::open(&st, &slots, mint, maxt, sorted, ordered, labels_only);
            let id = e
                .next_cursor
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            w.u64(id);
            w.u32(cursor.series.len() as u32);
            write_series_page(&st, w, &cursor, 0, inline_series);
            drop(st);
            if labels_only {
                w.u32(0);
            } else {
                write_chunks_page(w, &cursor, 0, inline_series_max, inline_bytes);
            }
            // A cursor read whole with its first page needn't be kept.
            if cursor.series.len() > inline_series
                || !labels_only && cursor.series.len() > inline_series_max
            {
                e.cursors.lock().insert(id, Arc::new(cursor));
            }
        }
        OP_CURSOR_SERIES => {
            let id = r.u64()?;
            let (from, max) = (r.u32()? as usize, r.u32()? as usize);
            let cursor = e
                .cursors
                .lock()
                .get(&id)
                .cloned()
                .ok_or("query cursor is closed")?;
            let st = e.state.read();
            write_series_page(&st, w, &cursor, from, max);
        }
        OP_CURSOR_CHUNKS => {
            let id = r.u64()?;
            let (from, max_series, max_bytes) =
                (r.u32()? as usize, r.u32()? as usize, r.u32()? as usize);
            let cursor = e
                .cursors
                .lock()
                .get(&id)
                .cloned()
                .ok_or("query cursor is closed")?;
            write_chunks_page(w, &cursor, from, max_series, max_bytes);
        }
        OP_CURSOR_CLOSE => {
            e.cursors.lock().remove(&r.u64()?);
        }
        OP_LABEL_NAMES | OP_LABEL_VALUES => {
            let name = if op == OP_LABEL_VALUES { r.str()? } else { "" };
            let (mint, maxt) = (r.i64()?, r.i64()?);
            let limit = r.u32()? as usize;
            let filters = query::read_filters(r)?;
            let st = e.state.read();
            let out = if op == OP_LABEL_NAMES {
                query::label_names(&st, &filters, mint, maxt, limit)
            } else {
                query::label_values(&st, name, &filters, mint, maxt, limit)
            };
            w.u32(out.len() as u32);
            for s in out {
                w.bytes(s.as_bytes());
            }
        }
        OP_POSTINGS => {
            let head_only = r.bool()?;
            let all = r.bool()?;
            let st = e.state.read();
            let slots: Vec<u32> = if all {
                st.head.iter().map(|(s, _)| s).collect()
            } else {
                query::select(&st, &query::read_filters(r)?)
            };
            let at = w.placeholder();
            let mut n = 0;
            for slot in slots {
                if !head_only || st.head.get(slot).is_some_and(|s| s.in_head) {
                    w.u64(head::ref_of(slot));
                    n += 1;
                }
            }
            w.patch_u32(at, n);
        }
        OP_SERIES_LABELS => {
            let refs = r.u64s()?;
            let st = e.state.read();
            for rf in refs {
                match st.head.by_ref(rf) {
                    Some(s) => {
                        w.bool(true);
                        query::write_labels(&st, w, &s.labels);
                    }
                    None => w.bool(false),
                }
            }
        }
        OP_VALUES_IF_CHANGED => {
            let name = r.str()?;
            let known = r.u64()?;
            let st = e.state.read();
            let sym = st.head.symbols.lookup(name.as_bytes());
            let generation = sym.map_or(0, |n| st.head.postings.generation(n));
            w.u64(generation);
            if generation == known {
                w.bool(false);
            } else {
                w.bool(true);
                let mut values: Vec<&str> = sym
                    .and_then(|n| st.head.postings.by_name.get(&n))
                    .map(|by_value| by_value.keys().map(|&v| st.head.symbols.get(v)).collect())
                    .unwrap_or_default();
                values.sort_unstable();
                w.u32(values.len() as u32);
                for v in values {
                    w.bytes(v.as_bytes());
                }
            }
        }
        OP_VALUES => {
            let name = r.str()?;
            let head_only = r.bool()?;
            let st = e.state.read();
            let mut values: Vec<&str> = match st
                .head
                .symbols
                .lookup(name.as_bytes())
                .and_then(|n| st.head.postings.by_name.get(&n))
            {
                Some(by_value) => by_value
                    .iter()
                    .filter(|(_, l)| {
                        !head_only || l.iter().any(|&s| st.head.get(s).is_some_and(|x| x.in_head))
                    })
                    .map(|(&v, _)| st.head.symbols.get(v))
                    .collect(),
                None => Vec::new(),
            };
            values.sort_unstable();
            w.u32(values.len() as u32);
            for v in values {
                w.bytes(v.as_bytes());
            }
        }
        OP_HEAD_SERIES => {
            let (from, max) = (r.u32()? as usize, r.u32()? as usize);
            let st = e.state.read();
            let at_next = w.placeholder();
            let at_n = w.placeholder();
            let mut n = 0;
            let mut next = st.head.slots.len();
            for (i, s) in st.head.slots.iter().enumerate().skip(from) {
                if n == max {
                    next = i;
                    break;
                }
                let Some(s) = s.as_deref() else { continue };
                if !s.in_head {
                    continue;
                }
                w.u64(head::ref_of(i as u32));
                query::write_labels(&st, w, &s.labels);
                n += 1;
            }
            w.patch_u32(at_next, next as u32);
            w.patch_u32(at_n, n as u32);
        }
        OP_LABEL_BYTES => {
            let st = e.state.read();
            w.u32(st.head.postings.by_name.len() as u32);
            for (&n, values) in &st.head.postings.by_name {
                w.bytes(st.head.symbols.get(n).as_bytes());
                w.u64(
                    values
                        .keys()
                        .map(|&v| st.head.symbols.get(v).len() as u64)
                        .sum(),
                );
            }
        }
        OP_FLUSH => write_removed(w, &e.flush(r.i64()?, None)?),
        OP_EVICT => {
            let mut slots: Vec<u32> = r.u64s()?.into_iter().filter_map(head::slot_of).collect();
            slots.sort_unstable();
            slots.dedup();
            write_removed(w, &e.flush(i64::MAX, Some(&slots))?);
        }
        OP_EVICT_IDLE => write_removed(w, &e.evict_idle(r.i64()?)?),
        OP_RETAIN => e.retain(r.i64()?)?,
        OP_COMPACT_OOO => e.compact_ooo()?,
        OP_CONFIGURE => {
            let window = r.i64()?;
            let max_exemplars = r.u64()? as usize;
            let mut st = e.state.write();
            st.window = window.max(0);
            st.max_exemplars = max_exemplars;
        }
        OP_CLOSE_CHECKPOINT => e.close_checkpoint()?,
        OP_RECOVERY => w.bool(e.restored_clean),
        OP_EXEMPLARS => {
            let (mint, maxt) = (r.i64()?, r.i64()?);
            let all = r.bool()?;
            let refs = r.u64s()?;
            let st = e.state.read();
            let slots: Vec<u32> = if all {
                st.head.iter().map(|(s, _)| s).collect()
            } else {
                refs.into_iter().filter_map(head::slot_of).collect()
            };
            let at = w.placeholder();
            let mut n = 0;
            for slot in slots {
                let Some(s) = st.head.get(slot) else { continue };
                let Some(extra) = &s.extra else { continue };
                let es: Vec<_> = extra
                    .exemplars
                    .iter()
                    .filter(|x| x.t >= mint && x.t <= maxt)
                    .collect();
                if es.is_empty() {
                    continue;
                }
                n += 1;
                w.u64(head::ref_of(slot));
                query::write_labels(&st, w, &s.labels);
                w.u32(es.len() as u32);
                for x in es {
                    w.i64(x.t);
                    w.u64(x.v.to_bits());
                    w.u32(x.labels.len() as u32);
                    for &(a, b) in x.labels.iter() {
                        w.bytes(st.head.symbols.get(a).as_bytes());
                        w.bytes(st.head.symbols.get(b).as_bytes());
                    }
                }
            }
            w.patch_u32(at, n);
        }
        OP_PURGE => purge(),
        OP_ACTIVE => {
            let cutoff = r.i64()?;
            let trackers = r.u32()? as usize;
            let report = r.bool()?;
            let (counts, changes) = e.active(cutoff, trackers, report);
            w.u64(counts.total);
            w.u64(counts.otlp);
            w.u64(counts.histograms);
            w.u64(counts.buckets);
            for t in &counts.trackers {
                for x in t {
                    w.u64(*x);
                }
            }
            let st = e.state.read();
            w.u32(changes.len() as u32);
            for (labels, active, buckets) in changes {
                w.bool(active);
                w.u32(buckets as u32);
                query::write_labels(&st, w, &labels);
            }
        }
        OP_SET_TRACKERS => {
            let n = r.count(16)?;
            let masks = (0..n)
                .map(|_| Ok((r.u64()?, r.u64()?)))
                .collect::<Result<Vec<_>>>()?;
            e.set_trackers(&masks);
        }
        OP_DEACTIVATE => {
            let all = r.bool()?;
            let refs = r.u64s()?;
            e.deactivate((!all).then_some(&refs[..]));
        }
        OP_IS_ACTIVE => {
            let rf = r.u64()?;
            let (active, buckets) = e.is_active(rf, r.i64()?);
            w.bool(active);
            w.u32(buckets as u32);
        }
        OP_MEMORY => {
            let (allocated, resident) = allocator_stats();
            w.u64(allocated);
            w.u64(resident);
            let st = e.state.read();
            let series: usize = st.head.iter().map(|(_, s)| s.heap_bytes()).sum();
            w.u64(series as u64);
            w.u64(st.head.symbols.heap_bytes() as u64);
            w.u64(st.head.postings.heap_bytes() as u64);
            w.u64((st.head.slots.capacity() * 8 + st.head.table.capacity() * 25) as u64);
        }
        _ => return Err(format!("unknown operation {op}")),
    }
    Ok(())
}

/// The bytes the allocator has allocated and resident.
#[cfg(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos")))]
fn allocator_stats() -> (u64, u64) {
    use tikv_jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    (
        stats::allocated::read().unwrap_or(0) as u64,
        stats::resident::read().unwrap_or(0) as u64,
    )
}

#[cfg(not(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos"))))]
fn allocator_stats() -> (u64, u64) {
    (0, 0)
}

/// Gives freed memory back to the system, like Go's debug.FreeOSMemory.
#[cfg(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos")))]
fn purge() {
    // MALLCTL_ARENAS_ALL is 4096; purging takes and returns nothing, which jemalloc checks.
    unsafe {
        tikv_jemalloc_sys::mallctl(
            c"arena.4096.purge".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
}

#[cfg(not(all(feature = "jemalloc", any(target_os = "linux", target_os = "macos"))))]
fn purge() {}

fn write_series_page(
    st: &engine::State,
    w: &mut Writer,
    cursor: &query::Cursor,
    from: usize,
    max: usize,
) {
    let part = &cursor.series
        [from.min(cursor.series.len())..from.saturating_add(max).min(cursor.series.len())];
    w.u32(part.len() as u32);
    for s in part {
        w.u64(head::ref_of(s.slot));
        query::write_labels(st, w, &s.labels);
        w.u32(s.count);
        // Go encodes histogram chunks in as many chunks as Prometheus needs, which it has to know first.
        let chunks = &cursor.chunks[s.first as usize..(s.first + s.count) as usize];
        w.bool(chunks.iter().any(|c| c.encoding == head::HIST_ENCODING));
    }
}

/// The chunks of the series from `from` on, series by series, while they fit in max_bytes, at least one.
fn write_chunks_page(
    w: &mut Writer,
    cursor: &query::Cursor,
    from: usize,
    max_series: usize,
    max_bytes: usize,
) {
    let at = w.placeholder();
    let mut n = 0;
    let mut bytes = 0;
    for s in cursor.series.iter().skip(from).take(max_series) {
        let chunks = &cursor.chunks[s.first as usize..(s.first + s.count) as usize];
        let size: usize = chunks.iter().map(|c| c.data.bytes().len() + 32).sum();
        if n > 0 && bytes + size > max_bytes {
            break;
        }
        bytes += size;
        w.u32(chunks.len() as u32);
        for c in chunks {
            w.i64(c.mint);
            w.i64(c.maxt);
            w.u8(c.encoding);
            w.bytes(c.data.bytes());
        }
        n += 1;
    }
    w.patch_u32(at, n);
}
