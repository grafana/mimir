// SPDX-License-Identifier: AGPL-3.0-only

//! The head, written at a clean shutdown and read back at the next open.

use crate::engine::{State, io};
use crate::head::{Chunk, Exemplar, Extra, Hist, Series, hash_pairs};
use crate::wire::{Reader, Result, Writer};
use crate::xor;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MAGIC: u32 = 0x3153_524d; // "MRS1"

pub fn write(path: &Path, st: &State) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = std::io::BufWriter::with_capacity(1 << 20, File::create(&tmp).map_err(io)?);
    let mut w = Writer::with_capacity(1 << 16);
    w.u32(MAGIC);
    w.i64(st.min_valid);
    w.i64(st.head_maxt);
    w.u64(st.cold_seq);
    w.u32(st.head.slots.len() as u32);
    w.u32(st.head.live as u32);
    for (slot, s) in st.head.iter() {
        w.u32(slot);
        w.u32(s.labels.len() as u32 / 2);
        for (n, v) in st.head.label_pairs(s) {
            w.bytes(n.as_bytes());
            w.bytes(v.as_bytes());
        }
        w.bool(s.in_head);
        w.bool(s.idle);
        w.i64(s.active_ms);
        w.u64(s.trackers);
        w.bool(s.otlp);
        w.u32(s.buckets as u32);
        w.i64(s.max_t);
        w.u64(s.last_v);
        w.u8(s.last_kind);
        for x in [s.cold_mint, s.cold_maxt, s.mem_mint, s.mem_maxt] {
            w.i64(x);
        }
        w.u32(s.sealed.len() as u32);
        for c in &s.sealed {
            w.i64(c.mint);
            w.i64(c.maxt);
            w.u8(c.encoding);
            w.bytes(&c.data);
        }
        match &s.head {
            Some(h) => {
                w.bool(true);
                h.write_state(&mut w.buf).map_err(io)?;
            }
            None => w.bool(false),
        }
        match &s.extra {
            Some(e) => {
                w.bool(true);
                w.u32(e.ooo.len() as u32);
                for &(t, v) in &e.ooo {
                    w.i64(t);
                    w.u64(v.to_bits());
                }
                w.u32(e.hist.len() as u32);
                for h in &e.hist {
                    w.i64(h.t);
                    w.u8(h.kind);
                    w.bytes(&h.data);
                }
                w.u32(e.exemplars.len() as u32);
                for x in &e.exemplars {
                    w.i64(x.t);
                    w.u64(x.v.to_bits());
                    w.u32(x.labels.len() as u32);
                    for &(n, v) in x.labels.iter() {
                        w.bytes(st.head.symbols.get(n).as_bytes());
                        w.bytes(st.head.symbols.get(v).as_bytes());
                    }
                }
            }
            None => w.bool(false),
        }
        if w.buf.len() >= 1 << 16 {
            file.write_all(&w.buf).map_err(io)?;
            w.buf.clear();
        }
    }
    w.u32(MAGIC);
    file.write_all(&w.buf).map_err(io)?;
    let file = file.into_inner().map_err(io)?;
    file.sync_all().map_err(io)?;
    fs::rename(&tmp, path).map_err(io)
}

pub fn read(path: &Path, st: &mut State) -> Result<()> {
    let data = fs::read(path).map_err(io)?;
    let mut r = Reader::new(&data);
    if r.u32()? != MAGIC {
        return Err("corrupt head snapshot".into());
    }
    st.min_valid = r.i64()?;
    st.head_maxt = r.i64()?;
    st.cold_seq = r.u64()?;
    let slots = r.u32()? as usize;
    let live = r.u32()? as usize;
    st.head.slots.resize_with(slots, || None);
    let mut exemplars = Vec::new();
    for _ in 0..live {
        let slot = r.u32()?;
        let n = r.count(8)?;
        let mut pairs = Vec::with_capacity(n * 2);
        for _ in 0..n {
            pairs.push(st.head.symbols.intern(r.str()?));
            pairs.push(st.head.symbols.intern(r.str()?));
        }
        let labels: Box<[u32]> = pairs.into();
        let (hash, check) = hash_pairs(&labels);
        let mut s = Series::new(labels, hash, check);
        s.in_head = r.bool()?;
        s.idle = r.bool()?;
        s.active_ms = r.i64()?;
        s.trackers = r.u64()?;
        s.otlp = r.bool()?;
        s.buckets = r.u32()? as i32;
        s.max_t = r.i64()?;
        s.last_v = r.u64()?;
        s.last_kind = r.u8()?;
        s.cold_mint = r.i64()?;
        s.cold_maxt = r.i64()?;
        s.mem_mint = r.i64()?;
        s.mem_maxt = r.i64()?;
        let n = r.count(21)?;
        for _ in 0..n {
            s.sealed.push(Chunk {
                mint: r.i64()?,
                maxt: r.i64()?,
                encoding: r.u8()?,
                data: r.bytes()?.into(),
            });
        }
        if r.bool()? {
            let rest = r.rest();
            let mut cursor = std::io::Cursor::new(rest);
            let app = xor::Appender::read_state(&mut cursor).map_err(io)?;
            r.skip(cursor.position() as usize)?;
            s.head = Some(app);
        }
        if r.bool()? {
            let mut e = Extra::default();
            let n = r.count(16)?;
            for _ in 0..n {
                e.ooo.push((r.i64()?, f64::from_bits(r.u64()?)));
            }
            let n = r.count(13)?;
            for _ in 0..n {
                e.hist.push(Hist {
                    t: r.i64()?,
                    kind: r.u8()?,
                    data: r.bytes()?.into(),
                });
            }
            let n = r.count(20)?;
            for _ in 0..n {
                let t = r.i64()?;
                let v = f64::from_bits(r.u64()?);
                let m = r.count(8)?;
                let mut ls = Vec::with_capacity(m);
                for _ in 0..m {
                    ls.push((
                        st.head.symbols.intern(r.str()?),
                        st.head.symbols.intern(r.str()?),
                    ));
                }
                exemplars.push((t, slot));
                e.exemplars.push(Exemplar {
                    t,
                    v,
                    labels: ls.into(),
                });
            }
            s.extra = Some(Box::new(e));
        }
        st.head.postings.add(&s.labels, slot);
        let (hash, check) = (s.hash, s.check);
        *st.head
            .slots
            .get_mut(slot as usize)
            .ok_or("corrupt head snapshot")? = Some(Box::new(s));
        st.head
            .table
            .insert_unique(hash, crate::head::Entry { hash, check, slot }, |e| e.hash);
        st.head.live += 1;
        if st.head.get(slot).unwrap().in_head {
            st.head.in_head += 1;
        }
    }
    if r.u32()? != MAGIC {
        return Err("corrupt head snapshot".into());
    }
    exemplars.sort_unstable();
    st.exemplar_count = exemplars.len();
    st.exemplar_order = exemplars.into_iter().map(|(t, slot)| (slot, t)).collect();
    st.recompute_bounds_pub();
    Ok(())
}
