// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable files of the chunks that left memory, one per block range per flush, read through a mapping.
//!
//! Layout: chunk bytes, then the series table (slot u32, first chunk u32, chunk count u32), then the chunk table
//! (mint i64, maxt i64, offset u64, length u32, encoding u8, 3 bytes padding), then the footer.

use crate::head::Chunk;
use crate::wire::Result;
use memmap2::Mmap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x3143_524d; // "MRC1"
const FOOTER: usize = 8 + 4 + 8 + 4 + 8 + 8 + 8 + 4;
const SERIES_ENTRY: usize = 12;
const CHUNK_ENTRY: usize = 32;

pub struct ColdFile {
    pub path: PathBuf,
    pub mint: i64,
    pub maxt: i64,
    pub created_ms: i64,
    map: Mmap,
    series_at: usize,
    series_len: usize,
    chunks_at: usize,
    chunks_len: usize,
}

#[derive(Clone, Copy)]
pub struct ChunkRef {
    pub mint: i64,
    pub maxt: i64,
    pub at: usize,
    pub len: usize,
    pub encoding: u8,
}

fn io(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl ColdFile {
    /// Writes the chunks of each series, sorted by slot, in the file at path, atomically.
    pub fn write(
        dir: &Path,
        name: &str,
        created_ms: i64,
        series: &[(u32, Vec<Chunk>)],
    ) -> Result<Option<ColdFile>> {
        let total_chunks: usize = series.iter().map(|(_, c)| c.len()).sum();
        if total_chunks == 0 {
            return Ok(None);
        }
        let path = dir.join(name);
        let tmp = dir.join(format!("{name}.tmp"));
        let file = File::create(&tmp).map_err(io)?;
        let mut w = BufWriter::with_capacity(1 << 20, file);
        let mut offset = 0u64;
        let (mut mint, mut maxt) = (i64::MAX, i64::MIN);
        let mut chunk_table = Vec::with_capacity(total_chunks * CHUNK_ENTRY);
        let mut series_table = Vec::with_capacity(series.len() * SERIES_ENTRY);
        let mut index = 0u32;
        for (slot, chunks) in series {
            if chunks.is_empty() {
                continue;
            }
            series_table.extend_from_slice(&slot.to_le_bytes());
            series_table.extend_from_slice(&index.to_le_bytes());
            series_table.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
            for c in chunks {
                w.write_all(&c.data).map_err(io)?;
                chunk_table.extend_from_slice(&c.mint.to_le_bytes());
                chunk_table.extend_from_slice(&c.maxt.to_le_bytes());
                chunk_table.extend_from_slice(&offset.to_le_bytes());
                chunk_table.extend_from_slice(&(c.data.len() as u32).to_le_bytes());
                chunk_table.extend_from_slice(&[c.encoding, 0, 0, 0]);
                offset += c.data.len() as u64;
                mint = mint.min(c.mint);
                maxt = maxt.max(c.maxt);
                index += 1;
            }
        }
        let series_at = offset;
        w.write_all(&series_table).map_err(io)?;
        let chunks_at = series_at + series_table.len() as u64;
        w.write_all(&chunk_table).map_err(io)?;
        let mut footer = Vec::with_capacity(FOOTER);
        footer.extend_from_slice(&series_at.to_le_bytes());
        footer.extend_from_slice(&((series_table.len() / SERIES_ENTRY) as u32).to_le_bytes());
        footer.extend_from_slice(&chunks_at.to_le_bytes());
        footer.extend_from_slice(&index.to_le_bytes());
        footer.extend_from_slice(&mint.to_le_bytes());
        footer.extend_from_slice(&maxt.to_le_bytes());
        footer.extend_from_slice(&created_ms.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        w.write_all(&footer).map_err(io)?;
        let file = w.into_inner().map_err(io)?;
        file.sync_all().map_err(io)?;
        drop(file);
        fs::rename(&tmp, &path).map_err(io)?;
        File::open(dir).and_then(|d| d.sync_all()).map_err(io)?;
        Self::open(&path).map(Some)
    }

    pub fn open(path: &Path) -> Result<ColdFile> {
        let file = File::open(path).map_err(io)?;
        let map = unsafe { Mmap::map(&file) }.map_err(io)?;
        let bad = || format!("corrupt cold file {}", path.display());
        if map.len() < FOOTER {
            return Err(bad());
        }
        let f = &map[map.len() - FOOTER..];
        let u64_at = |at: usize| u64::from_le_bytes(f[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(f[at..at + 4].try_into().unwrap());
        if u32_at(FOOTER - 4) != MAGIC {
            return Err(bad());
        }
        let series_at = u64_at(0) as usize;
        let series_len = u32_at(8) as usize;
        let chunks_at = u64_at(12) as usize;
        let chunks_len = u32_at(20) as usize;
        let end = map.len() - FOOTER;
        if series_at.checked_add(series_len * SERIES_ENTRY) != Some(chunks_at)
            || chunks_at.checked_add(chunks_len * CHUNK_ENTRY) != Some(end)
        {
            return Err(bad());
        }
        let cold = ColdFile {
            path: path.into(),
            mint: u64_at(24) as i64,
            maxt: u64_at(32) as i64,
            created_ms: u64_at(40) as i64,
            map,
            series_at,
            series_len,
            chunks_at,
            chunks_len,
        };
        for i in 0..chunks_len {
            let c = cold.chunk(i);
            if c.at.checked_add(c.len).is_none_or(|e| e > series_at) {
                return Err(bad());
            }
        }
        Ok(cold)
    }

    #[inline]
    fn series_entry(&self, i: usize) -> (u32, u32, u32) {
        let at = self.series_at + i * SERIES_ENTRY;
        let b = &self.map[at..at + SERIES_ENTRY];
        (
            u32::from_le_bytes(b[0..4].try_into().unwrap()),
            u32::from_le_bytes(b[4..8].try_into().unwrap()),
            u32::from_le_bytes(b[8..12].try_into().unwrap()),
        )
    }

    #[inline]
    fn chunk(&self, i: usize) -> ChunkRef {
        let at = self.chunks_at + i * CHUNK_ENTRY;
        let b = &self.map[at..at + CHUNK_ENTRY];
        ChunkRef {
            mint: i64::from_le_bytes(b[0..8].try_into().unwrap()),
            maxt: i64::from_le_bytes(b[8..16].try_into().unwrap()),
            at: u64::from_le_bytes(b[16..24].try_into().unwrap()) as usize,
            len: u32::from_le_bytes(b[24..28].try_into().unwrap()) as usize,
            encoding: b[28],
        }
    }

    pub fn series_count(&self) -> usize {
        self.series_len
    }

    pub fn slots(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.series_len).map(|i| self.series_entry(i).0)
    }

    /// The chunks of the series in the file, in order.
    pub fn chunks_of(&self, slot: u32) -> impl Iterator<Item = ChunkRef> + '_ {
        let (mut lo, mut hi) = (0, self.series_len);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.series_entry(mid).0 < slot {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let range = if lo < self.series_len && self.series_entry(lo).0 == slot {
            let (_, first, count) = self.series_entry(lo);
            first as usize..(first + count) as usize
        } else {
            0..0
        };
        range.map(|i| self.chunk(i))
    }

    pub fn contains(&self, slot: u32) -> bool {
        self.chunks_of(slot).next().is_some()
    }

    #[inline]
    pub fn bytes(&self, c: &ChunkRef) -> &[u8] {
        &self.map[c.at..c.at + c.len]
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks_len
    }
}
