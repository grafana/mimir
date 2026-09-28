//! Series that left the emulated head, in immutable files like the Go ingester's blocks: their
//! labels, chunk references and postings stay on disk, memory-mapped, so millions of series that
//! only queries of older ranges read cost no heap. The chunks themselves stay in the chunk files.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use memmap2::Mmap;

use super::*;

const MAGIC: &[u8; 8] = b"MIMIRCB1";

/// A series leaving the head, with every chunk it had in the chunk files.
pub(super) struct Frozen {
    pub tenant: String,
    pub labels: StoredLabels,
    pub chunks: ChunkList,
    pub native_histogram: bool,
}

enum Data {
    Mapped(Mmap),
    Memory(Vec<u8>),
}

impl Data {
    fn bytes(&self) -> &[u8] {
        match self {
            Data::Mapped(map) => map,
            Data::Memory(bytes) => bytes,
        }
    }
}

struct TenantIndex {
    min_time: i64,
    max_time: i64,
    // Byte offsets into the file of the series offsets table and the postings table.
    series_table: usize,
    series_count: usize,
    postings_table: usize,
    postings_count: usize,
}

/// One cold block of a store shard.
pub(super) struct ColdBlock {
    pub id: u64,
    pub min_time: i64,
    pub max_time: i64,
    path: Option<PathBuf>,
    data: Data,
    // File-local label name ids to process-wide names, and back.
    names: Vec<&'static str>,
    local_ids: HashMap<&'static str, u32>,
    tenants: HashMap<String, TenantIndex>,
}

/// A series of a cold block.
pub(super) struct ColdSeries<'a> {
    block: &'a ColdBlock,
    labels: &'a [u8],
    chunks: &'a [u8],
}

pub(super) fn block_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("cold-{id:016x}"))
}

impl ColdBlock {
    /// Writes `series` to a new block in `directory`, or keeps it in memory without one.
    pub fn build(directory: Option<&Path>, id: u64, mut series: Vec<Frozen>) -> Result<Self> {
        series.sort_by(|a, b| (&a.tenant, &a.labels).cmp(&(&b.tenant, &b.labels)));
        let mut names: Vec<&'static str> = Vec::new();
        let mut local: HashMap<&'static str, u32> = HashMap::new();
        for frozen in &series {
            for (name, _) in &frozen.labels {
                local.entry(name).or_insert_with(|| {
                    names.push(name);
                    names.len() as u32 - 1
                });
            }
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        put_varint(&mut out, names.len() as u64);
        for name in &names {
            put_varint(&mut out, name.len() as u64);
            out.extend_from_slice(name.as_bytes());
        }
        // Tenants: id, time range and where their tables are, patched once written.
        let mut tenant_ranges = Vec::new();
        let mut start = 0;
        while start < series.len() {
            let end = start
                + series[start..]
                    .iter()
                    .take_while(|frozen| frozen.tenant == series[start].tenant)
                    .count();
            tenant_ranges.push(start..end);
            start = end;
        }
        put_varint(&mut out, tenant_ranges.len() as u64);
        let mut tenant_headers = Vec::new();
        for range in &tenant_ranges {
            let tenant = &series[range.start].tenant;
            put_varint(&mut out, tenant.len() as u64);
            out.extend_from_slice(tenant.as_bytes());
            tenant_headers.push(out.len());
            // min, max, series table, series count, postings table, postings count.
            out.extend_from_slice(&[0; 48]);
        }
        let (mut block_min, mut block_max) = (i64::MAX, i64::MIN);
        for (range, header) in tenant_ranges.iter().zip(tenant_headers) {
            let mut offsets = Vec::with_capacity(range.len());
            let mut postings: Vec<(u32, u64, u32)> = Vec::new();
            let (mut min_time, mut max_time) = (i64::MAX, i64::MIN);
            for (index, frozen) in series[range.clone()].iter().enumerate() {
                offsets.push(out.len() as u64);
                let mut labels = Vec::new();
                for (name, value) in &frozen.labels {
                    put_varint(&mut labels, u64::from(local[name]));
                    put_varint(&mut labels, value.len() as u64);
                    labels.extend_from_slice(value.as_bytes());
                    postings.push((local[name], value_hash(value), index as u32));
                }
                put_varint(&mut out, labels.len() as u64);
                out.extend_from_slice(&labels);
                put_varint(&mut out, frozen.chunks.0.len() as u64);
                out.extend_from_slice(&frozen.chunks.0);
                out.push(u8::from(frozen.native_histogram));
                for chunk in frozen.chunks.iter() {
                    min_time = min_time.min(chunk.min_time);
                    max_time = max_time.max(chunk.max_time);
                }
            }
            let series_table = out.len();
            for offset in &offsets {
                out.extend_from_slice(&offset.to_le_bytes());
            }
            postings.sort_unstable();
            // Entries of (name, value hash, list offset), then the lists of series indexes.
            let mut entries: Vec<(u32, u64, Vec<u32>)> = Vec::new();
            for (name, hash, index) in postings {
                match entries.last_mut() {
                    Some(last) if last.0 == name && last.1 == hash => last.2.push(index),
                    _ => entries.push((name, hash, vec![index])),
                }
            }
            let postings_table = out.len();
            let lists_start = postings_table + entries.len() * 16;
            let mut list_offset = 0_u32;
            for (name, hash, list) in &entries {
                out.extend_from_slice(&name.to_le_bytes());
                out.extend_from_slice(&hash.to_le_bytes());
                out.extend_from_slice(&list_offset.to_le_bytes());
                list_offset += 4 + 4 * list.len() as u32;
            }
            debug_assert_eq!(out.len(), lists_start);
            for (_, _, list) in &entries {
                out.extend_from_slice(&(list.len() as u32).to_le_bytes());
                for index in list {
                    out.extend_from_slice(&index.to_le_bytes());
                }
            }
            let fields = [
                min_time as u64,
                max_time as u64,
                series_table as u64,
                offsets.len() as u64,
                postings_table as u64,
                entries.len() as u64,
            ];
            for (slot, field) in fields.iter().enumerate() {
                out[header + slot * 8..header + slot * 8 + 8].copy_from_slice(&field.to_le_bytes());
            }
            block_min = block_min.min(min_time);
            block_max = block_max.max(max_time);
        }
        let checksum = crc32fast::hash(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        let (data, path) = match directory {
            Some(directory) => {
                fs::create_dir_all(directory).with_context(|| {
                    format!("create cold block directory {}", directory.display())
                })?;
                let path = block_path(directory, id);
                let temporary = path.with_extension("tmp");
                let mut file = File::create(&temporary)
                    .with_context(|| format!("create cold block {}", temporary.display()))?;
                file.write_all(&out)?;
                file.sync_all()?;
                fs::rename(&temporary, &path)?;
                (Data::Mapped(map(&path)?), Some(path))
            }
            None => (Data::Memory(out), None),
        };
        let block = Self::parse(id, data, path)?;
        ensure!(
            block.min_time == block_min && block.max_time == block_max,
            "cold block {id} time range mismatch"
        );
        Ok(block)
    }

    pub fn open(path: &Path, id: u64) -> Result<Self> {
        Self::parse(id, Data::Mapped(map(path)?), Some(path.to_path_buf()))
            .with_context(|| format!("open cold block {}", path.display()))
    }

    fn parse(id: u64, data: Data, path: Option<PathBuf>) -> Result<Self> {
        let bytes = data.bytes();
        ensure!(bytes.len() >= MAGIC.len() + 4, "cold block too short");
        let (body, checksum) = bytes.split_at(bytes.len() - 4);
        ensure!(&body[..8] == MAGIC, "cold block has invalid magic");
        if crc32fast::hash(body) != u32::from_le_bytes(checksum.try_into().unwrap()) {
            bail!("cold block checksum mismatch");
        }
        let mut cursor = &body[8..];
        let mut names = Vec::new();
        for _ in 0..take_varint(&mut cursor) {
            let len = take_varint(&mut cursor) as usize;
            let name = std::str::from_utf8(&cursor[..len])?;
            names.push(crate::labels::names::name(crate::labels::names::intern(
                name,
            )));
            cursor = &cursor[len..];
        }
        let local_ids = names
            .iter()
            .enumerate()
            .map(|(id, name)| (*name, id as u32))
            .collect();
        let mut tenants = HashMap::new();
        let (mut min_time, mut max_time) = (i64::MAX, i64::MIN);
        for _ in 0..take_varint(&mut cursor) {
            let len = take_varint(&mut cursor) as usize;
            let tenant = std::str::from_utf8(&cursor[..len])?.to_owned();
            cursor = &cursor[len..];
            let field = |slot: usize| {
                u64::from_le_bytes(cursor[slot * 8..slot * 8 + 8].try_into().unwrap())
            };
            let index = TenantIndex {
                min_time: field(0) as i64,
                max_time: field(1) as i64,
                series_table: field(2) as usize,
                series_count: field(3) as usize,
                postings_table: field(4) as usize,
                postings_count: field(5) as usize,
            };
            cursor = &cursor[48..];
            min_time = min_time.min(index.min_time);
            max_time = max_time.max(index.max_time);
            tenants.insert(tenant, index);
        }
        Ok(Self {
            id,
            min_time,
            max_time,
            path,
            data,
            names,
            local_ids,
            tenants,
        })
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn bytes(&self) -> &[u8] {
        self.data.bytes()
    }

    /// Whether the tenant has samples in `[start, end]` here.
    pub fn overlaps(&self, tenant: &str, start: i64, end: i64) -> bool {
        self.tenants
            .get(tenant)
            .is_some_and(|index| index.min_time <= end && index.max_time >= start)
    }

    pub fn series_count(&self, tenant: &str) -> usize {
        self.tenants
            .get(tenant)
            .map_or(0, |index| index.series_count)
    }

    pub fn series(&self, tenant: &str, index: usize) -> ColdSeries<'_> {
        let table = &self.tenants[tenant];
        let at = table.series_table + index * 8;
        let offset = u64::from_le_bytes(self.bytes()[at..at + 8].try_into().unwrap()) as usize;
        let mut cursor = &self.bytes()[offset..];
        let labels_len = take_varint(&mut cursor) as usize;
        let (labels, rest) = cursor.split_at(labels_len);
        let mut cursor = rest;
        let chunks_len = take_varint(&mut cursor) as usize;
        let chunks = &cursor[..chunks_len];
        ColdSeries {
            block: self,
            labels,
            chunks,
        }
    }

    /// The series with `name="value"`, or None when the name never appears here, so nothing does.
    fn posting(&self, tenant: &str, name: &str, value: &str) -> Vec<u32> {
        let (Some(table), Some(local)) = (self.tenants.get(tenant), self.local_ids.get(name))
        else {
            return Vec::new();
        };
        let bytes = self.bytes();
        let entry = |index: usize| {
            let at = table.postings_table + index * 16;
            (
                u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()),
                u64::from_le_bytes(bytes[at + 4..at + 12].try_into().unwrap()),
                u32::from_le_bytes(bytes[at + 12..at + 16].try_into().unwrap()),
            )
        };
        let key = (*local, value_hash(value));
        let (mut low, mut high) = (0, table.postings_count);
        while low < high {
            let middle = (low + high) / 2;
            let (name, hash, _) = entry(middle);
            if (name, hash) < key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low == table.postings_count {
            return Vec::new();
        }
        let (name, hash, offset) = entry(low);
        if (name, hash) != key {
            return Vec::new();
        }
        let lists = table.postings_table + table.postings_count * 16;
        let at = lists + offset as usize;
        let count = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        (0..count)
            .map(|index| {
                let at = at + 4 + index * 4;
                u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
            })
            .collect()
    }

    /// Indexes of the tenant's series that may match `matchers`: the smallest posting list of
    /// their equality matchers, or every series. Callers still check the matchers.
    pub fn candidates(&self, tenant: &str, matchers: &[CompiledMatcher]) -> Vec<u32> {
        let mut best: Option<Vec<u32>> = None;
        for matcher in matchers {
            if let CompiledMatcher::Equal(name, value) = matcher
                && !value.is_empty()
            {
                let list = self.posting(tenant, name, value);
                if best.as_ref().is_none_or(|best| list.len() < best.len()) {
                    best = Some(list);
                }
            }
        }
        best.unwrap_or_else(|| (0..self.series_count(tenant) as u32).collect())
    }
}

impl ColdSeries<'_> {
    pub fn pairs(&self) -> impl Iterator<Item = (&'static str, &str)> + Clone {
        let names = &self.block.names;
        let mut cursor = self.labels;
        std::iter::from_fn(move || {
            if cursor.is_empty() {
                return None;
            }
            let name = names[take_varint(&mut cursor) as usize];
            let len = take_varint(&mut cursor) as usize;
            let (value, rest) = cursor.split_at(len);
            cursor = rest;
            // SAFETY: values were written from `&str`.
            Some((name, unsafe { std::str::from_utf8_unchecked(value) }))
        })
    }

    // The map shortens the names' `'static` to the values' lifetime.
    #[allow(clippy::map_identity)]
    pub fn labels(&self) -> StoredLabels {
        StoredLabels::from_sorted(self.pairs().map(|(name, value)| (name, value)))
    }

    pub fn chunks(&self) -> ChunkIter<'_> {
        ChunkIter {
            bytes: self.chunks,
            reference: 0,
            min_time: 0,
        }
    }

    pub fn has_data(&self, start: i64, end: i64) -> bool {
        self.chunks()
            .any(|chunk| chunk.min_time <= end && chunk.max_time >= start)
    }
}

impl crate::trackers::LabelSet for ColdSeries<'_> {
    fn value(&self, name: &str) -> &str {
        self.pairs()
            .find(|(label, _)| *label == name)
            .map_or("", |(_, value)| value)
    }

    fn for_each(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, value) in self.pairs() {
            visit(name, value);
        }
    }
}

fn map(path: &Path) -> Result<Mmap> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    // SAFETY: cold blocks are written once, before they are mapped, and never modified.
    Ok(unsafe { Mmap::map(&file) }?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen(tenant: &str, pairs: &[(&str, &str)], chunks: &[(i64, i64)]) -> Frozen {
        Frozen {
            tenant: tenant.into(),
            labels: StoredLabels::from_sorted(pairs.iter().copied()),
            chunks: ChunkList::from_metas(
                &chunks
                    .iter()
                    .enumerate()
                    .map(|(index, (min, max))| ChunkMeta {
                        reference: index as u64 * 100,
                        min_time: *min,
                        max_time: *max,
                        len: 10,
                        encoding: XOR_ENCODING,
                        out_of_order: false,
                    })
                    .collect::<Vec<_>>(),
            ),
            native_histogram: false,
        }
    }

    #[test]
    fn cold_blocks_find_series_by_postings_and_survive_reopening() {
        let directory =
            std::env::temp_dir().join(format!("mimir-rust-cold-{}", std::process::id()));
        let series = vec![
            frozen("b", &[("__name__", "up"), ("job", "api")], &[(10, 20)]),
            frozen(
                "a",
                &[("__name__", "up"), ("job", "db")],
                &[(5, 8), (30, 40)],
            ),
            frozen("a", &[("__name__", "up"), ("job", "api")], &[(1, 2)]),
            frozen("a", &[("__name__", "down"), ("job", "api")], &[(50, 60)]),
        ];
        for block in [ColdBlock::build(None, 1, series).unwrap(), {
            let series = vec![
                frozen("b", &[("__name__", "up"), ("job", "api")], &[(10, 20)]),
                frozen(
                    "a",
                    &[("__name__", "up"), ("job", "db")],
                    &[(5, 8), (30, 40)],
                ),
                frozen("a", &[("__name__", "up"), ("job", "api")], &[(1, 2)]),
                frozen("a", &[("__name__", "down"), ("job", "api")], &[(50, 60)]),
            ];
            ColdBlock::build(Some(&directory), 2, series).unwrap();
            ColdBlock::open(&block_path(&directory, 2), 2).unwrap()
        }] {
            assert_eq!((block.min_time, block.max_time), (1, 60));
            assert_eq!(block.series_count("a"), 3);
            assert!(block.overlaps("a", 35, 45) && !block.overlaps("b", 35, 45));
            let labels = |indexes: Vec<u32>| {
                indexes
                    .into_iter()
                    .map(|index| format!("{:?}", block.series("a", index as usize).labels()))
                    .collect::<Vec<_>>()
            };
            // Sorted by labels.
            assert_eq!(
                labels((0..3).collect()),
                [
                    r#"[("__name__", "down"), ("job", "api")]"#,
                    r#"[("__name__", "up"), ("job", "api")]"#,
                    r#"[("__name__", "up"), ("job", "db")]"#,
                ]
            );
            let equal = |name: &str, value: &str| CompiledMatcher::Equal(name.into(), value.into());
            assert_eq!(
                labels(block.candidates("a", &[equal("__name__", "up"), equal("job", "db")])),
                [r#"[("__name__", "up"), ("job", "db")]"#]
            );
            assert!(block.candidates("a", &[equal("job", "missing")]).is_empty());
            assert!(block.candidates("a", &[equal("unknown", "x")]).is_empty());
            let db = block.series("a", 2);
            assert_eq!(
                db.chunks()
                    .map(|chunk| (chunk.min_time, chunk.max_time))
                    .collect::<Vec<_>>(),
                [(5, 8), (30, 40)]
            );
            assert!(db.has_data(9, 31) && !db.has_data(9, 29));
        }
        let path = block_path(&directory, 2);
        let mut corrupt = fs::read(&path).unwrap();
        corrupt[20] ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(ColdBlock::open(&path, 2).is_err());
        fs::remove_dir_all(directory).unwrap();
    }
}
