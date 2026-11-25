//! Series that left the emulated head, in immutable files like the Go ingester's blocks: their
//! labels, chunk references and postings stay on disk, memory-mapped, so millions of series that
//! only queries of older ranges read cost no heap. The chunks themselves stay in the chunk files.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, bail, ensure};
use memmap2::Mmap;

use super::*;

const MAGIC: &[u8; 8] = b"MIMIRCB2";
const V1_MAGIC: &[u8; 8] = b"MIMIRCB1";

/// A series leaving the head, with every chunk it had in the chunk files.
pub(super) struct Frozen {
    pub tenant: String,
    pub labels: StoredLabels,
    pub chunks: ChunkList,
    pub native_histogram: bool,
    pub idle_evicted: bool,
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
    // Each series' `labels.StableHash`, by which sharded queries pick series: computed on the
    // first sharded query rather than from the labels for every series of every one.
    shard_hashes: std::sync::OnceLock<Box<[u64]>>,
    // Each metric name, sorted, with its series: built on the first query by a name matcher other
    // than an equality, from the series' own labels, since the postings only hold values' hashes.
    metric_names: std::sync::OnceLock<MetricNames>,
    // The series with each label queries asked for, from `with_label`: the block never changes.
    with_label: std::sync::Mutex<HashMap<String, std::sync::Arc<[u32]>>>,
    // The series of the names each name matcher other than an equality accepted.
    name_matches: std::sync::Mutex<HashMap<NameMatcherKey, std::sync::Arc<[u32]>>>,
    values: Mutex<HashMap<&'static str, Arc<OnceLock<Option<ValueIndex>>>>>,
}

// Each metric name, sorted, with the indexes of its series.
type MetricNames = Box<[(Box<str>, Box<[u32]>)]>;
type ValueIndex = BTreeMap<Box<str>, Vec<u32>>;
// Labels such as pod have reusable dictionaries. An ID per series for several
// distinct labels would make data that belongs in mmap resident in the heap.
const MAX_VALUE_INDEX_NAMES: usize = 8;
const MAX_VALUE_INDEX_BYTES: usize = 2 << 20;

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
    oldest_chunks: OnceLock<(i64, i64, u64)>,
    chunk_files: OnceLock<HashSet<u32>>,
    has_idle: bool,
    value_index_bytes: AtomicUsize,
}

/// A series of a cold block.
pub(super) struct ColdSeries<'a> {
    block: &'a ColdBlock,
    labels: &'a [u8],
    chunks: &'a [u8],
    pub(super) native_histogram: bool,
    pub(super) idle_evicted: bool,
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
        out.push(u8::from(series.iter().any(|entry| entry.idle_evicted)));
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
                out.push(u8::from(frozen.native_histogram) | u8::from(frozen.idle_evicted) << 1);
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
        ensure!(
            &body[..8] == MAGIC || &body[..8] == V1_MAGIC,
            "cold block has invalid magic"
        );
        if crc32fast::hash(body) != u32::from_le_bytes(checksum.try_into().unwrap()) {
            bail!("cold block checksum mismatch");
        }
        let version_two = &body[..8] == MAGIC;
        let mut cursor = &body[8..];
        let has_idle = if version_two {
            let (&flag, rest) = cursor.split_first().context("cold block missing flags")?;
            cursor = rest;
            flag != 0
        } else {
            false
        };
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
                shard_hashes: std::sync::OnceLock::new(),
                metric_names: std::sync::OnceLock::new(),
                with_label: std::sync::Mutex::default(),
                name_matches: std::sync::Mutex::default(),
                values: Mutex::default(),
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
            oldest_chunks: OnceLock::new(),
            chunk_files: OnceLock::new(),
            has_idle,
            value_index_bytes: AtomicUsize::new(0),
        })
    }

    pub fn drop_expired(
        &self,
        retained: Retained,
        cutoffs: &HashMap<String, i64>,
    ) -> Option<Vec<Frozen>> {
        let latest_cutoff = self
            .tenants
            .keys()
            .filter_map(|id| cutoffs.get(id))
            .copied()
            .max()
            .unwrap_or(retained.cutoff)
            .max(retained.cutoff);
        if retained.ooo_from.unwrap_or(0) == 0 && self.min_time >= latest_cutoff {
            return None;
        }
        let &(oldest_end, oldest_in_order, oldest_ooo) = self.oldest_chunks.get_or_init(|| {
            let (mut oldest_end, mut oldest_in_order, mut oldest_ooo) =
                (i64::MAX, i64::MAX, u64::MAX);
            for (_, series) in self.entries() {
                for chunk in series.chunks() {
                    oldest_end = oldest_end.min(chunk.max_time);
                    if chunk.out_of_order {
                        oldest_ooo = oldest_ooo.min(chunk.reference);
                    } else {
                        oldest_in_order = oldest_in_order.min(chunk.max_time);
                    }
                }
            }
            (oldest_end, oldest_in_order, oldest_ooo)
        });
        let unchanged = match retained.ooo_from {
            Some(from) => oldest_in_order >= latest_cutoff && oldest_ooo >= from,
            None => oldest_end >= latest_cutoff,
        };
        if unchanged {
            return None;
        }
        let mut changed = false;
        let kept = self
            .entries()
            .filter_map(|(tenant, series)| {
                let retained = Retained {
                    cutoff: retained
                        .cutoff
                        .max(cutoffs.get(tenant).copied().unwrap_or(i64::MIN)),
                    ..retained
                };
                let chunks: Vec<_> = series
                    .chunks()
                    .filter(|chunk| {
                        let keep = retained.includes(chunk);
                        changed |= !keep;
                        keep
                    })
                    .collect();
                (!chunks.is_empty()).then(|| Frozen {
                    tenant: tenant.clone(),
                    labels: series.labels(),
                    chunks: ChunkList::from_metas(&chunks),
                    native_histogram: series.native_histogram,
                    idle_evicted: series.idle_evicted,
                })
            })
            .collect();
        changed.then_some(kept)
    }

    pub fn chunk_files(&self) -> impl Iterator<Item = u32> + '_ {
        self.chunk_files
            .get_or_init(|| {
                let mut files = HashSet::new();
                for (_, series) in self.entries() {
                    files.extend(series.chunks().map(|chunk| (chunk.reference >> 32) as u32));
                }
                files
            })
            .iter()
            .copied()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, ColdSeries<'_>)> {
        self.tenants.iter().flat_map(move |(tenant, table)| {
            (0..table.series_count).map(move |id| (tenant, self.series_in(table, id)))
        })
    }

    pub fn has_idle(&self) -> bool {
        self.has_idle
    }

    pub fn size_bytes(&self) -> usize {
        self.bytes().len()
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
        self.series_in(&self.tenants[tenant], index)
    }

    fn series_in<'a>(&'a self, table: &TenantIndex, index: usize) -> ColdSeries<'a> {
        let at = table.series_table + index * 8;
        let offset = u64::from_le_bytes(self.bytes()[at..at + 8].try_into().unwrap()) as usize;
        let mut cursor = &self.bytes()[offset..];
        let labels_len = take_varint(&mut cursor) as usize;
        let (labels, rest) = cursor.split_at(labels_len);
        let mut cursor = rest;
        let chunks_len = take_varint(&mut cursor) as usize;
        let (chunks, rest) = cursor.split_at(chunks_len);
        ColdSeries {
            block: self,
            labels,
            chunks,
            native_histogram: rest[0] & 1 != 0,
            idle_evicted: rest[0] & 2 != 0,
        }
    }

    /// The tenant's series here, its table looked up once rather than for each series.
    pub fn tenant(&self, tenant: &str) -> Option<ColdTenant<'_>> {
        self.tenants
            .get(tenant)
            .map(|table| ColdTenant { block: self, table })
    }

    /// Indexes of the tenant's series with label `name`, from every posting list under it: the
    /// table is sorted by name, so they are one range of it.
    pub fn with_label(&self, tenant: &str, name: &str) -> std::sync::Arc<[u32]> {
        let Some(table) = self.tenants.get(tenant) else {
            return std::sync::Arc::from([]);
        };
        let mut cache = table.with_label.lock().expect("with label lock poisoned");
        if let Some(series) = cache.get(name) {
            return std::sync::Arc::clone(series);
        }
        #[cfg(test)]
        COLD_LABEL_LISTS.with(|lists| lists.set(lists.get() + 1));
        let series: std::sync::Arc<[u32]> = self.with_label_uncached(table, name).into();
        cache.insert(name.to_owned(), std::sync::Arc::clone(&series));
        series
    }

    fn with_label_uncached(&self, table: &TenantIndex, name: &str) -> Vec<u32> {
        let Some(local) = self.local_ids.get(name) else {
            return Vec::new();
        };
        let bytes = self.bytes();
        let entry = |index: usize| {
            let at = table.postings_table + index * 16;
            (
                u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()),
                u32::from_le_bytes(bytes[at + 12..at + 16].try_into().unwrap()),
            )
        };
        let (mut low, mut high) = (0, table.postings_count);
        while low < high {
            let middle = (low + high) / 2;
            if entry(middle).0 < *local {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let lists = table.postings_table + table.postings_count * 16;
        let mut series = Vec::new();
        for index in low..table.postings_count {
            let (entry_name, offset) = entry(index);
            if entry_name != *local {
                break;
            }
            let at = lists + offset as usize;
            let count = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            series.extend((0..count).map(|index| {
                let at = at + 4 + index * 4;
                u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
            }));
        }
        series.sort_unstable();
        series.dedup();
        series
    }

    /// The tenant's series whose name `matcher`, a name matcher other than an equality, accepts,
    /// sorted: once per block and matcher, since the block never changes.
    fn name_matches(&self, tenant: &str, matcher: &CompiledMatcher) -> std::sync::Arc<[u32]> {
        let Some(table) = self.tenants.get(tenant) else {
            return std::sync::Arc::from([]);
        };
        let names = self.metric_names(tenant);
        if let CompiledMatcher::Regex(_, regex) = matcher
            && let Some(values) = regex.values()
        {
            let mut series = values
                .iter()
                .filter_map(|value| names.binary_search_by(|(name, _)| (**name).cmp(value)).ok())
                .flat_map(|index| names[index].1.iter().copied())
                .collect::<Vec<_>>();
            series.sort_unstable();
            return series.into();
        }
        let key = name_matcher_key(matcher);
        if let Some(series) = table
            .name_matches
            .lock()
            .expect("name matches lock poisoned")
            .get(&key)
        {
            return std::sync::Arc::clone(series);
        }
        #[cfg(test)]
        COLD_NAME_MATCHES.with(|matches| matches.set(matches.get() + 1));
        let mut series = names
            .iter()
            .filter(|(name, _)| matcher.matches_value(name))
            .flat_map(|(_, series)| series.iter().copied())
            .collect::<Vec<_>>();
        series.sort_unstable();
        let series: std::sync::Arc<[u32]> = series.into();
        let mut cache = table
            .name_matches
            .lock()
            .expect("name matches lock poisoned");
        // Bounded, so arbitrary queries can't grow it.
        if cache.len() >= 256 {
            cache.clear();
        }
        cache.insert(key, std::sync::Arc::clone(&series));
        series
    }

    /// The tenant's metric names, sorted, each with its series.
    fn metric_names(&self, tenant: &str) -> &[(Box<str>, Box<[u32]>)] {
        let Some(table) = self.tenants.get(tenant) else {
            return &[];
        };
        table.metric_names.get_or_init(|| {
            let mut by_name: HashMap<String, Vec<u32>> = HashMap::new();
            for index in 0..table.series_count {
                let series = self.series_in(table, index);
                let mut pairs = series.pairs();
                let name = pairs
                    .find(|(label, _)| *label == "__name__")
                    .map_or("", |(_, value)| value);
                match by_name.get_mut(name) {
                    Some(list) => list.push(index as u32),
                    None => {
                        by_name.insert(name.to_owned(), vec![index as u32]);
                    }
                }
            }
            let mut names = by_name
                .into_iter()
                .map(|(name, series)| (name.into_boxed_str(), series.into_boxed_slice()))
                .collect::<Vec<_>>();
            names.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            names.into_boxed_slice()
        })
    }

    /// The series with `name="value"`, in series order.
    fn posting(&self, tenant: &str, name: &str, value: &str) -> Vec<u32> {
        posting_ids(self.posting_bytes(tenant, name, value)).collect()
    }

    fn posting_bytes(&self, tenant: &str, name: &str, value: &str) -> &[u8] {
        let (Some(table), Some(local)) = (self.tenants.get(tenant), self.local_ids.get(name))
        else {
            return &[];
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
            return &[];
        }
        let (name, hash, offset) = entry(low);
        if (name, hash) != key {
            return &[];
        }
        let lists = table.postings_table + table.postings_count * 16;
        let at = lists + offset as usize;
        let count = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        &bytes[at + 4..at + 4 + count * 4]
    }

    fn union_postings(&self, tenant: &str, name: &str, values: &[Box<str>]) -> Vec<u32> {
        let lists: SmallVec<[&[u8]; 8]> = values
            .iter()
            .map(|value| self.posting_bytes(tenant, name, value))
            .filter(|list| !list.is_empty())
            .collect();
        match lists.as_slice() {
            [] => return Vec::new(),
            [list] => {
                let mut ids: Vec<_> = posting_ids(list).collect();
                ids.dedup();
                return ids;
            }
            _ => {}
        }

        let series_count = self.series_count(tenant);
        let postings_count = lists.iter().map(|list| list.len() / 4).sum::<usize>();
        // For dense unions, a bitset avoids sorting interleaved IDs. Skip it when
        // scanning the whole ID space would cost more than sorting a short list.
        if postings_count >= series_count.div_ceil(8) {
            let mut bits = vec![0_u64; series_count.div_ceil(64)];
            for id in lists.iter().flat_map(|list| posting_ids(list)) {
                bits[id as usize / 64] |= 1 << (id % 64);
            }
            let mut ids = Vec::with_capacity(postings_count.min(series_count));
            for (word, mut bits) in bits.into_iter().enumerate() {
                while bits != 0 {
                    ids.push(word as u32 * 64 + bits.trailing_zeros());
                    bits &= bits - 1;
                }
            }
            ids
        } else {
            let mut ids = Vec::with_capacity(postings_count);
            ids.extend(lists.iter().flat_map(|list| posting_ids(list)));
            ids.sort_unstable();
            ids.dedup();
            ids
        }
    }

    fn value_index(
        &self,
        tenant: &str,
        table: &TenantIndex,
        name: &'static str,
    ) -> Option<Arc<OnceLock<Option<ValueIndex>>>> {
        let cache = {
            let mut values = table.values.lock().expect("value index lock poisoned");
            if let Some(index) = values.get(name) {
                index.clone()
            } else {
                if values.len() >= MAX_VALUE_INDEX_NAMES {
                    return None;
                }
                let index = Arc::new(OnceLock::new());
                values.insert(name, index.clone());
                index
            }
        };
        cache.get_or_init(|| {
            let mut index = ValueIndex::new();
            let mut remaining = MAX_VALUE_INDEX_BYTES;
            for &id in self.with_label(tenant, name).iter() {
                let series = self.series_in(table, id as usize);
                let (_, value) = series.pairs().find(|(label, _)| *label == name)?;
                remaining = remaining.checked_sub(size_of::<u32>())?;
                if let Some(ids) = index.get_mut(value) {
                    ids.push(id);
                } else {
                    remaining = remaining.checked_sub(value.len() + 64)?;
                    index.insert(value.into(), vec![id]);
                }
            }
            let used = MAX_VALUE_INDEX_BYTES - remaining;
            self.value_index_bytes
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                    (total + used <= MAX_VALUE_INDEX_BYTES).then_some(total + used)
                })
                .ok()?;
            Some(index)
        });
        Some(cache)
    }

    pub fn collect_label_values(
        &self,
        tenant_id: &str,
        name: &str,
        visible: impl Fn(&ColdSeries<'_>) -> bool,
        matchers: &[CompiledMatcher],
        limit: usize,
        result: &mut BTreeSet<String>,
    ) {
        let (Some(tenant), Some(&name)) = (
            self.tenants.get(tenant_id),
            self.local_ids.get_key_value(name).map(|(name, _)| name),
        ) else {
            return;
        };
        // Seek known values before building a dictionary, including a regex
        // accepting a small finite set under the label being returned.
        let selected = matchers
            .iter()
            .filter_map(|matcher| match matcher {
                CompiledMatcher::Equal(label, value) if label == name => Some(vec![value.as_str()]),
                CompiledMatcher::Regex(label, regex) if label == name => regex
                    .values()
                    .map(|values| values.iter().map(|v| v.as_ref()).collect()),
                _ => None,
            })
            .min_by_key(Vec::len);
        let accepts = |series: &ColdSeries<'_>| visible(series) && matches(series, matchers);
        if let Some(selected) = selected {
            for value in selected {
                if !result.contains(value)
                    && self.posting(tenant_id, name, value).into_iter().any(|id| {
                        let series = self.series_in(tenant, id as usize);
                        // Posting keys are hashes and can collide.
                        series
                            .pairs()
                            .any(|(label, stored)| label == name && stored == value)
                            && accepts(&series)
                    })
                {
                    result.insert(value.to_owned());
                }
            }
            return;
        }

        if matchers
            .iter()
            .all(|matcher| matcher.label_name() == Some(name))
            && let Some(cache) = self.value_index(tenant_id, tenant, name)
            && let Some(Some(index)) = cache.get()
        {
            let mut found = 0;
            for (value, ids) in index {
                if limit != 0 && found == limit {
                    break;
                }
                if !matchers.iter().all(|matcher| matcher.matches_value(value)) {
                    continue;
                }
                if limit == 0 && result.contains(value.as_ref()) {
                    continue;
                }
                if ids
                    .iter()
                    .any(|&id| visible(&self.series_in(tenant, id as usize)))
                {
                    result.insert(value.to_string());
                    found += 1;
                }
            }
            return;
        }

        let candidates = (!matchers.is_empty()).then(|| self.candidates(tenant_id, matchers));
        // A handful of matching series is cheaper to inspect than collecting every
        // posting for the returned label, especially on the first lookup of a block.
        let with_label;
        let ids = match &candidates {
            Some(ids) if ids.len() <= 64 || ids.len() < tenant.series_count / 8 => ids.as_slice(),
            _ => {
                with_label = self.with_label(tenant_id, name);
                match &candidates {
                    Some(ids) if ids.len() < with_label.len() => ids.as_slice(),
                    _ => &with_label,
                }
            }
        };
        for &id in ids {
            let series = self.series_in(tenant, id as usize);
            if let Some((_, value)) = series.pairs().find(|(label, _)| *label == name)
                && !result.contains(value)
                && accepts(&series)
            {
                result.insert(value.to_owned());
            }
        }
    }

    /// Indexes of the tenant's series that may match `matchers`: the smallest posting list of
    /// their equality matchers or, without one, of the series with the label of a matcher that
    /// rejects the empty value, or every series. Callers still check the matchers.
    pub fn candidates(&self, tenant: &str, matchers: &[CompiledMatcher]) -> Vec<u32> {
        let mut best: Option<Vec<u32>> = None;
        for matcher in matchers {
            let list = match matcher {
                CompiledMatcher::Equal(name, value) if !value.is_empty() => {
                    self.posting(tenant, name, value)
                }
                // A regex that accepts few values, not the empty one, takes their postings.
                CompiledMatcher::Regex(name, regex) if !regex.is_match("") => {
                    let Some(values) = regex.values() else {
                        continue;
                    };
                    self.union_postings(tenant, name, values)
                }
                _ => continue,
            };
            if list.is_empty() {
                return list;
            }
            best = Some(match best {
                Some(previous)
                    if previous.len().max(list.len()) <= previous.len().min(list.len()) * 4 =>
                {
                    intersect_postings(previous, &list)
                }
                Some(previous) if previous.len() <= list.len() => previous,
                _ => list,
            });
            if best.as_ref().is_some_and(Vec::is_empty) {
                break;
            }
        }
        if best.is_none() {
            // A name regex takes the series of the names it accepts, each name checked once.
            for matcher in matchers {
                if matcher.label_name() == Some("__name__")
                    && !matches!(matcher, CompiledMatcher::Equal(..))
                {
                    let list = self.name_matches(tenant, matcher).to_vec();
                    if best.as_ref().is_none_or(|best| list.len() < best.len()) {
                        best = Some(list);
                    }
                }
            }
            for matcher in matchers {
                if let Some(name) = matcher.label_name()
                    && name != "__name__"
                    && !matches!(matcher, CompiledMatcher::Equal(..))
                    && !matcher.matches_value("")
                {
                    let list = self.with_label(tenant, name).to_vec();
                    if best.as_ref().is_none_or(|best| list.len() < best.len()) {
                        best = Some(list);
                    }
                }
            }
        }
        best.unwrap_or_else(|| (0..self.series_count(tenant) as u32).collect())
    }
}

fn posting_ids(bytes: &[u8]) -> impl ExactSizeIterator<Item = u32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .copied()
        .map(u32::from_le_bytes)
}

fn intersect_postings(mut left: Vec<u32>, right: &[u32]) -> Vec<u32> {
    let mut right = right.iter().copied().peekable();
    left.retain(|&id| {
        while right.peek().is_some_and(|&candidate| candidate < id) {
            right.next();
        }
        right.peek() == Some(&id)
    });
    left
}

/// One tenant's series of a cold block.
#[derive(Clone, Copy)]
pub(super) struct ColdTenant<'a> {
    block: &'a ColdBlock,
    table: &'a TenantIndex,
}

impl<'a> ColdTenant<'a> {
    pub fn block_id(&self) -> u64 {
        self.block.id
    }

    pub fn series_count(&self) -> usize {
        self.table.series_count
    }

    /// See `ColdBlock::with_label`.
    pub fn with_label(&self, tenant: &str, name: &str) -> std::sync::Arc<[u32]> {
        self.block.with_label(tenant, name)
    }

    pub fn series(&self, index: usize) -> ColdSeries<'a> {
        self.block.series_in(self.table, index)
    }

    /// The query shard hash of the series at `index`.
    pub fn shard_hash(&self, index: usize) -> u64 {
        self.table.shard_hashes.get_or_init(|| {
            #[cfg(test)]
            COLD_SHARD_HASHINGS.with(|hashings| hashings.set(hashings.get() + 1));
            (0..self.table.series_count)
                .map(|index| stable_hash_set(&self.series(index)))
                .collect()
        })[index]
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
            idle_evicted: false,
        }
    }

    #[test]
    fn regex_posting_union_returns_sorted_unique_ids() {
        let entries = (0..256)
            .map(|id| {
                let instance = format!("{id:03}");
                let pod = format!("pod-{:02}", id % 64);
                frozen("a", &[("instance", &instance), ("pod", &pod)], &[(1, 2)])
            })
            .collect();
        let block = ColdBlock::build(None, 0, entries).unwrap();
        for count in [1, 2, 8, 9, 64] {
            let mut values: Vec<Box<str>> =
                (0..count).map(|id| format!("pod-{id:02}").into()).collect();
            assert_eq!(
                block.union_postings("a", "pod", &values),
                (0..256).filter(|id| id % 64 < count).collect::<Vec<_>>()
            );
            values.extend(["absent".into(), "pod-00".into()]);
            assert_eq!(
                block.union_postings("a", "pod", &values),
                (0..256).filter(|id| id % 64 < count).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn limited_label_values_use_ordered_visible_postings() {
        let block = ColdBlock::build(
            None,
            0,
            vec![
                frozen("a", &[("__name__", "up"), ("job", "aa-old")], &[(1, 2)]),
                frozen("a", &[("__name__", "up"), ("job", "api")], &[(14, 20)]),
                frozen("a", &[("__name__", "db"), ("job", "api")], &[(1, 2)]),
                frozen("a", &[("__name__", "up"), ("job", "batch")], &[(14, 20)]),
                frozen("a", &[("__name__", "up"), ("job", "cron")], &[(14, 20)]),
            ],
        )
        .unwrap();
        let mut values = BTreeSet::new();
        block.collect_label_values(
            "a",
            "job",
            |series| series.has_data(10, 30),
            &[CompiledMatcher::Equal("job".into(), "batch".into())],
            1,
            &mut values,
        );
        assert_eq!(values, BTreeSet::from(["batch".into()]));
        assert!(block.tenants["a"].values.lock().unwrap().is_empty());
        values.clear();
        block.collect_label_values(
            "a",
            "job",
            |series| series.has_data(10, 30),
            &[CompiledMatcher::Equal("__name__".into(), "up".into())],
            0,
            &mut values,
        );
        assert_eq!(
            values,
            BTreeSet::from(["api".into(), "batch".into(), "cron".into()])
        );
        assert!(block.tenants["a"].with_label.lock().unwrap().is_empty());
        values.clear();
        block.collect_label_values(
            "a",
            "job",
            |series| series.has_data(10, 30),
            &[],
            2,
            &mut values,
        );
        assert_eq!(values, BTreeSet::from(["api".into(), "batch".into()]));
        assert!(
            block.tenants["a"].values.lock().unwrap()["job"]
                .get()
                .is_some()
        );
        values.clear();
        block.collect_label_values(
            "a",
            "job",
            |series| series.has_data(10, 30),
            &[CompiledMatcher::Equal("__name__".into(), "db".into())],
            0,
            &mut values,
        );
        assert!(values.is_empty());
    }

    #[test]
    fn cold_merge_and_retention_keep_chunk_order_and_pinned_readers() {
        let mut cold = ColdState::default();
        for id in 0..7 {
            let time = id as i64 * 10;
            cold.blocks.push(Arc::new(
                ColdBlock::build(
                    None,
                    id,
                    vec![
                        frozen("tenant", &[("__name__", "up")], &[(time, time + 9)]),
                        frozen("tenant", &[("__name__", "old")], &[(0, 1)]),
                    ],
                )
                .unwrap(),
            ));
        }
        cold.next_id = 7;
        cold.merge_small_blocks().unwrap();
        assert_eq!(cold.blocks.len(), 4);
        let before: Vec<_> = cold
            .blocks
            .iter()
            .flat_map(|block| {
                block
                    .entries()
                    .filter(|(_, series)| series.pairs().any(|(_, value)| value == "up"))
                    .flat_map(|(_, series)| series.chunks().collect::<Vec<_>>())
                    .map(|chunk| chunk.min_time)
            })
            .collect();
        assert_eq!(before, [0, 10, 20, 30, 40, 50, 60]);
        let pinned = cold.blocks.clone();
        cold.prune_retained(Retained {
            cutoff: 25,
            ooo_from: None,
        })
        .unwrap();
        for block in &cold.blocks {
            for (_, series) in block.entries() {
                assert!(series.pairs().any(|(_, value)| value == "up"));
                assert!(series.chunks().all(|chunk| chunk.max_time >= 25));
            }
        }
        assert!(
            pinned
                .iter()
                .flat_map(|block| block.entries())
                .any(|(_, series)| series.has_data(0, 1))
        );
        let next_id = cold.next_id;
        cold.prune_retained(Retained {
            cutoff: 26,
            ooo_from: None,
        })
        .unwrap();
        assert_eq!(
            cold.next_id, next_id,
            "a chunk crossing the cutoff stays without rewrites"
        );
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
