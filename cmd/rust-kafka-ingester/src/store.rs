use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use hashbrown::HashTable;
use prost::Message;
use prost::bytes::Bytes;
use rayon::prelude::*;
use regex::Regex;

use crate::chunk_disk::{ChunkDiskMapper, ChunkRef};
use crate::exemplars::{self, Rejection, TenantExemplars};
use crate::limits::{Limits, Overrides};
use crate::metrics;
use crate::ooo_merge;
use crate::proto::{cortex, cortexpb};
use crate::record::{DecodedRequest, DecodedSeries};
use crate::trackers::OVERFLOW_VALUE;
use crate::{histogram, xor};

type StoredLabel = (Arc<str>, CompactString);
type StoredLabels = Vec<StoredLabel>;
// Compare the fingerprint first so ingest does not compare every label at each tree level.
type SeriesKey = (u64, Arc<StoredLabels>);

const XOR_ENCODING: u8 = 4;
const SAMPLES_PER_CHUNK: usize = 120;
const CHUNK_RANGE_MS: i64 = 2 * 60 * 60 * 1000;
const OUT_OF_ORDER_CAPACITY: usize = 32;
const TARGET_BYTES_PER_HISTOGRAM_CHUNK: usize = 1024;
const MIN_SAMPLES_PER_HISTOGRAM_CHUNK: usize = 10;

/// A completed chunk in the chunk disk mapper, like Prometheus's `mmappedChunk`.
#[derive(Clone, Copy, Debug)]
struct ChunkMeta {
    reference: ChunkRef,
    min_time: i64,
    max_time: i64,
    len: u32,
    encoding: u8,
    // Out-of-order chunks sort after in-order ones with the same min time when merging.
    out_of_order: bool,
}

#[derive(Debug)]
struct FloatHead {
    appender: xor::Appender,
    min_time: i64,
    next_at: i64,
}

// Only open chunks live on the heap; completed chunks are referenced in the chunk disk mapper.
#[derive(Debug, Default)]
struct Series {
    chunks: Vec<ChunkMeta>,
    float_head: Option<FloatHead>,
    histogram_head: HistogramHead,
    histogram_next_at: i64,
    // Whether `histogram_next_at` was already estimated from the chunk's fill rate.
    histogram_end_computed: bool,
    // The open out-of-order chunk, like Prometheus's OOO head chunk, sorted by timestamp.
    out_of_order: Vec<(i64, ooo_merge::Value)>,
    last_ingested_ms: i64,
    last_bucket_count: u32,
    // Whether the last request's samples ended with a native histogram, as Mimir's active series
    // tracker records it.
    native_histogram: bool,
    // Whether the series is in the emulated Go head window, as of the last head tick.
    in_head: bool,
    // Whether Mimir's owned series recompute cleared the tenant's active series (it owned no
    // ranges) since this series' last sample. Unlike deleting one series, clearing leaves the
    // cost attribution counts in place.
    active_cleared: bool,
    // Evicted from the emulated head as non-owned, like Mimir's early compaction of non-owned
    // series, until its next sample; its data stays queryable as a compacted block's.
    head_evicted: bool,
    // When an owned series recompute first found it non-owned, in Unix seconds, or 0.
    non_owned_since_s: u32,
    // Mimir's `ShardByAllLabels`, which decides which partition owns the series.
    owned_hash: u32,
    // Custom trackers this series matches, computed for one overrides generation.
    tracker_generation: u64,
    tracker_matches: Box<[u16]>,
}

impl Series {
    fn is_active(&self, cutoff: i64) -> bool {
        !self.active_cleared && self.last_ingested_ms >= cutoff
    }
}

// The open histogram chunk, kept encoded like Prometheus's head chunk. Boxed because most series
// are floats, and every series pays for what the store keeps inline.
#[derive(Debug, Default)]
struct HistogramHead(Option<Box<histogram::HistogramAppender>>);

impl HistogramHead {
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn last(&self) -> Option<&cortexpb::Histogram> {
        self.0.as_deref().map(histogram::HistogramAppender::last)
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.0
            .as_deref()
            .map(histogram::HistogramAppender::first_timestamp)
    }

    /// Rebuilds the open chunk from its samples, the first carrying the chunk header as its hint.
    fn from_samples(samples: Vec<cortexpb::Histogram>) -> Self {
        let Some(first) = samples.first() else {
            return Self(None);
        };
        let mut appender = histogram::HistogramAppender::empty(
            matches!(first.count, Some(cortexpb::histogram::Count::CountFloat(_))),
            histogram::Header::from_hint(first.reset_hint),
        );
        for sample in samples {
            appender.raw_append(sample);
        }
        Self(Some(Box::new(appender)))
    }

    fn clear(&mut self) {
        self.0 = None;
    }

    fn take(&mut self) -> Option<histogram::HistogramAppender> {
        self.0.take().map(|head| *head)
    }

    fn encoded(&self) -> Option<histogram::EncodedHistogram> {
        self.0.as_deref().map(histogram::HistogramAppender::encoded)
    }

    /// The samples of the open chunk for [`HistogramHead::from_samples`].
    fn decoded(&self) -> Vec<cortexpb::Histogram> {
        let Some(appender) = &self.0 else {
            return Vec::new();
        };
        let encoded = appender.encoded();
        let mut histograms =
            histogram::decode(encoded.encoding, &encoded.data).expect("decode own histogram chunk");
        if let Some(first) = histograms.first_mut() {
            first.reset_hint = appender.header().hint();
        }
        histograms
    }
}

// Series grouped by metric name, so a `__name__` matcher only visits its own metric, plus
// postings for the other labels like the Go head index, so a selective equality matcher only
// visits its series. Within a name, series are found by label hash so ingesting into an existing
// series allocates nothing.
#[derive(Default)]
struct SeriesByName {
    names: HashMap<CompactString, u32>,
    groups: Vec<HashTable<(SeriesKey, Series)>>,
    // Label name, then value, to the (name group, label hash) of every series with that label.
    postings: HashMap<Arc<str>, HashMap<CompactString, PostingList>>,
    len: usize,
}

impl SeriesByName {
    fn get_or_insert_with(
        &mut self,
        name: &str,
        hash: u64,
        is_same: impl Fn(&StoredLabels) -> bool,
        labels: impl FnOnce() -> Arc<StoredLabels>,
    ) -> (&Arc<StoredLabels>, &mut Series) {
        let group = match self.names.get(name) {
            Some(group) => *group,
            None => {
                let group = self.groups.len() as u32;
                self.names.insert(CompactString::from(name), group);
                self.groups.push(HashTable::new());
                group
            }
        };
        let table = &mut self.groups[group as usize];
        let len = &mut self.len;
        let postings = &mut self.postings;
        match table.entry(
            hash,
            |((entry_hash, entry_labels), _)| *entry_hash == hash && is_same(entry_labels),
            |((entry_hash, _), _)| *entry_hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(entry) => {
                let ((_, labels), series) = entry.into_mut();
                (&*labels, series)
            }
            hashbrown::hash_table::Entry::Vacant(entry) => {
                *len += 1;
                let labels = labels();
                add_postings(postings, &labels, group, hash);
                let ((_, labels), series) =
                    entry.insert(((hash, labels), Series::default())).into_mut();
                (&*labels, series)
            }
        }
    }

    fn insert(&mut self, key: SeriesKey, series: Series) -> bool {
        let (hash, labels) = key;
        let mut series = Some(series);
        let inserted = std::cell::Cell::new(false);
        let (_, stored) = self.get_or_insert_with(
            metric_name(&labels),
            hash,
            |existing| existing == labels.as_ref(),
            || {
                inserted.set(true);
                Arc::clone(&labels)
            },
        );
        if inserted.get() {
            *stored = series.take().expect("series inserted once");
        }
        inserted.get()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> impl Iterator<Item = (&SeriesKey, &Series)> {
        self.groups
            .iter()
            .flat_map(|table| table.iter().map(|(key, series)| (key, series)))
    }

    fn values(&self) -> impl Iterator<Item = &Series> {
        self.iter().map(|(_, series)| series)
    }

    fn matching<'a>(
        &'a self,
        matchers: &'a [CompiledMatcher],
    ) -> Box<dyn Iterator<Item = (&'a SeriesKey, &'a Series)> + 'a> {
        let name_matcher = matchers
            .iter()
            .find(|matcher| matcher.label_name() == Some("__name__"));
        let name_group = match name_matcher {
            Some(CompiledMatcher::Equal(_, name)) => match self.names.get(name.as_str()) {
                Some(group) => Some(*group),
                None => return Box::new(std::iter::empty()),
            },
            _ => None,
        };
        // The smallest posting list of an equality matcher, when smaller than the name group.
        let mut best: Option<&PostingList> = None;
        for matcher in matchers {
            if let CompiledMatcher::Equal(label, value) = matcher
                && label != "__name__"
                && !value.is_empty()
            {
                let Some(list) = self
                    .postings
                    .get(label.as_str())
                    .and_then(|values| values.get(value.as_str()))
                else {
                    return Box::new(std::iter::empty());
                };
                if best.is_none_or(|best| list.len() < best.len()) {
                    best = Some(list);
                }
            }
        }
        let group_len = name_group.map(|group| self.groups[group as usize].len());
        let candidates: Box<dyn Iterator<Item = (&SeriesKey, &Series)>> = match (best, name_group) {
            (Some(list), _) if group_len.is_none_or(|len| list.len() < len) => {
                let mut refs = list.as_slice().to_vec();
                refs.sort_unstable();
                refs.dedup();
                Box::new(refs.into_iter().flat_map(move |(group, hash)| {
                    self.groups[group as usize]
                        .iter_hash(hash)
                        .filter(move |((entry_hash, _), _)| *entry_hash == hash)
                        .map(|(key, series)| (key, series))
                }))
            }
            (_, Some(group)) => Box::new(
                self.groups[group as usize]
                    .iter()
                    .map(|(key, series)| (key, series)),
            ),
            (_, None) => match name_matcher {
                Some(matcher) => Box::new(
                    self.names
                        .iter()
                        .filter(move |(name, _)| matcher.matches_value(name))
                        .flat_map(move |(_, group)| {
                            self.groups[*group as usize]
                                .iter()
                                .map(|(key, series)| (key, series))
                        }),
                ),
                None => Box::new(self.iter()),
            },
        };
        Box::new(candidates.filter(move |((_, labels), _)| matches(labels, matchers)))
    }

    fn for_each_mut(&mut self, mut visit: impl FnMut(&StoredLabels, &mut Series)) {
        for table in &mut self.groups {
            for ((_, labels), series) in table.iter_mut() {
                visit(labels, series);
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&SeriesKey, &mut Series) -> bool) {
        let mut len = 0;
        for table in &mut self.groups {
            table.retain(|(key, series)| keep(key, series));
            len += table.len();
        }
        if len != self.len {
            // Removals are rare and batched by retention, so the postings are rebuilt.
            self.postings.clear();
            for (group, table) in self.groups.iter().enumerate() {
                for ((hash, labels), _) in table.iter() {
                    add_postings(&mut self.postings, labels, group as u32, *hash);
                }
            }
        }
        self.len = len;
    }
}

// Most label values of high-cardinality labels belong to a single series, which is kept inline.
#[derive(Debug)]
enum PostingList {
    One((u32, u64)),
    Many(Vec<(u32, u64)>),
}

impl PostingList {
    fn len(&self) -> usize {
        match self {
            PostingList::One(_) => 1,
            PostingList::Many(list) => list.len(),
        }
    }

    fn as_slice(&self) -> &[(u32, u64)] {
        match self {
            PostingList::One(entry) => std::slice::from_ref(entry),
            PostingList::Many(list) => list,
        }
    }

    fn push(&mut self, entry: (u32, u64)) {
        match self {
            PostingList::One(first) => *self = PostingList::Many(vec![*first, entry]),
            PostingList::Many(list) => list.push(entry),
        }
    }
}

fn add_postings(
    postings: &mut HashMap<Arc<str>, HashMap<CompactString, PostingList>>,
    labels: &StoredLabels,
    group: u32,
    hash: u64,
) {
    for (name, value) in labels {
        if name.as_ref() == "__name__" {
            continue;
        }
        let values = match postings.get_mut(name) {
            Some(values) => values,
            None => postings.entry(Arc::clone(name)).or_default(),
        };
        match values.get_mut(value) {
            Some(list) => list.push((group, hash)),
            None => {
                values.insert(value.clone(), PostingList::One((group, hash)));
            }
        }
    }
}

fn metric_name(labels: &[StoredLabel]) -> &str {
    labels
        .binary_search_by(|(label, _)| label.as_ref().cmp("__name__"))
        .map_or("", |index| labels[index].1.as_str())
}

// Metadata of one metric family, keyed by type, help and unit, with when each was last seen.
type MetricMetadataSet = BTreeMap<(i32, String, String), (cortexpb::MetricMetadata, i64)>;

struct Tenant {
    series: SeriesByName,
    label_names: HashSet<Arc<str>>,
    // Tenant metadata, ingestion rates and the head's max time live only in shard 0.
    metadata: BTreeMap<String, MetricMetadataSet>,
    ingested: VecDeque<(Instant, i32, u64)>,
    max_time: i64,
    // The oldest accepted sample, like the Go head's min time before its first compaction.
    min_time: i64,
    // The min time of the emulated Go head, which head compaction moves in block-range steps.
    head_min: i64,
    // The ranges the owned series were last recomputed with, and whether a head compaction asks
    // for another recompute, which are Mimir's reasons to recompute owned series.
    owned_ranges_seen: Option<Option<Vec<u32>>>,
    owned_recompute: bool,
    // The emulated head's series count at the last head tick.
    head_series: u64,
    // Where the last compaction truncated the head, Prometheus's minValidTime.
    truncated_to: i64,
    // By start, the block ranges this shard's series have samples in, with their oldest and newest
    // sample: the blocks the Go ingester compacts the head into.
    block_ranges: BTreeMap<i64, (i64, i64)>,
}

impl Tenant {
    fn mark_block_ranges(&mut self, series: &Series) {
        for (min, max) in series_bounds(series) {
            let mut range = range_start(min);
            while range <= max {
                let upper = range.saturating_add(CHUNK_RANGE_MS - 1);
                mark_block_range(
                    &mut self.block_ranges,
                    range,
                    min.max(range),
                    max.min(upper),
                );
                range = range.saturating_add(CHUNK_RANGE_MS);
            }
        }
    }
}

fn mark_block_range(ranges: &mut BTreeMap<i64, (i64, i64)>, range: i64, min: i64, max: i64) {
    ranges
        .entry(range)
        .and_modify(|(oldest, newest)| {
            *oldest = (*oldest).min(min);
            *newest = (*newest).max(max);
        })
        .or_insert((min, max));
}

impl Default for Tenant {
    fn default() -> Self {
        Self {
            series: SeriesByName::default(),
            label_names: HashSet::new(),
            metadata: BTreeMap::new(),
            ingested: VecDeque::new(),
            max_time: i64::MIN,
            min_time: i64::MAX,
            head_min: i64::MIN,
            owned_ranges_seen: None,
            owned_recompute: false,
            head_series: 0,
            truncated_to: i64::MIN,
            block_ranges: BTreeMap::new(),
        }
    }
}

struct State {
    tenants: HashMap<String, Tenant>,
    disk: ChunkDiskMapper,
}

/// Series are sharded by label hash so records apply to several shards in parallel, the way Go
/// ingests with concurrent appenders; a series always lives in one shard, which keeps its samples
/// in order.
pub struct Store {
    shards: Vec<RwLock<State>>,
    pool: rayon::ThreadPool,
    active_window_ms: i64,
    retention_ms: Option<i64>,
    overrides: Arc<Overrides>,
    exemplars: Mutex<HashMap<String, TenantExemplars<Arc<StoredLabels>>>>,
    cost_attribution: Mutex<HashMap<(String, String), CostAttributionState>>,
    // Per tenant, the token ranges of this partition in the tenant's shuffle shard (None when the
    // tenant does not use it); unknown until the ring sidecar answered.
    owned_ranges: RwLock<Option<Arc<HashMap<String, Option<Vec<u32>>>>>>,
    // `-cost-attribution.cleanup-interval` and `-cost-attribution.eviction-interval`.
    cost_attribution_intervals: (i64, i64),
    cost_attribution_last_cleanup: Mutex<i64>,
    // Samples ingested since startup, for the ingestion rate EWMA.
    ingested_samples: std::sync::atomic::AtomicU64,
    // Series per ingest pusher flush, `-ingest-storage.kafka.ingestion-concurrency-batch-size`,
    // or 0 when `-ingest-storage.kafka.ingestion-concurrency-max` is 0 and records push alone.
    flush_series: usize,
    pusher_shards: PusherShards,
    postings_cache: PostingsCacheConfig,
    non_owned_eviction: Option<NonOwnedEviction>,
    early_head_compaction: Option<EarlyHeadCompaction>,
    // Whether the last head tick compacted, so this one checks for an early compaction with
    // counts taken after it, like Mimir's check right after its regular compaction.
    compacted_last_tick: std::sync::atomic::AtomicBool,
}

/// Mimir's `-blocks-storage.tsdb.early-head-compaction-min-in-memory-series` and
/// `-...-min-estimated-series-reduction-percentage`: with that many series in memory, the
/// tenants whose inactive series would get the count back under it, or that would drop at least
/// the percentage, compact their head up to the active series idle timeout ago.
#[derive(Clone, Copy, Debug)]
pub struct EarlyHeadCompaction {
    pub min_in_memory_series: u64,
    pub min_reduction_percentage: u64,
}

/// Mimir's `filterUsersToCompactToReduceInMemorySeries`, from each tenant's in-memory and
/// estimated removable series.
fn tenants_to_compact_early(
    memory_series: u64,
    config: EarlyHeadCompaction,
    estimations: &mut [(String, u64, u64)],
) -> Vec<String> {
    let total_reduction: u64 = estimations.iter().map(|(_, count, _)| count).sum();
    if memory_series == 0 || total_reduction * 100 / memory_series < config.min_reduction_percentage
    {
        return Vec::new();
    }
    let target = memory_series.saturating_sub(config.min_in_memory_series);
    estimations.sort_by_key(|estimation| std::cmp::Reverse(estimation.1));
    let mut sum = 0;
    let mut tenants = Vec::new();
    for (tenant, count, percentage) in estimations.iter() {
        if sum < target || *percentage >= config.min_reduction_percentage {
            tenants.push(tenant.clone());
            sum += count;
        }
    }
    tenants
}

/// Mimir's `-ingester.early-compaction-non-owned-series-*`: series found non-owned by an owned
/// series recompute leave the head at the next compaction once non-owned for the min grace period
/// (the tenant's head holding its local `early_head_compaction_owned_series_threshold`) or the max
/// grace period (any tenant with a threshold), plus a per-process jitter.
#[derive(Clone, Copy, Debug)]
pub struct NonOwnedEviction {
    pub min_grace_ms: i64,
    pub max_grace_ms: i64,
    pub jitter_ms: i64,
}

/// How Mimir's ingest pusher sizes a tenant's shards for the records of one fetch:
/// `-ingest-storage.kafka.ingestion-concurrency-max`, `-...-estimated-bytes-per-sample` and
/// `-...-target-flushes-per-shard`.
#[derive(Clone, Copy, Debug)]
pub struct PusherShards {
    pub max: usize,
    pub bytes_per_sample: usize,
    pub target_flushes: usize,
}

impl PusherShards {
    fn count(&self, bytes: usize, flush_series: usize) -> usize {
        if flush_series == 0 {
            return 1;
        }
        (bytes / self.bytes_per_sample.max(1) / flush_series / self.target_flushes.max(1))
            .min(self.max)
            .max(1)
    }
}

impl Default for PusherShards {
    fn default() -> Self {
        Self {
            max: 8,
            bytes_per_sample: 200,
            target_flushes: 40,
        }
    }
}

/// Mimir's postings-for-matchers cache settings. A block query only records how many series its
/// index lookup selected when it goes through the cache.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostingsCacheConfig {
    pub head_force: bool,
    pub block_force: bool,
    pub shared: bool,
    pub head_invalidation: bool,
}

impl PostingsCacheConfig {
    // Prometheus's PostingsForMatchersCache skips the cache for non-concurrent (unsharded) calls
    // unless forced, and a shared cache with head invalidation needs a metric name to version.
    fn used(&self, head: bool, sharded: bool, matchers: &[cortex::LabelMatcher]) -> bool {
        let force = if head {
            self.head_force
        } else {
            self.block_force
        };
        let key = !self.shared
            || !(head && self.head_invalidation)
            || matchers
                .iter()
                .any(|matcher| matcher.r#type == 0 && matcher.name == "__name__");
        (sharded || force) && key
    }
}

/// A block a query read, as Mimir's block querier reports it: its generation, the series its
/// index lookup selected (0 without the postings cache) and the series it returned.
#[derive(Clone, Debug, PartialEq)]
pub struct QueriedBlock {
    pub generation: String,
    pub index_series: u64,
    pub series: u64,
}

// The time range of a queried block, the head's ending at its max time.
struct BlockRange {
    lower: i64,
    upper: i64,
    head: bool,
    count_index: bool,
    generation: String,
}

/// Per-tenant state of the emulated Go head, for the memory, owned series and head metrics.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeadReport {
    pub tenant: String,
    pub memory_series: u64,
    pub series_created: u64,
    pub series_removed: u64,
    pub owned_series: u64,
    pub head_chunks: u64,
    pub head_min_time: i64,
    pub head_max_time: i64,
    // Series evicted from the head as non-owned in this tick.
    pub non_owned_evicted: u64,
    pub active_series: u64,
    // Whether a compaction truncated the head.
    pub truncated: bool,
}

// Mimir's head appender rejects in-order samples more than half a block range behind the head.
const MIN_VALID_TIME_WINDOW_MS: i64 = CHUNK_RANGE_MS / 2;

/// Why a sample was discarded, as the `reason` of `cortex_discarded_samples_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DiscardReason {
    OutOfOrder,
    OutOfBounds,
    TooOld,
    NewValueForTimestamp,
    TooFarInFuture,
    TooFarInPast,
    InvalidNativeHistogram,
}

impl DiscardReason {
    pub fn label(self) -> &'static str {
        match self {
            DiscardReason::OutOfOrder => "sample-out-of-order",
            DiscardReason::OutOfBounds => "sample-timestamp-too-old",
            DiscardReason::TooOld => "sample-too-old",
            DiscardReason::NewValueForTimestamp => "new-value-for-timestamp",
            DiscardReason::TooFarInFuture => "sample-too-far-in-future",
            DiscardReason::TooFarInPast => "sample-too-far-in-past",
            DiscardReason::InvalidNativeHistogram => "invalid-native-histogram",
        }
    }
}

// The head state a record's samples are checked against, taken before the record applies like
// the head appender Mimir creates for each request.
// The head appender of one of Mimir's ingest pusher flushes.
struct FlushState {
    id: u64,
    series: usize,
    rules: Option<AppendRules>,
}

// A series as committed before the current flush: Prometheus's head appender checks samples
// against it, and only drops those that no longer fit when committing.
#[derive(Clone, Debug, Default)]
struct Committed {
    max: Option<i64>,
    last_float: Option<(i64, u64)>,
    last_histogram: Option<cortexpb::Histogram>,
}

impl Committed {
    fn of(series: &Series) -> Self {
        Self {
            max: series_max_time(series),
            last_float: series.float_head.as_ref().and_then(|head| {
                Some((
                    head.appender.last_timestamp()?,
                    head.appender.last_value()?.to_bits(),
                ))
            }),
            last_histogram: series.histogram_head.last().cloned(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AppendRules {
    head_max_time: i64,
    min_valid_time: i64,
    out_of_order_window_ms: i64,
}

impl AppendRules {
    /// Like Prometheus's `appendableMinValidTime`: half a block range behind the head's max time,
    /// and never before where the last compaction truncated the head.
    fn new(head_max_time: i64, truncated_to: i64, out_of_order_window_ms: i64) -> Self {
        Self {
            head_max_time,
            min_valid_time: if head_max_time == i64::MIN {
                i64::MIN
            } else {
                head_max_time
                    .saturating_sub(MIN_VALID_TIME_WINDOW_MS)
                    .max(truncated_to)
            },
            out_of_order_window_ms,
        }
    }

    /// Prometheus's `appendable` for a sample at `timestamp` in a series whose in-order samples
    /// end at `series_max` (None for a series without any).
    fn classify(&self, timestamp: i64, series_max: Option<i64>) -> Result<Append, DiscardReason> {
        match series_max {
            Some(max) if timestamp > max && timestamp >= self.min_valid_time => {
                return Ok(Append::InOrder);
            }
            Some(max) if timestamp == max => return Ok(Append::Duplicate),
            None if timestamp >= self.min_valid_time => return Ok(Append::InOrder),
            _ => {}
        }
        if self.out_of_order_window_ms > 0 {
            if timestamp
                >= self
                    .head_max_time
                    .saturating_sub(self.out_of_order_window_ms)
            {
                return Ok(Append::OutOfOrder);
            }
            return Err(DiscardReason::TooOld);
        }
        if timestamp < self.min_valid_time {
            return Err(DiscardReason::OutOfBounds);
        }
        Err(DiscardReason::OutOfOrder)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Appended {
    // Whether the sample opened a new head chunk, for `cortex_ingester_tsdb_head_chunks_created_total`.
    InOrder { opened: bool },
    OutOfOrder { opened: bool },
    // A duplicate of a stored sample, accepted without storing it again.
    Noop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Append {
    InOrder,
    Duplicate,
    OutOfOrder,
}

// Samples a shard applied, rejected, and exemplars to store once the shards finish.
#[derive(Default)]
struct ShardOutcome {
    accepted: HashMap<usize, u64>,
    out_of_order: HashMap<usize, u64>,
    chunks_created: HashMap<usize, u64>,
    discarded: HashMap<(usize, DiscardReason), u64>,
    // By position of the series in the batch, which is the order Mimir appends exemplars in.
    exemplars: Vec<PendingExemplars>,
    // Exemplars of series that don't exist, which Mimir counts as failed.
    exemplars_without_series: u64,
}

#[derive(Default)]
struct CostAttributionState {
    overflow_since: Option<i64>,
}

/// A decoded Kafka record to apply to the store.
pub struct IngestRecord {
    pub tenant: String,
    pub request: DecodedRequest,
    pub ingested_ms: i64,
    pub track_rate: bool,
    // The Kafka record's value size, from which Mimir's ingest pusher sizes a tenant's shards.
    pub bytes: usize,
}

pub const DEFAULT_SHARDS: usize = 16;

#[derive(Clone, Debug)]
pub struct SeriesView {
    pub labels: Vec<(String, String)>,
    pub exemplars: Vec<cortexpb::Exemplar>,
}

#[derive(Clone, Debug)]
pub struct QuerySeriesView {
    pub encoded_labels: Bytes,
    pub chunks: Arc<Vec<EncodedChunk>>,
    pub chunk_start: usize,
    pub chunk_end: usize,
}

#[derive(Clone, Debug)]
pub struct EncodedChunk {
    pub start_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
    // XOR and histogram chunks start with a big-endian sample count.
    pub samples: u32,
    pub wire: Bytes,
}

#[derive(Clone, Debug)]
pub struct ActiveSeriesView {
    pub labels: Vec<(String, String)>,
    pub bucket_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct UserStatsView {
    pub num_series: u64,
    pub ingestion_rate: f64,
    pub api_ingestion_rate: f64,
    pub rule_ingestion_rate: f64,
}

impl Default for Store {
    fn default() -> Self {
        Self::with_shards(20 * 60 * 1000, None, None, 4, 2).expect("create in-memory store")
    }
}

/// Per-tenant counts for the active series metrics.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ActiveSeriesReport {
    pub tenant: String,
    pub series: u64,
    pub active: u64,
    pub active_native_histograms: u64,
    pub active_native_histogram_buckets: u64,
    /// Per custom tracker name: active series, native histogram series and buckets.
    pub custom_trackers: Vec<(String, [u64; 3])>,
    pub cost_attribution: Vec<AttributedSeries>,
    pub metadata: u64,
    pub exemplars: u64,
    pub exemplar_series: u64,
    pub oldest_exemplar_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttributedSeries {
    pub tracker: String,
    pub internal: bool,
    pub output_labels: Vec<String>,
    /// Attribution values and their active series, native histogram series and buckets. An
    /// overflowing tracker reports a single `__overflow__` entry.
    pub values: Vec<(Vec<String>, [u64; 3])>,
    pub overflow: bool,
    /// Distinct attribution values, whether or not the tracker overflows.
    pub cardinality: usize,
}

impl Store {
    /// Completed chunks go to `chunk_dir`, which is cleared first; without one they use anonymous memory.
    pub fn new(
        active_window_ms: i64,
        retention_ms: Option<i64>,
        chunk_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Self::with_shards(
            active_window_ms,
            retention_ms,
            chunk_dir,
            DEFAULT_SHARDS,
            default_threads(),
        )
    }

    pub fn with_shards(
        active_window_ms: i64,
        retention_ms: Option<i64>,
        chunk_dir: Option<PathBuf>,
        shards: usize,
        threads: usize,
    ) -> Result<Self> {
        if let Some(directory) = &chunk_dir
            && directory.exists()
        {
            std::fs::remove_dir_all(directory)
                .with_context(|| format!("remove chunk directory {}", directory.display()))?;
        }
        let shards = (0..shards.max(1))
            .map(|shard| {
                Ok(RwLock::new(State {
                    tenants: HashMap::new(),
                    disk: ChunkDiskMapper::open(
                        chunk_dir
                            .as_ref()
                            .map(|directory| shard_dir(directory, shard)),
                    )?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_shards(shards, threads, active_window_ms, retention_ms)
    }

    fn from_shards(
        shards: Vec<RwLock<State>>,
        threads: usize,
        active_window_ms: i64,
        retention_ms: Option<i64>,
    ) -> Result<Self> {
        Ok(Self {
            shards,
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads.max(1))
                .thread_name(|index| format!("store-{index}"))
                .build()
                .context("create store thread pool")?,
            active_window_ms,
            retention_ms,
            overrides: Arc::default(),
            exemplars: Mutex::default(),
            cost_attribution: Mutex::default(),
            owned_ranges: RwLock::new(None),
            cost_attribution_intervals: (3 * 60_000, 20 * 60_000),
            cost_attribution_last_cleanup: Mutex::new(now_ms()),
            ingested_samples: std::sync::atomic::AtomicU64::new(0),
            flush_series: 150,
            pusher_shards: PusherShards::default(),
            postings_cache: PostingsCacheConfig::default(),
            non_owned_eviction: None,
            early_head_compaction: None,
            compacted_last_tick: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Limits come from `overrides`; without them the store uses Mimir's defaults.
    pub fn with_overrides(mut self, overrides: Arc<Overrides>) -> Self {
        self.overrides = overrides;
        self
    }

    pub fn with_flush_series(mut self, flush_series: usize) -> Self {
        self.flush_series = flush_series;
        self
    }

    pub fn with_early_head_compaction(mut self, config: Option<EarlyHeadCompaction>) -> Self {
        self.early_head_compaction = config;
        self
    }

    pub fn with_non_owned_eviction(mut self, eviction: Option<NonOwnedEviction>) -> Self {
        self.non_owned_eviction = eviction;
        self
    }

    pub fn with_pusher_shards(mut self, pusher_shards: PusherShards) -> Self {
        self.pusher_shards = pusher_shards;
        self
    }

    pub fn with_postings_cache(mut self, postings_cache: PostingsCacheConfig) -> Self {
        self.postings_cache = postings_cache;
        self
    }

    pub fn with_cost_attribution_intervals(mut self, cleanup_ms: i64, eviction_ms: i64) -> Self {
        self.cost_attribution_intervals = (cleanup_ms, eviction_ms);
        self
    }

    pub fn ingested_samples(&self) -> u64 {
        self.ingested_samples
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn overrides(&self) -> &Arc<Overrides> {
        &self.overrides
    }

    pub fn ingest(&self, tenant_id: &str, request: DecodedRequest) -> Result<()> {
        self.ingest_batch(vec![IngestRecord {
            tenant: tenant_id.to_owned(),
            request,
            ingested_ms: now_ms(),
            track_rate: true,
            bytes: 0,
        }])
    }

    pub fn ingest_recovered(
        &self,
        tenant_id: &str,
        request: DecodedRequest,
        ingested_ms: i64,
    ) -> Result<()> {
        self.ingest_batch(vec![IngestRecord {
            tenant: tenant_id.to_owned(),
            request,
            ingested_ms,
            track_rate: false,
            bytes: 0,
        }])
    }

    /// Applies records in order: each shard receives its series in record order, and shards run in
    /// parallel.
    pub fn ingest_batch(&self, records: Vec<IngestRecord>) -> Result<()> {
        self.ingest_flushes(records).map(|_| ())
    }

    /// Like `ingest_batch`, returning how many head appends the Go ingester's pusher would make.
    pub fn ingest_flushes(&self, records: Vec<IngestRecord>) -> Result<u64> {
        let shard_count = self.shards.len();
        let mut buckets = (0..shard_count).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut tenant_ids = Vec::with_capacity(records.len());
        let mut record_rules = Vec::with_capacity(records.len());
        let mut all_series = Vec::new();
        // Like `idealShardsFor`, a tenant's records spread over shards by their expected series.
        let mut tenant_bytes: HashMap<&str, usize> = HashMap::new();
        for record in &records {
            *tenant_bytes.entry(record.tenant.as_str()).or_default() += record.bytes;
        }
        let pusher = self.pusher_shards;
        let tenant_shards = tenant_bytes
            .into_iter()
            .map(|(tenant, bytes)| (tenant.to_owned(), pusher.count(bytes, self.flush_series)))
            .collect::<HashMap<_, _>>();
        let mut flushes: HashMap<(String, i32), Vec<Option<FlushState>>> = HashMap::new();
        let mut next_flush = 0_u64;
        let mut early_discards: HashMap<(usize, DiscardReason), u64> = HashMap::new();
        let mut exemplar_failures = 0_u64;
        let wall_now = now_ms();
        {
            let mut home = self.shards[0].write().expect("store lock poisoned");
            for (index, record) in records.into_iter().enumerate() {
                let IngestRecord {
                    tenant,
                    mut request,
                    ingested_ms,
                    track_rate,
                    bytes: _,
                } = record;
                let tenant_limits = self.overrides.tenant(&tenant);
                let limits = &tenant_limits.limits;
                let home_tenant = tenant_mut(&mut home.tenants, &tenant);
                self.add_metadata(home_tenant, &tenant, limits, request.metadata, wall_now);
                if track_rate {
                    let samples = request
                        .series
                        .iter()
                        .map(|series| (series.samples.len() + series.histograms.len()) as u64)
                        .sum();
                    let instant = Instant::now();
                    home_tenant
                        .ingested
                        .push_back((instant, request.source, samples));
                    while home_tenant.ingested.front().is_some_and(|(at, _, _)| {
                        instant.duration_since(*at) > Duration::from_secs(60)
                    }) {
                        home_tenant.ingested.pop_front();
                    }
                }
                // Like the push path, check wall-clock grace periods before appending.
                let max_timestamp = wall_now.saturating_add(limits.creation_grace_period_ms);
                let min_timestamp = if limits.past_grace_period_ms > 0 {
                    wall_now
                        .saturating_sub(limits.past_grace_period_ms)
                        .saturating_sub(limits.out_of_order_time_window_ms)
                } else {
                    i64::MIN
                };
                let classify = |timestamp: i64| {
                    if timestamp > max_timestamp {
                        Some(DiscardReason::TooFarInFuture)
                    } else if timestamp < min_timestamp {
                        Some(DiscardReason::TooFarInPast)
                    } else {
                        None
                    }
                };
                let keep_exemplars = limits.max_global_exemplars_per_user > 0;
                let window = limits.out_of_order_time_window_ms;
                let flush_key = (tenant.clone(), request.source);
                let shards = tenant_shards.get(&tenant).copied().unwrap_or(1);
                let mut series_rules = Vec::with_capacity(request.series.len());
                for series in &mut request.series {
                    // Mimir's pusher routes each series to a shard by the hash of its labels.
                    let shard = if shards > 1 {
                        (stable_hash_pairs(
                            series
                                .labels
                                .iter()
                                .map(|(name, value)| (name.as_str(), value.as_str())),
                        ) % shards as u64) as usize
                    } else {
                        0
                    };
                    // Mimir's fast path: without an out-of-order window, a series whose samples
                    // are all behind the head's min valid time is rejected as a whole, before
                    // the grace period checks.
                    let min_append = flushes
                        .get(&flush_key)
                        .and_then(|flushes| flushes[shard].as_ref())
                        .and_then(|flush| flush.rules)
                        .map(|rules| rules.min_valid_time)
                        .or_else(|| {
                            (home_tenant.max_time != i64::MIN).then(|| {
                                home_tenant
                                    .max_time
                                    .saturating_sub(MIN_VALID_TIME_WINDOW_MS)
                                    .max(home_tenant.truncated_to)
                            })
                        });
                    let histograms_count = limits.native_histograms_ingestion_enabled;
                    if window <= 0
                        && let Some(min_append) = min_append
                        && series.exemplars.is_empty()
                        && (!series.samples.is_empty()
                            || (histograms_count && !series.histograms.is_empty()))
                        && series
                            .samples
                            .iter()
                            .all(|sample| sample.timestamp_ms < min_append)
                        && (!histograms_count
                            || series
                                .histograms
                                .iter()
                                .all(|histogram| histogram.timestamp < min_append))
                    {
                        let rejected = series.samples.len()
                            + if histograms_count {
                                series.histograms.len()
                            } else {
                                0
                            };
                        *early_discards
                            .entry((index, DiscardReason::OutOfBounds))
                            .or_default() += rejected as u64;
                        series.samples.clear();
                        series.histograms.clear();
                        series.created_timestamp = 0;
                    }
                    series
                        .samples
                        .retain(|sample| match classify(sample.timestamp_ms) {
                            Some(reason) => {
                                count_early_discard(&mut early_discards, index, reason);
                                false
                            }
                            None => true,
                        });
                    if limits.native_histograms_ingestion_enabled {
                        series
                            .histograms
                            .retain(|histogram| match classify(histogram.timestamp) {
                                Some(reason) => {
                                    count_early_discard(&mut early_discards, index, reason);
                                    false
                                }
                                None => true,
                            });
                    } else {
                        // Ignored without an error, like Mimir.
                        series.histograms.clear();
                    }
                    if keep_exemplars {
                        let before = series.exemplars.len();
                        series
                            .exemplars
                            .retain(|exemplar| classify(exemplar.timestamp_ms).is_none());
                        exemplar_failures += (before - series.exemplars.len()) as u64;
                    } else {
                        series.exemplars.clear();
                    }
                    let first_sample = series
                        .samples
                        .first()
                        .map(|sample| sample.timestamp_ms)
                        .or_else(|| {
                            series
                                .histograms
                                .first()
                                .map(|histogram| histogram.timestamp)
                        });
                    // Like Mimir's ingest pusher, each flush of up to `flush_series` series of a
                    // tenant's shard gets its own head appender, which takes the head's max time
                    // when created, or the first sample's for a head that has none yet.
                    let flush = flushes
                        .entry(flush_key.clone())
                        .or_insert_with(|| (0..shards).map(|_| None).collect())[shard]
                        .get_or_insert_with(|| {
                            next_flush += 1;
                            FlushState {
                                id: next_flush,
                                series: 0,
                                rules: None,
                            }
                        });
                    if self.flush_series > 0 && flush.series >= self.flush_series {
                        next_flush += 1;
                        *flush = FlushState {
                            id: next_flush,
                            series: 0,
                            rules: None,
                        };
                    }
                    if flush.rules.is_none()
                        && let Some(first) = first_sample
                    {
                        let head_max = if home_tenant.max_time == i64::MIN {
                            first
                        } else {
                            home_tenant.max_time
                        };
                        flush.rules =
                            Some(AppendRules::new(head_max, home_tenant.truncated_to, window));
                    }
                    flush.series += 1;
                    let rules = flush.rules.unwrap_or_else(|| {
                        AppendRules::new(home_tenant.max_time, home_tenant.truncated_to, window)
                    });
                    // Older samples are rejected whatever the series holds.
                    let acceptable_from = if window > 0 {
                        rules
                            .min_valid_time
                            .min(rules.head_max_time.saturating_sub(window))
                    } else {
                        rules.min_valid_time
                    };
                    let created = series.created_timestamp;
                    if created > 0 && created >= acceptable_from {
                        home_tenant.min_time = home_tenant.min_time.min(created);
                    }
                    for timestamp in series
                        .samples
                        .iter()
                        .map(|sample| sample.timestamp_ms)
                        .chain(
                            series
                                .histograms
                                .iter()
                                .map(|histogram| histogram.timestamp),
                        )
                    {
                        // Every sample above the head's max time is accepted.
                        home_tenant.max_time = home_tenant.max_time.max(timestamp);
                        if timestamp >= acceptable_from {
                            home_tenant.min_time = home_tenant.min_time.min(timestamp);
                        }
                    }
                    series_rules.push((rules, flush.id));
                }
                if self.flush_series == 0 {
                    // Without concurrency, Mimir pushes every record on its own.
                    flushes.remove(&flush_key);
                }
                record_rules.push(keep_exemplars);
                all_series.extend(
                    request
                        .series
                        .into_iter()
                        .zip(series_rules)
                        .map(|(series, (rules, flush))| (index, series, ingested_ms, rules, flush)),
                );
                tenant_ids.push(tenant);
            }
        }
        // Sorting and hashing labels is most of the per-series cost outside the shards.
        let hashes = self.pool.install(|| {
            all_series
                .par_iter_mut()
                .map(|(_, series, _, _, _)| {
                    series.labels.sort();
                    if series.labels.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                        return None;
                    }
                    Some(hash_label_pairs(
                        series
                            .labels
                            .iter()
                            .map(|(name, value)| (name.as_str(), value.as_str())),
                    ))
                })
                .collect::<Vec<_>>()
        });
        for (position, ((index, series, ingested_ms, rules, flush), hash)) in
            all_series.into_iter().zip(hashes).enumerate()
        {
            if let Some(hash) = hash {
                buckets[shard_for(hash, shard_count)].push(BatchSeries {
                    index,
                    position,
                    hash,
                    series,
                    ingested_ms,
                    rules,
                    flush,
                });
            }
        }
        let apply = |shard: usize, bucket: Vec<BatchSeries>| -> Result<ShardOutcome> {
            let mut guard = self.shards[shard].write().expect("store lock poisoned");
            let State { tenants, disk } = &mut *guard;
            let mut outcome = ShardOutcome::default();
            let mut committed = HashMap::new();
            for BatchSeries {
                index,
                position,
                hash,
                series,
                ingested_ms,
                rules,
                flush,
            } in bucket
            {
                let tenant = tenant_mut(tenants, &tenant_ids[index]);
                let keep_exemplars = record_rules[index];
                ingest_series(
                    tenant,
                    disk,
                    series,
                    hash,
                    SeriesContext {
                        tenant_id: &tenant_ids[index],
                        rules,
                        keep_exemplars,
                        index,
                        position,
                        flush,
                        ingested_ms,
                    },
                    &mut committed,
                    &mut outcome,
                )?;
            }
            Ok(outcome)
        };
        let work = buckets
            .into_iter()
            .enumerate()
            .filter(|(_, bucket)| !bucket.is_empty())
            .collect::<Vec<_>>();
        let outcomes = if work.len() <= 1 {
            work.into_iter()
                .map(|(shard, bucket)| apply(shard, bucket))
                .collect::<Result<Vec<_>>>()?
        } else {
            self.pool.install(|| {
                work.into_par_iter()
                    .map(|(shard, bucket)| apply(shard, bucket))
                    .collect::<Result<Vec<_>>>()
            })?
        };
        self.record_outcomes(&tenant_ids, early_discards, exemplar_failures, outcomes);
        Ok(next_flush)
    }

    fn record_outcomes(
        &self,
        tenant_ids: &[String],
        early_discards: HashMap<(usize, DiscardReason), u64>,
        mut exemplar_failures: u64,
        outcomes: Vec<ShardOutcome>,
    ) {
        let mut accepted: HashMap<&str, u64> = HashMap::new();
        let mut discarded: HashMap<(&str, DiscardReason), u64> = HashMap::new();
        for ((index, reason), count) in early_discards {
            *discarded.entry((&tenant_ids[index], reason)).or_default() += count;
        }
        let mut exemplars = Vec::new();
        for outcome in outcomes {
            for (index, count) in outcome.accepted {
                *accepted.entry(&tenant_ids[index]).or_default() += count;
            }
            for (index, count) in outcome.out_of_order {
                metrics::OUT_OF_ORDER_SAMPLES_APPENDED
                    .with_label_values(&[&tenant_ids[index]])
                    .inc_by(count);
            }
            for (index, count) in outcome.chunks_created {
                metrics::HEAD_CHUNKS_CREATED
                    .with_label_values(&[&tenant_ids[index]])
                    .inc_by(count);
            }
            for ((index, reason), count) in outcome.discarded {
                *discarded.entry((&tenant_ids[index], reason)).or_default() += count;
            }
            exemplars.extend(outcome.exemplars);
            exemplar_failures += outcome.exemplars_without_series;
        }
        exemplars.sort_by_key(|(_, position, _, _, _, _)| *position);
        for (tenant, count) in accepted {
            metrics::INGESTED_SAMPLES
                .with_label_values(&[tenant])
                .inc_by(count);
            self.ingested_samples
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        }
        let mut failures: HashMap<&str, u64> = HashMap::new();
        for ((tenant, reason), count) in discarded {
            metrics::DISCARDED_SAMPLES
                .with_label_values(&[reason.label(), tenant, ""])
                .inc_by(count);
            *failures.entry(tenant).or_default() += count;
        }
        for (tenant, count) in failures {
            metrics::INGESTED_SAMPLES_FAILURES
                .with_label_values(&[tenant])
                .inc_by(count);
        }
        if exemplars.is_empty() {
            if exemplar_failures > 0 {
                metrics::INGESTED_EXEMPLARS_FAILURES.inc_by(exemplar_failures);
            }
            return;
        }
        let mut ingested = 0_u64;
        let mut storage = self.exemplars.lock().expect("exemplar lock poisoned");
        // The newest exemplar of each series when its flush started, which Mimir's head appender
        // validates against; the storage itself only changes when the flush commits.
        let mut newest_at_flush: HashMap<(u64, u64), Option<cortexpb::Exemplar>> = HashMap::new();
        for (index, _, flush, hash, labels, series_exemplars) in exemplars {
            let tenant = &tenant_ids[index];
            let tenant_limits = self.overrides.tenant(tenant);
            let capacity = self.overrides.max_exemplars(&tenant_limits.limits);
            let window = tenant_limits.limits.out_of_order_time_window_ms;
            let tenant_storage = storage
                .entry(tenant.clone())
                .or_insert_with(|| TenantExemplars::new(capacity));
            if tenant_storage.capacity() != capacity {
                tenant_storage.resize(capacity);
            }
            let newest = newest_at_flush
                .entry((flush, hash))
                .or_insert_with(|| tenant_storage.newest(hash).cloned())
                .clone();
            let mut appended = 0;
            for exemplar in series_exemplars {
                if exemplars::validate(capacity, newest.as_ref(), &exemplar, window).is_err() {
                    exemplar_failures += 1;
                    continue;
                }
                ingested += 1;
                match tenant_storage.add(hash, || Arc::clone(&labels), exemplar, window) {
                    Ok(true) => appended += 1,
                    Ok(false) => {}
                    // Rejected when committing, which Mimir doesn't report.
                    Err(Rejection::OutOfOrder) => metrics::OUT_OF_ORDER_EXEMPLARS.inc(),
                    Err(Rejection::Disabled | Rejection::LabelLength) => {}
                }
            }
            if appended > 0 {
                metrics::EXEMPLARS_APPENDED
                    .with_label_values(&[tenant])
                    .inc_by(appended);
            }
        }
        metrics::INGESTED_EXEMPLARS.inc_by(ingested);
        metrics::INGESTED_EXEMPLARS_FAILURES.inc_by(exemplar_failures);
    }

    // Mimir's `userMetricsMetadata.add`: a new metric needs room under the per-user limit, and any
    // entry, even a known one, needs room under the per-metric limit.
    fn add_metadata(
        &self,
        tenant: &mut Tenant,
        tenant_id: &str,
        limits: &Limits,
        metadata: Vec<cortexpb::MetricMetadata>,
        now: i64,
    ) {
        if metadata.is_empty() {
            return;
        }
        let per_user = self.overrides.max_metadata_per_user(limits);
        let per_metric = self.overrides.max_metadata_per_metric(limits);
        for metadata in metadata {
            if !tenant.metadata.contains_key(&metadata.metric_family_name)
                && tenant.metadata.len() >= per_user
            {
                metrics::DISCARDED_METADATA
                    .with_label_values(&["per_user_metadata_limit", tenant_id])
                    .inc();
                metrics::INGESTED_METADATA_FAILURES.inc();
                continue;
            }
            let set = tenant
                .metadata
                .entry(metadata.metric_family_name.clone())
                .or_default();
            if set.len() >= per_metric {
                metrics::DISCARDED_METADATA
                    .with_label_values(&["per_metric_metadata_limit", tenant_id])
                    .inc();
                metrics::INGESTED_METADATA_FAILURES.inc();
                continue;
            }
            let key = (
                metadata.r#type,
                metadata.help.clone(),
                metadata.unit.clone(),
            );
            if set.insert(key, (metadata, now)).is_none() {
                metrics::MEMORY_METADATA_CREATED
                    .with_label_values(&[tenant_id])
                    .inc();
            }
            metrics::INGESTED_METADATA.inc();
        }
    }

    /// Drops metadata not seen for `retain_ms`, like `-ingester.metadata-retain-period`.
    pub fn purge_metadata(&self, retain_ms: i64) {
        let cutoff = now_ms().saturating_sub(retain_ms);
        let mut home = self.shards[0].write().expect("store lock poisoned");
        for (tenant_id, tenant) in home.tenants.iter_mut() {
            let mut removed = 0;
            tenant.metadata.retain(|_, set| {
                let before = set.len();
                set.retain(|_, (_, seen)| *seen >= cutoff);
                removed += before - set.len();
                !set.is_empty()
            });
            if removed > 0 {
                metrics::MEMORY_METADATA_REMOVED
                    .with_label_values(&[tenant_id])
                    .inc_by(removed as u64);
            }
        }
    }

    pub fn prune_expired(&self) -> Result<()> {
        let Some(retention_ms) = self.retention_ms else {
            return Ok(());
        };
        self.prune_before(now_ms().saturating_sub(retention_ms))
    }

    fn prune_before(&self, cutoff: i64) -> Result<()> {
        self.pool.install(|| {
            self.shards.par_iter().try_for_each(|shard| {
                let mut state = shard.write().expect("store lock poisoned");
                for tenant in state.tenants.values_mut() {
                    tenant
                        .series
                        .retain(|_, series| prune_series(series, cutoff));
                    tenant
                        .block_ranges
                        .retain(|_, (_, newest)| *newest >= cutoff);
                }
                state.disk.truncate_before(cutoff)
            })
        })
    }

    /// Runs `query` on the tenant in every shard in parallel.
    fn per_shard<T: Send>(
        &self,
        tenant_id: &str,
        query: impl Fn(&Tenant, &ChunkDiskMapper) -> T + Sync,
    ) -> Vec<T> {
        self.pool.install(|| {
            self.shards
                .par_iter()
                .filter_map(|shard| {
                    let state = shard.read().expect("store lock poisoned");
                    state
                        .tenants
                        .get(tenant_id)
                        .map(|tenant| query(tenant, &state.disk))
                })
                .collect()
        })
    }

    pub fn select_chunks(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<QuerySeriesView>> {
        Ok(self
            .select_chunks_with_blocks(tenant_id, start, end, matchers)?
            .0)
    }

    /// Like Prometheus's DB.ChunkQuerier, a query reads the head when it ends after the head's
    /// min time, and each compacted block, emulated as a block range with samples below the
    /// head, it overlaps.
    fn queried_blocks(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Vec<BlockRange> {
        let head = self.head_view(tenant_id);
        let head_lower = head.head_min.max(head.min_time);
        let sharded = matchers
            .iter()
            .any(|matcher| matcher.r#type == 0 && matcher.name == "__query_shard__");
        let mut blocks = Vec::new();
        if head.max_time != i64::MIN && end >= head_lower {
            blocks.push(BlockRange {
                lower: head_lower,
                upper: i64::MAX,
                head: true,
                count_index: self.postings_cache.used(true, sharded, matchers),
                generation: "0".into(),
            });
        }
        if head.head_min == i64::MIN {
            return blocks;
        }
        let last = end.min(head.head_min.saturating_sub(1));
        let first = range_start(start);
        if first > last {
            return blocks;
        }
        let mut ranges = BTreeMap::new();
        for shard in self.per_shard(tenant_id, |tenant, _| {
            tenant
                .block_ranges
                .range(first..=last)
                .map(|(range, bounds)| (*range, *bounds))
                .collect::<Vec<_>>()
        }) {
            for (range, (oldest, newest)) in shard {
                mark_block_range(&mut ranges, range, oldest, newest);
            }
        }
        let count_index = self.postings_cache.used(false, sharded, matchers);
        for (lower, (oldest, newest)) in ranges {
            // A block's time range is that of its samples.
            // Blocks end where the head starts, which forced compactions don't align.
            let newest = newest.min(head.head_min.saturating_sub(1));
            if oldest > newest || oldest > end || newest < start {
                continue;
            }
            let generation = ((head_lower - oldest) / CHUNK_RANGE_MS).max(1);
            blocks.push(BlockRange {
                lower,
                upper: (lower + CHUNK_RANGE_MS - 1).min(head.head_min.saturating_sub(1)),
                head: false,
                count_index,
                generation: if generation > 100 {
                    "100+".into()
                } else {
                    generation.to_string()
                },
            });
        }
        blocks
    }

    /// The selected series and, per block the Go ingester would read, what it reports.
    pub fn select_chunks_with_blocks(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<(Vec<QuerySeriesView>, Vec<QueriedBlock>)> {
        let compiled = compile_matchers(matchers)?;
        let blocks = self.queried_blocks(tenant_id, start, end, matchers);
        // Counting what an index lookup selects needs the series of every query shard.
        let count_index = blocks.iter().any(|block| block.count_index);
        let (shard_matchers, index_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .cloned()
            .partition(|matcher| matcher.r#type == 0 && matcher.name == "__query_shard__");
        let (lookup, shard) = if count_index {
            (
                compile_matchers(&index_matchers)?,
                compile_matchers(&shard_matchers)?,
            )
        } else {
            (compiled, Vec::new())
        };
        let head = self.head_view(tenant_id);
        let per_shard = self.per_shard(tenant_id, |tenant, disk| {
            let mut counts = vec![[0_u64; 2]; blocks.len()];
            let selected = tenant
                .series
                .matching(&lookup)
                .filter_map(|((_, labels), series)| {
                    let in_shard = shard.is_empty() || matches(labels, &shard);
                    for (block, counts) in blocks.iter().zip(&mut counts) {
                        let indexed = if block.head {
                            head.holds(series)
                        } else {
                            matches_time_range(series, block.lower, block.upper)
                        };
                        if !indexed {
                            continue;
                        }
                        counts[0] += u64::from(block.count_index);
                        if in_shard
                            && matches_time_range(
                                series,
                                start.max(block.lower),
                                end.min(block.upper),
                            )
                        {
                            counts[1] += 1;
                        }
                    }
                    if !in_shard || !matches_time_range(series, start, end) {
                        return None;
                    }
                    let chunks = query_chunks(series, disk, start, end);
                    (!chunks.is_empty()).then(|| {
                        (
                            Arc::clone(labels),
                            QuerySeriesView {
                                encoded_labels: encode_series_labels(labels),
                                chunk_start: 0,
                                chunk_end: chunks.len(),
                                chunks: Arc::new(chunks),
                            },
                        )
                    })
                })
                .collect::<Vec<_>>();
            (selected, counts)
        });
        let mut totals = vec![[0_u64; 2]; blocks.len()];
        let mut selected = Vec::new();
        for (series, counts) in per_shard {
            selected.extend(series);
            for (total, count) in totals.iter_mut().zip(counts) {
                total[0] += count[0];
                total[1] += count[1];
            }
        }
        // The distributor k-way merges each ingester's stream and requires label order, which the
        // Go ingester gets from sorted postings.
        selected.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        let blocks = blocks
            .into_iter()
            .zip(totals)
            .map(|(block, [index_series, series])| QueriedBlock {
                generation: block.generation,
                index_series,
                series,
            })
            .collect();
        Ok((selected.into_iter().map(|(_, view)| view).collect(), blocks))
    }

    pub fn select_exemplars(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<SeriesView>> {
        let compiled = compile_matchers(matchers)?;
        let storage = self.exemplars.lock().expect("exemplar lock poisoned");
        let Some(tenant) = storage.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let mut selected = tenant.select(start, end, |labels| matches(labels, &compiled));
        drop(storage);
        // The distributor merges exemplar sets assuming they are sorted by series labels.
        selected.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        Ok(selected
            .into_iter()
            .map(|(labels, exemplars)| SeriesView {
                labels: owned_labels(&labels),
                exemplars,
            })
            .collect())
    }

    pub fn select_labels(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<Vec<(String, String)>>> {
        let compiled = compile_matchers(matchers)?;
        Ok(self
            .per_shard(tenant_id, |tenant, _| {
                tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| matches_time_range(series, start, end))
                    .map(|((_, labels), _)| owned_labels(labels))
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn label_names(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<String>> {
        let compiled = compile_matchers(matchers)?;
        let window = self.head_view(tenant_id).label_window(start, end);
        let names = self
            .per_shard(tenant_id, |tenant, _| {
                let mut names = BTreeSet::new();
                for ((_, labels), _) in tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| window.includes(series))
                {
                    names.extend(labels.iter().map(|(name, _)| name.to_string()));
                }
                names
            })
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        Ok(names.into_iter().collect())
    }

    pub fn label_values(
        &self,
        tenant_id: &str,
        name: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<String>> {
        let compiled = compile_matchers(matchers)?;
        let window = self.head_view(tenant_id).label_window(start, end);
        let values = self
            .per_shard(tenant_id, |tenant, _| {
                let mut values = BTreeSet::new();
                for ((_, labels), _) in tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| window.includes(series))
                {
                    if let Some((_, value)) =
                        labels.iter().find(|(label, _)| label.as_ref() == name)
                    {
                        values.insert(value.to_string());
                    }
                }
                values
            })
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        Ok(values.into_iter().collect())
    }

    fn head_view(&self, tenant_id: &str) -> HeadView {
        let home = self.shards[0].read().expect("store lock poisoned");
        home.tenants
            .get(tenant_id)
            .map_or(HeadView::EMPTY, |tenant| HeadView {
                head_min: tenant.head_min,
                min_time: tenant.min_time,
                max_time: tenant.max_time,
            })
    }

    pub fn has_tenant(&self, tenant_id: &str) -> bool {
        self.shards[0]
            .read()
            .expect("store lock poisoned")
            .tenants
            .contains_key(tenant_id)
    }

    pub fn num_series(&self, tenant_id: &str) -> u64 {
        self.per_shard(tenant_id, |tenant, _| tenant.series.len() as u64)
            .into_iter()
            .sum()
    }

    pub fn active_series(
        &self,
        tenant_id: &str,
        matchers: &[cortex::LabelMatcher],
        histograms_only: bool,
    ) -> Result<Vec<ActiveSeriesView>> {
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        Ok(self
            .per_shard(tenant_id, |tenant, _| {
                tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| series.is_active(cutoff))
                    .filter_map(|((_, labels), series)| {
                        let bucket_count = u64::from(series.last_bucket_count);
                        (!histograms_only || series.native_histogram).then(|| ActiveSeriesView {
                            labels: owned_labels(labels),
                            bucket_count,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn set_owned_ranges(&self, ranges: HashMap<String, Option<Vec<u32>>>) {
        *self
            .owned_ranges
            .write()
            .expect("owned ranges lock poisoned") = Some(Arc::new(ranges));
    }

    pub fn tenant_ids(&self) -> Vec<String> {
        let home = self.shards[0].read().expect("store lock poisoned");
        let mut tenants = home.tenants.keys().cloned().collect::<Vec<_>>();
        tenants.sort();
        tenants
    }

    /// Updates which series the Go ingester would still hold in its head, which of those this
    /// partition owns, and, when `compact`, moves each tenant's head min time like head compaction:
    /// while the head spans more than 1.5 block ranges, its oldest block range is compacted away.
    ///
    /// With `track_owned`, a recompute of a tenant's owned series (on a new tenant, changed
    /// ranges or after head compaction) removes its non-owned series from the active series until
    /// their next sample, like Mimir's computeOwnedSeries.
    pub fn head_tick(&self, compact: bool, track_owned: bool) -> Vec<HeadReport> {
        let owned = self
            .owned_ranges
            .read()
            .expect("owned ranges lock poisoned")
            .clone();
        let head_bounds = {
            let mut home = self.shards[0].write().expect("store lock poisoned");
            home.tenants
                .iter_mut()
                .map(|(id, tenant)| {
                    let current = owned.as_ref().and_then(|owned| owned.get(id));
                    let recompute = track_owned
                        && current.is_some_and(|current| {
                            tenant.owned_recompute
                                || tenant.owned_ranges_seen.as_ref() != Some(current)
                        });
                    if recompute {
                        tenant.owned_ranges_seen = current.cloned();
                        tenant.owned_recompute = false;
                    }
                    let evict_before = self
                        .non_owned_eviction
                        .filter(|_| compact && track_owned)
                        .and_then(|eviction| {
                            let limits = &self.overrides.tenant(id).limits;
                            let threshold = limits.early_head_compaction_owned_series_threshold;
                            if threshold <= 0 {
                                return None;
                            }
                            let local = self.overrides.local_limit(limits, threshold);
                            let grace = if local > 0 && tenant.head_series >= local as u64 {
                                eviction.min_grace_ms
                            } else if eviction.max_grace_ms > 0 {
                                eviction.max_grace_ms
                            } else {
                                return None;
                            };
                            Some(now_ms() - grace - eviction.jitter_ms)
                        });
                    (id.clone(), (tenant.head_min, recompute, evict_before))
                })
                .collect::<HashMap<_, _>>()
        };
        let per_shard = self.pool.install(|| {
            self.shards
                .par_iter()
                .map(|shard| {
                    let mut state = shard.write().expect("store lock poisoned");
                    state
                        .tenants
                        .iter_mut()
                        .map(|(tenant_id, tenant)| {
                            let (head_min, recompute, evict_before) = head_bounds
                                .get(tenant_id)
                                .copied()
                                .unwrap_or((i64::MIN, false, None));
                            let track_non_owned = track_owned && self.non_owned_eviction.is_some();
                            let active_cutoff = now_ms().saturating_sub(self.active_window_ms);
                            let now_s = (now_ms() / 1000) as u32;
                            let ranges = owned.as_ref().map(|owned| owned.get(tenant_id));
                            let mut report = HeadReport {
                                tenant: tenant_id.clone(),
                                ..HeadReport::default()
                            };
                            let mut min_time = i64::MAX;
                            tenant.series.for_each_mut(|_, series| {
                                let newest = series_newest(series);
                                if let Some(oldest) = series_oldest(series) {
                                    min_time = min_time.min(oldest);
                                }
                                let mut in_head = !series.head_evicted
                                    && newest.is_some_and(|newest| newest >= head_min);
                                if in_head
                                    && series.non_owned_since_s != 0
                                    && evict_before.is_some_and(|before| {
                                        i64::from(series.non_owned_since_s) * 1000 <= before
                                    })
                                {
                                    series.head_evicted = true;
                                    series.non_owned_since_s = 0;
                                    report.non_owned_evicted += 1;
                                    in_head = false;
                                }
                                match (series.in_head, in_head) {
                                    (false, true) => report.series_created += 1,
                                    (true, false) => report.series_removed += 1,
                                    _ => {}
                                }
                                series.in_head = in_head;
                                if !in_head {
                                    return;
                                }
                                report.memory_series += 1;
                                report.active_series += u64::from(series.is_active(active_cutoff));
                                // Unknown ranges or a tenant the ring has not been asked about
                                // yet count as owned, like new series in Go.
                                let is_owned = match ranges {
                                    None | Some(None) => true,
                                    Some(Some(None)) => false,
                                    Some(Some(Some(ranges))) => {
                                        ranges_include(ranges, series.owned_hash)
                                    }
                                };
                                report.owned_series += u64::from(is_owned);
                                if recompute && track_non_owned {
                                    // Like addPendingNonOwnedRefs, a series keeps the time it
                                    // was first found non-owned while it stays so.
                                    if is_owned {
                                        series.non_owned_since_s = 0;
                                    } else if series.non_owned_since_s == 0 {
                                        series.non_owned_since_s = now_s.max(1);
                                    }
                                }
                                if recompute && !is_owned {
                                    // Mimir clears all active series when the tenant owns no
                                    // ranges, and deletes the non-owned ones otherwise.
                                    match ranges {
                                        Some(Some(Some(ranges))) if !ranges.is_empty() => {
                                            series.last_ingested_ms = i64::MIN
                                        }
                                        _ => series.active_cleared = true,
                                    }
                                }
                                report.head_chunks += series
                                    .chunks
                                    .iter()
                                    .filter(|chunk| chunk.max_time >= head_min)
                                    .count()
                                    as u64
                                    + u64::from(series.float_head.is_some())
                                    + u64::from(!series.histogram_head.is_empty())
                                    + u64::from(!series.out_of_order.is_empty());
                            });
                            report.head_min_time = min_time;
                            report
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });
        let mut merged: BTreeMap<String, HeadReport> = BTreeMap::new();
        for report in per_shard.into_iter().flatten() {
            let total = merged
                .entry(report.tenant.clone())
                .or_insert_with(|| HeadReport {
                    tenant: report.tenant.clone(),
                    head_min_time: i64::MAX,
                    ..HeadReport::default()
                });
            total.memory_series += report.memory_series;
            total.series_created += report.series_created;
            total.series_removed += report.series_removed;
            total.owned_series += report.owned_series;
            total.head_chunks += report.head_chunks;
            total.non_owned_evicted += report.non_owned_evicted;
            total.active_series += report.active_series;
            total.head_min_time = total.head_min_time.min(report.head_min_time);
        }
        let check_early = self
            .compacted_last_tick
            .swap(compact, std::sync::atomic::Ordering::Relaxed);
        let early = self
            .early_head_compaction
            .filter(|_| check_early)
            .and_then(|config| {
                let memory_series: u64 = merged.values().map(|report| report.memory_series).sum();
                if memory_series < config.min_in_memory_series {
                    return None;
                }
                let mut estimations = merged
                    .values()
                    .filter(|report| report.memory_series > 0)
                    .map(|report| {
                        let count = report.memory_series.saturating_sub(report.active_series);
                        (
                            report.tenant.clone(),
                            count,
                            count * 100 / report.memory_series,
                        )
                    })
                    .collect::<Vec<_>>();
                let tenants = tenants_to_compact_early(memory_series, config, &mut estimations);
                (!tenants.is_empty()).then(|| {
                    eprintln!(
                        "phase=early_head_compaction in_memory_series={memory_series} tenants={}",
                        tenants.join(",")
                    );
                    tenants.into_iter().collect::<HashSet<_>>()
                })
            });
        let forced_max_time = now_ms().saturating_sub(self.active_window_ms);
        let mut home = self.shards[0].write().expect("store lock poisoned");
        for report in merged.values_mut() {
            let tenant = tenant_mut(&mut home.tenants, &report.tenant);
            tenant.head_series = report.memory_series;
            // A forced compaction up to the idle timeout ago truncates the head right after it.
            if early
                .as_ref()
                .is_some_and(|tenants| tenants.contains(&report.tenant))
                && tenant.max_time != i64::MIN
            {
                let truncated = forced_max_time.min(tenant.max_time).saturating_add(1);
                if tenant.head_min == i64::MIN || truncated > tenant.head_min {
                    tenant.head_min = truncated;
                    tenant.truncated_to = truncated;
                    tenant.owned_recompute = true;
                }
            }
            // Like Mimir, an early compaction asks for another owned series recompute.
            if report.non_owned_evicted > 0 {
                tenant.owned_recompute = true;
            }
            if compact {
                if tenant.head_min == i64::MIN && report.head_min_time != i64::MAX {
                    tenant.head_min = report.head_min_time;
                }
                while tenant.max_time != i64::MIN
                    && tenant.head_min != i64::MIN
                    && tenant.max_time - tenant.head_min > CHUNK_RANGE_MS / 2 * 3
                {
                    tenant.head_min = range_end(tenant.head_min);
                    tenant.truncated_to = tenant.head_min;
                    tenant.owned_recompute = true;
                }
            }
            report.head_min_time = report.head_min_time.max(tenant.head_min);
            report.truncated = tenant.truncated_to != i64::MIN;
            report.head_max_time = tenant.max_time;
        }
        merged.into_values().collect()
    }

    /// Active series counts per tenant for the ingester's metrics, including custom trackers and
    /// cost attribution. Tracker matches are cached per series until the overrides change.
    pub fn active_series_report(&self) -> Vec<ActiveSeriesReport> {
        let now = now_ms();
        let cutoff = now.saturating_sub(self.active_window_ms);
        let generation = self.overrides.generation();
        type CostCounts = Vec<HashMap<Vec<String>, [u64; 3]>>;
        let per_shard = self.pool.install(|| {
            self.shards
                .par_iter()
                .map(|shard| {
                    let mut state = shard.write().expect("store lock poisoned");
                    state
                        .tenants
                        .iter_mut()
                        .map(|(tenant_id, tenant)| {
                            let limits = self.overrides.tenant(tenant_id);
                            let trackers = &limits.custom_trackers;
                            let cost = &limits.cost_attribution;
                            let mut report = ActiveSeriesReport {
                                tenant: tenant_id.clone(),
                                series: tenant.series.len() as u64,
                                metadata: tenant
                                    .metadata
                                    .values()
                                    .map(|set| set.len() as u64)
                                    .sum(),
                                custom_trackers: trackers
                                    .names()
                                    .iter()
                                    .map(|name| (name.clone(), [0; 3]))
                                    .collect(),
                                ..ActiveSeriesReport::default()
                            };
                            let mut cost_counts: CostCounts =
                                vec![HashMap::new(); cost.trackers.len()];
                            tenant.series.for_each_mut(|labels, series| {
                                if series.last_ingested_ms < cutoff {
                                    return;
                                }
                                let active = !series.active_cleared;
                                if series.tracker_generation != generation {
                                    series.tracker_matches = trackers.matching(labels).into();
                                    series.tracker_generation = generation;
                                }
                                let histogram = series.native_histogram;
                                let buckets = if histogram {
                                    u64::from(series.last_bucket_count)
                                } else {
                                    0
                                };
                                let counts = [1, u64::from(histogram), buckets];
                                if active {
                                    report.active += 1;
                                    report.active_native_histograms += counts[1];
                                    report.active_native_histogram_buckets += buckets;
                                    for index in series.tracker_matches.iter() {
                                        let entry =
                                            &mut report.custom_trackers[usize::from(*index)].1;
                                        for (total, count) in entry.iter_mut().zip(counts) {
                                            *total += count;
                                        }
                                    }
                                }
                                for (tracker, combinations) in
                                    cost.trackers.iter().zip(&mut cost_counts)
                                {
                                    let entry =
                                        combinations.entry(tracker.key(labels)).or_default();
                                    for (total, count) in entry.iter_mut().zip(counts) {
                                        *total += count;
                                    }
                                }
                            });
                            (report, cost_counts)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });
        let mut merged: BTreeMap<String, (ActiveSeriesReport, CostCounts)> = BTreeMap::new();
        for (report, cost_counts) in per_shard.into_iter().flatten() {
            match merged.get_mut(&report.tenant) {
                None => {
                    merged.insert(report.tenant.clone(), (report, cost_counts));
                }
                Some((total, total_cost)) => {
                    total.series += report.series;
                    total.active += report.active;
                    total.active_native_histograms += report.active_native_histograms;
                    total.active_native_histogram_buckets += report.active_native_histogram_buckets;
                    total.metadata += report.metadata;
                    for ((_, total), (_, counts)) in
                        total.custom_trackers.iter_mut().zip(report.custom_trackers)
                    {
                        for (total, count) in total.iter_mut().zip(counts) {
                            *total += count;
                        }
                    }
                    for (total, counts) in total_cost.iter_mut().zip(cost_counts) {
                        for (key, counts) in counts {
                            let entry = total.entry(key).or_default();
                            for (total, count) in entry.iter_mut().zip(counts) {
                                *total += count;
                            }
                        }
                    }
                }
            }
        }
        let exemplars = self.exemplars.lock().expect("exemplar lock poisoned");
        let mut cost_state = self
            .cost_attribution
            .lock()
            .expect("cost attribution lock poisoned");
        let mut live_trackers = HashSet::new();
        let cleanup_due = {
            let mut last = self
                .cost_attribution_last_cleanup
                .lock()
                .expect("cost attribution lock poisoned");
            let due = now.saturating_sub(*last) >= self.cost_attribution_intervals.0;
            if due {
                *last = now;
            }
            due
        };
        let deadline = now.saturating_sub(self.cost_attribution_intervals.1);
        let reports = merged
            .into_values()
            .map(|(mut report, cost_counts)| {
                if let Some(storage) = exemplars.get(&report.tenant) {
                    report.exemplars = storage.len() as u64;
                    report.exemplar_series = storage.series_count() as u64;
                    report.oldest_exemplar_ms = storage.oldest_timestamp();
                }
                let limits = self.overrides.tenant(&report.tenant);
                let max_cardinality =
                    usize::try_from(limits.limits.max_cost_attribution_cardinality).unwrap_or(0);
                for (tracker, combinations) in
                    limits.cost_attribution.trackers.iter().zip(cost_counts)
                {
                    let key = (report.tenant.clone(), tracker.name.clone());
                    live_trackers.insert(key.clone());
                    let state = cost_state.entry(key).or_default();
                    // Like Mimir's tracker: overflow once the cardinality exceeds the maximum. The
                    // cleanup, every `-cost-attribution.cleanup-interval`, recovers a tracker whose
                    // overflow started a cooldown before the eviction deadline if it went back
                    // below the maximum, and otherwise restarts the overflow.
                    let cardinality = combinations.len();
                    match state.overflow_since {
                        None if cardinality > max_cardinality => state.overflow_since = Some(now),
                        Some(since)
                            if cleanup_due
                                && since
                                    .saturating_add(limits.limits.cost_attribution_cooldown_ms)
                                    < deadline =>
                        {
                            state.overflow_since = (cardinality > max_cardinality).then_some(now);
                        }
                        _ => {}
                    }
                    let overflow = state.overflow_since.is_some();
                    let mut values = if overflow {
                        let mut total = [0; 3];
                        for counts in combinations.values() {
                            for (total, count) in total.iter_mut().zip(counts) {
                                *total += count;
                            }
                        }
                        vec![(vec![OVERFLOW_VALUE.to_owned(); tracker.labels.len()], total)]
                    } else {
                        combinations.into_iter().collect::<Vec<_>>()
                    };
                    values.sort();
                    report.cost_attribution.push(AttributedSeries {
                        tracker: tracker.name.clone(),
                        internal: tracker.internal,
                        output_labels: tracker
                            .labels
                            .iter()
                            .map(|label| label.output.clone())
                            .collect(),
                        values,
                        overflow,
                        cardinality,
                    });
                }
                report
            })
            .collect();
        cost_state.retain(|key, _| live_trackers.contains(key));
        reports
    }

    pub fn user_stats(&self, tenant_id: &str, active: bool) -> UserStatsView {
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let head = self.head_view(tenant_id);
        self.per_shard(tenant_id, |tenant, _| {
            tenant_stats_at(tenant, active, cutoff, head)
        })
        .into_iter()
        .fold(UserStatsView::default(), add_stats)
    }

    pub fn all_user_stats(&self, active: bool) -> Vec<(String, UserStatsView)> {
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let heads = {
            let home = self.shards[0].read().expect("store lock poisoned");
            home.tenants
                .iter()
                .map(|(id, tenant)| {
                    (
                        id.clone(),
                        HeadView {
                            head_min: tenant.head_min,
                            min_time: tenant.min_time,
                            max_time: tenant.max_time,
                        },
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        let per_shard = self.pool.install(|| {
            self.shards
                .par_iter()
                .map(|shard| {
                    let state = shard.read().expect("store lock poisoned");
                    state
                        .tenants
                        .iter()
                        .map(|(id, tenant)| {
                            let head = heads.get(id).copied().unwrap_or(HeadView::EMPTY);
                            (id.clone(), tenant_stats_at(tenant, active, cutoff, head))
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });
        let mut merged: BTreeMap<String, UserStatsView> = BTreeMap::new();
        for (id, stats) in per_shard.into_iter().flatten() {
            let entry = merged.entry(id).or_default();
            *entry = add_stats(std::mem::take(entry), stats);
        }
        merged.into_iter().collect()
    }

    pub fn label_names_and_values(
        &self,
        tenant_id: &str,
        matchers: &[cortex::LabelMatcher],
        active: bool,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        // Like Go, these read the head index: its series, or the active ones.
        let head = self.head_view(tenant_id);
        let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for shard in self.per_shard(tenant_id, |tenant, _| {
            let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for ((_, labels), _) in tenant.series.matching(&compiled).filter(|(_, series)| {
                if active {
                    series.is_active(cutoff)
                } else {
                    head.holds(series)
                }
            }) {
                for (name, value) in labels.iter() {
                    result
                        .entry(name.to_string())
                        .or_default()
                        .insert(value.to_string());
                }
            }
            result
        }) {
            for (name, values) in shard {
                result.entry(name).or_default().extend(values);
            }
        }
        Ok(result)
    }

    pub fn label_values_cardinality(
        &self,
        tenant_id: &str,
        label_names: &[String],
        matchers: &[cortex::LabelMatcher],
        active: bool,
    ) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        // Like Go, these read the head index: its series, or the active ones.
        let head = self.head_view(tenant_id);
        let wanted = label_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut result: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for shard in self.per_shard(tenant_id, |tenant, _| {
            let mut result: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
            for ((_, labels), _) in tenant.series.matching(&compiled).filter(|(_, series)| {
                if active {
                    series.is_active(cutoff)
                } else {
                    head.holds(series)
                }
            }) {
                for (name, value) in labels.iter() {
                    if wanted.contains(name.as_ref()) {
                        *result
                            .entry(name.to_string())
                            .or_default()
                            .entry(value.to_string())
                            .or_default() += 1;
                    }
                }
            }
            result
        }) {
            for (name, values) in shard {
                let merged = result.entry(name).or_default();
                for (value, count) in values {
                    *merged.entry(value).or_default() += count;
                }
            }
        }
        Ok(result)
    }

    pub fn metadata(&self, tenant_id: &str) -> Vec<cortexpb::MetricMetadata> {
        self.per_shard(tenant_id, |tenant, _| {
            tenant
                .metadata
                .values()
                .flat_map(|set| set.values().map(|(metadata, _)| metadata.clone()))
                .collect::<Vec<_>>()
        })
        .into_iter()
        .flatten()
        .collect()
    }
}

fn default_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |threads| threads.get())
}

fn shard_dir(directory: &std::path::Path, shard: usize) -> PathBuf {
    directory.join(format!("shard-{shard:03}"))
}

// The hash table inside each shard indexes on the low bits, so shard on the high bits.
fn shard_for(hash: u64, shards: usize) -> usize {
    ((hash >> 32) % shards as u64) as usize
}

fn tenant_mut<'a>(tenants: &'a mut HashMap<String, Tenant>, tenant_id: &str) -> &'a mut Tenant {
    if !tenants.contains_key(tenant_id) {
        tenants.insert(tenant_id.to_owned(), Tenant::default());
    }
    tenants.get_mut(tenant_id).expect("tenant exists")
}

fn add_stats(total: UserStatsView, shard: UserStatsView) -> UserStatsView {
    UserStatsView {
        num_series: total.num_series + shard.num_series,
        ingestion_rate: total.ingestion_rate + shard.ingestion_rate,
        api_ingestion_rate: total.api_ingestion_rate + shard.api_ingestion_rate,
        rule_ingestion_rate: total.rule_ingestion_rate + shard.rule_ingestion_rate,
    }
}

/// The emulated Go head of a tenant, which is all that the Go ingester's head-only APIs see.
#[derive(Clone, Copy, Debug)]
struct HeadView {
    head_min: i64,
    min_time: i64,
    max_time: i64,
}

impl HeadView {
    const EMPTY: Self = Self {
        head_min: i64::MIN,
        min_time: i64::MAX,
        max_time: i64::MIN,
    };

    fn holds(&self, series: &Series) -> bool {
        if series.head_evicted {
            return false;
        }
        series_newest(series).is_some_and(|newest| newest >= self.head_min)
    }

    /// Which series a label lookup over `[start, end]` sees: like Prometheus's head index, every
    /// head series once the range overlaps the head, and like compacted blocks, every series with
    /// data in a block range the lookup overlaps.
    fn label_window(&self, start: i64, end: i64) -> LabelWindow {
        let head_lower = self.head_min.max(self.min_time);
        let head = self.max_time != i64::MIN && start <= self.max_time && end >= head_lower;
        let blocks = (self.head_min != i64::MIN).then(|| {
            let lower = range_start(start);
            let upper = range_end(end).min(self.head_min).saturating_sub(1);
            (lower, upper)
        });
        LabelWindow {
            head,
            head_view: *self,
            blocks: blocks.filter(|(lower, upper)| lower <= upper),
            range: (start, end),
        }
    }
}

struct LabelWindow {
    head: bool,
    head_view: HeadView,
    blocks: Option<(i64, i64)>,
    range: (i64, i64),
}

impl LabelWindow {
    fn includes(&self, series: &Series) -> bool {
        (self.head && self.head_view.holds(series))
            || self
                .blocks
                .is_some_and(|(lower, upper)| matches_time_range(series, lower, upper))
            // Evicted series are in their own compacted block.
            || (series.head_evicted && matches_time_range(series, self.range.0, self.range.1))
    }
}

fn tenant_stats_at(tenant: &Tenant, active: bool, cutoff: i64, head: HeadView) -> UserStatsView {
    // Like `Head.NumSeries`, only series still in the head count.
    let num_series = tenant
        .series
        .values()
        .filter(|series| {
            if active {
                series.is_active(cutoff)
            } else {
                head.holds(series)
            }
        })
        .count() as u64;
    let now = Instant::now();
    let mut api = 0;
    let mut rule = 0;
    for (at, source, count) in &tenant.ingested {
        if now.duration_since(*at) > Duration::from_secs(60) {
            continue;
        }
        if *source == 1 {
            rule += count;
        } else {
            api += count;
        }
    }
    UserStatsView {
        num_series,
        ingestion_rate: (api + rule) as f64 / 60.0,
        api_ingestion_rate: api as f64 / 60.0,
        rule_ingestion_rate: rule as f64 / 60.0,
    }
}

struct SeriesContext<'a> {
    tenant_id: &'a str,
    rules: AppendRules,
    keep_exemplars: bool,
    index: usize,
    position: usize,
    flush: u64,
    ingested_ms: i64,
}

// A series of an `ingest_batch`, with the record it came from and its position in the batch.
struct BatchSeries {
    index: usize,
    position: usize,
    hash: u64,
    series: DecodedSeries,
    ingested_ms: i64,
    rules: AppendRules,
    flush: u64,
}

// A series' exemplars by (record index, position, flush, series hash), to add after its samples.
type PendingExemplars = (
    usize,
    usize,
    u64,
    u64,
    Arc<StoredLabels>,
    Vec<cortexpb::Exemplar>,
);

// `decoded` has sorted, unique labels whose hash is `hash`.
fn ingest_series(
    tenant: &mut Tenant,
    disk: &mut ChunkDiskMapper,
    mut decoded: DecodedSeries,
    hash: u64,
    context: SeriesContext<'_>,
    committed: &mut HashMap<(u64, u64), Committed>,
    outcome: &mut ShardOutcome,
) -> Result<()> {
    let decoded_labels = std::mem::take(&mut decoded.labels);
    let name = decoded_labels
        .binary_search_by(|(label, _)| label.as_str().cmp("__name__"))
        .map_or("", |index| decoded_labels[index].1.as_str());
    let Tenant {
        series: by_name,
        label_names,
        block_ranges,
        ..
    } = tenant;
    let created = std::cell::Cell::new(false);
    let (key_labels, series) = by_name.get_or_insert_with(
        name,
        hash,
        |stored| {
            stored.len() == decoded_labels.len()
                && stored.iter().zip(&decoded_labels).all(
                    |((name, value), (decoded_name, decoded_value))| {
                        name.as_ref() == decoded_name && value.as_str() == decoded_value
                    },
                )
        },
        || {
            created.set(true);
            decoded_labels
                .iter()
                .map(|(name, value)| {
                    let name = match label_names.get(name.as_str()) {
                        Some(stored) => Arc::clone(stored),
                        None => {
                            let stored: Arc<str> = name.as_str().into();
                            label_names.insert(Arc::clone(&stored));
                            stored
                        }
                    };
                    (name, CompactString::from(value.as_str()))
                })
                .collect::<StoredLabels>()
                .into()
        },
    );
    if created.get() {
        series.owned_hash = shard_by_all_labels(context.tenant_id, key_labels);
    }
    let existed = has_samples(series);
    let rules = context.rules;
    let committed = committed
        .entry((context.flush, hash))
        .or_insert_with(|| Committed::of(series))
        .clone();
    // Like Mimir's active series tracker, the bucket count comes from the request's last
    // histogram when no float follows it.
    let last_float = decoded.samples.last().map(|sample| sample.timestamp_ms);
    let bucket_count = decoded
        .histograms
        .last()
        .filter(|last| last_float.is_none_or(|float| float < last.timestamp))
        .map(|last| histogram_bucket_count(last) as u32);
    let mut accepted = 0_u64;
    let mut out_of_order = 0_u64;
    let mut chunks_created = 0_u64;
    // The block range of the last accepted samples, with their oldest and newest.
    let mut marked: Option<(i64, i64, i64)> = None;
    let mut mark = |timestamp: i64| {
        let range = range_start(timestamp);
        match &mut marked {
            Some((current, oldest, newest)) if *current == range => {
                *oldest = (*oldest).min(timestamp);
                *newest = (*newest).max(timestamp);
            }
            _ => {
                if let Some((range, oldest, newest)) = marked {
                    mark_block_range(block_ranges, range, oldest, newest);
                }
                marked = Some((range, timestamp, timestamp));
            }
        }
    };
    let mut discard = |reason: DiscardReason| {
        *outcome
            .discarded
            .entry((context.index, reason))
            .or_default() += 1;
    };
    // Like Mimir's push path, the created timestamp adds one zero sample before the first sample it
    // precedes: a float, or a zero histogram of the same layout before a histogram.
    let created = decoded.created_timestamp;
    let mut created_pending = created > 0;
    let first_histogram = decoded
        .histograms
        .first()
        .map(|histogram| histogram.timestamp);
    for sample in decoded.samples {
        if created_pending
            && created < sample.timestamp_ms
            && first_histogram.is_none_or(|first| first >= sample.timestamp_ms)
        {
            created_pending = false;
            match append_created_zero(
                series,
                disk,
                &rules,
                &committed,
                ooo_merge::Value::Float(0.0),
                created,
            )? {
                Ok(Some(appended)) => {
                    accepted += 1;
                    mark(created);
                    count_appended(appended, &mut out_of_order, &mut chunks_created);
                }
                Ok(None) => {}
                Err(reason) => discard(reason),
            }
        }
        match append_float(
            series,
            disk,
            &rules,
            &committed,
            sample.timestamp_ms,
            sample.value,
        )? {
            Ok(appended) => {
                accepted += 1;
                mark(sample.timestamp_ms);
                count_appended(appended, &mut out_of_order, &mut chunks_created);
            }
            Err(reason) => discard(reason),
        }
    }
    for histogram in decoded.histograms {
        if created_pending && created < histogram.timestamp {
            created_pending = false;
            let zero = cortexpb::Histogram {
                timestamp: created,
                count: Some(match histogram.count {
                    Some(cortexpb::histogram::Count::CountFloat(_)) => {
                        cortexpb::histogram::Count::CountFloat(0.0)
                    }
                    _ => cortexpb::histogram::Count::CountInt(0),
                }),
                zero_count: Some(match histogram.count {
                    Some(cortexpb::histogram::Count::CountFloat(_)) => {
                        cortexpb::histogram::ZeroCount::ZeroCountFloat(0.0)
                    }
                    _ => cortexpb::histogram::ZeroCount::ZeroCountInt(0),
                }),
                schema: histogram.schema,
                zero_threshold: histogram.zero_threshold,
                custom_values: histogram.custom_values.clone(),
                reset_hint: 1,
                ..Default::default()
            };
            match append_created_zero(
                series,
                disk,
                &rules,
                &committed,
                ooo_merge::Value::Histogram(Box::new(zero)),
                created,
            )? {
                Ok(Some(appended)) => {
                    accepted += 1;
                    mark(created);
                    count_appended(appended, &mut out_of_order, &mut chunks_created);
                }
                Ok(None) => {}
                Err(reason) => discard(reason),
            }
        }
        let timestamp = histogram.timestamp;
        match append_histogram(series, disk, &rules, &committed, histogram)? {
            Ok(appended) => {
                accepted += 1;
                mark(timestamp);
                count_appended(appended, &mut out_of_order, &mut chunks_created);
            }
            Err(reason) => discard(reason),
        }
    }
    if let Some((range, oldest, newest)) = marked {
        mark_block_range(block_ranges, range, oldest, newest);
    }
    if accepted > 0 {
        if let Some(bucket_count) = bucket_count {
            series.last_bucket_count = bucket_count;
        }
        series.native_histogram = bucket_count.is_some();
        series.last_ingested_ms = context.ingested_ms;
        series.active_cleared = false;
        series.head_evicted = false;
        *outcome.accepted.entry(context.index).or_default() += accepted;
    }
    if out_of_order > 0 {
        *outcome.out_of_order.entry(context.index).or_default() += out_of_order;
    }
    if chunks_created > 0 {
        *outcome.chunks_created.entry(context.index).or_default() += chunks_created;
    }
    // Exemplars need an existing series, like `AppendExemplar`.
    if context.keep_exemplars && !decoded.exemplars.is_empty() && !existed && accepted == 0 {
        outcome.exemplars_without_series += decoded.exemplars.len() as u64;
    }
    if context.keep_exemplars && !decoded.exemplars.is_empty() && (existed || accepted > 0) {
        outcome.exemplars.push((
            context.index,
            context.position,
            context.flush,
            hash,
            Arc::clone(key_labels),
            decoded.exemplars,
        ));
    }
    Ok(())
}

// Mimir's soft error processor doesn't know `too-far-in-past`, so those samples are dropped
// without being counted.
fn count_early_discard(
    discards: &mut HashMap<(usize, DiscardReason), u64>,
    index: usize,
    reason: DiscardReason,
) {
    if reason != DiscardReason::TooFarInPast {
        *discards.entry((index, reason)).or_default() += 1;
    }
}

fn count_appended(appended: Appended, out_of_order: &mut u64, chunks_created: &mut u64) {
    match appended {
        Appended::InOrder { opened } => *chunks_created += u64::from(opened),
        Appended::OutOfOrder { opened } => {
            *out_of_order += 1;
            *chunks_created += u64::from(opened);
        }
        Appended::Noop => {}
    }
}

// A created-timestamp zero sample is only appended in order; like Mimir, being out of order or a
// duplicate is not an error, other rejections are counted.
fn append_created_zero(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    rules: &AppendRules,
    committed: &Committed,
    value: ooo_merge::Value,
    timestamp: i64,
) -> Result<Result<Option<Appended>, DiscardReason>> {
    match rules.classify(timestamp, committed.max) {
        Ok(Append::InOrder) => {}
        Ok(Append::Duplicate | Append::OutOfOrder) => return Ok(Ok(None)),
        Err(DiscardReason::OutOfOrder) => return Ok(Ok(None)),
        Err(reason) => return Ok(Err(reason)),
    }
    // Counted as ingested when appended, like a successful `AppendSTZeroSample`.
    Ok(Ok(match value {
        ooo_merge::Value::Float(value) => {
            append_float(series, disk, rules, committed, timestamp, value)?.ok()
        }
        ooo_merge::Value::Histogram(histogram) => {
            append_histogram(series, disk, rules, committed, *histogram)?.ok()
        }
    }))
}

// A series' newest and oldest samples; heads hold the newest when present.
fn series_newest(series: &Series) -> Option<i64> {
    series_max_time(series).or_else(|| {
        series
            .chunks
            .iter()
            .map(|chunk| chunk.max_time)
            .chain(series.out_of_order.last().map(|(timestamp, _)| *timestamp))
            .max()
    })
}

fn series_oldest(series: &Series) -> Option<i64> {
    series
        .chunks
        .iter()
        .map(|chunk| chunk.min_time)
        .chain(series.float_head.as_ref().map(|head| head.min_time))
        .chain(series.histogram_head.first_timestamp())
        .chain(series.out_of_order.first().map(|(timestamp, _)| *timestamp))
        .min()
}

/// dskit's `TokenRanges.IncludesKey`: sorted pairs of inclusive range bounds.
fn ranges_include(ranges: &[u32], key: u32) -> bool {
    match ranges.binary_search(&key) {
        Ok(_) => true,
        Err(index) => index % 2 == 1,
    }
}

/// Mimir's `ShardByAllLabels`: 32-bit FNV-1 over the tenant ID and every label name and value.
pub(crate) fn shard_by_all_labels(tenant_id: &str, labels: &[StoredLabel]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    let mut add = |bytes: &[u8]| {
        for byte in bytes {
            hash = hash.wrapping_mul(16_777_619);
            hash ^= u32::from(*byte);
        }
    };
    add(tenant_id.as_bytes());
    for (name, value) in labels {
        add(name.as_bytes());
        add(value.as_bytes());
    }
    hash
}

fn has_samples(series: &Series) -> bool {
    series.float_head.is_some()
        || !series.histogram_head.is_empty()
        || !series.chunks.is_empty()
        || !series.out_of_order.is_empty()
}

// The newest in-order sample, float or histogram, like the head chunk's max time.
fn series_max_time(series: &Series) -> Option<i64> {
    let float = series
        .float_head
        .as_ref()
        .and_then(|head| head.appender.last_timestamp());
    let histogram = series.histogram_head.last().map(|last| last.timestamp);
    float.max(histogram)
}

// Follows the Prometheus head appender: in-order samples extend the open chunk, a repeated
// timestamp keeps the first value, and older samples within the out-of-order window go to a
// separate out-of-order chunk. The outer error is a storage failure, the inner one a rejection.
fn append_float(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    rules: &AppendRules,
    committed: &Committed,
    timestamp: i64,
    value: f64,
) -> Result<Result<Appended, DiscardReason>> {
    match rules.classify(timestamp, committed.max) {
        Err(reason) => return Ok(Err(reason)),
        Ok(Append::Duplicate) => {
            return Ok(match committed.last_float {
                Some((last_timestamp, bits))
                    if last_timestamp == timestamp && bits == value.to_bits() =>
                {
                    Ok(Appended::Noop)
                }
                _ => Err(DiscardReason::NewValueForTimestamp),
            });
        }
        Ok(Append::OutOfOrder | Append::InOrder) => {}
    }
    // Like Prometheus's commit, the accepted sample is checked again against the series as this
    // flush left it, and anything that no longer fits is dropped without an error.
    match rules.classify(timestamp, series_max_time(series)) {
        Err(_) | Ok(Append::Duplicate) => return Ok(Ok(Appended::Noop)),
        Ok(Append::OutOfOrder) => {
            return insert_out_of_order(series, disk, timestamp, ooo_merge::Value::Float(value))
                .map(Ok);
        }
        Ok(Append::InOrder) => {}
    }
    if let Some(head) = &mut series.float_head {
        let samples = head.appender.len();
        if samples == SAMPLES_PER_CHUNK / 4 {
            head.next_at = compute_chunk_end_time(
                head.min_time,
                head.appender.last_timestamp().expect("head has samples"),
                head.next_at,
                4.0,
            );
        }
        if timestamp >= head.next_at || samples >= SAMPLES_PER_CHUNK * 2 {
            cut_float_head(series, disk)?;
        }
    }
    let opened = series.float_head.is_none();
    let head = series.float_head.get_or_insert_with(|| FloatHead {
        appender: xor::Appender::default(),
        min_time: timestamp,
        next_at: range_end(timestamp),
    });
    // A float after an older float in the head can only follow a histogram at a later time,
    // which the in-order check above rules out.
    if head
        .appender
        .last_timestamp()
        .is_some_and(|last| timestamp <= last)
    {
        return Ok(Ok(Appended::Noop));
    }
    head.appender.append(timestamp, value);
    Ok(Ok(Appended::InOrder { opened }))
}

fn append_histogram(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    rules: &AppendRules,
    committed: &Committed,
    histogram: cortexpb::Histogram,
) -> Result<Result<Appended, DiscardReason>> {
    // Like the head appender, invalid histograms are rejected before anything else.
    if !histogram::is_valid(&histogram) {
        return Ok(Err(DiscardReason::InvalidNativeHistogram));
    }
    match rules.classify(histogram.timestamp, committed.max) {
        Err(reason) => return Ok(Err(reason)),
        Ok(Append::Duplicate) => {
            return Ok(match &committed.last_histogram {
                Some(last)
                    if last.timestamp == histogram.timestamp
                        && histogram::equal_values(last, &histogram) =>
                {
                    Ok(Appended::Noop)
                }
                _ => Err(DiscardReason::NewValueForTimestamp),
            });
        }
        Ok(Append::OutOfOrder | Append::InOrder) => {}
    }
    // Checked again when committing, like floats.
    match rules.classify(histogram.timestamp, series_max_time(series)) {
        Err(_) | Ok(Append::Duplicate) => return Ok(Ok(Appended::Noop)),
        Ok(Append::OutOfOrder) => {
            let timestamp = histogram.timestamp;
            return insert_out_of_order(
                series,
                disk,
                timestamp,
                ooo_merge::Value::Histogram(Box::new(histogram)),
            )
            .map(Ok);
        }
        Ok(Append::InOrder) => {}
    }
    let timestamp = histogram.timestamp;
    // Prometheus's `histogramsAppendPreprocessor`: cut on the estimated end time or twice the
    // target size, with at least a few samples unless a new block range starts.
    if let Some(head) = &series.histogram_head.0 {
        let samples = head.len();
        let bytes = head.encoded_len();
        let next_range_start = if series.histogram_end_computed {
            range_end(head.first_timestamp())
        } else {
            series.histogram_next_at
        };
        if !series.histogram_end_computed && bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            series.histogram_next_at = compute_chunk_end_time(
                head.first_timestamp(),
                head.last().timestamp,
                series.histogram_next_at,
                TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64 / bytes as f64,
            );
            series.histogram_end_computed = true;
        }
        if (timestamp >= series.histogram_next_at || bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2)
            && (samples >= MIN_SAMPLES_PER_HISTOGRAM_CHUNK || timestamp >= next_range_start)
        {
            let next = histogram::HistogramAppender::new(histogram, Some(head));
            cut_histogram_head(series, disk)?;
            series.histogram_head = HistogramHead(Some(Box::new(next)));
            series.histogram_next_at = range_end(timestamp);
            series.histogram_end_computed = false;
            return Ok(Ok(Appended::InOrder { opened: true }));
        }
    }
    let Some(head) = &mut series.histogram_head.0 else {
        series.histogram_head = HistogramHead(Some(Box::new(histogram::HistogramAppender::new(
            histogram, None,
        ))));
        series.histogram_next_at = range_end(timestamp);
        series.histogram_end_computed = false;
        return Ok(Ok(Appended::InOrder { opened: true }));
    };
    match head.append(histogram) {
        histogram::Appended::InChunk | histogram::Appended::Recoded => {
            Ok(Ok(Appended::InOrder { opened: false }))
        }
        histogram::Appended::NewChunk(next) => {
            cut_histogram_head(series, disk)?;
            series.histogram_head = HistogramHead(Some(next));
            series.histogram_next_at = range_end(timestamp);
            series.histogram_end_computed = false;
            Ok(Ok(Appended::InOrder { opened: true }))
        }
    }
}

fn cut_float_head(series: &mut Series, disk: &mut ChunkDiskMapper) -> Result<()> {
    let Some(head) = series.float_head.take() else {
        return Ok(());
    };
    let max_time = head.appender.last_timestamp().expect("head has samples");
    let data = head.appender.into_bytes();
    write_chunk(
        series,
        disk,
        histogram::EncodedHistogram {
            encoding: i32::from(XOR_ENCODING),
            data,
        },
        head.min_time,
        max_time,
        false,
    )
}

fn cut_histogram_head(series: &mut Series, disk: &mut ChunkDiskMapper) -> Result<()> {
    if series.histogram_head.is_empty() {
        return Ok(());
    }
    let head = series
        .histogram_head
        .take()
        .expect("histogram head is open");
    write_chunk(
        series,
        disk,
        head.encoded(),
        head.first_timestamp(),
        head.last().timestamp,
        false,
    )
}

// Like the OOO head chunk's `Insert`, a timestamp already in the open out-of-order chunk is
// dropped; once it holds `OUT_OF_ORDER_CAPACITY` samples it is written out like a memory-mapped
// OOO chunk.
fn insert_out_of_order(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    timestamp: i64,
    value: ooo_merge::Value,
) -> Result<Appended> {
    match series
        .out_of_order
        .binary_search_by_key(&timestamp, |(timestamp, _)| *timestamp)
    {
        Ok(_) => return Ok(Appended::Noop),
        Err(index) => series.out_of_order.insert(index, (timestamp, value)),
    }
    let opened = series.out_of_order.len() == 1;
    if series.out_of_order.len() >= OUT_OF_ORDER_CAPACITY {
        flush_out_of_order(series, disk)?;
    }
    Ok(Appended::OutOfOrder { opened })
}

fn flush_out_of_order(series: &mut Series, disk: &mut ChunkDiskMapper) -> Result<()> {
    if series.out_of_order.is_empty() {
        return Ok(());
    }
    let samples = std::mem::take(&mut series.out_of_order);
    for chunk in ooo_merge::encode_out_of_order(&samples) {
        write_chunk(
            series,
            disk,
            histogram::EncodedHistogram {
                encoding: chunk.encoding,
                data: chunk.data,
            },
            chunk.min_time,
            chunk.max_time,
            true,
        )?;
    }
    Ok(())
}

fn write_chunk(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    encoded: histogram::EncodedHistogram,
    min_time: i64,
    max_time: i64,
    out_of_order: bool,
) -> Result<()> {
    let reference = disk.write(&encoded.data, max_time)?;
    series.chunks.push(ChunkMeta {
        reference,
        min_time,
        max_time,
        len: u32::try_from(encoded.data.len()).context("chunk exceeds u32")?,
        encoding: u8::try_from(encoded.encoding).context("chunk encoding exceeds u8")?,
        out_of_order,
    });
    Ok(())
}

// Saturating, since lookups ask for ranges up to the ends of i64.
fn range_start(timestamp: i64) -> i64 {
    timestamp.saturating_sub(timestamp.rem_euclid(CHUNK_RANGE_MS))
}

fn range_end(timestamp: i64) -> i64 {
    range_start(timestamp).saturating_add(CHUNK_RANGE_MS)
}

// Port of Prometheus computeChunkEndTime: spread the remaining range evenly over chunks of the
// observed sample rate so chunk boundaries align with block ranges.
fn compute_chunk_end_time(start: i64, current: i64, max: i64, ratio_to_full: f64) -> i64 {
    let n = (max - start) as f64 / ((current - start + 1) as f64 * ratio_to_full);
    if n <= 1.0 {
        return max;
    }
    (start as f64 + (max - start) as f64 / n.floor()) as i64
}

fn series_bounds(series: &Series) -> impl Iterator<Item = (i64, i64)> + '_ {
    let float_head = series.float_head.as_ref().map(|head| {
        (
            head.min_time,
            head.appender.last_timestamp().expect("head has samples"),
        )
    });
    let histogram_head = series
        .histogram_head
        .first_timestamp()
        .zip(series.histogram_head.last())
        .map(|(first, last)| (first, last.timestamp));
    let out_of_order = series
        .out_of_order
        .first()
        .zip(series.out_of_order.last())
        .map(|(first, last)| (first.0, last.0));
    series
        .chunks
        .iter()
        .map(|chunk| (chunk.min_time, chunk.max_time))
        .chain(float_head)
        .chain(histogram_head)
        .chain(out_of_order)
}

fn matches_time_range(series: &Series, start: i64, end: i64) -> bool {
    series_bounds(series).any(|(min, max)| min <= end && max >= start)
}

// With out-of-order samples, overlapping chunks are merged as the Go ingester's out-of-order
// querier does; otherwise every chunk is returned as stored.
fn query_chunks(
    series: &Series,
    disk: &ChunkDiskMapper,
    start: i64,
    end: i64,
) -> Vec<EncodedChunk> {
    let overlaps = |min: i64, max: i64| min <= end && max >= start;
    let mut candidates = Vec::new();
    let (mut in_order, mut out_of_order) = (0, 0);
    let mut add = |chunk: ooo_merge::Chunk, ooo: bool| {
        let counter = if ooo {
            &mut out_of_order
        } else {
            &mut in_order
        };
        *counter += 1;
        if overlaps(chunk.min_time, chunk.max_time) {
            candidates.push(ooo_merge::Candidate {
                chunk,
                order: (ooo, *counter),
            });
        }
    };
    for chunk in &series.chunks {
        let stored = ooo_merge::Chunk {
            min_time: chunk.min_time,
            max_time: chunk.max_time,
            encoding: i32::from(chunk.encoding),
            data: Vec::new(),
        };
        if overlaps(chunk.min_time, chunk.max_time) {
            add(
                ooo_merge::Chunk {
                    data: disk.read(chunk.reference, chunk.len).to_vec(),
                    ..stored
                },
                chunk.out_of_order,
            );
        } else {
            add(stored, chunk.out_of_order);
        }
    }
    if let Some(head) = &series.float_head {
        add(
            ooo_merge::Chunk {
                min_time: head.min_time,
                max_time: head.appender.last_timestamp().expect("head has samples"),
                encoding: i32::from(XOR_ENCODING),
                data: head.appender.bytes().to_vec(),
            },
            false,
        );
    }
    if let (Some(first), Some(last)) = (
        series.histogram_head.first_timestamp(),
        series.histogram_head.last(),
    ) && overlaps(first, last.timestamp)
    {
        let encoded = series
            .histogram_head
            .encoded()
            .expect("histogram head is open");
        add(
            ooo_merge::Chunk {
                min_time: first,
                max_time: last.timestamp,
                encoding: encoded.encoding,
                data: encoded.data,
            },
            false,
        );
    }
    // The open out-of-order chunk shares one position, like Prometheus's OOO head chunk.
    let ooo_head = ooo_merge::encode_out_of_order(&series.out_of_order);
    out_of_order += 1;
    for chunk in ooo_head {
        if overlaps(chunk.min_time, chunk.max_time) {
            candidates.push(ooo_merge::Candidate {
                chunk,
                order: (true, out_of_order),
            });
        }
    }
    let merged = if candidates.iter().any(|candidate| candidate.order.0) {
        match ooo_merge::merge_overlapping(candidates) {
            Ok(merged) => merged,
            Err(error) => {
                eprintln!("phase=query_merge_error error={error:#}");
                return Vec::new();
            }
        }
    } else {
        let mut chunks = candidates
            .into_iter()
            .map(|candidate| candidate.chunk)
            .collect::<Vec<_>>();
        chunks.sort_by_key(|chunk| chunk.min_time);
        chunks
    };
    merged
        .into_iter()
        .map(|chunk| wire_chunk(chunk.min_time, chunk.max_time, chunk.encoding, &chunk.data))
        .collect()
}

fn wire_chunk(min_time: i64, max_time: i64, encoding: i32, data: &[u8]) -> EncodedChunk {
    EncodedChunk {
        start_timestamp_ms: min_time,
        end_timestamp_ms: max_time,
        samples: data.get(..2).map_or(0, |count| {
            u32::from(u16::from_be_bytes([count[0], count[1]]))
        }),
        wire: cortex::Chunk {
            start_timestamp_ms: min_time,
            end_timestamp_ms: max_time,
            encoding,
            data: Bytes::copy_from_slice(data),
        }
        .encode_to_vec()
        .into(),
    }
}

// Like head truncation, retention drops whole chunks, so a chunk that straddles the cutoff stays.
fn prune_series(series: &mut Series, cutoff: i64) -> bool {
    series.chunks.retain(|chunk| chunk.max_time >= cutoff);
    if series
        .float_head
        .as_ref()
        .is_some_and(|head| head.appender.last_timestamp().expect("head has samples") < cutoff)
    {
        series.float_head = None;
    }
    if series
        .histogram_head
        .last()
        .is_some_and(|last| last.timestamp < cutoff)
    {
        series.histogram_head.clear();
    }
    series
        .out_of_order
        .retain(|(timestamp, _)| *timestamp >= cutoff);
    has_samples(series)
}

fn histogram_bucket_count(histogram: &cortexpb::Histogram) -> u64 {
    histogram
        .positive_spans
        .iter()
        .chain(&histogram.negative_spans)
        .map(|span| u64::from(span.length))
        .sum()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

enum CompiledMatcher {
    Equal(String, String),
    NotEqual(String, String),
    Regex(String, Regex),
    NotRegex(String, Regex),
    Shard(u64, u64),
}

fn compile_matchers(matchers: &[cortex::LabelMatcher]) -> Result<Vec<CompiledMatcher>> {
    matchers
        .iter()
        .map(|matcher| {
            if matcher.name == "__query_shard__" && matcher.r#type == 0 {
                let (index, count) = parse_shard(&matcher.value)?;
                return Ok(CompiledMatcher::Shard(index, count));
            }
            Ok(match matcher.r#type {
                0 => CompiledMatcher::Equal(matcher.name.clone(), matcher.value.clone()),
                1 => CompiledMatcher::NotEqual(matcher.name.clone(), matcher.value.clone()),
                2 => CompiledMatcher::Regex(
                    matcher.name.clone(),
                    Regex::new(&format!("^(?:{})$", matcher.value))?,
                ),
                3 => CompiledMatcher::NotRegex(
                    matcher.name.clone(),
                    Regex::new(&format!("^(?:{})$", matcher.value))?,
                ),
                value => bail!("invalid matcher type {value}"),
            })
        })
        .collect()
}

impl CompiledMatcher {
    fn label_name(&self) -> Option<&str> {
        match self {
            CompiledMatcher::Equal(name, _)
            | CompiledMatcher::NotEqual(name, _)
            | CompiledMatcher::Regex(name, _)
            | CompiledMatcher::NotRegex(name, _) => Some(name),
            CompiledMatcher::Shard(_, _) => None,
        }
    }

    fn matches_value(&self, value: &str) -> bool {
        match self {
            CompiledMatcher::Equal(_, expected) => value == expected,
            CompiledMatcher::NotEqual(_, expected) => value != expected,
            CompiledMatcher::Regex(_, regex) => regex.is_match(value),
            CompiledMatcher::NotRegex(_, regex) => !regex.is_match(value),
            CompiledMatcher::Shard(_, _) => true,
        }
    }
}

fn matches(labels: &[StoredLabel], matchers: &[CompiledMatcher]) -> bool {
    matchers.iter().all(|matcher| match matcher {
        CompiledMatcher::Shard(index, count) => {
            stable_hash_pairs(
                labels
                    .iter()
                    .map(|(name, value)| (name.as_ref(), value.as_str())),
            ) % count
                == *index
        }
        _ => {
            let name = matcher.label_name().expect("label matcher");
            let value = labels
                .binary_search_by(|(label, _)| label.as_ref().cmp(name))
                .map_or("", |index| labels[index].1.as_str());
            matcher.matches_value(value)
        }
    })
}

fn parse_shard(value: &str) -> Result<(u64, u64)> {
    let mut parts = value.split('_');
    let one_based: u64 = parts.next().context("missing shard index")?.parse()?;
    if parts.next() != Some("of") {
        bail!("invalid shard ID {value:?}");
    }
    let count: u64 = parts.next().context("missing shard count")?.parse()?;
    if parts.next().is_some() || one_based == 0 || count == 0 || one_based > count {
        bail!("invalid shard ID {value:?}");
    }
    Ok((one_based - 1, count))
}

fn labels_hash(labels: &[StoredLabel]) -> u64 {
    hash_label_pairs(
        labels
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_str())),
    )
}

// Go's `labels.Hash` of stringlabels; the buffer is reused because ingest hashes every incoming
// series.
fn hash_label_pairs<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> u64 {
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUFFER.with_borrow_mut(|bytes| {
        bytes.clear();
        for (name, value) in pairs {
            encode_label_size(bytes, name.len());
            bytes.extend_from_slice(name.as_bytes());
            encode_label_size(bytes, value.len());
            bytes.extend_from_slice(value.as_bytes());
        }
        xxhash64(bytes)
    })
}

fn series_key(labels: StoredLabels) -> SeriesKey {
    (labels_hash(&labels), Arc::new(labels))
}

fn encode_label_size(bytes: &mut Vec<u8>, size: usize) {
    if size < 255 {
        bytes.push(size as u8);
    } else {
        bytes.push(255);
        bytes.push(size as u8);
        bytes.push((size >> 8) as u8);
        bytes.push((size >> 16) as u8);
    }
}

/// Go's `labels.StableHash`, which the head shards queries by and Mimir's ingest pusher routes
/// series with.
fn stable_hash_pairs<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> u64 {
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUFFER.with_borrow_mut(|bytes| {
        bytes.clear();
        for (name, value) in pairs {
            bytes.extend_from_slice(name.as_bytes());
            bytes.push(0xff);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0xff);
        }
        xxhash64(bytes)
    })
}

fn xxhash64(bytes: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;
    let round = |acc: u64, value: u64| {
        acc.wrapping_add(value.wrapping_mul(P2))
            .rotate_left(31)
            .wrapping_mul(P1)
    };
    let mut index = 0;
    let mut hash = if bytes.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0_u64.wrapping_sub(P1);
        while index + 32 <= bytes.len() {
            v1 = round(v1, read_u64(bytes, index));
            v2 = round(v2, read_u64(bytes, index + 8));
            v3 = round(v3, read_u64(bytes, index + 16));
            v4 = round(v4, read_u64(bytes, index + 24));
            index += 32;
        }
        let mut hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        for value in [v1, v2, v3, v4] {
            hash ^= round(0, value);
            hash = hash.wrapping_mul(P1).wrapping_add(P4);
        }
        hash
    } else {
        P5
    };
    hash = hash.wrapping_add(bytes.len() as u64);
    while index + 8 <= bytes.len() {
        hash ^= round(0, read_u64(bytes, index));
        hash = hash.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        index += 8;
    }
    if index + 4 <= bytes.len() {
        hash ^= u64::from(read_u32(bytes, index)).wrapping_mul(P1);
        hash = hash.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        index += 4;
    }
    while index < bytes.len() {
        hash ^= u64::from(bytes[index]).wrapping_mul(P5);
        hash = hash.rotate_left(11).wrapping_mul(P1);
        index += 1;
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(P3);
    hash ^ (hash >> 32)
}

fn read_u64(bytes: &[u8], index: usize) -> u64 {
    u64::from_le_bytes(bytes[index..index + 8].try_into().expect("length checked"))
}

fn read_u32(bytes: &[u8], index: usize) -> u32 {
    u32::from_le_bytes(bytes[index..index + 4].try_into().expect("length checked"))
}

fn owned_labels(labels: &[StoredLabel]) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn stored_label_pairs(labels: &[StoredLabel]) -> Vec<cortexpb::LabelPair> {
    labels
        .iter()
        .map(|(name, value)| cortexpb::LabelPair {
            name: name.as_bytes().to_vec().into(),
            value: value.as_bytes().to_vec().into(),
        })
        .collect()
}

fn encode_series_labels(labels: &[StoredLabel]) -> Bytes {
    cortex::QueryStreamSeries {
        labels: stored_label_pairs(labels),
        chunk_count: 0,
    }
    .encode_to_vec()
    .into()
}

#[path = "head_snapshot.rs"]
mod head_snapshot;
pub use head_snapshot::{Restored, SnapshotOffset};

#[cfg(test)]
#[path = "store_memory_benchmark.rs"]
mod memory_benchmark;

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn matchers_find_sorted_labels_and_treat_missing_labels_as_empty() {
        let labels = vec![
            (Arc::from("__name__"), "metric".into()),
            (Arc::from("first"), "one".into()),
            (Arc::from("last"), "two".into()),
        ];
        for (kind, name, value, expected) in [
            (0, "last", "two", true),
            (0, "last", "one", false),
            (0, "absent", "", true),
            (1, "absent", "", false),
            (1, "absent", "two", true),
            (2, "absent", ".*", true),
            (3, "absent", ".+", true),
        ] {
            let matchers = compile_matchers(&[cortex::LabelMatcher {
                r#type: kind,
                name: name.into(),
                value: value.into(),
            }])
            .unwrap();
            assert_eq!(matches(&labels, &matchers), expected);
        }
    }

    #[test]
    fn conflicting_samples_keep_the_first_value_and_continue() {
        let store = Store::default();
        let request = |timestamp_ms, value| DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![("__name__".into(), "metric".into())],
                samples: vec![cortexpb::Sample {
                    timestamp_ms,
                    value,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        };
        store.ingest("tenant", request(1, 1.0)).unwrap();
        store.ingest("tenant", request(1, 2.0)).unwrap();
        store.ingest("tenant", request(2, 3.0)).unwrap();

        assert_eq!(
            float_samples(&store, i64::MIN, i64::MAX),
            vec![(1, 1.0), (2, 3.0)]
        );
    }

    #[test]
    fn invalid_series_does_not_abort_other_series() {
        let store = Store::default();
        let series = |labels: Vec<(String, String)>| DecodedSeries {
            labels,
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1,
                value: 1.0,
            }],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            created_timestamp: 0,
        };
        store
            .ingest(
                "tenant",
                DecodedRequest {
                    source: 0,
                    series: vec![
                        series(vec![
                            ("name".into(), "a".into()),
                            ("name".into(), "b".into()),
                        ]),
                        series(vec![("__name__".into(), "valid".into())]),
                    ],
                    metadata: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(store.num_series("tenant"), 1);
    }

    fn store_with(limits: Limits) -> Store {
        let overrides = Overrides::new(limits);
        overrides.set_active_partitions(1);
        Store::default().with_overrides(Arc::new(overrides))
    }

    fn out_of_order_limits() -> Limits {
        Limits {
            out_of_order_time_window_ms: 2 * HOUR,
            ..Limits::default()
        }
    }

    const HOUR: i64 = 3_600_000;

    fn series_request(name: &str, samples: impl IntoIterator<Item = (i64, f64)>) -> DecodedRequest {
        let mut request = float_request(samples);
        request.series[0].labels[0].1 = name.into();
        request
    }

    fn discarded(reason: DiscardReason, tenant: &str) -> u64 {
        metrics::DISCARDED_SAMPLES
            .with_label_values(&[reason.label(), tenant, ""])
            .get()
    }

    fn float_request(samples: impl IntoIterator<Item = (i64, f64)>) -> DecodedRequest {
        DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![("__name__".into(), "metric".into())],
                samples: samples
                    .into_iter()
                    .map(|(timestamp_ms, value)| cortexpb::Sample {
                        timestamp_ms,
                        value,
                    })
                    .collect(),
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        }
    }

    fn histogram_at(timestamp: i64, buckets: u32) -> cortexpb::Histogram {
        cortexpb::Histogram {
            timestamp,
            count: Some(cortexpb::histogram::Count::CountInt(u64::from(buckets))),
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: buckets,
            }],
            // One observation per bucket, so the count matches like `Histogram.Validate` wants.
            positive_deltas: (0..buckets).map(|bucket| i64::from(bucket == 0)).collect(),
            ..Default::default()
        }
    }

    fn query(store: &Store, start: i64, end: i64) -> Vec<cortex::Chunk> {
        store
            .select_chunks("tenant", start, end, &[])
            .unwrap()
            .iter()
            .flat_map(|view| view.chunks[view.chunk_start..view.chunk_end].to_vec())
            .map(|chunk| cortex::Chunk::decode(chunk.wire.as_ref()).unwrap())
            .collect()
    }

    fn float_samples(store: &Store, start: i64, end: i64) -> Vec<(i64, f64)> {
        let mut samples = query(store, start, end)
            .iter()
            .filter(|chunk| chunk.encoding == i32::from(XOR_ENCODING))
            .flat_map(|chunk| xor::decode(&chunk.data))
            .collect::<Vec<_>>();
        samples.sort_by_key(|(timestamp, _)| *timestamp);
        samples
    }

    fn with_series<T>(store: &Store, check: impl FnOnce(&Series) -> T) -> T {
        for shard in &store.shards {
            let state = shard.read().unwrap();
            if let Some(series) = state
                .tenants
                .get("tenant")
                .and_then(|tenant| tenant.series.values().next())
            {
                return check(series);
            }
        }
        panic!("tenant has no series")
    }

    #[test]
    fn moves_completed_float_chunks_to_disk_at_range_boundaries() {
        let directory =
            std::env::temp_dir().join(format!("mimir-rust-store-chunks-{}", std::process::id()));
        let store = Store::new(20 * 60 * 1000, None, Some(directory.clone())).unwrap();
        let samples = (0..2_000)
            .map(|index| (CHUNK_RANGE_MS - 3_600_000 + index * 15_000, index as f64))
            .collect::<Vec<_>>();
        for batch in samples.chunks(100) {
            store
                .ingest("tenant", float_request(batch.to_vec()))
                .unwrap();
        }
        with_series(&store, |series| {
            assert!(series.chunks.len() >= 8, "chunks={}", series.chunks.len());
            let head = series.float_head.as_ref().unwrap();
            assert!(head.appender.len() <= SAMPLES_PER_CHUNK * 2);
            for chunk in &series.chunks {
                assert_eq!(
                    range_end(chunk.min_time),
                    range_end(chunk.max_time),
                    "chunk {}..{} crosses a range boundary",
                    chunk.min_time,
                    chunk.max_time
                );
            }
        });
        assert!(fs::read_dir(&directory).unwrap().count() > 0);
        assert_eq!(float_samples(&store, i64::MIN, i64::MAX), samples);
        let recent = float_samples(&store, samples[1_990].0, i64::MAX)
            .into_iter()
            .filter(|(timestamp, _)| *timestamp >= samples[1_990].0)
            .collect::<Vec<_>>();
        assert_eq!(recent, samples[1_990..]);
        assert!(query(&store, samples[1_990].0, i64::MAX).len() <= 2);
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn returns_out_of_order_samples_with_in_order_ones() {
        let store = store_with(out_of_order_limits());
        store
            .ingest(
                "tenant",
                float_request((10..20).map(|t| (t * 1000, t as f64))),
            )
            .unwrap();
        store
            .ingest(
                "tenant",
                float_request([(5_000, 5.0), (3_000, 3.0), (5_000, 50.0)]),
            )
            .unwrap();
        store
            .ingest("tenant", float_request([(19_000, 190.0), (21_000, 21.0)]))
            .unwrap();
        let mut expected = vec![(3_000, 3.0), (5_000, 5.0)];
        expected.extend((10..20).map(|t| (t * 1000, t as f64)));
        expected.push((21_000, 21.0));
        assert_eq!(float_samples(&store, i64::MIN, i64::MAX), expected);

        store
            .ingest(
                "tenant",
                float_request((0..OUT_OF_ORDER_CAPACITY as i64).map(|t| (100 + t, 0.0))),
            )
            .unwrap();
        with_series(&store, |series| {
            assert_eq!(series.chunks.len(), 1);
            assert_eq!(series.out_of_order.len(), 2);
        });
        assert_eq!(
            float_samples(&store, i64::MIN, i64::MAX).len(),
            expected.len() + OUT_OF_ORDER_CAPACITY
        );
    }

    #[test]
    fn series_stay_small() {
        // The store keeps every series of the retention inline in its tables, most of them floats
        // no longer written to, so what a series holds inline is paid millions of times.
        assert!(
            std::mem::size_of::<Series>() <= 208,
            "{}",
            std::mem::size_of::<Series>()
        );
    }

    #[test]
    fn keeps_every_sample_type_of_a_series() {
        let store = Store::default();
        let mut request = series_request("mixed", [(1_000, 12.5)]);
        request.series[0].created_timestamp = 500;
        let mut float = histogram_at(1_200, 1);
        float.count = Some(cortexpb::histogram::Count::CountFloat(1.0));
        float.positive_deltas = Vec::new();
        float.positive_counts = vec![1.0];
        request.series[0].histograms = vec![histogram_at(1_100, 1), float];
        store.ingest("tenant", request).unwrap();
        let chunks = query(&store, 0, 2_000);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.start_timestamp_ms, chunk.end_timestamp_ms))
                .collect::<Vec<_>>(),
            [(500, 1_000), (1_100, 1_100), (1_200, 1_200)]
        );
    }

    #[test]
    fn picks_tenants_to_compact_early_like_mimir() {
        let config = EarlyHeadCompaction {
            min_in_memory_series: 100,
            min_reduction_percentage: 15,
        };
        let mut estimations = vec![
            ("small".to_owned(), 5, 50),
            ("big".to_owned(), 30, 10),
            ("bigger".to_owned(), 40, 10),
        ];
        // 160 series: getting under 100 needs the two biggest reductions; small drops half.
        assert_eq!(
            tenants_to_compact_early(160, config, &mut estimations),
            ["bigger", "big", "small"]
        );
        // Under 15% of reductions in total, compacting is not worth it.
        let mut few = vec![("a".to_owned(), 10, 5)];
        assert!(tenants_to_compact_early(1_000, config, &mut few).is_empty());
    }

    #[test]
    fn compacts_the_head_early_to_the_idle_timeout_and_rejects_older_samples() {
        let tenant = "early";
        let store = Store::new(1, None, None)
            .unwrap()
            .with_early_head_compaction(Some(EarlyHeadCompaction {
                min_in_memory_series: 2,
                min_reduction_percentage: 15,
            }));
        let now = now_ms();
        for (name, timestamp) in [("a", now - 20 * 60_000), ("b", now - 10 * 60_000)] {
            store
                .ingest(tenant, series_request(name, [(timestamp, 1.0)]))
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
        // Every series is inactive; the check runs on the tick after a compaction.
        assert_eq!(store.head_tick(true, false)[0].memory_series, 2);
        let checked = store.head_tick(false, false).remove(0);
        assert_eq!(checked.head_min_time, now - 10 * 60_000 + 1);
        assert_eq!(store.head_tick(false, false)[0].memory_series, 0);
        // The truncation is Prometheus's min valid time for in-order samples.
        store
            .ingest(tenant, series_request("c", [(now - 15 * 60_000, 1.0)]))
            .unwrap();
        assert_eq!(discarded(DiscardReason::OutOfBounds, tenant), 1);
    }

    #[test]
    fn evicts_non_owned_series_after_the_max_grace_period_below_the_threshold() {
        let tenant = "evict-max";
        let overrides = Arc::new(Overrides::new(Limits {
            early_head_compaction_owned_series_threshold: 400_000_000,
            ..Limits::default()
        }));
        overrides.set_active_partitions(12);
        let store = Store::default()
            .with_overrides(overrides)
            .with_non_owned_eviction(Some(NonOwnedEviction {
                min_grace_ms: 30_000,
                max_grace_ms: 1,
                jitter_ms: 0,
            }));
        for name in ["a", "b", "c"] {
            store
                .ingest(tenant, series_request(name, [(now_ms(), 1.0)]))
                .unwrap();
        }
        // Owned ranges that include no series.
        store.set_owned_ranges(HashMap::from([(tenant.to_owned(), Some(vec![0, 0]))]));
        let first = store.head_tick(true, true).remove(0);
        assert_eq!((first.memory_series, first.owned_series), (3, 0));
        std::thread::sleep(Duration::from_millis(1_100));
        let evicted = store.head_tick(true, true).remove(0);
        assert_eq!(evicted.non_owned_evicted, 3);
        assert_eq!(evicted.memory_series, 0);
    }

    #[test]
    fn evicts_non_owned_series_from_the_head_like_mimir_early_compaction() {
        let tenant = "evict";
        let store = store_with(Limits {
            early_head_compaction_owned_series_threshold: 1,
            ..Limits::default()
        })
        .with_non_owned_eviction(Some(NonOwnedEviction {
            min_grace_ms: 0,
            max_grace_ms: 0,
            jitter_ms: 0,
        }));
        let ingest = |name: &str| {
            store
                .ingest(tenant, series_request(name, [(now_ms(), 1.0)]))
                .unwrap();
        };
        ingest("a");
        ingest("b");
        let hash = |name: &str| {
            store
                .per_shard(tenant, |tenant, _| {
                    tenant
                        .series
                        .iter()
                        .filter(|((_, labels), _)| labels[0].1 == name)
                        .map(|(_, series)| series.owned_hash)
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .flatten()
                .next()
                .unwrap()
        };
        let owned = hash("a");
        store.set_owned_ranges(HashMap::from([(
            tenant.to_owned(),
            Some(vec![owned, owned]),
        )]));
        let tick = |compact| store.head_tick(compact, true).remove(0);
        // The recompute finds b non-owned; nothing is evicted before a compaction.
        let first = tick(false);
        assert_eq!((first.memory_series, first.owned_series), (2, 1));
        let compacted = tick(true);
        assert_eq!(compacted.non_owned_evicted, 1);
        assert_eq!(compacted.memory_series, 1);
        assert_eq!(compacted.series_removed, 1);
        assert_eq!(store.user_stats(tenant, false).num_series, 1);
        // Its data stays in a block for label lookups, and a new sample brings it back.
        let names = || {
            store
                .label_values(tenant, "__name__", i64::MIN, i64::MAX, &[])
                .unwrap()
        };
        assert_eq!(names(), ["a", "b"]);
        ingest("b");
        let back = tick(false);
        assert_eq!((back.memory_series, back.series_created), (2, 1));
    }

    #[test]
    fn shards_like_the_go_ingester() {
        // labels.StableHash values from Go.
        let hash = |pairs: &[(&str, &str)]| stable_hash_pairs(pairs.iter().copied());
        assert_eq!(
            hash(&[("__name__", "up"), ("job", "api")]),
            2_852_606_813_363_628_783
        );
        assert_eq!(
            hash(&[("__name__", "sharded"), ("n", "1")]),
            609_427_224_681_409_917
        );
        let store = Store::default();
        let mut request = series_request("sharded", [(1_000, 1.0)]);
        request.series[0].labels.push(("n".into(), "1".into()));
        store.ingest("tenant", request).unwrap();
        let shard = |value: &str| {
            store
                .select_chunks(
                    "tenant",
                    i64::MIN,
                    i64::MAX,
                    &[cortex::LabelMatcher {
                        r#type: 0,
                        name: "__query_shard__".into(),
                        value: value.into(),
                    }],
                )
                .unwrap()
                .len()
        };
        let index = 609_427_224_681_409_917_u64 % 3;
        assert_eq!(shard(&format!("{}_of_3", index + 1)), 1);
        assert_eq!(shard(&format!("{}_of_3", (index + 1) % 3 + 1)), 0);
        // mimirpb.ShardByAllLabels from Go, which owned series are counted by.
        let labels: StoredLabels = [("__name__", "up"), ("job", "api")]
            .into_iter()
            .map(|(name, value)| (Arc::<str>::from(name), CompactString::from(value)))
            .collect();
        assert_eq!(shard_by_all_labels("18657", &labels), 4_096_482_777);
        // Mimir's idealShardsFor.
        let pusher = PusherShards {
            max: 2,
            ..PusherShards::default()
        };
        assert_eq!(pusher.count(2 * 200 * 150 * 40 - 1, 150), 1);
        assert_eq!(pusher.count(2 * 200 * 150 * 40, 150), 2);
        assert_eq!(pusher.count(usize::MAX, 150), 2);
        assert_eq!(pusher.count(usize::MAX, 0), 1);
    }

    #[test]
    fn lookups_over_the_whole_time_range_see_compacted_blocks() {
        let store = Store::default();
        let start = 10 * HOUR;
        let mut request = series_request(
            "long",
            (0..=300).map(|minute| (start + minute * 60_000, 1.0)),
        );
        let old = series_request("old", [(start, 1.0)]).series.remove(0);
        request.series.push(old);
        store.ingest("tenant", request).unwrap();
        store.head_tick(true, false);
        let names = |start, end| {
            store
                .label_values("tenant", "__name__", start, end, &[])
                .unwrap()
        };
        assert_eq!(names(12 * HOUR, 15 * HOUR), ["long"]);
        assert_eq!(names(0, i64::MAX), ["long", "old"]);
        assert_eq!(names(i64::MIN, i64::MAX), ["long", "old"]);
        let (_, blocks) = store
            .select_chunks_with_blocks("tenant", i64::MIN, i64::MAX, &[])
            .unwrap();
        assert_eq!(
            blocks
                .iter()
                .map(|block| (block.generation.as_str(), block.series))
                .collect::<Vec<_>>(),
            [("0", 1), ("1", 2)]
        );
    }

    #[test]
    fn keeps_dense_histograms_in_one_chunk_like_the_prometheus_head() {
        let store = store_with(out_of_order_limits());
        let base = 1_790_568_000_000;
        let histograms = (0..300)
            .map(|index| {
                let timestamp = base + index * 10;
                cortexpb::Histogram {
                    sum: (timestamp % 997) as f64,
                    ..histogram_at(timestamp, 5)
                }
            })
            .collect();
        let mut request = series_request("dense", []);
        request.series[0].histograms = histograms;
        store.ingest("tenant", request).unwrap();
        // A Prometheus head, given the same samples, keeps them in one 918-byte chunk.
        let chunks = query(&store, i64::MIN, i64::MAX);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (
                    chunk.start_timestamp_ms - base,
                    chunk.end_timestamp_ms - base,
                    chunk.data.len()
                ))
                .collect::<Vec<_>>(),
            [(0, 2_990, 918)]
        );
    }

    #[test]
    fn appends_histograms_like_the_prometheus_head() {
        let store = store_with(out_of_order_limits());
        let request = |histograms: Vec<cortexpb::Histogram>| DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![("__name__".into(), "metric".into())],
                samples: Vec::new(),
                histograms,
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        };
        store
            .ingest(
                "tenant",
                request((0..20).map(|t| histogram_at(1_000 + t * 10, 5)).collect()),
            )
            .unwrap();
        // More buckets recode the open chunk instead of cutting it; the older sample waits in the
        // out-of-order chunk.
        store
            .ingest(
                "tenant",
                request(vec![histogram_at(10_000, 7), histogram_at(500, 5)]),
            )
            .unwrap();
        with_series(&store, |series| {
            assert!(series.chunks.is_empty());
            assert_eq!(series.out_of_order.len(), 1);
            assert_eq!(
                series
                    .histogram_head
                    .0
                    .as_ref()
                    .map_or(0, |head| head.len()),
                21
            );
            assert_eq!(series.last_bucket_count, 5);
        });
        let chunks = query(&store, 3_500, 10_000);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.start_timestamp_ms, chunk.end_timestamp_ms))
                .collect::<Vec<_>>(),
            vec![(1_000, 10_000)]
        );
        // A counter reset starts a new chunk.
        let mut reset = histogram_at(11_000, 7);
        reset.positive_deltas = vec![0; 7];
        reset.count = Some(cortexpb::histogram::Count::CountInt(0));
        store.ingest("tenant", request(vec![reset])).unwrap();
        with_series(&store, |series| {
            assert_eq!(
                series
                    .chunks
                    .iter()
                    .map(|chunk| (chunk.min_time, chunk.max_time))
                    .collect::<Vec<_>>(),
                vec![(1_000, 10_000)]
            );
            let head = series.histogram_head.0.as_ref().unwrap();
            assert_eq!(head.header(), histogram::Header::CounterReset);
        });
    }

    #[test]
    fn selects_float_chunk_spanning_histogram_chunks() {
        let store = Store::default();
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        store
            .ingest(
                "tenant",
                DecodedRequest {
                    source: 0,
                    series: vec![DecodedSeries {
                        labels: vec![("__name__".into(), "metric".into())],
                        samples: [1005, 3995]
                            .into_iter()
                            .map(|timestamp_ms| cortexpb::Sample {
                                timestamp_ms,
                                value: stale,
                            })
                            .collect(),
                        histograms: (1000..4000)
                            .step_by(10)
                            .map(|t| histogram_at(t, 1))
                            .collect(),
                        exemplars: Vec::new(),
                        created_timestamp: 0,
                    }],
                    metadata: Vec::new(),
                },
            )
            .unwrap();
        for (start, end) in [(3500, 3999), (2500, 3999), (1100, 1200), (3995, 3995)] {
            let samples = float_samples(&store, start, end);
            assert!(
                samples.iter().any(|(timestamp, _)| *timestamp == 3995),
                "window {start}..{end} missed the stale marker"
            );
        }
    }

    #[test]
    fn selects_series_through_metric_name_groups() {
        let store = Store::default();
        let now = now_ms();
        let series = |name: Option<&str>, job: &str, timestamp_ms: i64| DecodedSeries {
            labels: name
                .map(|name| ("__name__".to_owned(), name.to_owned()))
                .into_iter()
                .chain([("job".to_owned(), job.to_owned())])
                .collect(),
            samples: vec![cortexpb::Sample {
                timestamp_ms,
                value: 1.0,
            }],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            created_timestamp: 0,
        };
        store
            .ingest(
                "tenant",
                DecodedRequest {
                    source: 0,
                    series: vec![
                        series(Some("up"), "a", now),
                        series(Some("up"), "b", now),
                        series(Some("down"), "a", now - 1_000),
                        series(None, "a", now),
                    ],
                    metadata: Vec::new(),
                },
            )
            .unwrap();
        let count = |matchers: &[(i32, &str, &str)]| {
            let matchers = matchers
                .iter()
                .map(|(kind, name, value)| cortex::LabelMatcher {
                    r#type: *kind,
                    name: (*name).into(),
                    value: (*value).into(),
                })
                .collect::<Vec<_>>();
            store
                .select_labels("tenant", i64::MIN, i64::MAX, &matchers)
                .unwrap()
                .len()
        };
        assert_eq!(count(&[(0, "__name__", "up")]), 2);
        assert_eq!(count(&[(0, "__name__", "up"), (0, "job", "b")]), 1);
        assert_eq!(count(&[(2, "__name__", "up|down")]), 3);
        assert_eq!(count(&[(1, "__name__", "up")]), 2);
        assert_eq!(count(&[(0, "__name__", "")]), 1);
        assert_eq!(count(&[(0, "job", "a")]), 3);
        assert_eq!(count(&[(0, "__name__", "missing")]), 0);
        assert_eq!(store.num_series("tenant"), 4);
        store.prune_before(now - 1).unwrap();
        assert_eq!(store.num_series("tenant"), 3);
        assert_eq!(count(&[(0, "__name__", "down")]), 0);
    }

    #[test]
    fn returns_query_series_in_label_order() {
        let store = Store::default();
        let series = (0..200)
            .map(|index| DecodedSeries {
                labels: vec![
                    ("__name__".into(), format!("metric_{}", index % 7)),
                    ("instance".into(), format!("{:03}", (index * 37) % 200)),
                    ("Zone".into(), format!("{}", index % 3)),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 1,
                    value: 1.0,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            })
            .collect();
        store
            .ingest(
                "tenant",
                DecodedRequest {
                    source: 0,
                    series,
                    metadata: Vec::new(),
                },
            )
            .unwrap();
        let labels = store
            .select_chunks("tenant", i64::MIN, i64::MAX, &[])
            .unwrap()
            .iter()
            .map(|view| {
                cortex::QueryStreamSeries::decode(view.encoded_labels.as_ref())
                    .unwrap()
                    .labels
            })
            .map(|pairs| {
                pairs
                    .into_iter()
                    .map(|pair| (pair.name.to_vec(), pair.value.to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(labels.len(), 200);
        assert!(labels.windows(2).all(|pair| pair[0] < pair[1]));
    }

    fn mixed_records(records: i64) -> Vec<IngestRecord> {
        (0..records)
            .map(|record| IngestRecord {
                tenant: format!("tenant-{}", record % 3),
                request: DecodedRequest {
                    source: 0,
                    series: (0..50)
                        .map(|series| DecodedSeries {
                            labels: vec![
                                ("__name__".into(), format!("metric_{}", series % 5)),
                                ("id".into(), series.to_string()),
                            ],
                            // Every seventh record repeats an older timestamp to exercise out-of-order data.
                            samples: vec![cortexpb::Sample {
                                timestamp_ms: if record % 7 == 6 { record - 5 } else { record }
                                    * 1000,
                                value: (record * series) as f64,
                            }],
                            histograms: if series % 10 == 0 {
                                vec![histogram_at(record * 1000 + 1, 3)]
                            } else {
                                Vec::new()
                            },
                            exemplars: Vec::new(),
                            created_timestamp: 0,
                        })
                        .collect(),
                    metadata: Vec::new(),
                },
                ingested_ms: 0,
                track_rate: false,
                bytes: 0,
            })
            .collect()
    }

    #[test]
    fn sharded_store_matches_a_single_shard() {
        let single = Store::with_shards(20 * 60 * 1000, None, None, 1, 1).unwrap();
        let sharded = Store::with_shards(20 * 60 * 1000, None, None, 8, 4).unwrap();
        let mut records = mixed_records(400).into_iter();
        let mut records_again = mixed_records(400).into_iter();
        loop {
            let batch = records.by_ref().take(37).collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            single.ingest_batch(batch).unwrap();
            sharded
                .ingest_batch(records_again.by_ref().take(37).collect())
                .unwrap();
        }
        for tenant in ["tenant-0", "tenant-1", "tenant-2"] {
            let chunks = |store: &Store| {
                store
                    .select_chunks(tenant, i64::MIN, i64::MAX, &[])
                    .unwrap()
                    .into_iter()
                    .map(|view| (view.encoded_labels, view.chunks.as_ref().clone()))
                    .map(|(labels, chunks)| {
                        (
                            labels,
                            chunks
                                .into_iter()
                                .map(|chunk| chunk.wire)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let expected = chunks(&single);
            assert_eq!(expected.len(), 50);
            assert_eq!(chunks(&sharded), expected);
            assert_eq!(sharded.num_series(tenant), single.num_series(tenant));
            assert_eq!(
                sharded
                    .label_values(tenant, "id", i64::MIN, i64::MAX, &[])
                    .unwrap(),
                single
                    .label_values(tenant, "id", i64::MIN, i64::MAX, &[])
                    .unwrap()
            );
        }
        assert_eq!(
            sharded
                .all_user_stats(false)
                .into_iter()
                .map(|(id, stats)| (id, stats.num_series))
                .collect::<Vec<_>>(),
            single
                .all_user_stats(false)
                .into_iter()
                .map(|(id, stats)| (id, stats.num_series))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn parallel_batch_applies_each_series_in_record_order() {
        let store = Store::with_shards(20 * 60 * 1000, None, None, 8, 4).unwrap();
        let batch = (0..200)
            .map(|timestamp| IngestRecord {
                tenant: "tenant".into(),
                request: float_request([(timestamp, timestamp as f64)]),
                ingested_ms: 0,
                track_rate: false,
                bytes: 0,
            })
            .collect();
        store.ingest_batch(batch).unwrap();
        with_series(&store, |series| {
            assert!(series.out_of_order.is_empty());
            assert!(
                series
                    .chunks
                    .iter()
                    .all(|chunk| chunk.min_time <= chunk.max_time)
            );
        });
        assert_eq!(
            float_samples(&store, i64::MIN, i64::MAX),
            (0..200)
                .map(|timestamp| (timestamp, timestamp as f64))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn prunes_whole_expired_chunks_and_series() {
        let store = Store::default();
        let now = now_ms();
        store
            .ingest(
                "tenant",
                float_request(
                    (0..300).map(|index| (now - 3 * CHUNK_RANGE_MS + index * 15_000, 1.0)),
                ),
            )
            .unwrap();
        store.ingest("tenant", float_request([(now, 2.0)])).unwrap();
        with_series(&store, |series| assert!(!series.chunks.is_empty()));
        store.prune_before(now - 1_000).unwrap();
        with_series(&store, |series| {
            assert!(series.chunks.is_empty());
            assert!(series.float_head.is_some());
        });
        assert_eq!(float_samples(&store, i64::MIN, i64::MAX), vec![(now, 2.0)]);
        store.prune_before(now + 1).unwrap();
        assert_eq!(store.num_series("tenant"), 0);
    }

    fn samples_of(store: &Store, tenant: &str, name: &str) -> Vec<(i64, f64)> {
        store
            .select_chunks(
                tenant,
                i64::MIN,
                i64::MAX,
                &[cortex::LabelMatcher {
                    r#type: 0,
                    name: "__name__".into(),
                    value: name.into(),
                }],
            )
            .unwrap()
            .into_iter()
            .flat_map(|series| series.chunks[series.chunk_start..series.chunk_end].to_vec())
            .flat_map(|chunk| {
                let decoded = cortex::Chunk::decode(chunk.wire.clone()).unwrap();
                xor::decode(&decoded.data)
            })
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect()
    }

    #[test]
    fn without_an_out_of_order_window_rejects_like_the_head_appender() {
        let tenant = "ooo-disabled";
        let store = store_with(Limits::default());
        store
            .ingest(tenant, series_request("a", [(10 * HOUR, 1.0)]))
            .unwrap();
        // Older than the series but within an hour of the head: out of order.
        store
            .ingest(tenant, series_request("a", [(10 * HOUR - 1, 2.0)]))
            .unwrap();
        // More than an hour behind the head: out of bounds, even for a new series.
        store
            .ingest(tenant, series_request("a", [(9 * HOUR - 1, 3.0)]))
            .unwrap();
        store
            .ingest(tenant, series_request("b", [(9 * HOUR - 1, 3.0)]))
            .unwrap();
        // A new series within the hour is in order.
        store
            .ingest(tenant, series_request("c", [(9 * HOUR + 1, 4.0)]))
            .unwrap();
        // Same timestamp: the same value is a no-op, another value is rejected.
        store
            .ingest(
                tenant,
                series_request("a", [(10 * HOUR, 1.0), (10 * HOUR, 5.0)]),
            )
            .unwrap();
        assert_eq!(samples_of(&store, tenant, "a"), [(10 * HOUR, 1.0)]);
        assert!(samples_of(&store, tenant, "b").is_empty());
        assert_eq!(samples_of(&store, tenant, "c"), [(9 * HOUR + 1, 4.0)]);
        assert_eq!(discarded(DiscardReason::OutOfOrder, tenant), 1);
        assert_eq!(discarded(DiscardReason::OutOfBounds, tenant), 2);
        assert_eq!(discarded(DiscardReason::NewValueForTimestamp, tenant), 1);
        assert_eq!(
            metrics::INGESTED_SAMPLES.with_label_values(&[tenant]).get(),
            3
        );
    }

    #[test]
    fn out_of_order_window_accepts_recent_samples_and_rejects_older_ones() {
        let tenant = "ooo-enabled";
        let store = store_with(out_of_order_limits());
        store
            .ingest(tenant, series_request("a", [(10 * HOUR, 1.0)]))
            .unwrap();
        store
            .ingest(
                tenant,
                series_request("a", [(8 * HOUR + 1, 2.0), (8 * HOUR - 1, 3.0)]),
            )
            .unwrap();
        store
            .ingest(tenant, series_request("b", [(8 * HOUR + 2, 4.0)]))
            .unwrap();
        assert_eq!(
            samples_of(&store, tenant, "a"),
            [(8 * HOUR + 1, 2.0), (10 * HOUR, 1.0)]
        );
        assert_eq!(samples_of(&store, tenant, "b"), [(8 * HOUR + 2, 4.0)]);
        assert_eq!(discarded(DiscardReason::TooOld, tenant), 1);
    }

    #[test]
    fn head_max_time_is_per_tenant_and_taken_per_flush() {
        let store = store_with(Limits::default());
        let record = |tenant: &str, name: &str, timestamp: i64| IngestRecord {
            tenant: tenant.into(),
            request: series_request(name, [(timestamp, 1.0)]),
            ingested_ms: 0,
            track_rate: false,
            bytes: 0,
        };
        // An empty head takes its max time from the flush's first sample, like the head's
        // initAppender.
        store
            .ingest_batch(vec![
                record("per-record", "a", 10 * HOUR),
                record("per-record", "b", 8 * HOUR),
                record("per-record-other", "b", 8 * HOUR),
            ])
            .unwrap();
        assert!(samples_of(&store, "per-record", "b").is_empty());
        assert_eq!(
            samples_of(&store, "per-record-other", "b"),
            [(8 * HOUR, 1.0)]
        );
        // A head with samples keeps the max time it had when the flush's appender was created.
        store
            .ingest_batch(vec![record("same-record", "a", 8 * HOUR)])
            .unwrap();
        store
            .ingest_batch(vec![IngestRecord {
                tenant: "same-record".into(),
                request: DecodedRequest {
                    source: 0,
                    series: vec![
                        series_request("a", [(10 * HOUR, 1.0)]).series.remove(0),
                        series_request("b", [(7 * HOUR + 1, 1.0)]).series.remove(0),
                    ],
                    metadata: Vec::new(),
                },
                ingested_ms: 0,
                track_rate: false,
                bytes: 0,
            }])
            .unwrap();
        assert_eq!(
            samples_of(&store, "same-record", "b"),
            [(7 * HOUR + 1, 1.0)]
        );
    }

    #[test]
    fn rejects_samples_outside_the_grace_periods() {
        let tenant = "grace";
        let store = store_with(Limits {
            past_grace_period_ms: HOUR,
            ..Limits::default()
        });
        let now = now_ms();
        store
            .ingest(
                tenant,
                series_request(
                    "a",
                    [(now - 2 * HOUR, 1.0), (now, 2.0), (now + 11 * 60_000, 3.0)],
                ),
            )
            .unwrap();
        assert_eq!(samples_of(&store, tenant, "a"), [(now, 2.0)]);
        assert_eq!(discarded(DiscardReason::TooFarInFuture, tenant), 1);
        // Mimir's ingest pusher drops samples past the grace period without counting them.
        assert_eq!(discarded(DiscardReason::TooFarInPast, tenant), 0);
    }

    #[test]
    fn drops_native_histograms_when_disabled() {
        let store = store_with(Limits {
            native_histograms_ingestion_enabled: false,
            ..Limits::default()
        });
        let mut request = series_request("h", []);
        request.series[0].histograms = vec![histogram_at(1_000, 2)];
        store.ingest("no-histograms", request).unwrap();
        assert_eq!(store.num_series("no-histograms"), 1);
        assert!(query(&store, i64::MIN, i64::MAX).is_empty());
    }

    fn metadata(name: &str, help: &str) -> cortexpb::MetricMetadata {
        cortexpb::MetricMetadata {
            r#type: 1,
            metric_family_name: name.into(),
            help: help.into(),
            unit: String::new(),
        }
    }

    #[test]
    fn metadata_limits_and_retention_follow_mimir() {
        let tenant = "metadata-limits";
        let store = store_with(Limits {
            max_global_metadata_per_user: 2,
            max_global_metadata_per_metric: 2,
            ..Limits::default()
        });
        let ingest = |entries: Vec<cortexpb::MetricMetadata>| {
            store
                .ingest(
                    tenant,
                    DecodedRequest {
                        source: 0,
                        series: Vec::new(),
                        metadata: entries,
                    },
                )
                .unwrap()
        };
        ingest(vec![
            metadata("a", "1"),
            metadata("a", "2"),
            metadata("a", "3"),
        ]);
        ingest(vec![metadata("b", "1"), metadata("c", "1")]);
        // A known entry of a full metric is rejected too, as Mimir checks the set size first.
        ingest(vec![metadata("a", "1")]);
        let mut names = store
            .metadata(tenant)
            .into_iter()
            .map(|entry| format!("{}:{}", entry.metric_family_name, entry.help))
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["a:1", "a:2", "b:1"]);
        let discarded = |reason| {
            metrics::DISCARDED_METADATA
                .with_label_values(&[reason, tenant])
                .get()
        };
        assert_eq!(discarded("per_metric_metadata_limit"), 2);
        assert_eq!(discarded("per_user_metadata_limit"), 1);
        store.purge_metadata(60_000);
        assert_eq!(store.metadata(tenant).len(), 3);
        store.purge_metadata(-60_000);
        assert!(store.metadata(tenant).is_empty());
    }

    fn exemplar_request(name: &str, sample: Option<i64>, exemplars: &[i64]) -> DecodedRequest {
        let mut request = series_request(name, sample.map(|timestamp| (timestamp, 1.0)));
        request.series[0].exemplars = exemplars
            .iter()
            .map(|timestamp_ms| cortexpb::Exemplar {
                labels: vec![cortexpb::LabelPair {
                    name: "trace_id".into(),
                    value: format!("{timestamp_ms}").into(),
                }],
                value: 1.0,
                timestamp_ms: *timestamp_ms,
            })
            .collect();
        request
    }

    fn exemplar_timestamps(store: &Store, tenant: &str) -> Vec<(String, Vec<i64>)> {
        store
            .select_exemplars(tenant, i64::MIN, i64::MAX, &[])
            .unwrap()
            .into_iter()
            .map(|series| {
                (
                    series.labels[0].1.clone(),
                    series.exemplars.iter().map(|e| e.timestamp_ms).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn exemplars_are_bounded_per_tenant_and_need_a_series() {
        let tenant = "exemplar-limits";
        let overrides = Arc::new(Overrides::new(Limits {
            max_global_exemplars_per_user: 6,
            ..Limits::default()
        }));
        overrides.set_active_partitions(2);
        let store = Store::default().with_overrides(Arc::clone(&overrides));
        // No sample and no existing series: dropped.
        store
            .ingest(tenant, exemplar_request("a", None, &[1]))
            .unwrap();
        assert!(exemplar_timestamps(&store, tenant).is_empty());
        store
            .ingest(tenant, exemplar_request("a", Some(10), &[1, 2]))
            .unwrap();
        store
            .ingest(tenant, exemplar_request("b", Some(10), &[3]))
            .unwrap();
        // An existing series takes exemplars without samples; the oldest are evicted at 6 / 2 = 3.
        store
            .ingest(tenant, exemplar_request("a", None, &[4]))
            .unwrap();
        assert_eq!(
            exemplar_timestamps(&store, tenant),
            [("a".into(), vec![2, 4]), ("b".into(), vec![3])]
        );
        // Disabling exemplars stops storing new ones.
        overrides
            .apply_runtime_config(
                serde_json::json!({"overrides": {tenant: {"max_global_exemplars_per_user": 0}}})
                    .as_object()
                    .unwrap(),
            )
            .unwrap();
        store
            .ingest(tenant, exemplar_request("b", Some(20), &[5]))
            .unwrap();
        assert_eq!(
            exemplar_timestamps(&store, tenant)[1],
            ("b".into(), vec![3])
        );
    }

    #[test]
    fn owned_series_recompute_removes_non_owned_active_series_like_mimir() {
        let tenant = "owned";
        let store = store_with(Limits {
            cost_attribution_trackers: Arc::new(
                crate::trackers::CostAttributionTrackers::from_value(&serde_json::json!({
                    "by-name": {"labels": [{"input": "__name__"}]},
                }))
                .unwrap(),
            ),
            max_cost_attribution_cardinality: 10,
            ..Limits::default()
        });
        let ingest = |name: &str| {
            store
                .ingest(tenant, series_request(name, [(now_ms(), 1.0)]))
                .unwrap();
        };
        ingest("a");
        ingest("b");
        let report = || {
            let report = store
                .active_series_report()
                .into_iter()
                .find(|report| report.tenant == tenant)
                .unwrap();
            let attributed = report.cost_attribution[0]
                .values
                .iter()
                .map(|(_, counts)| counts[0])
                .sum::<u64>();
            (report.active, attributed)
        };
        let owned = |ranges: Option<Vec<u32>>| {
            store.set_owned_ranges(HashMap::from([(tenant.to_owned(), ranges)]));
            store.head_tick(false, true)[0].owned_series
        };
        // Without tracking, owned series never change the active series.
        store.set_owned_ranges(HashMap::from([(tenant.to_owned(), None)]));
        store.head_tick(false, false);
        assert_eq!(report(), (2, 2));
        // Owning no ranges clears the active series but keeps cost attribution counting them.
        assert_eq!(owned(None), 0);
        assert_eq!(report(), (0, 2));
        ingest("a");
        assert_eq!(report(), (1, 2));
        // Unchanged ranges don't recompute, so the new sample stays active.
        assert_eq!(owned(None), 0);
        assert_eq!(report(), (1, 2));
        // Owning only a's hash deletes b, cost attribution included.
        ingest("b");
        assert_eq!(report(), (2, 2));
        let hash = store
            .per_shard(tenant, |tenant, _| {
                tenant
                    .series
                    .iter()
                    .map(|(_, series)| series.owned_hash)
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .min()
            .unwrap();
        assert_eq!(owned(Some(vec![hash, hash])), 1);
        assert_eq!(report().0, 1);
    }

    #[test]
    fn reports_active_series_custom_trackers_and_cost_attribution() {
        let tenant = "active-report";
        let limits = Limits {
            active_series_custom_trackers: Arc::new(
                crate::trackers::CustomTrackers::new(BTreeMap::from([
                    ("api".into(), r#"{job="api"}"#.into()),
                    ("all".into(), r#"{__name__=~".+"}"#.into()),
                ]))
                .unwrap(),
            ),
            cost_attribution_trackers: Arc::new(
                crate::trackers::CostAttributionTrackers::from_value(&serde_json::json!({
                    "by-team": {"labels": [{"input": "team"}]},
                    "internal": {"internal": true, "labels": [{"input": "job", "output": "service"}]},
                }))
                .unwrap(),
            ),
            max_cost_attribution_cardinality: 3,
            ..Limits::default()
        };
        let store = store_with(limits);
        let now = now_ms();
        let ingest = |job: &str, team: &str| {
            let mut request = series_request("up", [(now, 1.0)]);
            request.series[0].labels.push(("job".into(), job.into()));
            request.series[0].labels.push(("team".into(), team.into()));
            store.ingest(tenant, request).unwrap();
        };
        for (job, team) in [("api", "a"), ("api", "b"), ("web", "a")] {
            ingest(job, team);
        }
        let mut histogram = series_request("h", []);
        histogram.series[0].histograms = vec![histogram_at(now, 3)];
        store.ingest(tenant, histogram).unwrap();
        let report = || {
            store
                .active_series_report()
                .into_iter()
                .find(|report| report.tenant == tenant)
                .unwrap()
        };
        let first = report();
        assert_eq!(first.active, 4);
        assert_eq!(first.active_native_histograms, 1);
        assert_eq!(first.active_native_histogram_buckets, 3);
        assert_eq!(
            first.custom_trackers,
            [("all".to_owned(), [4, 1, 3]), ("api".to_owned(), [2, 0, 0])]
        );
        let values = |pairs: &[(&str, [u64; 3])]| {
            pairs
                .iter()
                .map(|(value, counts)| (vec![value.to_string()], *counts))
                .collect::<Vec<_>>()
        };
        let by_team = &first.cost_attribution[0];
        assert_eq!(by_team.tracker, "by-team");
        assert!(!by_team.internal && !by_team.overflow);
        assert_eq!(
            by_team.values,
            values(&[
                ("__missing__", [1, 1, 3]),
                ("a", [2, 0, 0]),
                ("b", [1, 0, 0])
            ])
        );
        let internal = &first.cost_attribution[1];
        assert!(internal.internal);
        assert_eq!(internal.output_labels, ["service"]);
        assert_eq!(
            internal.values,
            values(&[
                ("__missing__", [1, 1, 3]),
                ("api", [2, 0, 0]),
                ("web", [1, 0, 0])
            ])
        );
        // A fourth team exceeds the cardinality: one overflow entry with every series.
        ingest("api", "c");
        let second = report();
        let by_team = &second.cost_attribution[0];
        assert!(by_team.overflow);
        assert_eq!(by_team.values, values(&[(OVERFLOW_VALUE, [5, 1, 3])]));
        assert!(!second.cost_attribution[1].overflow);
    }

    #[test]
    fn postings_select_the_same_series_as_a_full_scan() {
        let mut by_name = SeriesByName::default();
        let label = |name: &str| -> Arc<str> { name.into() };
        for id in 0..400_u64 {
            let mut labels: StoredLabels = vec![
                (label("__name__"), format!("metric_{}", id % 7).into()),
                (label("job"), format!("job_{}", id % 5).into()),
                (label("pod"), format!("pod_{id}").into()),
            ];
            if id % 3 == 0 {
                labels.push((label("zone"), "a".into()));
            }
            labels.sort();
            by_name.insert(
                series_key(labels),
                Series {
                    last_ingested_ms: id as i64,
                    ..Series::default()
                },
            );
            assert_eq!(by_name.len(), id as usize + 1);
        }
        let matcher = |kind: i32, name: &str, value: &str| cortex::LabelMatcher {
            r#type: kind,
            name: name.into(),
            value: value.into(),
        };
        let cases = [
            vec![matcher(0, "job", "job_2")],
            vec![
                matcher(0, "__name__", "metric_3"),
                matcher(0, "job", "job_1"),
            ],
            vec![
                matcher(0, "__name__", "metric_3"),
                matcher(0, "pod", "pod_101"),
            ],
            vec![matcher(0, "pod", "pod_12"), matcher(0, "zone", "a")],
            vec![matcher(0, "zone", ""), matcher(0, "job", "job_4")],
            vec![
                matcher(2, "__name__", "metric_[12]"),
                matcher(0, "job", "job_0"),
            ],
            vec![matcher(0, "job", "missing")],
            vec![matcher(1, "job", "job_0")],
        ];
        let check = |by_name: &SeriesByName| {
            for case in &cases {
                let compiled = compile_matchers(case).unwrap();
                let mut expected = by_name
                    .iter()
                    .filter(|((_, labels), _)| matches(labels, &compiled))
                    .map(|((hash, _), _)| *hash)
                    .collect::<Vec<_>>();
                let mut actual = by_name
                    .matching(&compiled)
                    .map(|((hash, _), _)| *hash)
                    .collect::<Vec<_>>();
                expected.sort_unstable();
                actual.sort_unstable();
                assert_eq!(actual, expected, "{case:?}");
            }
        };
        check(&by_name);
        // Retention rebuilds the postings without the removed series.
        by_name.retain(|_, series| series.last_ingested_ms % 2 == 0);
        assert_eq!(by_name.len(), 200);
        check(&by_name);
    }
}
