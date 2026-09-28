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
use crate::exemplars::{Rejection, TenantExemplars};
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
    histogram_head: Vec<cortexpb::Histogram>,
    histogram_next_at: i64,
    // The open out-of-order chunk, like Prometheus's OOO head chunk, sorted by timestamp.
    out_of_order: Vec<(i64, ooo_merge::Value)>,
    last_ingested_ms: i64,
    last_bucket_count: u32,
    // Whether the last request's samples ended with a native histogram, as Mimir's active series
    // tracker records it.
    native_histogram: bool,
    // Whether the series is in the emulated Go head window, as of the last head tick.
    in_head: bool,
    // Mimir's `ShardByAllLabels`, which decides which partition owns the series.
    owned_hash: u32,
    // Custom trackers this series matches, computed for one overrides generation.
    tracker_generation: u64,
    tracker_matches: Box<[u16]>,
}

// Series grouped by metric name, so a `__name__` matcher only visits its own metric like a
// postings lookup would, without duplicating series keys in a separate index. Within a name,
// series are found by label hash so ingesting into an existing series allocates nothing.
#[derive(Default)]
struct SeriesByName {
    names: HashMap<CompactString, HashTable<(SeriesKey, Series)>>,
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
        if !self.names.contains_key(name) {
            self.names
                .insert(CompactString::from(name), HashTable::new());
        }
        let table = self.names.get_mut(name).expect("name group exists");
        let len = &mut self.len;
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
                let ((_, labels), series) = entry
                    .insert(((hash, labels()), Series::default()))
                    .into_mut();
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
        self.names
            .values()
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
        let candidates: Box<dyn Iterator<Item = (&SeriesKey, &Series)>> = match name_matcher {
            Some(CompiledMatcher::Equal(_, name)) => Box::new(
                self.names
                    .get(name.as_str())
                    .into_iter()
                    .flat_map(|table| table.iter().map(|(key, series)| (key, series))),
            ),
            Some(matcher) => Box::new(
                self.names
                    .iter()
                    .filter(move |(name, _)| matcher.matches_value(name))
                    .flat_map(|(_, table)| table.iter().map(|(key, series)| (key, series))),
            ),
            None => Box::new(self.iter()),
        };
        Box::new(candidates.filter(move |((_, labels), _)| matches(labels, matchers)))
    }

    fn for_each_mut(&mut self, mut visit: impl FnMut(&StoredLabels, &mut Series)) {
        for table in self.names.values_mut() {
            for ((_, labels), series) in table.iter_mut() {
                visit(labels, series);
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&SeriesKey, &mut Series) -> bool) {
        let mut len = 0;
        self.names.retain(|_, table| {
            table.retain(|(key, series)| keep(key, series));
            len += table.len();
            !table.is_empty()
        });
        self.len = len;
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
    // The min time of the emulated Go head, which head compaction moves in block-range steps.
    head_min: i64,
}

impl Default for Tenant {
    fn default() -> Self {
        Self {
            series: SeriesByName::default(),
            label_names: HashSet::new(),
            metadata: BTreeMap::new(),
            ingested: VecDeque::new(),
            max_time: i64::MIN,
            head_min: i64::MIN,
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
        }
    }
}

// The head state a record's samples are checked against, taken before the record applies like
// the head appender Mimir creates for each request.
#[derive(Clone, Copy, Debug)]
struct AppendRules {
    head_max_time: i64,
    min_valid_time: i64,
    out_of_order_window_ms: i64,
}

impl AppendRules {
    fn new(head_max_time: i64, out_of_order_window_ms: i64) -> Self {
        Self {
            head_max_time,
            min_valid_time: if head_max_time == i64::MIN {
                i64::MIN
            } else {
                head_max_time.saturating_sub(MIN_VALID_TIME_WINDOW_MS)
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
    exemplars: Vec<(usize, u64, Arc<StoredLabels>, Vec<cortexpb::Exemplar>)>,
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
        })
    }

    /// Limits come from `overrides`; without them the store uses Mimir's defaults.
    pub fn with_overrides(mut self, overrides: Arc<Overrides>) -> Self {
        self.overrides = overrides;
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
        }])
    }

    /// Applies records in order: each shard receives its series in record order, and shards run in
    /// parallel.
    pub fn ingest_batch(&self, records: Vec<IngestRecord>) -> Result<()> {
        let shard_count = self.shards.len();
        let mut buckets = (0..shard_count).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut tenant_ids = Vec::with_capacity(records.len());
        let mut record_rules = Vec::with_capacity(records.len());
        let mut all_series = Vec::new();
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
                let rules =
                    AppendRules::new(home_tenant.max_time, limits.out_of_order_time_window_ms);
                let mut record_max = home_tenant.max_time;
                for series in &mut request.series {
                    series
                        .samples
                        .retain(|sample| match classify(sample.timestamp_ms) {
                            Some(reason) => {
                                *early_discards.entry((index, reason)).or_default() += 1;
                                false
                            }
                            None => {
                                record_max = record_max.max(sample.timestamp_ms);
                                true
                            }
                        });
                    if limits.native_histograms_ingestion_enabled {
                        series
                            .histograms
                            .retain(|histogram| match classify(histogram.timestamp) {
                                Some(reason) => {
                                    *early_discards.entry((index, reason)).or_default() += 1;
                                    false
                                }
                                None => {
                                    record_max = record_max.max(histogram.timestamp);
                                    true
                                }
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
                }
                // Every sample above the head's max time is accepted, so the max is known now.
                home_tenant.max_time = record_max;
                record_rules.push((rules, keep_exemplars));
                all_series.extend(
                    request
                        .series
                        .into_iter()
                        .map(|series| (index, series, ingested_ms)),
                );
                tenant_ids.push(tenant);
            }
        }
        // Sorting and hashing labels is most of the per-series cost outside the shards.
        let hashes = self.pool.install(|| {
            all_series
                .par_iter_mut()
                .map(|(_, series, _)| {
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
        for ((index, series, ingested_ms), hash) in all_series.into_iter().zip(hashes) {
            if let Some(hash) = hash {
                buckets[shard_for(hash, shard_count)].push((index, hash, series, ingested_ms));
            }
        }
        let apply =
            |shard: usize, bucket: Vec<(usize, u64, DecodedSeries, i64)>| -> Result<ShardOutcome> {
                let mut guard = self.shards[shard].write().expect("store lock poisoned");
                let State { tenants, disk } = &mut *guard;
                let mut outcome = ShardOutcome::default();
                for (index, hash, series, ingested_ms) in bucket {
                    let tenant = tenant_mut(tenants, &tenant_ids[index]);
                    let (rules, keep_exemplars) = record_rules[index];
                    ingest_series(
                        tenant,
                        disk,
                        series,
                        hash,
                        ingested_ms,
                        SeriesContext {
                            tenant_id: &tenant_ids[index],
                            rules,
                            keep_exemplars,
                            index,
                        },
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
        Ok(())
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
        }
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
        let mut stored = 0_u64;
        let mut storage = self.exemplars.lock().expect("exemplar lock poisoned");
        for (index, hash, labels, series_exemplars) in exemplars {
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
            let mut appended = 0;
            for exemplar in series_exemplars {
                match tenant_storage.add(hash, || Arc::clone(&labels), exemplar, window) {
                    Ok(true) => appended += 1,
                    // Duplicates succeed without being stored again, like `AddExemplar`.
                    Ok(false) => stored += 1,
                    Err(Rejection::OutOfOrder) => {
                        metrics::OUT_OF_ORDER_EXEMPLARS.inc();
                        exemplar_failures += 1;
                    }
                    Err(Rejection::Disabled | Rejection::LabelLength) => exemplar_failures += 1,
                }
            }
            stored += appended;
            if appended > 0 {
                metrics::EXEMPLARS_APPENDED
                    .with_label_values(&[tenant])
                    .inc_by(appended);
            }
        }
        metrics::INGESTED_EXEMPLARS.inc_by(stored);
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
        let compiled = compile_matchers(matchers)?;
        let mut selected = self
            .per_shard(tenant_id, |tenant, disk| {
                tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| matches_time_range(series, start, end))
                    .filter_map(|((_, labels), series)| {
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
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // The distributor k-way merges each ingester's stream and requires label order, which the
        // Go ingester gets from sorted postings.
        selected.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        Ok(selected.into_iter().map(|(_, view)| view).collect())
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
        let names = self
            .per_shard(tenant_id, |tenant, _| {
                let mut names = BTreeSet::new();
                for ((_, labels), _) in tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| matches_time_range(series, start, end))
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
        let values = self
            .per_shard(tenant_id, |tenant, _| {
                let mut values = BTreeSet::new();
                for ((_, labels), _) in tenant
                    .series
                    .matching(&compiled)
                    .filter(|(_, series)| matches_time_range(series, start, end))
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
                    .filter(|(_, series)| series.last_ingested_ms >= cutoff)
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
    pub fn head_tick(&self, compact: bool) -> Vec<HeadReport> {
        let owned = self
            .owned_ranges
            .read()
            .expect("owned ranges lock poisoned")
            .clone();
        let head_bounds = {
            let home = self.shards[0].read().expect("store lock poisoned");
            home.tenants
                .iter()
                .map(|(id, tenant)| (id.clone(), (tenant.head_min, tenant.max_time)))
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
                            let (head_min, _) = head_bounds
                                .get(tenant_id)
                                .copied()
                                .unwrap_or((i64::MIN, i64::MIN));
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
                                let in_head = newest.is_some_and(|newest| newest >= head_min);
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
            total.head_min_time = total.head_min_time.min(report.head_min_time);
        }
        let mut home = self.shards[0].write().expect("store lock poisoned");
        for report in merged.values_mut() {
            let tenant = tenant_mut(&mut home.tenants, &report.tenant);
            if compact {
                if tenant.head_min == i64::MIN && report.head_min_time != i64::MAX {
                    tenant.head_min = report.head_min_time;
                }
                while tenant.max_time != i64::MIN
                    && tenant.head_min != i64::MIN
                    && tenant.max_time - tenant.head_min > CHUNK_RANGE_MS / 2 * 3
                {
                    tenant.head_min = range_end(tenant.head_min);
                }
            }
            report.head_min_time = report.head_min_time.max(tenant.head_min);
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
                                report.active += 1;
                                report.active_native_histograms += counts[1];
                                report.active_native_histogram_buckets += buckets;
                                for index in series.tracker_matches.iter() {
                                    let entry = &mut report.custom_trackers[usize::from(*index)].1;
                                    for (total, count) in entry.iter_mut().zip(counts) {
                                        *total += count;
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
        self.per_shard(tenant_id, |tenant, _| {
            tenant_stats_at(tenant, active, cutoff)
        })
        .into_iter()
        .fold(UserStatsView::default(), add_stats)
    }

    pub fn all_user_stats(&self, active: bool) -> Vec<(String, UserStatsView)> {
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let per_shard = self.pool.install(|| {
            self.shards
                .par_iter()
                .map(|shard| {
                    let state = shard.read().expect("store lock poisoned");
                    state
                        .tenants
                        .iter()
                        .map(|(id, tenant)| (id.clone(), tenant_stats_at(tenant, active, cutoff)))
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
        let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for shard in self.per_shard(tenant_id, |tenant, _| {
            let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for ((_, labels), _) in tenant
                .series
                .matching(&compiled)
                .filter(|(_, series)| !active || series.last_ingested_ms >= cutoff)
            {
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
        let wanted = label_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut result: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for shard in self.per_shard(tenant_id, |tenant, _| {
            let mut result: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
            for ((_, labels), _) in tenant
                .series
                .matching(&compiled)
                .filter(|(_, series)| !active || series.last_ingested_ms >= cutoff)
            {
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

fn tenant_stats_at(tenant: &Tenant, active: bool, cutoff: i64) -> UserStatsView {
    let num_series = if active {
        tenant
            .series
            .values()
            .filter(|series| series.last_ingested_ms >= cutoff)
            .count()
    } else {
        tenant.series.len()
    } as u64;
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
}

// `decoded` has sorted, unique labels whose hash is `hash`.
fn ingest_series(
    tenant: &mut Tenant,
    disk: &mut ChunkDiskMapper,
    mut decoded: DecodedSeries,
    hash: u64,
    now: i64,
    context: SeriesContext<'_>,
    outcome: &mut ShardOutcome,
) -> Result<()> {
    let decoded_labels = std::mem::take(&mut decoded.labels);
    let name = decoded_labels
        .binary_search_by(|(label, _)| label.as_str().cmp("__name__"))
        .map_or("", |index| decoded_labels[index].1.as_str());
    let Tenant {
        series: by_name,
        label_names,
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
            if let Some(reason) =
                append_created_zero(series, disk, &rules, ooo_merge::Value::Float(0.0), created)?
            {
                discard(reason);
            }
        }
        match append_float(series, disk, &rules, sample.timestamp_ms, sample.value)? {
            Ok(appended) => {
                accepted += 1;
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
            if let Some(reason) = append_created_zero(
                series,
                disk,
                &rules,
                ooo_merge::Value::Histogram(Box::new(zero)),
                created,
            )? {
                discard(reason);
            }
        }
        match append_histogram(series, disk, &rules, histogram)? {
            Ok(appended) => {
                accepted += 1;
                count_appended(appended, &mut out_of_order, &mut chunks_created);
            }
            Err(reason) => discard(reason),
        }
    }
    if accepted > 0 {
        if let Some(bucket_count) = bucket_count {
            series.last_bucket_count = bucket_count;
        }
        series.native_histogram = bucket_count.is_some();
        series.last_ingested_ms = now;
        *outcome.accepted.entry(context.index).or_default() += accepted;
    }
    if out_of_order > 0 {
        *outcome.out_of_order.entry(context.index).or_default() += out_of_order;
    }
    if chunks_created > 0 {
        *outcome.chunks_created.entry(context.index).or_default() += chunks_created;
    }
    // Exemplars need an existing series, like `AppendExemplar`.
    if context.keep_exemplars && !decoded.exemplars.is_empty() && (existed || accepted > 0) {
        outcome.exemplars.push((
            context.index,
            hash,
            Arc::clone(key_labels),
            decoded.exemplars,
        ));
    }
    Ok(())
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
    value: ooo_merge::Value,
    timestamp: i64,
) -> Result<Option<DiscardReason>> {
    match rules.classify(timestamp, series_max_time(series)) {
        Ok(Append::InOrder) => {}
        Ok(Append::Duplicate | Append::OutOfOrder) => return Ok(None),
        Err(DiscardReason::OutOfOrder) => return Ok(None),
        Err(reason) => return Ok(Some(reason)),
    }
    match value {
        ooo_merge::Value::Float(value) => {
            append_float(series, disk, rules, timestamp, value)?.ok();
        }
        ooo_merge::Value::Histogram(histogram) => {
            append_histogram(series, disk, rules, *histogram)?.ok();
        }
    }
    Ok(None)
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
        .chain(series.histogram_head.first().map(|first| first.timestamp))
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
    timestamp: i64,
    value: f64,
) -> Result<Result<Appended, DiscardReason>> {
    let series_max = series_max_time(series);
    match rules.classify(timestamp, series_max) {
        Err(reason) => return Ok(Err(reason)),
        Ok(Append::Duplicate) => {
            let last = series.float_head.as_ref().and_then(|head| {
                (head.appender.last_timestamp() == Some(timestamp))
                    .then(|| head.appender.last_value())
                    .flatten()
            });
            return Ok(match last {
                Some(last) if last.to_bits() == value.to_bits() => Ok(Appended::Noop),
                _ => Err(DiscardReason::NewValueForTimestamp),
            });
        }
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
    histogram: cortexpb::Histogram,
) -> Result<Result<Appended, DiscardReason>> {
    let series_max = series_max_time(series);
    match rules.classify(histogram.timestamp, series_max) {
        Err(reason) => return Ok(Err(reason)),
        Ok(Append::Duplicate) => {
            return Ok(match series.histogram_head.last() {
                Some(last) if last.timestamp == histogram.timestamp && *last == histogram => {
                    Ok(Appended::Noop)
                }
                _ => Err(DiscardReason::NewValueForTimestamp),
            });
        }
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
    if let Some(last) = series.histogram_head.last() {
        if histogram.timestamp <= last.timestamp {
            return Ok(Ok(Appended::Noop));
        }
        if series.histogram_head.len() >= SAMPLES_PER_CHUNK
            || histogram.timestamp >= series.histogram_next_at
            || !histogram::compatible(last, &histogram)
        {
            cut_histogram_head(series, disk)?;
        }
    }
    let opened = series.histogram_head.is_empty();
    if opened {
        series.histogram_next_at = range_end(histogram.timestamp);
    }
    series.histogram_head.push(histogram);
    Ok(Ok(Appended::InOrder { opened }))
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
    let head = std::mem::take(&mut series.histogram_head);
    write_chunk(
        series,
        disk,
        histogram::encode_sequence(&head),
        head[0].timestamp,
        head[head.len() - 1].timestamp,
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

fn range_end(timestamp: i64) -> i64 {
    (timestamp.div_euclid(CHUNK_RANGE_MS) + 1) * CHUNK_RANGE_MS
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
        .first()
        .zip(series.histogram_head.last())
        .map(|(first, last)| (first.timestamp, last.timestamp));
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
    if let (Some(first), Some(last)) = (series.histogram_head.first(), series.histogram_head.last())
        && overlaps(first.timestamp, last.timestamp)
    {
        let encoded = histogram::encode_sequence(&series.histogram_head);
        add(
            ooo_merge::Chunk {
                min_time: first.timestamp,
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
        CompiledMatcher::Shard(index, count) => labels_hash(labels) % count == *index,
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

// Matches Go's labels hash so query sharding agrees with the Go ingesters; the buffer is reused
// because ingest hashes every incoming series.
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
            positive_deltas: vec![1; buckets as usize],
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
    fn cuts_histogram_chunks_on_size_and_layout_changes() {
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
                request((0..300).map(|t| histogram_at(1_000 + t * 10, 5)).collect()),
            )
            .unwrap();
        store
            .ingest(
                "tenant",
                request(vec![histogram_at(10_000, 7), histogram_at(500, 5)]),
            )
            .unwrap();
        with_series(&store, |series| {
            let spans = series
                .chunks
                .iter()
                .map(|chunk| (chunk.min_time, chunk.max_time))
                .collect::<Vec<_>>();
            assert_eq!(spans, vec![(1_000, 2_190), (2_200, 3_390), (3_400, 3_990)]);
            // Like Prometheus's OOO head, the out-of-order histogram waits in the open OOO chunk.
            assert_eq!(series.out_of_order.len(), 1);
            assert_eq!(series.histogram_head.len(), 1);
            assert_eq!(series.last_bucket_count, 5);
        });
        let chunks = query(&store, 3_500, 10_000);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.start_timestamp_ms, chunk.end_timestamp_ms))
                .collect::<Vec<_>>(),
            vec![(3_400, 3_990), (10_000, 10_000)]
        );
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
                        series(Some("down"), "a", now - 10 * CHUNK_RANGE_MS),
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
    fn head_max_time_is_per_tenant_and_taken_per_record() {
        let store = store_with(Limits::default());
        let record = |tenant: &str, name: &str, timestamp: i64| IngestRecord {
            tenant: tenant.into(),
            request: series_request(name, [(timestamp, 1.0)]),
            ingested_ms: 0,
            track_rate: false,
        };
        // In one batch, the second record sees the first one's samples, like a new appender.
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
        // Samples of the same record are checked against the head before it.
        store
            .ingest_batch(vec![IngestRecord {
                tenant: "same-record".into(),
                request: DecodedRequest {
                    source: 0,
                    series: vec![
                        series_request("a", [(10 * HOUR, 1.0)]).series.remove(0),
                        series_request("b", [(8 * HOUR, 1.0)]).series.remove(0),
                    ],
                    metadata: Vec::new(),
                },
                ingested_ms: 0,
                track_rate: false,
            }])
            .unwrap();
        assert_eq!(samples_of(&store, "same-record", "b"), [(8 * HOUR, 1.0)]);
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
        assert_eq!(discarded(DiscardReason::TooFarInPast, tenant), 1);
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
}
