use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use prost::Message;
use prost::bytes::Bytes;
use regex::Regex;

use crate::chunk_disk::{ChunkDiskMapper, ChunkRef};
use crate::proto::{cortex, cortexpb};
use crate::record::{DecodedRequest, DecodedSeries};
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
    out_of_order: Vec<(i64, f64)>,
    exemplars: Vec<cortexpb::Exemplar>,
    last_ingested_ms: i64,
    last_bucket_count: u32,
}

#[derive(Default)]
struct Tenant {
    series: BTreeMap<SeriesKey, Series>,
    label_names: HashSet<Arc<str>>,
    metadata: BTreeMap<(String, i32, String, String), cortexpb::MetricMetadata>,
    ingested: VecDeque<(Instant, i32, u64)>,
}

struct State {
    tenants: HashMap<String, Tenant>,
    disk: ChunkDiskMapper,
}

pub struct Store {
    state: RwLock<State>,
    active_window_ms: i64,
    retention_ms: Option<i64>,
}

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
        Self::new(20 * 60 * 1000, None, None).expect("create in-memory store")
    }
}

impl Store {
    /// Completed chunks go to `chunk_dir`, which is cleared first; without one they use anonymous memory.
    pub fn new(
        active_window_ms: i64,
        retention_ms: Option<i64>,
        chunk_dir: Option<PathBuf>,
    ) -> Result<Self> {
        Ok(Self {
            state: RwLock::new(State {
                tenants: HashMap::new(),
                disk: ChunkDiskMapper::open(chunk_dir)?,
            }),
            active_window_ms,
            retention_ms,
        })
    }

    pub fn ingest(&self, tenant_id: &str, request: DecodedRequest) -> Result<()> {
        self.ingest_at(tenant_id, request, now_ms(), true)
    }

    pub fn ingest_recovered(
        &self,
        tenant_id: &str,
        request: DecodedRequest,
        ingested_ms: i64,
    ) -> Result<()> {
        self.ingest_at(tenant_id, request, ingested_ms, false)
    }

    pub fn prune_expired(&self) -> Result<()> {
        let Some(retention_ms) = self.retention_ms else {
            return Ok(());
        };
        self.prune_before(now_ms().saturating_sub(retention_ms))
    }

    fn prune_before(&self, cutoff: i64) -> Result<()> {
        let mut state = self.state.write().expect("store lock poisoned");
        for tenant in state.tenants.values_mut() {
            tenant
                .series
                .retain(|_, series| prune_series(series, cutoff));
        }
        state.disk.truncate_before(cutoff)
    }

    fn ingest_at(
        &self,
        tenant_id: &str,
        request: DecodedRequest,
        ingested_ms: i64,
        track_rate: bool,
    ) -> Result<()> {
        let mut guard = self.state.write().expect("store lock poisoned");
        let State { tenants, disk } = &mut *guard;
        let tenant = tenants.entry(tenant_id.to_owned()).or_default();
        let source = request.source;
        for metadata in request.metadata {
            let key = (
                metadata.metric_family_name.clone(),
                metadata.r#type,
                metadata.help.clone(),
                metadata.unit.clone(),
            );
            tenant.metadata.insert(key, metadata);
        }
        let mut sample_count = 0;
        for decoded in request.series {
            sample_count += (decoded.samples.len() + decoded.histograms.len()) as u64;
            ingest_series(tenant, disk, decoded, ingested_ms)?;
        }
        if track_rate {
            let instant = Instant::now();
            tenant.ingested.push_back((instant, source, sample_count));
            while tenant
                .ingested
                .front()
                .is_some_and(|(at, _, _)| instant.duration_since(*at) > Duration::from_secs(60))
            {
                tenant.ingested.pop_front();
            }
        }
        Ok(())
    }

    pub fn select_chunks(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<QuerySeriesView>> {
        let state = self.state.read().expect("store lock poisoned");
        let Some(tenant) = state.tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        let mut selected = Vec::new();
        for ((_, labels), series) in &tenant.series {
            if !matches_time_range(series, start, end) || !matches(labels, &compiled) {
                continue;
            }
            let chunks = query_chunks(series, &state.disk, start, end);
            if chunks.is_empty() {
                continue;
            }
            selected.push(QuerySeriesView {
                encoded_labels: encode_series_labels(labels),
                chunk_start: 0,
                chunk_end: chunks.len(),
                chunks: Arc::new(chunks),
            });
        }
        Ok(selected)
    }

    pub fn select_exemplars(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<SeriesView>> {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        Ok(tenant
            .series
            .iter()
            .filter(|((_, labels), _)| matches(labels, &compiled))
            .filter_map(|((_, labels), series)| {
                let exemplars = series
                    .exemplars
                    .iter()
                    .filter(|item| item.timestamp_ms >= start && item.timestamp_ms <= end)
                    .cloned()
                    .collect::<Vec<_>>();
                (!exemplars.is_empty()).then(|| SeriesView {
                    labels: owned_labels(labels),
                    exemplars,
                })
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
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        Ok(tenant
            .series
            .iter()
            .filter(|((_, labels), series)| {
                matches_time_range(series, start, end) && matches(labels, &compiled)
            })
            .map(|((_, labels), _)| owned_labels(labels))
            .collect())
    }

    pub fn label_names(
        &self,
        tenant_id: &str,
        start: i64,
        end: i64,
        matchers: &[cortex::LabelMatcher],
    ) -> Result<Vec<String>> {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        let mut names = BTreeSet::new();
        for ((_, labels), _) in tenant.series.iter().filter(|((_, labels), series)| {
            matches_time_range(series, start, end) && matches(labels, &compiled)
        }) {
            names.extend(labels.iter().map(|(name, _)| name.to_string()));
        }
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
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        let mut values = BTreeSet::new();
        for ((_, labels), _) in tenant.series.iter().filter(|((_, labels), series)| {
            matches_time_range(series, start, end) && matches(labels, &compiled)
        }) {
            if let Some((_, value)) = labels.iter().find(|(label, _)| label.as_ref() == name) {
                values.insert(value.to_string());
            }
        }
        Ok(values.into_iter().collect())
    }

    pub fn num_series(&self, tenant_id: &str) -> u64 {
        self.state
            .read()
            .expect("store lock poisoned")
            .tenants
            .get(tenant_id)
            .map_or(0, |tenant| tenant.series.len() as u64)
    }

    pub fn active_series(
        &self,
        tenant_id: &str,
        matchers: &[cortex::LabelMatcher],
        histograms_only: bool,
    ) -> Result<Vec<ActiveSeriesView>> {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        Ok(tenant
            .series
            .iter()
            .filter(|((_, labels), series)| {
                series.last_ingested_ms >= cutoff && matches(labels, &compiled)
            })
            .filter_map(|((_, labels), series)| {
                let bucket_count = u64::from(series.last_bucket_count);
                (!histograms_only || bucket_count > 0).then(|| ActiveSeriesView {
                    labels: owned_labels(labels),
                    bucket_count,
                })
            })
            .collect())
    }

    pub fn user_stats(&self, tenant_id: &str, active: bool) -> UserStatsView {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        tenants
            .get(tenant_id)
            .map_or_else(UserStatsView::default, |tenant| {
                tenant_stats(tenant, active, self.active_window_ms)
            })
    }

    pub fn all_user_stats(&self, active: bool) -> Vec<(String, UserStatsView)> {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let mut result = tenants
            .iter()
            .map(|(id, tenant)| (id.clone(), tenant_stats_at(tenant, active, cutoff)))
            .collect::<Vec<_>>();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    pub fn label_names_and_values(
        &self,
        tenant_id: &str,
        matchers: &[cortex::LabelMatcher],
        active: bool,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(BTreeMap::new());
        };
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for ((_, labels), _) in tenant.series.iter().filter(|((_, labels), series)| {
            (!active || series.last_ingested_ms >= cutoff) && matches(labels, &compiled)
        }) {
            for (name, value) in labels.iter() {
                result
                    .entry(name.to_string())
                    .or_default()
                    .insert(value.to_string());
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
        let state = self.state.read().expect("store lock poisoned");
        let tenants = &state.tenants;
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(BTreeMap::new());
        };
        let compiled = compile_matchers(matchers)?;
        let cutoff = now_ms().saturating_sub(self.active_window_ms);
        let wanted = label_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut result: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for ((_, labels), _) in tenant.series.iter().filter(|((_, labels), series)| {
            (!active || series.last_ingested_ms >= cutoff) && matches(labels, &compiled)
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
        Ok(result)
    }

    pub fn metadata(&self, tenant_id: &str) -> Vec<cortexpb::MetricMetadata> {
        self.state
            .read()
            .expect("store lock poisoned")
            .tenants
            .get(tenant_id)
            .map_or_else(Vec::new, |tenant| {
                tenant.metadata.values().cloned().collect()
            })
    }
}

fn tenant_stats(tenant: &Tenant, active: bool, active_window_ms: i64) -> UserStatsView {
    tenant_stats_at(tenant, active, now_ms().saturating_sub(active_window_ms))
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

fn ingest_series(
    tenant: &mut Tenant,
    disk: &mut ChunkDiskMapper,
    mut decoded: DecodedSeries,
    now: i64,
) -> Result<()> {
    decoded.labels.sort();
    for labels in decoded.labels.windows(2) {
        if labels[0].0 == labels[1].0 {
            return Ok(());
        }
    }
    let labels = std::mem::take(&mut decoded.labels)
        .into_iter()
        .map(|(name, value)| {
            let name = if let Some(stored) = tenant.label_names.get(name.as_str()) {
                Arc::clone(stored)
            } else {
                let stored: Arc<str> = name.into();
                tenant.label_names.insert(Arc::clone(&stored));
                stored
            };
            (name, value.into())
        })
        .collect();
    let series = tenant.series.entry(series_key(labels)).or_default();
    if decoded.created_timestamp != 0 {
        let first_timestamp = decoded
            .samples
            .iter()
            .map(|sample| sample.timestamp_ms)
            .chain(
                decoded
                    .histograms
                    .iter()
                    .map(|histogram| histogram.timestamp),
            )
            .min();
        if first_timestamp.is_some_and(|first| decoded.created_timestamp < first) {
            append_float(series, disk, decoded.created_timestamp, 0.0)?;
        }
    }
    for sample in decoded.samples {
        append_float(series, disk, sample.timestamp_ms, sample.value)?;
    }
    for histogram in decoded.histograms {
        append_histogram(series, disk, histogram)?;
    }
    series.exemplars.extend(decoded.exemplars);
    series
        .exemplars
        .sort_by_key(|exemplar| exemplar.timestamp_ms);
    series.last_ingested_ms = now;
    Ok(())
}

// Follows the Prometheus head appender: in-order samples extend the open chunk, a repeated
// timestamp keeps the first value, and older samples go to a separate out-of-order chunk.
fn append_float(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    timestamp: i64,
    value: f64,
) -> Result<()> {
    if series
        .histogram_head
        .binary_search_by_key(&timestamp, |histogram| histogram.timestamp)
        .is_ok()
    {
        return Ok(());
    }
    let last = series
        .float_head
        .as_ref()
        .and_then(|head| head.appender.last_timestamp());
    if last.is_some_and(|last| timestamp <= last) {
        if last == Some(timestamp) || has_float_at(series, disk, timestamp) {
            return Ok(());
        }
        if let Err(index) = series
            .out_of_order
            .binary_search_by_key(&timestamp, |(timestamp, _)| *timestamp)
        {
            series.out_of_order.insert(index, (timestamp, value));
        }
        if series.out_of_order.len() >= OUT_OF_ORDER_CAPACITY {
            flush_out_of_order(series, disk)?;
        }
        return Ok(());
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
    let head = series.float_head.get_or_insert_with(|| FloatHead {
        appender: xor::Appender::default(),
        min_time: timestamp,
        next_at: range_end(timestamp),
    });
    head.appender.append(timestamp, value);
    Ok(())
}

// Only used for out-of-order samples, so decoding the overlapping chunks is acceptable.
fn has_float_at(series: &Series, disk: &ChunkDiskMapper, timestamp: i64) -> bool {
    let contains = |data: &[u8]| {
        xor::decode(data)
            .binary_search_by_key(&timestamp, |(time, _)| *time)
            .is_ok()
    };
    series
        .float_head
        .as_ref()
        .is_some_and(|head| head.min_time <= timestamp && contains(head.appender.bytes()))
        || series.chunks.iter().any(|chunk| {
            chunk.encoding == XOR_ENCODING
                && chunk.min_time <= timestamp
                && timestamp <= chunk.max_time
                && contains(disk.read(chunk.reference, chunk.len))
        })
}

fn append_histogram(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    histogram: cortexpb::Histogram,
) -> Result<()> {
    if series
        .float_head
        .as_ref()
        .and_then(|head| head.appender.last_timestamp())
        == Some(histogram.timestamp)
    {
        return Ok(());
    }
    series.last_bucket_count = histogram_bucket_count(&histogram) as u32;
    if let Some(last) = series.histogram_head.last() {
        if histogram.timestamp <= last.timestamp {
            if histogram.timestamp != last.timestamp
                && series
                    .histogram_head
                    .binary_search_by_key(&histogram.timestamp, |item| item.timestamp)
                    .is_err()
            {
                write_chunk(
                    series,
                    disk,
                    histogram::encode(&histogram),
                    histogram.timestamp,
                    histogram.timestamp,
                )?;
            }
            return Ok(());
        }
        if series.histogram_head.len() >= SAMPLES_PER_CHUNK
            || histogram.timestamp >= series.histogram_next_at
            || !histogram::compatible(last, &histogram)
        {
            cut_histogram_head(series, disk)?;
        }
    }
    if series.histogram_head.is_empty() {
        series.histogram_next_at = range_end(histogram.timestamp);
    }
    series.histogram_head.push(histogram);
    Ok(())
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
    )
}

fn flush_out_of_order(series: &mut Series, disk: &mut ChunkDiskMapper) -> Result<()> {
    if series.out_of_order.is_empty() {
        return Ok(());
    }
    let samples = std::mem::take(&mut series.out_of_order);
    write_chunk(
        series,
        disk,
        histogram::EncodedHistogram {
            encoding: i32::from(XOR_ENCODING),
            data: xor::encode(&samples),
        },
        samples[0].0,
        samples[samples.len() - 1].0,
    )
}

fn write_chunk(
    series: &mut Series,
    disk: &mut ChunkDiskMapper,
    encoded: histogram::EncodedHistogram,
    min_time: i64,
    max_time: i64,
) -> Result<()> {
    let reference = disk.write(&encoded.data, max_time)?;
    series.chunks.push(ChunkMeta {
        reference,
        min_time,
        max_time,
        len: u32::try_from(encoded.data.len()).context("chunk exceeds u32")?,
        encoding: u8::try_from(encoded.encoding).context("chunk encoding exceeds u8")?,
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

fn query_chunks(
    series: &Series,
    disk: &ChunkDiskMapper,
    start: i64,
    end: i64,
) -> Vec<EncodedChunk> {
    let overlaps = |min: i64, max: i64| min <= end && max >= start;
    let mut chunks = Vec::new();
    for chunk in &series.chunks {
        if overlaps(chunk.min_time, chunk.max_time) {
            chunks.push(wire_chunk(
                chunk.min_time,
                chunk.max_time,
                i32::from(chunk.encoding),
                disk.read(chunk.reference, chunk.len),
            ));
        }
    }
    if let Some(head) = &series.float_head {
        let max = head.appender.last_timestamp().expect("head has samples");
        if overlaps(head.min_time, max) {
            chunks.push(wire_chunk(
                head.min_time,
                max,
                i32::from(XOR_ENCODING),
                head.appender.bytes(),
            ));
        }
    }
    if let (Some(first), Some(last)) = (series.histogram_head.first(), series.histogram_head.last())
    {
        if overlaps(first.timestamp, last.timestamp) {
            let encoded = histogram::encode_sequence(&series.histogram_head);
            chunks.push(wire_chunk(
                first.timestamp,
                last.timestamp,
                encoded.encoding,
                &encoded.data,
            ));
        }
    }
    if let (Some(first), Some(last)) = (series.out_of_order.first(), series.out_of_order.last()) {
        if overlaps(first.0, last.0) {
            chunks.push(wire_chunk(
                first.0,
                last.0,
                i32::from(XOR_ENCODING),
                &xor::encode(&series.out_of_order),
            ));
        }
    }
    chunks.sort_by_key(|chunk| chunk.start_timestamp_ms);
    chunks
}

fn wire_chunk(min_time: i64, max_time: i64, encoding: i32, data: &[u8]) -> EncodedChunk {
    EncodedChunk {
        start_timestamp_ms: min_time,
        end_timestamp_ms: max_time,
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
    series
        .exemplars
        .retain(|exemplar| exemplar.timestamp_ms >= cutoff);
    !series.chunks.is_empty()
        || series.float_head.is_some()
        || !series.histogram_head.is_empty()
        || !series.out_of_order.is_empty()
        || !series.exemplars.is_empty()
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

fn matches(labels: &[StoredLabel], matchers: &[CompiledMatcher]) -> bool {
    matchers.iter().all(|matcher| {
        let name = match matcher {
            CompiledMatcher::Equal(name, _)
            | CompiledMatcher::NotEqual(name, _)
            | CompiledMatcher::Regex(name, _)
            | CompiledMatcher::NotRegex(name, _) => name,
            CompiledMatcher::Shard(index, count) => {
                return labels_hash(labels) % count == *index;
            }
        };
        let value = labels
            .binary_search_by(|(label, _)| label.as_ref().cmp(name))
            .map_or("", |index| labels[index].1.as_str());
        match matcher {
            CompiledMatcher::Equal(_, expected) => value == expected,
            CompiledMatcher::NotEqual(_, expected) => value != expected,
            CompiledMatcher::Regex(_, regex) => regex.is_match(value),
            CompiledMatcher::NotRegex(_, regex) => !regex.is_match(value),
            CompiledMatcher::Shard(_, _) => unreachable!(),
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
    let mut bytes = Vec::new();
    for (name, value) in labels {
        encode_label_size(&mut bytes, name.len());
        bytes.extend_from_slice(name.as_bytes());
        encode_label_size(&mut bytes, value.len());
        bytes.extend_from_slice(value.as_bytes());
    }
    xxhash64(&bytes)
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
        let state = store.state.read().unwrap();
        check(state.tenants["tenant"].series.values().next().unwrap())
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
        let store = Store::default();
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
        let store = Store::default();
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
            assert_eq!(
                spans,
                vec![(1_000, 2_190), (2_200, 3_390), (3_400, 3_990), (500, 500)]
            );
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
}
