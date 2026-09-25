use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use compact_str::CompactString;
use prost::Message;
use prost::bytes::Bytes;
use regex::Regex;

use crate::proto::{cortex, cortexpb};
use crate::record::{DecodedRequest, DecodedSeries};
use crate::sample_store::SampleStore;
use crate::{histogram, xor};

type StoredLabel = (Arc<str>, CompactString);
type StoredLabels = Vec<StoredLabel>;
// Compare the fingerprint first so ingest does not compare every label at each tree level.
type SeriesKey = (u64, Arc<StoredLabels>);

#[derive(Debug, Default)]
struct Series {
    samples: SampleStore,
    histograms: Vec<cortexpb::Histogram>,
    chunks: OnceLock<Arc<Vec<EncodedChunk>>>,
    reusable_chunks: Mutex<Option<ReusableChunks>>,
    encoded_labels: OnceLock<Bytes>,
    exemplars: Vec<cortexpb::Exemplar>,
    last_ingested_ms: i64,
}

#[derive(Debug)]
struct ReusableChunks {
    chunks: Arc<Vec<EncodedChunk>>,
    histogram_count: usize,
    float_count: usize,
    float_range: Option<(i64, i64)>,
}

#[derive(Default)]
struct Tenant {
    series: BTreeMap<SeriesKey, Series>,
    label_names: HashSet<Arc<str>>,
    metadata: BTreeMap<(String, i32, String, String), cortexpb::MetricMetadata>,
    ingested: VecDeque<(Instant, i32, u64)>,
}

pub struct Store {
    tenants: RwLock<HashMap<String, Tenant>>,
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
        Self::new(20 * 60 * 1000, None)
    }
}

impl Store {
    pub fn new(active_window_ms: i64, retention_ms: Option<i64>) -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            active_window_ms,
            retention_ms,
        }
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

    pub fn prune_expired(&self) {
        let Some(retention_ms) = self.retention_ms else {
            return;
        };
        let cutoff = now_ms().saturating_sub(retention_ms);
        let mut tenants = self.tenants.write().expect("store lock poisoned");
        for tenant in tenants.values_mut() {
            prune_tenant(tenant, cutoff);
        }
    }

    pub fn warm_query_cache(&self) -> usize {
        self.warm_query_cache_until(&AtomicBool::new(false))
    }

    pub fn warm_query_cache_until(&self, stopped: &AtomicBool) -> usize {
        let tenants = self.tenants.read().expect("store lock poisoned");
        let mut warmed = 0;
        for tenant in tenants.values() {
            for ((_, labels), series) in &tenant.series {
                if stopped.load(Ordering::Relaxed) {
                    return warmed;
                }
                cached_chunks(series);
                series
                    .encoded_labels
                    .get_or_init(|| encode_series_labels(labels));
                warmed += 1;
            }
        }
        warmed
    }

    pub fn write_image(&self, writer: &mut impl Write, include_chunks: bool) -> Result<usize> {
        let tenants = self.tenants.read().expect("store lock poisoned");
        image_len(writer, tenants.len())?;
        let mut count = 0;
        for (tenant_id, tenant) in tenants.iter() {
            image_bytes(writer, tenant_id.as_bytes())?;
            image_len(writer, tenant.metadata.len())?;
            for metadata in tenant.metadata.values() {
                image_bytes(writer, &metadata.encode_to_vec())?;
            }
            image_len(writer, tenant.series.len())?;
            for ((_, labels), series) in &tenant.series {
                image_len(writer, labels.len())?;
                for (name, value) in labels.iter() {
                    image_bytes(writer, name.as_bytes())?;
                    image_bytes(writer, value.as_bytes())?;
                }
                writer.write_all(&series.last_ingested_ms.to_le_bytes())?;
                image_len(writer, series.samples.len())?;
                for (timestamp, value) in series.samples.iter() {
                    writer.write_all(&timestamp.to_le_bytes())?;
                    writer.write_all(&value.to_bits().to_le_bytes())?;
                }
                image_len(writer, series.histograms.len())?;
                for histogram in &series.histograms {
                    image_bytes(writer, &histogram.encode_to_vec())?;
                }
                image_len(writer, series.exemplars.len())?;
                for exemplar in &series.exemplars {
                    image_bytes(writer, &exemplar.encode_to_vec())?;
                }
                if include_chunks {
                    let chunks = cached_chunks(series);
                    image_len(writer, chunks.len())?;
                    for chunk in chunks.iter() {
                        writer.write_all(&chunk.start_timestamp_ms.to_le_bytes())?;
                        writer.write_all(&chunk.end_timestamp_ms.to_le_bytes())?;
                        image_bytes(writer, &chunk.wire)?;
                    }
                    let encoded_labels = series
                        .encoded_labels
                        .get_or_init(|| encode_series_labels(labels));
                    image_bytes(writer, encoded_labels)?;
                }
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn from_image(
        reader: &mut impl Read,
        active_window_ms: i64,
        retention_ms: Option<i64>,
        include_chunks: bool,
    ) -> Result<Self> {
        let mut tenants = HashMap::new();
        for _ in 0..image_count(reader, 100_000)? {
            let tenant_id = image_string(reader)?;
            let mut tenant = Tenant::default();
            for _ in 0..image_count(reader, 1_000_000)? {
                let metadata =
                    cortexpb::MetricMetadata::decode(image_bytes_read(reader)?.as_slice())?;
                let key = (
                    metadata.metric_family_name.clone(),
                    metadata.r#type,
                    metadata.help.clone(),
                    metadata.unit.clone(),
                );
                tenant.metadata.insert(key, metadata);
            }
            for _ in 0..image_count(reader, 10_000_000)? {
                let mut labels = Vec::new();
                for _ in 0..image_count(reader, 1_000)? {
                    let name = image_string(reader)?;
                    let name = if let Some(stored) = tenant.label_names.get(name.as_str()) {
                        Arc::clone(stored)
                    } else {
                        let stored: Arc<str> = name.into();
                        tenant.label_names.insert(Arc::clone(&stored));
                        stored
                    };
                    labels.push((name, image_string(reader)?.into()));
                }
                let last_ingested_ms = image_i64(reader)?;
                let mut samples = SampleStore::default();
                for _ in 0..image_count(reader, 2_000_000)? {
                    let timestamp = image_i64(reader)?;
                    let value = f64::from_bits(image_u64(reader)?);
                    samples.insert(samples.len(), timestamp, value);
                }
                let histograms = (0..image_count(reader, 2_000_000)?)
                    .map(|_| {
                        Ok(cortexpb::Histogram::decode(
                            image_bytes_read(reader)?.as_slice(),
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let exemplars = (0..image_count(reader, 2_000_000)?)
                    .map(|_| {
                        Ok(cortexpb::Exemplar::decode(
                            image_bytes_read(reader)?.as_slice(),
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let series = Series {
                    samples,
                    histograms,
                    exemplars,
                    last_ingested_ms,
                    ..Series::default()
                };
                if include_chunks {
                    let chunks = (0..image_count(reader, 2_000_000)?)
                        .map(|_| {
                            Ok(EncodedChunk {
                                start_timestamp_ms: image_i64(reader)?,
                                end_timestamp_ms: image_i64(reader)?,
                                wire: image_bytes_read(reader)?.into(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let _ = series.chunks.set(Arc::new(chunks));
                    let _ = series.encoded_labels.set(image_bytes_read(reader)?.into());
                }
                let key = series_key(labels);
                if tenant.series.insert(key, series).is_some() {
                    bail!("duplicate series in recovery image");
                }
            }
            if tenants.insert(tenant_id, tenant).is_some() {
                bail!("duplicate tenant in recovery image");
            }
        }
        Ok(Self {
            tenants: RwLock::new(tenants),
            active_window_ms,
            retention_ms,
        })
    }

    fn ingest_at(
        &self,
        tenant_id: &str,
        request: DecodedRequest,
        ingested_ms: i64,
        track_rate: bool,
    ) -> Result<()> {
        let mut tenants = self.tenants.write().expect("store lock poisoned");
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
            ingest_series(tenant, decoded, ingested_ms)?;
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
        let tenants = self.tenants.read().expect("store lock poisoned");
        let Some(tenant) = tenants.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let compiled = compile_matchers(matchers)?;
        let mut selected = Vec::new();
        for ((_, labels), series) in &tenant.series {
            if !matches(labels, &compiled) {
                continue;
            }
            let has_samples = samples_overlap(&series.samples, start, end);
            let has_histograms = histograms_overlap(&series.histograms, start, end);
            if !has_samples && !has_histograms {
                continue;
            }
            let chunks = cached_chunks(series);
            let chunk_start = chunks.partition_point(|chunk| chunk.end_timestamp_ms < start);
            let chunk_end = chunks.partition_point(|chunk| chunk.start_timestamp_ms <= end);
            selected.push(QuerySeriesView {
                encoded_labels: series
                    .encoded_labels
                    .get_or_init(|| encode_series_labels(labels))
                    .clone(),
                chunks: Arc::clone(chunks),
                chunk_start,
                chunk_end,
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        self.tenants
            .read()
            .expect("store lock poisoned")
            .get(tenant_id)
            .map_or(0, |tenant| tenant.series.len() as u64)
    }

    pub fn active_series(
        &self,
        tenant_id: &str,
        matchers: &[cortex::LabelMatcher],
        histograms_only: bool,
    ) -> Result<Vec<ActiveSeriesView>> {
        let tenants = self.tenants.read().expect("store lock poisoned");
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
                let bucket_count = series
                    .histograms
                    .last()
                    .map(histogram_bucket_count)
                    .unwrap_or(0);
                (!histograms_only || bucket_count > 0).then(|| ActiveSeriesView {
                    labels: owned_labels(labels),
                    bucket_count,
                })
            })
            .collect())
    }

    pub fn user_stats(&self, tenant_id: &str, active: bool) -> UserStatsView {
        let tenants = self.tenants.read().expect("store lock poisoned");
        tenants
            .get(tenant_id)
            .map_or_else(UserStatsView::default, |tenant| {
                tenant_stats(tenant, active, self.active_window_ms)
            })
    }

    pub fn all_user_stats(&self, active: bool) -> Vec<(String, UserStatsView)> {
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        let tenants = self.tenants.read().expect("store lock poisoned");
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
        self.tenants
            .read()
            .expect("store lock poisoned")
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

fn ingest_series(tenant: &mut Tenant, mut decoded: DecodedSeries, now: i64) -> Result<()> {
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
    let previous_histogram_count = series.histograms.len();
    let previous_float_count = series.samples.len();
    let previous_float_range = float_tail_range(&series.samples);
    let mut chunks_changed = false;
    let mut histogram_out_of_order = false;
    let mut float_out_of_order = false;
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
        if let Some(first_timestamp) = first_timestamp {
            if decoded.created_timestamp < first_timestamp {
                if let Err(index) = series.samples.binary_search(decoded.created_timestamp) {
                    float_out_of_order |= index != series.samples.len();
                    series.samples.insert(index, decoded.created_timestamp, 0.0);
                    chunks_changed = true;
                }
            }
        }
    }
    for sample in decoded.samples {
        if series
            .histograms
            .binary_search_by_key(&sample.timestamp_ms, |histogram| histogram.timestamp)
            .is_ok()
        {
            continue;
        }
        match series.samples.binary_search(sample.timestamp_ms) {
            Ok(index)
                if series.samples.get(index).expect("found sample").1.to_bits()
                    != sample.value.to_bits() =>
            {
                continue;
            }
            Ok(_) => continue,
            Err(index) => {
                float_out_of_order |= index != series.samples.len();
                series
                    .samples
                    .insert(index, sample.timestamp_ms, sample.value);
                chunks_changed = true;
            }
        }
    }
    for histogram in decoded.histograms {
        if series.samples.binary_search(histogram.timestamp).is_ok() {
            continue;
        }
        match series
            .histograms
            .binary_search_by_key(&histogram.timestamp, |item| item.timestamp)
        {
            Ok(index) if series.histograms[index] != histogram => continue,
            Ok(_) => continue,
            Err(index) => {
                histogram_out_of_order |= index != series.histograms.len();
                series.histograms.insert(index, histogram);
                chunks_changed = true;
            }
        }
    }
    series.exemplars.extend(decoded.exemplars);
    series
        .exemplars
        .sort_by_key(|exemplar| exemplar.timestamp_ms);
    series.last_ingested_ms = now;
    if chunks_changed {
        let cached = series.chunks.take();
        if histogram_out_of_order || float_out_of_order || cached.is_some() {
            let mut reusable = series
                .reusable_chunks
                .lock()
                .expect("query cache lock poisoned");
            if histogram_out_of_order || float_out_of_order {
                reusable.take();
            } else if reusable.is_none() {
                *reusable = cached.map(|chunks| ReusableChunks {
                    chunks,
                    histogram_count: previous_histogram_count,
                    float_count: previous_float_count,
                    float_range: previous_float_range,
                });
            }
        }
    }
    Ok(())
}

fn matches_time_range(series: &Series, start: i64, end: i64) -> bool {
    samples_overlap(&series.samples, start, end)
        || histograms_overlap(&series.histograms, start, end)
}

fn samples_overlap(samples: &SampleStore, start: i64, end: i64) -> bool {
    let index = samples.binary_search(start).unwrap_or_else(|index| index);
    samples
        .get(index)
        .is_some_and(|(timestamp, _)| timestamp <= end)
}

fn histograms_overlap(histograms: &[cortexpb::Histogram], start: i64, end: i64) -> bool {
    let index = histograms.partition_point(|histogram| histogram.timestamp < start);
    histograms
        .get(index)
        .is_some_and(|histogram| histogram.timestamp <= end)
}

fn float_tail_start(count: usize) -> usize {
    count.saturating_sub(1) / MAX_FLOAT_CHUNK_SAMPLES * MAX_FLOAT_CHUNK_SAMPLES
}

fn float_tail_range(samples: &SampleStore) -> Option<(i64, i64)> {
    Some((
        samples.get(float_tail_start(samples.len()))?.0,
        samples.last()?.0,
    ))
}

fn cached_chunks(series: &Series) -> &Arc<Vec<EncodedChunk>> {
    series.chunks.get_or_init(|| {
        let reusable = series
            .reusable_chunks
            .lock()
            .expect("query cache lock poisoned")
            .take();
        Arc::new(reusable.map_or_else(
            || encode_chunks(series),
            |previous| encode_chunks_reusing(series, previous),
        ))
    })
}

fn encode_chunks(series: &Series) -> Vec<EncodedChunk> {
    let mut chunks = Vec::with_capacity(series.histograms.len() / 120 + 2);
    chunks.extend(encode_float_chunks(&series.samples, 0));
    chunks.extend(encode_histogram_chunks(&series.histograms));
    chunks.sort_by_key(|chunk| chunk.start_timestamp_ms);
    chunks
}

fn encode_chunks_reusing(series: &Series, previous: ReusableChunks) -> Vec<EncodedChunk> {
    if previous.histogram_count > series.histograms.len()
        || previous.float_count > series.samples.len()
    {
        return encode_chunks(series);
    }
    let mut chunks = Arc::unwrap_or_clone(previous.chunks);
    if series.samples.len() > previous.float_count {
        if let Some(float_range) = previous.float_range {
            chunks
                .retain(|chunk| (chunk.start_timestamp_ms, chunk.end_timestamp_ms) != float_range);
        }
        chunks.extend(encode_float_chunks(
            &series.samples,
            float_tail_start(previous.float_count),
        ));
    }
    let old_tail_start = histogram_tail_start(&series.histograms[..previous.histogram_count]);
    if let Some(start) = old_tail_start {
        let end = series.histograms[previous.histogram_count - 1].timestamp;
        chunks.retain(|chunk| (chunk.start_timestamp_ms, chunk.end_timestamp_ms) != (start, end));
    }
    let start_index = old_tail_start.map_or(previous.histogram_count, |start| {
        series
            .histograms
            .partition_point(|histogram| histogram.timestamp < start)
    });
    chunks.extend(encode_histogram_chunks(&series.histograms[start_index..]));
    chunks.sort_by_key(|chunk| chunk.start_timestamp_ms);
    chunks
}

const MAX_FLOAT_CHUNK_SAMPLES: usize = 120;

fn encode_float_chunks(samples: &SampleStore, start_index: usize) -> Vec<EncodedChunk> {
    let mut chunks =
        Vec::with_capacity((samples.len() - start_index) / MAX_FLOAT_CHUNK_SAMPLES + 1);
    for start_index in (start_index..samples.len()).step_by(MAX_FLOAT_CHUNK_SAMPLES) {
        let count = (samples.len() - start_index).min(MAX_FLOAT_CHUNK_SAMPLES);
        let start = samples.get(start_index).expect("float chunk start").0;
        let end = samples
            .get(start_index + count - 1)
            .expect("float chunk end")
            .0;
        let chunk = cortex::Chunk {
            start_timestamp_ms: start,
            end_timestamp_ms: end,
            encoding: 4,
            data: xor::encode_iter(count, samples.iter_from(start_index).take(count)).into(),
        };
        chunks.push(EncodedChunk {
            start_timestamp_ms: start,
            end_timestamp_ms: end,
            wire: chunk.encode_to_vec().into(),
        });
    }
    chunks
}

const MAX_HISTOGRAM_CHUNK_SAMPLES: usize = 120;

fn histogram_tail_start(histograms: &[cortexpb::Histogram]) -> Option<i64> {
    let mut start = 0;
    histograms.first()?;
    for index in 1..histograms.len() {
        if index - start == MAX_HISTOGRAM_CHUNK_SAMPLES
            || !histogram::compatible(&histograms[index - 1], &histograms[index])
        {
            start = index;
        }
    }
    Some(histograms[start].timestamp)
}

fn encode_histogram_chunks(histograms: &[cortexpb::Histogram]) -> Vec<EncodedChunk> {
    let mut chunks = Vec::with_capacity(histograms.len() / MAX_HISTOGRAM_CHUNK_SAMPLES + 1);
    let mut start = 0;
    while start < histograms.len() {
        let mut end = start + 1;
        while end < histograms.len()
            && end - start < MAX_HISTOGRAM_CHUNK_SAMPLES
            && histogram::compatible(&histograms[end - 1], &histograms[end])
        {
            end += 1;
        }
        chunks.push(encode_histogram_chunk(&histograms[start..end]));
        start = end;
    }
    chunks
}

fn encode_histogram_chunk(items: &[cortexpb::Histogram]) -> EncodedChunk {
    let encoded = histogram::encode_sequence(items);
    let chunk = cortex::Chunk {
        start_timestamp_ms: items[0].timestamp,
        end_timestamp_ms: items[items.len() - 1].timestamp,
        encoding: encoded.encoding,
        data: encoded.data.into(),
    };
    EncodedChunk {
        start_timestamp_ms: items[0].timestamp,
        end_timestamp_ms: items[items.len() - 1].timestamp,
        wire: chunk.encode_to_vec().into(),
    }
}

fn prune_tenant(tenant: &mut Tenant, cutoff: i64) {
    tenant.series.retain(|_, series| {
        let samples_before = series.samples.len();
        let histograms_before = series.histograms.len();
        series.samples.retain_since(cutoff);
        series
            .histograms
            .retain(|histogram| histogram.timestamp >= cutoff);
        series
            .exemplars
            .retain(|exemplar| exemplar.timestamp_ms >= cutoff);
        if series.samples.len() != samples_before || series.histograms.len() != histograms_before {
            series.chunks.take();
            series
                .reusable_chunks
                .get_mut()
                .expect("query cache lock poisoned")
                .take();
        }
        !series.samples.is_empty() || !series.histograms.is_empty() || !series.exemplars.is_empty()
    });
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

fn image_len(writer: &mut impl Write, len: usize) -> Result<()> {
    writer.write_all(
        &u32::try_from(len)
            .context("recovery image field exceeds u32")?
            .to_le_bytes(),
    )?;
    Ok(())
}

fn image_bytes(writer: &mut impl Write, bytes: &[u8]) -> Result<()> {
    image_len(writer, bytes.len())?;
    writer.write_all(bytes)?;
    Ok(())
}

fn image_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn image_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn image_i64(reader: &mut impl Read) -> Result<i64> {
    Ok(image_u64(reader)? as i64)
}

fn image_count(reader: &mut impl Read, maximum: u32) -> Result<usize> {
    let count = image_u32(reader)?;
    if count > maximum {
        bail!("recovery image collection exceeds {maximum} entries");
    }
    Ok(count as usize)
}

fn image_bytes_read(reader: &mut impl Read) -> Result<Vec<u8>> {
    let len = image_count(reader, 64 * 1024 * 1024)?;
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn image_string(reader: &mut impl Read) -> Result<String> {
    Ok(String::from_utf8(image_bytes_read(reader)?)?)
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

        let tenants = store.tenants.read().unwrap();
        let series = tenants["tenant"].series.values().next().unwrap();
        assert_eq!(
            series.samples.iter().collect::<Vec<_>>(),
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

    #[test]
    fn warms_query_chunks_and_labels_after_ingest() {
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
        {
            let tenants = store.tenants.read().unwrap();
            let series = tenants["tenant"].series.values().next().unwrap();
            assert!(series.chunks.get().is_none());
            assert!(series.encoded_labels.get().is_none());
        }
        assert_eq!(store.warm_query_cache_until(&AtomicBool::new(true)), 0);
        assert_eq!(store.warm_query_cache(), 1);
        {
            let tenants = store.tenants.read().unwrap();
            let series = tenants["tenant"].series.values().next().unwrap();
            assert_eq!(series.chunks.get().unwrap().len(), 1);
            assert!(series.encoded_labels.get().is_some());
        }
        store.ingest("tenant", request(2, 2.0)).unwrap();
        {
            let tenants = store.tenants.read().unwrap();
            let series = tenants["tenant"].series.values().next().unwrap();
            assert!(series.chunks.get().is_none());
        }
        assert_eq!(store.warm_query_cache(), 1);
    }

    #[test]
    fn reuses_encoded_histograms_without_changing_query_chunks() {
        let store = Store::default();
        let request = |samples: Vec<(i64, f64)>, timestamps: Vec<i64>| DecodedRequest {
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
                histograms: timestamps
                    .into_iter()
                    .map(|timestamp| cortexpb::Histogram {
                        timestamp,
                        count: Some(cortexpb::histogram::Count::CountInt(50)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 50,
                        }],
                        positive_deltas: vec![1; 50],
                        ..Default::default()
                    })
                    .collect(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        };
        let check = |store: &Store| {
            let selected = store
                .select_chunks("tenant", i64::MIN, i64::MAX, &[])
                .unwrap();
            assert_eq!(selected.len(), 1);
            let tenants = store.tenants.read().unwrap();
            let series = tenants["tenant"].series.values().next().unwrap();
            let expected = encode_chunks(series);
            assert_eq!(selected[0].chunks.len(), expected.len());
            for (actual, expected) in selected[0].chunks.iter().zip(expected) {
                assert_eq!(actual.start_timestamp_ms, expected.start_timestamp_ms);
                assert_eq!(actual.end_timestamp_ms, expected.end_timestamp_ms);
                assert_eq!(actual.wire, expected.wire);
            }
            Arc::clone(&selected[0].chunks)
        };

        store
            .ingest("tenant", request(vec![(200, 2.0)], vec![100, 300]))
            .unwrap();
        check(&store);
        store
            .ingest("tenant", request(vec![(500, 5.0)], vec![400]))
            .unwrap();
        check(&store);
        store.ingest("tenant", request(vec![], vec![250])).unwrap();
        let previous = check(&store);
        store.ingest("tenant", request(vec![], vec![400])).unwrap();
        assert!(Arc::ptr_eq(&previous, &check(&store)));
        store.ingest("tenant", request(vec![], vec![600])).unwrap();
        check(&store);
        {
            let mut tenants = store.tenants.write().unwrap();
            prune_tenant(tenants.get_mut("tenant").unwrap(), 300);
        }
        check(&store);
        store
            .ingest("tenant", request(vec![], (700..950).collect()))
            .unwrap();
        check(&store);
        store.ingest("tenant", request(vec![], vec![950])).unwrap();
        check(&store);
        store
            .ingest(
                "tenant",
                request(
                    (1000..1250).map(|time| (time, time as f64)).collect(),
                    vec![],
                ),
            )
            .unwrap();
        check(&store);
        store
            .ingest("tenant", request(vec![(1250, 1250.0)], vec![]))
            .unwrap();
        check(&store);
        store
            .ingest("tenant", request(vec![(975, 975.0)], vec![]))
            .unwrap();
        check(&store);
    }

    #[test]
    fn prunes_expired_series_without_new_ingestion() {
        let store = Store::new(20 * 60 * 1000, Some(1_000));
        let mut tenant = Tenant::default();
        let mut samples = SampleStore::default();
        samples.insert(0, now_ms() - 2_000, 1.0);
        tenant.series.insert(
            series_key(vec![(Arc::from("__name__"), "expired".into())]),
            Series {
                samples,
                ..Default::default()
            },
        );
        store
            .tenants
            .write()
            .unwrap()
            .insert("tenant".into(), tenant);

        store.prune_expired();
        assert_eq!(store.num_series("tenant"), 0);
    }
}
