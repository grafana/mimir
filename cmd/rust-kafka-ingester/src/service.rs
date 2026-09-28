use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use prost::Message;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::consistency::Consistency;
use crate::proto::{cortex, cortexpb};
use crate::store::{Store, UserStatsView};

const RESPONSE_TARGET_BYTES: usize = 1024 * 1024;
const SEARCH_BATCH_SIZE: usize = 256;
const SERIES_BATCH_SIZE: usize = 1024;

pub struct IngesterService {
    store: Arc<Store>,
    consistency: Option<Arc<Consistency>>,
}

impl IngesterService {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            consistency: None,
        }
    }

    pub fn with_consistency(store: Arc<Store>, consistency: Arc<Consistency>) -> Self {
        Self {
            store,
            consistency: Some(consistency),
        }
    }

    async fn enforce<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if let Some(consistency) = &self.consistency {
            consistency.enforce(request).await?;
        }
        Ok(())
    }
}

fn tenant<T>(request: &Request<T>) -> Result<String, Status> {
    request
        .metadata()
        .get("x-scope-orgid")
        .ok_or_else(|| Status::unauthenticated("missing x-scope-orgid"))?
        .to_str()
        .map(str::to_owned)
        .map_err(|_| Status::invalid_argument("invalid x-scope-orgid"))
}

fn internal(error: anyhow::Error) -> Status {
    Status::invalid_argument(error.to_string())
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl cortex::ingester_server::Ingester for IngesterService {
    type QueryStreamStream = ResponseStream<cortex::QueryStreamResponse>;
    type LabelNamesAndValuesStream = ResponseStream<cortex::LabelNamesAndValuesResponse>;
    type LabelValuesCardinalityStream = ResponseStream<cortex::LabelValuesCardinalityResponse>;
    type ActiveSeriesStream = ResponseStream<cortex::ActiveSeriesResponse>;
    type SearchLabelNamesStream = ResponseStream<cortex::SearchResultBatch>;
    type SearchLabelValuesStream = ResponseStream<cortex::SearchResultBatch>;

    async fn query_stream(
        &self,
        request: Request<cortex::QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        crate::metrics::QUERIES.inc();
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let (selected, blocks) = self
            .store
            .select_chunks_with_blocks(
                &tenant,
                request.start_timestamp_ms,
                request.end_timestamp_ms,
                &request.matchers,
            )
            .map_err(internal)?;
        // Like Go, observed once the tenant has a TSDB.
        if self.store.has_tenant(&tenant) {
            for block in &blocks {
                crate::metrics::QUERIED_BLOCKS
                    .with_label_values(&[&block.generation])
                    .inc();
                crate::metrics::QUERIED_SERIES
                    .with_label_values(&["single_block_index"])
                    .observe(block.index_series as f64);
                crate::metrics::QUERIED_SERIES
                    .with_label_values(&["single_block_filter"])
                    .observe(block.series as f64);
            }
            crate::metrics::QUERIED_SERIES
                .with_label_values(&["merged_blocks"])
                .observe(selected.len() as f64);
            crate::metrics::QUERIED_SAMPLES.observe(
                selected
                    .iter()
                    .flat_map(|series| &series.chunks[series.chunk_start..series.chunk_end])
                    .map(|chunk| f64::from(chunk.samples))
                    .sum(),
            );
        }
        let batch_size = if request.streaming_chunks_batch_size == 0 {
            1024
        } else {
            usize::try_from(request.streaming_chunks_batch_size).unwrap_or(usize::MAX)
        };
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            let selected_len = selected.len();
            for (batch_index, batch) in selected.chunks(SERIES_BATCH_SIZE).enumerate() {
                let response =
                    series_response(batch, (batch_index + 1) * SERIES_BATCH_SIZE >= selected_len);
                if sender.send(Ok(response)).await.is_err() {
                    return;
                }
            }
            if selected.is_empty()
                && sender
                    .send(Ok(cortex::QueryStreamResponse {
                        encoded_response: vec![0x20, 0x01].into(),
                        ..Default::default()
                    }))
                    .await
                    .is_err()
            {
                return;
            }
            let mut encoded = Vec::with_capacity(RESPONSE_TARGET_BYTES);
            let mut batch_items = 0;
            for (series_index, series) in selected.into_iter().enumerate() {
                if series.chunk_start == series.chunk_end {
                    continue;
                }
                let item_size = encoded_chunks_item_size(&series, series_index as u64);
                let response_item_size = 1 + varint_size(item_size as u64) + item_size;
                if batch_items > 0
                    && (batch_items >= batch_size
                        || encoded.len() + response_item_size > RESPONSE_TARGET_BYTES)
                {
                    if sender
                        .send(Ok(raw_response(std::mem::replace(
                            &mut encoded,
                            Vec::with_capacity(RESPONSE_TARGET_BYTES),
                        ))))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    batch_items = 0;
                }
                append_encoded_chunks(&mut encoded, &series, series_index as u64, item_size);
                batch_items += 1;
            }
            if batch_items > 0 {
                let _ = sender.send(Ok(raw_response(encoded))).await;
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }

    async fn query_exemplars(
        &self,
        request: Request<cortex::ExemplarQueryRequest>,
    ) -> Result<Response<cortex::ExemplarQueryResponse>, Status> {
        crate::metrics::QUERIES.inc();
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let mut result: BTreeMap<Vec<(String, String)>, Vec<cortexpb::Exemplar>> = BTreeMap::new();
        for matcher_set in request.matchers {
            for series in self
                .store
                .select_exemplars(
                    &tenant,
                    request.start_timestamp_ms,
                    request.end_timestamp_ms,
                    &matcher_set.matchers,
                )
                .map_err(internal)?
            {
                if !series.exemplars.is_empty() {
                    result
                        .entry(series.labels)
                        .or_default()
                        .extend(series.exemplars);
                }
            }
        }
        let timeseries = result
            .into_iter()
            .map(|(labels, mut exemplars)| {
                exemplars.sort_by_key(|item| item.timestamp_ms);
                exemplars.dedup();
                cortexpb::TimeSeries {
                    labels: label_pairs(&labels),
                    samples: Vec::new(),
                    exemplars,
                    histograms: Vec::new(),
                    created_timestamp: 0,
                }
            })
            .collect::<Vec<_>>();
        if self.store.has_tenant(&tenant) {
            crate::metrics::QUERIED_EXEMPLARS.observe(
                timeseries
                    .iter()
                    .map(|series| series.exemplars.len() as f64)
                    .sum(),
            );
        }
        Ok(Response::new(cortex::ExemplarQueryResponse { timeseries }))
    }

    async fn label_values(
        &self,
        request: Request<cortex::LabelValuesRequest>,
    ) -> Result<Response<cortex::LabelValuesResponse>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let matchers = request
            .matchers
            .map_or_else(Vec::new, |value| value.matchers);
        let mut values = self
            .store
            .label_values(
                &tenant,
                &request.label_name,
                request.start_timestamp_ms,
                request.end_timestamp_ms,
                &matchers,
            )
            .map_err(internal)?;
        apply_limit(&mut values, request.limit)?;
        Ok(Response::new(cortex::LabelValuesResponse {
            label_values: values,
        }))
    }

    async fn label_names(
        &self,
        request: Request<cortex::LabelNamesRequest>,
    ) -> Result<Response<cortex::LabelNamesResponse>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let matchers = request
            .matchers
            .map_or_else(Vec::new, |value| value.matchers);
        let mut names = self
            .store
            .label_names(
                &tenant,
                request.start_timestamp_ms,
                request.end_timestamp_ms,
                &matchers,
            )
            .map_err(internal)?;
        apply_limit(&mut names, request.limit)?;
        Ok(Response::new(cortex::LabelNamesResponse {
            label_names: names,
        }))
    }

    async fn user_stats(
        &self,
        request: Request<cortex::UserStatsRequest>,
    ) -> Result<Response<cortex::UserStatsResponse>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let active = count_active(request.get_ref().count_method)?;
        Ok(Response::new(stats(self.store.user_stats(&tenant, active))))
    }

    async fn all_user_stats(
        &self,
        request: Request<cortex::UserStatsRequest>,
    ) -> Result<Response<cortex::UsersStatsResponse>, Status> {
        let active = count_active(request.get_ref().count_method)?;
        Ok(Response::new(cortex::UsersStatsResponse {
            stats: self
                .store
                .all_user_stats(active)
                .into_iter()
                .map(|(user_id, view)| cortex::UserIdStatsResponse {
                    user_id,
                    data: Some(stats(view)),
                })
                .collect(),
        }))
    }

    async fn metrics_for_label_matchers(
        &self,
        request: Request<cortex::MetricsForLabelMatchersRequest>,
    ) -> Result<Response<cortex::MetricsForLabelMatchersResponse>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        if request.limit < 0 {
            return Err(Status::invalid_argument("limit must be >= 0"));
        }
        let mut labels = BTreeSet::new();
        for matcher_set in request.matchers_set {
            for series in self
                .store
                .select_labels(
                    &tenant,
                    request.start_timestamp_ms,
                    request.end_timestamp_ms,
                    &matcher_set.matchers,
                )
                .map_err(internal)?
            {
                labels.insert(series);
            }
        }
        let mut metric = labels
            .into_iter()
            .map(|labels| cortexpb::Metric {
                labels: label_pairs(&labels),
            })
            .collect::<Vec<_>>();
        apply_limit(&mut metric, request.limit)?;
        Ok(Response::new(cortex::MetricsForLabelMatchersResponse {
            metric,
        }))
    }

    async fn metrics_metadata(
        &self,
        request: Request<cortex::MetricsMetadataRequest>,
    ) -> Result<Response<cortex::MetricsMetadataResponse>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        if request.limit == 0 {
            return Ok(Response::new(cortex::MetricsMetadataResponse {
                metadata: Vec::new(),
            }));
        }
        let mut grouped: BTreeMap<String, Vec<cortexpb::MetricMetadata>> = BTreeMap::new();
        for item in self.store.metadata(&tenant) {
            grouped
                .entry(item.metric_family_name.clone())
                .or_default()
                .push(item);
        }
        let names = if !request.metric_names.is_empty() {
            request.metric_names
        } else if !request.metric.is_empty() {
            vec![request.metric]
        } else {
            grouped.keys().cloned().collect()
        };
        let mut metadata = Vec::new();
        let mut metric_count = 0;
        for name in names {
            let Some(mut items) = grouped.remove(&name) else {
                continue;
            };
            if request.limit > 0 && metric_count >= request.limit {
                break;
            }
            if request.limit_per_metric > 0 {
                items.truncate(request.limit_per_metric as usize);
            }
            metadata.extend(items);
            metric_count += 1;
        }
        Ok(Response::new(cortex::MetricsMetadataResponse { metadata }))
    }

    async fn label_names_and_values(
        &self,
        request: Request<cortex::LabelNamesAndValuesRequest>,
    ) -> Result<Response<Self::LabelNamesAndValuesStream>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let active = count_active(request.count_method)?;
        let items = self
            .store
            .label_names_and_values(&tenant, &request.matchers, active)
            .map_err(internal)?
            .into_iter()
            .map(|(label_name, values)| cortex::LabelValues {
                label_name,
                values: values.into_iter().collect(),
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(batch_messages(
            items,
            |items| cortex::LabelNamesAndValuesResponse { items },
        )))))
    }

    async fn label_values_cardinality(
        &self,
        request: Request<cortex::LabelValuesCardinalityRequest>,
    ) -> Result<Response<Self::LabelValuesCardinalityStream>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let active = count_active(request.count_method)?;
        let items = self
            .store
            .label_values_cardinality(&tenant, &request.label_names, &request.matchers, active)
            .map_err(internal)?
            .into_iter()
            .map(
                |(label_name, label_value_series)| cortex::LabelValueSeriesCount {
                    label_name,
                    label_value_series: label_value_series.into_iter().collect(),
                },
            )
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(batch_messages(
            items,
            |items| cortex::LabelValuesCardinalityResponse { items },
        )))))
    }

    async fn active_series(
        &self,
        request: Request<cortex::ActiveSeriesRequest>,
    ) -> Result<Response<Self::ActiveSeriesStream>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        if request.r#type != 0 && request.r#type != 1 {
            return Err(Status::invalid_argument(
                "invalid active series request type",
            ));
        }
        let histogram_only = request.r#type == 1;
        let series = self
            .store
            .active_series(&tenant, &request.matchers, histogram_only)
            .map_err(internal)?;
        let mut messages = Vec::new();
        let mut response = cortex::ActiveSeriesResponse {
            metric: Vec::new(),
            bucket_count: Vec::new(),
        };
        for item in series {
            response.metric.push(cortexpb::Metric {
                labels: label_pairs(&item.labels),
            });
            if histogram_only {
                response.bucket_count.push(item.bucket_count);
            }
            if response.encoded_len() >= RESPONSE_TARGET_BYTES {
                messages.push(Ok(std::mem::replace(
                    &mut response,
                    cortex::ActiveSeriesResponse {
                        metric: Vec::new(),
                        bucket_count: Vec::new(),
                    },
                )));
            }
        }
        if !response.metric.is_empty() {
            messages.push(Ok(response));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(messages))))
    }

    async fn search_label_names(
        &self,
        request: Request<cortex::SearchLabelNamesRequest>,
    ) -> Result<Response<Self::SearchLabelNamesStream>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let values = self
            .store
            .label_names(
                &tenant,
                request.start_timestamp_ms,
                request.end_timestamp_ms,
                &request.matchers,
            )
            .map_err(internal)?;
        Ok(Response::new(search(
            values,
            request.filter.as_ref(),
            request.ordering,
            request.limit,
        )?))
    }

    async fn search_label_values(
        &self,
        request: Request<cortex::SearchLabelValuesRequest>,
    ) -> Result<Response<Self::SearchLabelValuesStream>, Status> {
        self.enforce(&request).await?;
        let tenant = tenant(&request)?;
        let request = request.into_inner();
        let values = self
            .store
            .label_values(
                &tenant,
                &request.name,
                request.start_timestamp_ms,
                request.end_timestamp_ms,
                &request.matchers,
            )
            .map_err(internal)?;
        Ok(Response::new(search(
            values,
            request.filter.as_ref(),
            request.ordering,
            request.limit,
        )?))
    }
}

fn raw_response(encoded: Vec<u8>) -> cortex::QueryStreamResponse {
    cortex::QueryStreamResponse {
        encoded_response: encoded.into(),
        ..Default::default()
    }
}

fn label_pairs(labels: &[(String, String)]) -> Vec<cortexpb::LabelPair> {
    labels
        .iter()
        .map(|(name, value)| cortexpb::LabelPair {
            name: name.as_bytes().to_vec().into(),
            value: value.as_bytes().to_vec().into(),
        })
        .collect()
}

fn series_response(
    series: &[crate::store::QuerySeriesView],
    is_end: bool,
) -> cortex::QueryStreamResponse {
    let capacity = series
        .iter()
        .map(|item| item.encoded_labels.len() + 16)
        .sum::<usize>()
        + usize::from(is_end) * 2;
    let mut encoded = Vec::with_capacity(capacity);
    for item in series {
        let chunk_count = item.chunk_end - item.chunk_start;
        let series_size = item.encoded_labels.len()
            + usize::from(chunk_count > 0) * (1 + varint_size(chunk_count as u64));
        encoded.push(0x1a);
        put_varint(&mut encoded, series_size as u64);
        encoded.extend_from_slice(&item.encoded_labels);
        if chunk_count > 0 {
            encoded.push(0x10);
            put_varint(&mut encoded, chunk_count as u64);
        }
    }
    if is_end {
        encoded.extend_from_slice(&[0x20, 0x01]);
    }
    raw_response(encoded)
}

fn encoded_chunks_item_size(series: &crate::store::QuerySeriesView, series_index: u64) -> usize {
    let index_size = usize::from(series_index > 0) * (1 + varint_size(series_index));
    index_size
        + series.chunks[series.chunk_start..series.chunk_end]
            .iter()
            .map(|chunk| 1 + varint_size(chunk.wire.len() as u64) + chunk.wire.len())
            .sum::<usize>()
}

fn append_encoded_chunks(
    encoded: &mut Vec<u8>,
    series: &crate::store::QuerySeriesView,
    series_index: u64,
    item_size: usize,
) {
    encoded.push(0x2a);
    put_varint(encoded, item_size as u64);
    if series_index > 0 {
        encoded.push(0x08);
        put_varint(encoded, series_index);
    }
    for chunk in &series.chunks[series.chunk_start..series.chunk_end] {
        encoded.push(0x12);
        put_varint(encoded, chunk.wire.len() as u64);
        encoded.extend_from_slice(&chunk.wire);
    }
}

fn varint_size(value: u64) -> usize {
    (64 - value.leading_zeros() as usize).max(1).div_ceil(7)
}

fn put_varint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

fn stats(view: UserStatsView) -> cortex::UserStatsResponse {
    cortex::UserStatsResponse {
        ingestion_rate: view.ingestion_rate,
        num_series: view.num_series,
        api_ingestion_rate: view.api_ingestion_rate,
        rule_ingestion_rate: view.rule_ingestion_rate,
    }
}

fn count_active(method: i32) -> Result<bool, Status> {
    match method {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Status::invalid_argument("invalid count method")),
    }
}

fn apply_limit<T>(values: &mut Vec<T>, limit: i64) -> Result<(), Status> {
    if limit < 0 {
        return Err(Status::invalid_argument("limit must be >= 0"));
    }
    if limit > 0 {
        values.truncate(limit as usize);
    }
    Ok(())
}

fn batch_messages<T, M>(items: Vec<T>, build: impl Fn(Vec<T>) -> M) -> Vec<Result<M, Status>>
where
    T: Message,
{
    let mut messages = Vec::new();
    let mut batch = Vec::new();
    let mut size = 0;
    for item in items {
        let item_size = item.encoded_len();
        if !batch.is_empty() && size + item_size > RESPONSE_TARGET_BYTES {
            messages.push(Ok(build(std::mem::take(&mut batch))));
            size = 0;
        }
        size += item_size;
        batch.push(item);
    }
    if !batch.is_empty() {
        messages.push(Ok(build(batch)));
    }
    messages
}

fn search(
    values: Vec<String>,
    filter: Option<&cortex::SearchFilter>,
    ordering: i32,
    limit: i64,
) -> Result<ResponseStream<cortex::SearchResultBatch>, Status> {
    if limit < 0 {
        return Err(Status::invalid_argument("limit must be >= 0"));
    }
    if !(0..=2).contains(&ordering) {
        return Err(Status::invalid_argument("invalid search ordering"));
    }
    if let Some(filter) = filter {
        if !(0..=100).contains(&filter.fuzz_threshold) {
            return Err(Status::invalid_argument(
                "fuzz threshold must be between 0 and 100",
            ));
        }
        if filter.terms.iter().any(String::is_empty) {
            return Err(Status::invalid_argument("search terms must not be empty"));
        }
        if filter.fuzz_alg != 0 && filter.fuzz_alg != 1 {
            return Err(Status::invalid_argument("invalid fuzzy algorithm"));
        }
    }
    let mut results = values
        .into_iter()
        .filter_map(|value| {
            score(&value, filter).map(|score| cortex::search_result_batch::Result { value, score })
        })
        .collect::<Vec<_>>();
    results.sort_by(|a, b| match ordering {
        1 => b.value.cmp(&a.value),
        2 => b
            .score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.value.cmp(&b.value)),
        _ => a.value.cmp(&b.value),
    });
    if limit > 0 {
        results.truncate(limit as usize);
    }
    let batches = results
        .chunks(SEARCH_BATCH_SIZE)
        .map(|batch| {
            Ok(cortex::SearchResultBatch {
                results: batch.to_vec(),
                warnings: Vec::new(),
            })
        })
        .collect::<Vec<_>>();
    Ok(Box::pin(tokio_stream::iter(batches)))
}

fn score(value: &str, filter: Option<&cortex::SearchFilter>) -> Option<f64> {
    let Some(filter) = filter else {
        return Some(1.0);
    };
    if filter.terms.is_empty() {
        return Some(1.0);
    }
    let candidate = if filter.case_insensitive {
        value.to_lowercase()
    } else {
        value.to_owned()
    };
    let threshold = f64::from(filter.fuzz_threshold) / 100.0;
    filter
        .terms
        .iter()
        .filter_map(|term| {
            let term = if filter.case_insensitive {
                term.to_lowercase()
            } else {
                term.clone()
            };
            if filter.fuzz_alg == 1 {
                contains_score(&term, &candidate).or_else(|| {
                    let score = jaro_winkler(&term, &candidate);
                    (filter.fuzz_threshold > 0 && score >= threshold).then_some(score)
                })
            } else {
                let score = subsequence_score(&term, &candidate);
                (score > 0.0 && score >= threshold).then_some(score)
            }
        })
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

fn contains_score(term: &str, value: &str) -> Option<f64> {
    let index = value.find(term)?;
    if index == 0 {
        return Some(1.0);
    }
    Some(1.0 - 0.9 * index as f64 / (value.len() - term.len()) as f64)
}

fn subsequence_score(pattern: &str, text: &str) -> f64 {
    if pattern == text || text.starts_with(pattern) {
        return 1.0;
    }
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    if pattern.is_empty() || pattern.len() > text.len() {
        return 0.0;
    }
    let mut best = f64::NEG_INFINITY;
    for start in 0..=text.len() - pattern.len() {
        if text[start] != pattern[0] {
            continue;
        }
        let mut pattern_index = 0;
        let mut index = start;
        let mut score = -(start as f64 / text.len() as f64);
        let mut previous = None;
        while index < text.len() && pattern_index < pattern.len() {
            if text[index] == pattern[pattern_index] {
                let run_start = index;
                while index < text.len()
                    && pattern_index < pattern.len()
                    && text[index] == pattern[pattern_index]
                {
                    index += 1;
                    pattern_index += 1;
                }
                if let Some(previous) = previous {
                    score -= (run_start - previous - 1) as f64 / text.len() as f64;
                }
                let run = index - run_start;
                score += (run * run) as f64;
                previous = Some(index - 1);
            } else {
                index += 1;
            }
        }
        if pattern_index == pattern.len() {
            score -= (text.len() - index) as f64 / (2.0 * text.len() as f64);
            best = best.max(score);
        }
    }
    if !best.is_finite() {
        0.0
    } else {
        ((best / (pattern.len() * pattern.len()) as f64).clamp(0.0, 1.0)) * 0.999
    }
}

fn jaro_winkler(first: &str, second: &str) -> f64 {
    if first == second {
        return 1.0;
    }
    let mut first = first.chars().collect::<Vec<_>>();
    let mut second = second.chars().collect::<Vec<_>>();
    if first.is_empty() || second.is_empty() {
        return 0.0;
    }
    if first.len() > second.len() {
        std::mem::swap(&mut first, &mut second);
    }
    let distance = second.len() / 2;
    let distance = distance.saturating_sub(1);
    let mut first_matches = vec![false; first.len()];
    let mut second_matches = vec![false; second.len()];
    let mut matches = 0.0;
    for i in 0..first.len() {
        for j in i.saturating_sub(distance)..=(i + distance).min(second.len() - 1) {
            if !second_matches[j] && first[i] == second[j] {
                first_matches[i] = true;
                second_matches[j] = true;
                matches += 1.0;
                break;
            }
        }
    }
    if matches == 0.0 {
        return 0.0;
    }
    let matched_second = second
        .iter()
        .zip(second_matches)
        .filter_map(|(value, matched)| matched.then_some(value))
        .collect::<Vec<_>>();
    let transpositions = first
        .iter()
        .zip(first_matches)
        .filter_map(|(value, matched)| matched.then_some(value))
        .zip(matched_second)
        .filter(|(a, b)| a != b)
        .count() as f64;
    let jaro = (matches / first.len() as f64
        + matches / second.len() as f64
        + (matches - transpositions / 2.0) / matches)
        / 3.0;
    let prefix = first
        .iter()
        .zip(&second)
        .take(4)
        .take_while(|(a, b)| a == b)
        .count();
    jaro + prefix as f64 * 0.1 * (1.0 - jaro)
}
