//! Prometheus metrics named like the Go ingester's, so the same dashboards and recording rules
//! read both. Cost attribution trackers that are not internal go to a separate registry served on
//! Mimir's `-cost-attribution.registry-path`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use prometheus::{
    Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);
pub static USAGE_REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

fn register<T: prometheus::core::Collector + Clone + 'static>(
    registry: &Registry,
    collector: T,
) -> T {
    registry
        .register(Box::new(collector.clone()))
        .expect("register metric");
    collector
}

macro_rules! metric {
    ($name:ident: $type:ty = $make:expr) => {
        pub static $name: LazyLock<$type> = LazyLock::new(|| register(&REGISTRY, $make));
    };
}

metric!(DISCARDED_SAMPLES: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_discarded_samples_total", "The total number of samples that were discarded."),
    &["reason", "user", "group"],
).unwrap());
metric!(DISCARDED_METADATA: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_discarded_metadata_total", "The total number of metadata that were discarded."),
    &["reason", "user"],
).unwrap());
metric!(INGESTED_SAMPLES: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_ingested_samples_total", "The total number of samples ingested per user."),
    &["user"],
).unwrap());
metric!(INGESTED_SAMPLES_FAILURES: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_ingested_samples_failures_total", "The total number of samples that errored on ingestion per user."),
    &["user"],
).unwrap());
metric!(INGESTED_EXEMPLARS: IntCounter = IntCounter::new(
    "cortex_ingester_ingested_exemplars_total", "The total number of exemplars ingested.",
).unwrap());
metric!(INGESTED_EXEMPLARS_FAILURES: IntCounter = IntCounter::new(
    "cortex_ingester_ingested_exemplars_failures_total", "The total number of exemplars that errored on ingestion.",
).unwrap());
metric!(INGESTED_METADATA: IntCounter = IntCounter::new(
    "cortex_ingester_ingested_metadata_total", "The total number of metadata ingested.",
).unwrap());
metric!(INGESTED_METADATA_FAILURES: IntCounter = IntCounter::new(
    "cortex_ingester_ingested_metadata_failures_total", "The total number of metadata that errored on ingestion.",
).unwrap());
metric!(EXEMPLARS_IN_STORAGE: IntGauge = IntGauge::new(
    "cortex_ingester_tsdb_exemplar_exemplars_in_storage", "Number of TSDB exemplars currently in storage.",
).unwrap());
metric!(EXEMPLARS_APPENDED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_tsdb_exemplar_exemplars_appended_total", "Total number of TSDB exemplars appended."),
    &["user"],
).unwrap());
metric!(SERIES_WITH_EXEMPLARS: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage", "Number of TSDB series with exemplars currently in storage."),
    &["user"],
).unwrap());
metric!(LAST_EXEMPLARS_TIMESTAMP: GaugeVec = GaugeVec::new(
    Opts::new("cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds", "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series."),
    &["user"],
).unwrap());
metric!(OUT_OF_ORDER_EXEMPLARS: IntCounter = IntCounter::new(
    "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total", "Total number of out-of-order exemplar ingestion failed attempts.",
).unwrap());
metric!(OUT_OF_ORDER_SAMPLES_APPENDED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_tsdb_out_of_order_samples_appended_total", "Total number of out-of-order samples appended."),
    &["user"],
).unwrap());
metric!(MEMORY_SERIES_CREATED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_memory_series_created_total", "The total number of series that were created per user."),
    &["user"],
).unwrap());
metric!(MEMORY_SERIES_REMOVED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_memory_series_removed_total", "The total number of series that were removed per user."),
    &["user"],
).unwrap());
metric!(EARLY_COMPACTION_NON_OWNED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_tsdb_early_compaction_non_owned_series_triggered_total", "Total number of triggered early head compactions of non-owned series, per tenant."),
    &["user"],
).unwrap());
metric!(MEMORY_METADATA_CREATED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_memory_metadata_created_total", "The total number of metadata that were created per user"),
    &["user"],
).unwrap());
metric!(MEMORY_METADATA_REMOVED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_memory_metadata_removed_total", "The total number of metadata that were removed per user."),
    &["user"],
).unwrap());
metric!(OWNED_SERIES: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_owned_series", "Number of currently owned series per user."),
    &["user"],
).unwrap());
metric!(LOCAL_LIMITS: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_local_limits", "Local per-user limits used by this ingester.")
        .const_label("limit", "max_global_series_per_user"),
    &["user"],
).unwrap());
metric!(INSTANCE_LIMITS: GaugeVec = GaugeVec::new(
    Opts::new("cortex_ingester_instance_limits", "Instance limits used by this ingester."),
    &["limit"],
).unwrap());
metric!(HEAD_CHUNKS: IntGauge = IntGauge::new(
    "cortex_ingester_tsdb_head_chunks", "Total number of chunks in the TSDB head block.",
).unwrap());
metric!(HEAD_CHUNKS_CREATED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_tsdb_head_chunks_created_total", "Total number of series created in the TSDB head."),
    &["user"],
).unwrap());
metric!(HEAD_CHUNKS_REMOVED: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_tsdb_head_chunks_removed_total", "Total number of series removed in the TSDB head."),
    &["user"],
).unwrap());
metric!(HEAD_MIN_TIMESTAMP: Gauge = Gauge::new(
    "cortex_ingester_tsdb_head_min_timestamp_seconds", "Minimum timestamp of the head block across all tenants.",
).unwrap());
metric!(HEAD_MAX_TIMESTAMP: Gauge = Gauge::new(
    "cortex_ingester_tsdb_head_max_timestamp_seconds", "Maximum timestamp of the head block across all tenants.",
).unwrap());
metric!(INGESTION_RATE: Gauge = Gauge::new(
    "cortex_ingester_ingestion_rate_samples_per_second", "Current ingestion rate in samples/sec that ingester is using to limit access.",
).unwrap());
metric!(QUERIES: IntCounter = IntCounter::new(
    "cortex_ingester_queries_total", "The total number of queries the ingester has handled.",
).unwrap());
metric!(QUERIED_SAMPLES: prometheus::Histogram = prometheus::Histogram::with_opts(
    HistogramOpts::new("cortex_ingester_queried_samples", "The total number of samples returned from queries.")
        .buckets(prometheus::exponential_buckets(10.0, 8.0, 8).unwrap()),
).unwrap());
metric!(QUERIED_EXEMPLARS: prometheus::Histogram = prometheus::Histogram::with_opts(
    HistogramOpts::new("cortex_ingester_queried_exemplars", "The total number of exemplars returned from queries.")
        .buckets(prometheus::exponential_buckets(10.0, 5.0, 5).unwrap()),
).unwrap());
metric!(QUERIED_SERIES: HistogramVec = HistogramVec::new(
    HistogramOpts::new("cortex_ingester_queried_series", "The total number of series returned from queries.")
        .buckets(prometheus::exponential_buckets(10.0, 8.0, 6).unwrap()),
    &["stage"],
).unwrap());
metric!(QUERIED_BLOCKS: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_queried_blocks_total", "Number of times blocks were queried by generation. Generation 0 is the head block; higher generations count persisted blocks back from the head (1 = most recent)."),
    &["generation"],
).unwrap());
metric!(COST_ATTRIBUTION_CARDINALITY: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_cost_attribution_active_series_tracker_cardinality", "The cardinality of a cost attribution active series tracker for each user."),
    &["user", "tracker"],
).unwrap());
metric!(COST_ATTRIBUTION_OVERFLOWN: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_cost_attribution_active_series_tracker_overflown", "This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise."),
    &["user", "tracker"],
).unwrap());
metric!(MEMORY_SERIES: IntGauge = IntGauge::new(
    "cortex_ingester_memory_series", "The current number of series in memory.",
).unwrap());
metric!(MEMORY_USERS: IntGauge = IntGauge::new(
    "cortex_ingester_memory_users", "The current number of users in memory.",
).unwrap());
metric!(MEMORY_METADATA: IntGauge = IntGauge::new(
    "cortex_ingester_memory_metadata", "The current number of metadata in memory.",
).unwrap());
metric!(ACTIVE_SERIES: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_series", "Number of currently active series per user."),
    &["user"],
).unwrap());
metric!(ACTIVE_SERIES_CUSTOM_TRACKER: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_series_custom_tracker", "Number of currently active series matching a pre-configured label matchers per user."),
    &["user", "name"],
).unwrap());
metric!(ACTIVE_NATIVE_HISTOGRAM_SERIES: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_native_histogram_series", "Number of currently active native histogram series per user."),
    &["user"],
).unwrap());
metric!(ACTIVE_NATIVE_HISTOGRAM_SERIES_CUSTOM_TRACKER: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_native_histogram_series_custom_tracker", "Number of currently active native histogram series matching a pre-configured label matchers per user."),
    &["user", "name"],
).unwrap());
metric!(ACTIVE_NATIVE_HISTOGRAM_BUCKETS: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_native_histogram_buckets", "Number of currently active native histogram buckets per user."),
    &["user"],
).unwrap());
metric!(ACTIVE_NATIVE_HISTOGRAM_BUCKETS_CUSTOM_TRACKER: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_active_native_histogram_buckets_custom_tracker", "Number of currently active native histogram buckets matching a pre-configured label matchers per user."),
    &["user", "name"],
).unwrap());
metric!(ACTIVE_SERIES_LOADING: IntGauge = IntGauge::new(
    "cortex_ingester_active_series_loading", "1 if active series counts are still warming up and may be underreported, 0 once they are accurate.",
).unwrap());
metric!(REQUEST_DURATION: HistogramVec = HistogramVec::new(
    HistogramOpts::new("cortex_request_duration_seconds", "Time (in seconds) spent serving HTTP requests.")
        .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]),
    &["method", "route", "status_code", "ws"],
).unwrap());
metric!(INFLIGHT_REQUESTS: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_inflight_requests", "Current number of inflight requests."),
    &["method", "route"],
).unwrap());
metric!(CIRCUIT_BREAKER_TRANSITIONS: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_circuit_breaker_transitions_total", "Number of times the circuit breaker has entered a state."),
    &["request_type", "state"],
).unwrap());
metric!(CIRCUIT_BREAKER_RESULTS: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_circuit_breaker_results_total", "Results of executing requests via the circuit breaker."),
    &["request_type", "result"],
).unwrap());
metric!(CIRCUIT_BREAKER_REQUEST_TIMEOUTS: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_circuit_breaker_request_timeouts_total", "Number of times the circuit breaker recorded a request that reached timeout."),
    &["request_type"],
).unwrap());
metric!(CIRCUIT_BREAKER_CURRENT_STATE: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_circuit_breaker_current_state", "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name."),
    &["request_type", "state"],
).unwrap());
metric!(UTILIZATION_LIMITED_READS: IntCounterVec = IntCounterVec::new(
    Opts::new("cortex_ingester_utilization_limited_read_requests_total", "Total number of times read requests have been rejected due to utilization based limiting."),
    &["reason"],
).unwrap());
metric!(UTILIZATION_CPU: Gauge = Gauge::new(
    "utilization_limiter_current_cpu_load", "Current average CPU load calculated by utilization based limiter.",
).unwrap());
metric!(UTILIZATION_MEMORY: Gauge = Gauge::new(
    "utilization_limiter_current_memory_usage_bytes", "Current memory usage calculated by utilization based limiter.",
).unwrap());
metric!(RUNTIME_CONFIG_SUCCESS: IntGauge = IntGauge::new(
    "cortex_runtime_config_last_reload_successful", "Whether the last runtime-config reload attempt was successful.",
).unwrap());
metric!(RUNTIME_CONFIG_HASH: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_runtime_config_hash", "Hash of the currently active runtime configuration, merged from all configured files."),
    &["sha256"],
).unwrap());
metric!(RUNTIME_CONFIG_HTTP_DURATION: HistogramVec = HistogramVec::new(
    HistogramOpts::new("cortex_runtime_config_http_request_duration_seconds", "Time spent fetching runtime config from HTTP endpoints."),
    &["url", "status_code"],
).unwrap());

type AttributedFamilies = HashMap<(bool, Vec<String>), Arc<AttributedSeries>>;

static ATTRIBUTED: LazyLock<Mutex<AttributedFamilies>> = LazyLock::new(Mutex::default);

// Attributed active series, one family per registry; `internal` trackers use the main one.
pub struct AttributedSeries {
    pub active_series: GaugeVec,
    pub overflow_labels: GaugeVec,
    pub native_histogram_series: GaugeVec,
    pub native_histogram_buckets: GaugeVec,
}

/// Registered lazily per tracker label set, because the families' label names come from the
/// tracker configuration.
pub fn attributed_series(internal: bool, label_names: &[String]) -> Arc<AttributedSeries> {
    let mut families = ATTRIBUTED.lock().expect("metrics lock poisoned");
    let key = (internal, label_names.to_vec());
    if let Some(family) = families.get(&key) {
        return Arc::clone(family);
    }
    let registry = if internal {
        &*REGISTRY
    } else {
        &*USAGE_REGISTRY
    };
    let mut names = label_names.iter().map(String::as_str).collect::<Vec<_>>();
    names.push("tenant");
    names.push("tracker");
    let family = |name: &str, help: &str| {
        let gauge = GaugeVec::new(Opts::new(name, help), &names).expect("valid attribution labels");
        // Another label set can already own the name in this registry; Prometheus clients reject
        // mixed label names within one family, so that tracker's series are not exported.
        let _ = registry.register(Box::new(gauge.clone()));
        gauge
    };
    let series = Arc::new(AttributedSeries {
        active_series: family(
            "cortex_ingester_attributed_active_series",
            "The total number of active series per user and attribution.",
        ),
        overflow_labels: family(
            "cortex_attributed_series_overflow_labels",
            "The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.",
        ),
        native_histogram_series: family(
            "cortex_ingester_attributed_active_native_histogram_series",
            "The total number of active native histogram series per user and attribution.",
        ),
        native_histogram_buckets: family(
            "cortex_ingester_attributed_active_native_histogram_buckets",
            "The total number of active native histogram buckets per user and attribution.",
        ),
    });
    families.insert(key, Arc::clone(&series));
    series
}

/// Clears every attributed series, before an active series update sets the live ones again.
pub fn reset_attributed_series() {
    for family in ATTRIBUTED.lock().expect("metrics lock poisoned").values() {
        family.active_series.reset();
        family.overflow_labels.reset();
        family.native_histogram_series.reset();
        family.native_histogram_buckets.reset();
    }
}

pub fn encode(registry: &Registry) -> Vec<u8> {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut buffer)
        .expect("encode metrics");
    buffer
}

pub fn observe_runtime_config_request(url: &str, status: u16, elapsed: Duration) {
    RUNTIME_CONFIG_HTTP_DURATION
        .with_label_values(&[url, &status.to_string()])
        .observe(elapsed.as_secs_f64());
}

pub fn set_runtime_config_success(success: bool) {
    RUNTIME_CONFIG_SUCCESS.set(i64::from(success));
}

pub fn set_runtime_config_hash(hash: u64) {
    RUNTIME_CONFIG_HASH.reset();
    RUNTIME_CONFIG_HASH
        .with_label_values(&[&format!("{hash:016x}")])
        .set(1);
    RUNTIME_CONFIG_SUCCESS.set(1);
}

/// Drops the sidecar's `process_*` families, which describe the sidecar rather than the ingester
/// process whose own ones this registry exports.
pub fn sidecar_families(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for line in text.lines() {
        let name = line
            .strip_prefix("# HELP ")
            .or_else(|| line.strip_prefix("# TYPE "))
            .unwrap_or(line);
        if name.starts_with("process_") {
            continue;
        }
        output.push_str(line);
        output.push('\n');
    }
    output
}

async fn fetch_sidecar(url: &str) -> Option<String> {
    use http_body_util::{BodyExt, Empty};
    type HttpClient = hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Empty<bytes::Bytes>,
    >;
    static CLIENT: LazyLock<HttpClient> = LazyLock::new(|| {
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http()
    });
    let response = tokio::time::timeout(Duration::from_secs(2), CLIENT.get(url.parse().ok()?))
        .await
        .ok()?
        .ok()?;
    let body = response.into_body().collect().await.ok()?.to_bytes();
    String::from_utf8(body.to_vec()).ok()
}

pub fn register_process_metrics() {
    #[cfg(target_os = "linux")]
    {
        let _ = REGISTRY.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ));
    }
}

/// Serves `/metrics`, with the ring sidecar's metrics appended when `sidecar_url` is set, and
/// the cost attribution registry path.
pub async fn serve(
    address: &str,
    usage_path: &str,
    sidecar_url: Option<String>,
) -> anyhow::Result<()> {
    use axum::routing::get;
    register_process_metrics();
    let listener = tokio::net::TcpListener::bind(address).await?;
    let text = |registry: &'static Registry| {
        move || async move {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4",
                )],
                encode(registry),
            )
        }
    };
    let main = move || {
        let sidecar_url = sidecar_url.clone();
        async move {
            let mut body = encode(&REGISTRY);
            if let Some(url) = sidecar_url
                && let Some(sidecar) = fetch_sidecar(&url).await
            {
                body.extend_from_slice(sidecar_families(&sidecar).as_bytes());
            }
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4",
                )],
                body,
            )
        }
    };
    let mut app = axum::Router::new().route("/metrics", get(main));
    if !usage_path.is_empty() && usage_path != "/metrics" {
        app = app.route(usage_path, get(text(&USAGE_REGISTRY)));
    }
    eprintln!("phase=metrics_listen address={address} usage_path={usage_path}");
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("metrics server failed: {error:#}");
        }
    });
    Ok(())
}

/// Replaces the per-tenant active series, memory and exemplar gauges with `reports`. Custom
/// tracker series with no active series are not exported, like in Mimir.
pub fn export_active_series(reports: &[crate::store::ActiveSeriesReport]) {
    for gauge in [
        &*ACTIVE_SERIES,
        &*ACTIVE_NATIVE_HISTOGRAM_SERIES,
        &*ACTIVE_NATIVE_HISTOGRAM_BUCKETS,
        &*ACTIVE_SERIES_CUSTOM_TRACKER,
        &*ACTIVE_NATIVE_HISTOGRAM_SERIES_CUSTOM_TRACKER,
        &*ACTIVE_NATIVE_HISTOGRAM_BUCKETS_CUSTOM_TRACKER,
        &*SERIES_WITH_EXEMPLARS,
        &*COST_ATTRIBUTION_CARDINALITY,
        &*COST_ATTRIBUTION_OVERFLOWN,
    ] {
        gauge.reset();
    }
    reset_attributed_series();
    let (mut metadata, mut exemplars) = (0, 0);
    LAST_EXEMPLARS_TIMESTAMP.reset();
    for report in reports {
        let tenant = report.tenant.as_str();
        metadata += report.metadata;
        // Like Mimir, a tenant without active series has none of these.
        for (gauge, value) in [
            (&*ACTIVE_SERIES, report.active),
            (
                &*ACTIVE_NATIVE_HISTOGRAM_SERIES,
                report.active_native_histograms,
            ),
            (
                &*ACTIVE_NATIVE_HISTOGRAM_BUCKETS,
                report.active_native_histogram_buckets,
            ),
        ] {
            if value > 0 {
                gauge.with_label_values(&[tenant]).set(value as i64);
            }
        }
        exemplars += report.exemplars;
        // Like the per-tenant TSDB metrics, exported for every tenant with a head.
        SERIES_WITH_EXEMPLARS
            .with_label_values(&[tenant])
            .set(report.exemplar_series as i64);
        LAST_EXEMPLARS_TIMESTAMP.with_label_values(&[tenant]).set(
            report
                .oldest_exemplar_ms
                .map_or(0.0, |oldest| oldest as f64 / 1000.0),
        );
        for (name, [active, histograms, buckets]) in &report.custom_trackers {
            if *active > 0 {
                ACTIVE_SERIES_CUSTOM_TRACKER
                    .with_label_values(&[tenant, name])
                    .set(*active as i64);
            }
            if *histograms > 0 {
                ACTIVE_NATIVE_HISTOGRAM_SERIES_CUSTOM_TRACKER
                    .with_label_values(&[tenant, name])
                    .set(*histograms as i64);
                ACTIVE_NATIVE_HISTOGRAM_BUCKETS_CUSTOM_TRACKER
                    .with_label_values(&[tenant, name])
                    .set(*buckets as i64);
            }
        }
        for tracker in &report.cost_attribution {
            COST_ATTRIBUTION_CARDINALITY
                .with_label_values(&[tenant, &tracker.tracker])
                .set(tracker.cardinality as i64);
            if tracker.overflow {
                COST_ATTRIBUTION_OVERFLOWN
                    .with_label_values(&[tenant, &tracker.tracker])
                    .set(1);
            }
            let family = attributed_series(tracker.internal, &tracker.output_labels);
            let overflow_labels =
                vec![crate::trackers::OVERFLOW_VALUE; tracker.output_labels.len()];
            let mut labels = overflow_labels.clone();
            labels.extend([tenant, tracker.tracker.as_str()]);
            family.overflow_labels.with_label_values(&labels).set(1.0);
            for (values, [active, histograms, buckets]) in &tracker.values {
                let mut labels = values.iter().map(String::as_str).collect::<Vec<_>>();
                labels.extend([tenant, tracker.tracker.as_str()]);
                family
                    .active_series
                    .with_label_values(&labels)
                    .set(*active as f64);
                if *histograms > 0 {
                    family
                        .native_histogram_series
                        .with_label_values(&labels)
                        .set(*histograms as f64);
                    family
                        .native_histogram_buckets
                        .with_label_values(&labels)
                        .set(*buckets as f64);
                }
            }
        }
    }
    MEMORY_USERS.set(reports.len() as i64);
    MEMORY_METADATA.set(metadata as i64);
    EXEMPLARS_IN_STORAGE.set(exemplars as i64);
}

/// Exports the emulated Go head state, the per-tenant local series limit and instance limits.
pub fn export_head(
    reports: &[crate::store::HeadReport],
    local_series_limits: &[(String, usize)],
    instance: &crate::limits::InstanceLimits,
    owned_series: bool,
) {
    OWNED_SERIES.reset();
    LOCAL_LIMITS.reset();
    let (mut series, mut chunks) = (0, 0);
    let (mut min_time, mut max_time) = (i64::MAX, i64::MIN);
    for report in reports {
        let tenant = report.tenant.as_str();
        series += report.memory_series;
        chunks += report.head_chunks;
        if report.series_created > 0 {
            MEMORY_SERIES_CREATED
                .with_label_values(&[tenant])
                .inc_by(report.series_created);
        }
        if report.series_removed > 0 {
            MEMORY_SERIES_REMOVED
                .with_label_values(&[tenant])
                .inc_by(report.series_removed);
        }
        if report.non_owned_evicted > 0 {
            EARLY_COMPACTION_NON_OWNED
                .with_label_values(&[tenant])
                .inc();
        }
        if owned_series {
            OWNED_SERIES
                .with_label_values(&[tenant])
                .set(report.owned_series as i64);
        }
        // Removed chunks are the created ones no longer in the head, which never decreases.
        let created = HEAD_CHUNKS_CREATED.with_label_values(&[tenant]).get();
        let removed = HEAD_CHUNKS_REMOVED.with_label_values(&[tenant]);
        let now_removed = created.saturating_sub(report.head_chunks);
        if now_removed > removed.get() {
            removed.inc_by(now_removed - removed.get());
        }
        if report.memory_series > 0 {
            min_time = min_time.min(report.head_min_time);
            max_time = max_time.max(report.head_max_time);
        }
    }
    for (tenant, limit) in local_series_limits {
        LOCAL_LIMITS.with_label_values(&[tenant]).set(*limit as i64);
    }
    MEMORY_SERIES.set(series as i64);
    HEAD_CHUNKS.set(chunks as i64);
    // Like Go, no head data reports 0.
    HEAD_MIN_TIMESTAMP.set(if min_time == i64::MAX {
        0.0
    } else {
        min_time as f64 / 1000.0
    });
    HEAD_MAX_TIMESTAMP.set(if max_time == i64::MIN {
        0.0
    } else {
        max_time as f64 / 1000.0
    });
    for (limit, value) in [
        ("max_ingestion_rate", instance.max_ingestion_rate),
        ("max_tenants", instance.max_tenants as f64),
        ("max_series", instance.max_series as f64),
        (
            "max_inflight_push_requests",
            instance.max_inflight_push_requests as f64,
        ),
        (
            "max_inflight_push_requests_bytes",
            instance.max_inflight_push_requests_bytes as f64,
        ),
    ] {
        INSTANCE_LIMITS.with_label_values(&[limit]).set(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ActiveSeriesReport, AttributedSeries};

    #[test]
    fn exports_active_series_with_go_names_and_labels() {
        let report = ActiveSeriesReport {
            tenant: "export-test".into(),
            series: 5,
            active: 4,
            active_native_histograms: 1,
            active_native_histogram_buckets: 3,
            custom_trackers: vec![("api".into(), [2, 0, 0]), ("idle".into(), [0, 0, 0])],
            cost_attribution: vec![
                AttributedSeries {
                    tracker: "source-reservation".into(),
                    internal: true,
                    output_labels: vec!["source".into(), "reservation_id".into()],
                    values: vec![(vec!["k6".into(), "__missing__".into()], [4, 1, 3])],
                    overflow: false,
                    cardinality: 1,
                },
                AttributedSeries {
                    tracker: "by-team".into(),
                    internal: false,
                    output_labels: vec!["team".into()],
                    values: vec![(vec!["a".into()], [4, 0, 0])],
                    overflow: false,
                    cardinality: 1,
                },
            ],
            metadata: 2,
            exemplars: 7,
            exemplar_series: 3,
            oldest_exemplar_ms: Some(12_500),
        };
        export_active_series(&[report]);
        let main = String::from_utf8(encode(&REGISTRY)).unwrap();
        for line in [
            r#"cortex_ingester_active_series{user="export-test"} 4"#,
            r#"cortex_ingester_active_series_custom_tracker{name="api",user="export-test"} 2"#,
            r#"cortex_ingester_active_native_histogram_buckets{user="export-test"} 3"#,
            "cortex_ingester_tsdb_exemplar_exemplars_in_storage 7",
            r#"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="export-test"} 3"#,
            r#"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="export-test"} 12.5"#,
            r#"cortex_cost_attribution_active_series_tracker_cardinality{tracker="by-team",user="export-test"} 1"#,
            r#"cortex_ingester_attributed_active_series{reservation_id="__missing__",source="k6",tenant="export-test",tracker="source-reservation"} 4"#,
            r#"cortex_attributed_series_overflow_labels{reservation_id="__overflow__",source="__overflow__",tenant="export-test",tracker="source-reservation"} 1"#,
        ] {
            assert!(main.contains(line), "missing {line} in\n{main}");
        }
        assert!(!main.contains(r#"name="idle""#));
        // Non-internal trackers only go to the cost attribution registry.
        assert!(!main.contains(r#"cortex_ingester_attributed_active_series{team="a""#));
        let usage = String::from_utf8(encode(&USAGE_REGISTRY)).unwrap();
        assert!(usage.contains(
            r#"cortex_ingester_attributed_active_series{team="a",tenant="export-test",tracker="by-team"} 4"#
        ));
        // The next update replaces the previous one.
        export_active_series(&[]);
        assert!(
            !String::from_utf8(encode(&REGISTRY))
                .unwrap()
                .contains("export-test")
        );
    }
}

/// Mimir's `EwmaRate` with alpha 0.2 over one-second ticks, fed the ingested sample total.
#[derive(Default)]
pub struct IngestionRate {
    last_total: Option<u64>,
    rate: Option<f64>,
}

impl IngestionRate {
    pub fn tick(&mut self, total: u64) -> f64 {
        let events = total.saturating_sub(self.last_total.unwrap_or(0));
        self.last_total = Some(total);
        let instant = events as f64;
        let rate = match self.rate {
            Some(rate) => rate + 0.2 * (instant - rate),
            None => instant,
        };
        self.rate = Some(rate);
        rate
    }
}

#[cfg(test)]
mod sidecar_tests {
    #[test]
    fn keeps_sidecar_families_except_process_ones() {
        let text = "# HELP process_cpu_seconds_total x\n# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total 1\n# HELP rust_ingester_ring_ready y\nrust_ingester_ring_ready 1\nmemberlist_client_cluster_members_count 3\n";
        assert_eq!(
            super::sidecar_families(text),
            "# HELP rust_ingester_ring_ready y\nrust_ingester_ring_ready 1\nmemberlist_client_cluster_members_count 3\n"
        );
    }
}

#[cfg(test)]
mod rate_tests {
    use super::IngestionRate;

    #[test]
    fn ingestion_rate_is_an_ewma_of_samples_per_second() {
        let mut rate = IngestionRate::default();
        // The first tick counts everything since startup, like Go's first `Tick`.
        assert_eq!(rate.tick(1_000), 1_000.0);
        assert_eq!(rate.tick(1_500), 900.0);
        assert!((rate.tick(1_500) - 720.0).abs() < 1e-9);
    }
}
