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
metric!(EXEMPLARS_IN_STORAGE: IntGaugeVec = IntGaugeVec::new(
    Opts::new("cortex_ingester_tsdb_exemplar_exemplars_in_storage", "Number of exemplars currently in circular storage."),
    &["user"],
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

/// Serves `/metrics` and the cost attribution registry path.
pub async fn serve(address: &str, usage_path: &str) -> anyhow::Result<()> {
    use axum::routing::get;
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
    let mut app = axum::Router::new().route("/metrics", get(text(&REGISTRY)));
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
        &*EXEMPLARS_IN_STORAGE,
    ] {
        gauge.reset();
    }
    reset_attributed_series();
    let (mut series, mut metadata) = (0, 0);
    for report in reports {
        let tenant = report.tenant.as_str();
        series += report.series;
        metadata += report.metadata;
        ACTIVE_SERIES
            .with_label_values(&[tenant])
            .set(report.active as i64);
        ACTIVE_NATIVE_HISTOGRAM_SERIES
            .with_label_values(&[tenant])
            .set(report.active_native_histograms as i64);
        ACTIVE_NATIVE_HISTOGRAM_BUCKETS
            .with_label_values(&[tenant])
            .set(report.active_native_histogram_buckets as i64);
        if report.exemplars > 0 {
            EXEMPLARS_IN_STORAGE
                .with_label_values(&[tenant])
                .set(report.exemplars as i64);
        }
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
    MEMORY_SERIES.set(series as i64);
    MEMORY_USERS.set(reports.len() as i64);
    MEMORY_METADATA.set(metadata as i64);
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
                },
                AttributedSeries {
                    tracker: "by-team".into(),
                    internal: false,
                    output_labels: vec!["team".into()],
                    values: vec![(vec!["a".into()], [4, 0, 0])],
                    overflow: false,
                },
            ],
            metadata: 2,
            exemplars: 7,
        };
        export_active_series(&[report]);
        let main = String::from_utf8(encode(&REGISTRY)).unwrap();
        for line in [
            r#"cortex_ingester_active_series{user="export-test"} 4"#,
            r#"cortex_ingester_active_series_custom_tracker{name="api",user="export-test"} 2"#,
            r#"cortex_ingester_active_native_histogram_buckets{user="export-test"} 3"#,
            r#"cortex_ingester_tsdb_exemplar_exemplars_in_storage{user="export-test"} 7"#,
            r#"cortex_ingester_attributed_active_series{reservation_id="__missing__",source="k6",tenant="export-test",tracker="source-reservation"} 4"#,
            r#"cortex_attributed_series_overflow_labels{reservation_id="__overflow__",source="__overflow__",tenant="export-test",tracker="source-reservation"} 1"#,
        ] {
            assert!(main.contains(line), "missing {line} in\n{main}");
        }
        assert!(!main.contains(r#"name="idle""#));
        assert!(!main.contains(r#"tracker="by-team""#));
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
