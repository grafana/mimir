use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

fn rss_bytes() -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("run ps");
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
        * 1024
}

// Run explicitly because RSS varies by host and this fixture takes substantial memory.
#[test]
#[ignore]
fn high_cardinality_multi_tenant_memory() {
    const TENANTS: usize = 3;
    const SERIES_PER_TENANT: usize = 25_000;
    const SAMPLES_PER_SERIES: usize = 200;
    const RETENTION_MS: i64 = 2 * 60 * 60 * 1000;
    let histogram_every = std::env::var("RUST_INGESTER_BENCH_HISTOGRAM_EVERY")
        .ok()
        .map(|value| value.parse::<usize>().expect("histogram frequency"))
        .unwrap_or(10);
    let is_histogram = |id: usize| histogram_every != 0 && id % histogram_every == 0;
    let store = Store::new(20 * 60 * 1000, Some(RETENTION_MS));
    let baseline = rss_bytes();
    let started = Instant::now();
    let end = now_ms();
    for tenant in 0..TENANTS {
        for batch_start in (0..SERIES_PER_TENANT).step_by(350) {
            let batch_end = (batch_start + 350).min(SERIES_PER_TENANT);
            let series = (batch_start..batch_end)
                .map(|id| DecodedSeries {
                    labels: (0..19)
                        .map(|label| {
                            let name = if label == 0 {
                                "__name__".to_owned()
                            } else {
                                format!("label_{label:04}")
                            };
                            let value = if label == 0 {
                                format!("metric_{:04}", id % 100)
                            } else if label < 10 {
                                format!("shared_value_{:05}", (id + label * 17) % 1000)
                            } else {
                                format!("unique_value_{tenant:02}_{label:02}_{id:08}")
                            };
                            (name, value)
                        })
                        .collect(),
                    samples: if is_histogram(id) {
                        Vec::new()
                    } else {
                        (0..SAMPLES_PER_SERIES)
                            .map(|sample| cortexpb::Sample {
                                timestamp_ms: end - RETENTION_MS
                                    + 1
                                    + (sample as i64 * (RETENTION_MS - 1)
                                        / SAMPLES_PER_SERIES as i64),
                                value: (id + sample) as f64,
                            })
                            .collect()
                    },
                    histograms: if is_histogram(id) {
                        (0..SAMPLES_PER_SERIES)
                            .map(|sample| cortexpb::Histogram {
                                timestamp: end - RETENTION_MS
                                    + 1
                                    + (sample as i64 * (RETENTION_MS - 1)
                                        / SAMPLES_PER_SERIES as i64),
                                count: Some(cortexpb::histogram::Count::CountInt(50)),
                                positive_spans: vec![cortexpb::BucketSpan {
                                    offset: 0,
                                    length: 50,
                                }],
                                positive_deltas: vec![1; 50],
                                ..Default::default()
                            })
                            .collect()
                    } else {
                        Vec::new()
                    },
                    exemplars: Vec::new(),
                    created_timestamp: 0,
                })
                .collect();
            store
                .ingest_recovered(
                    &format!("tenant-{tenant}"),
                    DecodedRequest {
                        source: 0,
                        series,
                        metadata: Vec::new(),
                    },
                    end,
                )
                .unwrap();
        }
    }
    let ingest_seconds = started.elapsed().as_secs_f64();
    let rest = rss_bytes();
    let stopped = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(rest));
    let sampler = {
        let stopped = Arc::clone(&stopped);
        let peak = Arc::clone(&peak);
        thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                peak.fetch_max(rss_bytes(), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(2));
            }
        })
    };
    let started = Instant::now();
    for tenant in 0..TENANTS {
        let selected = store
            .select_chunks(&format!("tenant-{tenant}"), end - RETENTION_MS, end, &[])
            .unwrap();
        assert_eq!(selected.len(), SERIES_PER_TENANT);
        std::hint::black_box(selected);
    }
    let query_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    for tenant in 0..TENANTS {
        for batch_start in (0..SERIES_PER_TENANT).step_by(350) {
            let batch_end = (batch_start + 350).min(SERIES_PER_TENANT);
            let series = (batch_start..batch_end)
                .map(|id| DecodedSeries {
                    labels: (0..19)
                        .map(|label| {
                            let name = if label == 0 {
                                "__name__".to_owned()
                            } else {
                                format!("label_{label:04}")
                            };
                            let value = if label == 0 {
                                format!("metric_{:04}", id % 100)
                            } else if label < 10 {
                                format!("shared_value_{:05}", (id + label * 17) % 1000)
                            } else {
                                format!("unique_value_{tenant:02}_{label:02}_{id:08}")
                            };
                            (name, value)
                        })
                        .collect(),
                    samples: if is_histogram(id) {
                        Vec::new()
                    } else {
                        vec![cortexpb::Sample {
                            timestamp_ms: end,
                            value: (id + SAMPLES_PER_SERIES) as f64,
                        }]
                    },
                    histograms: if is_histogram(id) {
                        vec![cortexpb::Histogram {
                            timestamp: end,
                            count: Some(cortexpb::histogram::Count::CountInt(50)),
                            positive_spans: vec![cortexpb::BucketSpan {
                                offset: 0,
                                length: 50,
                            }],
                            positive_deltas: vec![1; 50],
                            ..Default::default()
                        }]
                    } else {
                        Vec::new()
                    },
                    exemplars: Vec::new(),
                    created_timestamp: 0,
                })
                .collect();
            store
                .ingest_recovered(
                    &format!("tenant-{tenant}"),
                    DecodedRequest {
                        source: 0,
                        series,
                        metadata: Vec::new(),
                    },
                    end,
                )
                .unwrap();
        }
    }
    let update_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    for tenant in 0..TENANTS {
        let selected = store
            .select_chunks(&format!("tenant-{tenant}"), end - RETENTION_MS, end, &[])
            .unwrap();
        assert_eq!(selected.len(), SERIES_PER_TENANT);
        std::hint::black_box(selected);
    }
    let query_after_update_seconds = started.elapsed().as_secs_f64();
    stopped.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let after = rss_bytes();
    println!(
        "series={} samples={} baseline_rss={} rest_rss={} query_peak_rss={} after_query_rss={} ingest_wall_s={:.3} query_wall_s={:.3} update_wall_s={:.3} query_after_update_wall_s={:.3}",
        TENANTS * SERIES_PER_TENANT,
        TENANTS * SERIES_PER_TENANT * SAMPLES_PER_SERIES,
        baseline,
        rest,
        peak.load(Ordering::Relaxed).max(after),
        after,
        ingest_seconds,
        query_seconds,
        update_seconds,
        query_after_update_seconds,
    );
}
