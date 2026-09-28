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
    let chunk_dir = std::env::temp_dir().join(format!("mimir-rust-memory-{}", std::process::id()));
    let store = Store::new(20 * 60 * 1000, Some(RETENTION_MS), Some(chunk_dir.clone())).unwrap();
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
    let started = Instant::now();
    store.write_snapshot(&[]).unwrap();
    let snapshot_write_seconds = started.elapsed().as_secs_f64();
    let snapshot_bytes = std::fs::metadata(chunk_dir.join("snapshot")).unwrap().len();
    drop(store);
    let started = Instant::now();
    let restored = Store::restore(20 * 60 * 1000, Some(RETENTION_MS), &chunk_dir, 2)
        .unwrap()
        .expect("snapshot restored");
    let snapshot_restore_seconds = started.elapsed().as_secs_f64();
    assert_eq!(
        restored
            .store
            .select_chunks("tenant-0", end - RETENTION_MS, end, &[])
            .unwrap()
            .len(),
        SERIES_PER_TENANT
    );
    println!(
        "snapshot_bytes={snapshot_bytes} snapshot_write_wall_s={snapshot_write_seconds:.3} snapshot_restore_wall_s={snapshot_restore_seconds:.3}"
    );
    drop(restored);
    std::fs::remove_dir_all(chunk_dir).unwrap();
}

// Run explicitly: compares ingest throughput for several store thread counts.
#[test]
#[ignore]
fn parallel_ingest_throughput() {
    let records = |start: usize, count: usize| {
        (start..start + count)
            .map(|record| IngestRecord {
                tenant: "tenant".into(),
                request: DecodedRequest {
                    source: 0,
                    series: (0..350)
                        .map(|series| DecodedSeries {
                            labels: (0..12)
                                .map(|label| {
                                    (
                                        format!("label_{label}"),
                                        format!("value_{}_{series}", label % 4),
                                    )
                                })
                                .chain([("__name__".into(), format!("metric_{}", series % 40))])
                                .collect(),
                            samples: vec![cortexpb::Sample {
                                timestamp_ms: record as i64 * 1000,
                                value: record as f64,
                            }],
                            histograms: Vec::new(),
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
            .collect::<Vec<_>>()
    };
    for threads in [1, 2, 4, 8] {
        let store = Store::with_shards(20 * 60 * 1000, None, None, 16, threads).unwrap();
        let batches = (0..100)
            .map(|batch| records(batch * 64, 64))
            .collect::<Vec<_>>();
        let started = Instant::now();
        for batch in batches {
            store.ingest_batch(batch).unwrap();
        }
        let elapsed = started.elapsed().as_secs_f64();
        println!(
            "threads={threads} records_per_second={:.0} series_samples_per_second={:.0}",
            6400.0 / elapsed,
            6400.0 * 350.0 / elapsed
        );
    }
}

// Like a partition of mimir-dev-15: series with 19 labels, 15s samples over 13h of retention, of
// which only the newest part is in the emulated Go head. Run with --release; the live heap per
// series is what the ingester pays for every stored series.
#[test]
#[ignore]
fn retained_series_heap_bytes() {
    const SERIES: usize = 5_000;
    const RETENTION_MS: i64 = 13 * 60 * 60 * 1000;
    const INTERVAL_MS: i64 = 15_000;
    let directory =
        std::env::temp_dir().join(format!("mimir-rust-retained-{}", std::process::id()));
    let store = Store::new(20 * 60 * 1000, Some(RETENTION_MS), Some(directory.clone())).unwrap();
    let end = now_ms();
    let before = crate::test_allocator::live_bytes();
    let labels = |id: usize| -> Vec<(String, String)> {
        let mut labels = (0..19)
            .map(|label| {
                if label == 0 {
                    ("__name__".to_owned(), format!("metric_{:04}", id % 200))
                } else if label < 12 {
                    (
                        format!("label_{label:02}"),
                        format!("shared_{:04}", (id + label) % 50),
                    )
                } else {
                    (
                        format!("label_{label:02}"),
                        format!("unique_{label:02}_{id:08}"),
                    )
                }
            })
            .collect::<Vec<_>>();
        labels.sort();
        labels
    };
    // Written in time order across all series, like Kafka records.
    let mut timestamp = end - RETENTION_MS;
    while timestamp <= end {
        for batch in (0..SERIES).collect::<Vec<_>>().chunks(1_000) {
            let series = batch
                .iter()
                .map(|&id| DecodedSeries {
                    labels: labels(id),
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: timestamp,
                        value: (id as f64) + (timestamp / INTERVAL_MS) as f64,
                    }],
                    histograms: Vec::new(),
                    exemplars: Vec::new(),
                    created_timestamp: 0,
                })
                .collect();
            store
                .ingest_recovered(
                    "tenant",
                    DecodedRequest {
                        source: 0,
                        series,
                        metadata: Vec::new(),
                    },
                    end,
                )
                .unwrap();
        }
        timestamp += INTERVAL_MS;
    }
    store.head_tick(true, false);
    let ingested = crate::test_allocator::live_bytes() - before;
    store
        .write_snapshot(&[SnapshotOffset {
            offset: Some(1),
            timestamp_ms: 1,
        }])
        .unwrap();
    drop(store);
    let before = crate::test_allocator::live_bytes();
    let restored = Store::restore(20 * 60 * 1000, Some(RETENTION_MS), &directory, 4)
        .unwrap()
        .unwrap();
    let restored_bytes = crate::test_allocator::live_bytes() - before;
    // Where the restored bytes are, from the structures' capacities.
    let (mut table, mut labels, mut chunks, mut heads, mut postings) = (0, 0, 0, 0, 0);
    for shard in &restored.store.shards {
        let state = shard.read().unwrap();
        for tenant in state.tenants.values() {
            for group in &tenant.series.groups {
                table += group.capacity() * std::mem::size_of::<(SeriesKey, Box<Series>)>()
                    + group.len() * std::mem::size_of::<Series>();
            }
            for ((_, key), series) in tenant.series.iter() {
                labels += key.heap_size();
                chunks += series.chunks.0.len();
                heads += series
                    .float_head
                    .as_ref()
                    .map_or(0, |head| head.appender.byte_capacity());
            }
            postings += tenant.series.refs.capacity() * 12;
            for values in tenant.series.postings.values() {
                postings += values.capacity() * std::mem::size_of::<(u64, PostingList)>();
                for list in values.values() {
                    if let PostingList::Many(list) = list {
                        postings += 24 + list.capacity() * 4;
                    }
                }
            }
        }
    }
    println!(
        "per_series table={} labels={} chunks={} float_heads={} postings={}",
        table / SERIES,
        labels / SERIES,
        chunks / SERIES,
        heads / SERIES,
        postings / SERIES
    );
    println!(
        "retained_series_heap_bytes series={SERIES} ingested_per_series={} restored_per_series={}",
        ingested / SERIES,
        restored_bytes / SERIES
    );
    drop(restored);
    std::fs::remove_dir_all(directory).unwrap();
}
