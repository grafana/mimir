use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::segment::SegmentLog;
use mimir_rust_kafka_ingester::store::Store;

const BASE_RECORDS: usize = 9_739;
const SERIES_PER_RECORD: usize = 20;

fn records() -> usize {
    let scale = std::env::var("MIMIR_RECOVERY_SCALE")
        .ok()
        .map(|value| value.parse::<usize>().expect("integer recovery scale"))
        .unwrap_or(1);
    assert!((1..=8).contains(&scale));
    BASE_RECORDS * scale
}

fn unique_records() -> usize {
    records() * 9 / 10
}

fn process_metrics() -> (u64, f64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    let rss = usage.ru_maxrss as u64 * if cfg!(target_os = "linux") { 1024 } else { 1 };
    let cpu = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.;
    (rss, cpu)
}

fn fixture_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("mimir-recovery-bench-{}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn request(frame: usize) -> DecodedRequest {
    let source_frame = frame % unique_records();
    let histogram_heavy = std::env::var_os("MIMIR_RECOVERY_HISTOGRAM_FIXTURE").is_some();
    let series = (0..SERIES_PER_RECORD)
        .map(|index| {
            let series_id = source_frame * SERIES_PER_RECORD + index;
            let histogram_series = histogram_heavy && series_id % 10 == 0;
            let mut labels = vec![("__name__".into(), format!("metric_{series_id}"))];
            for label in 0..19 {
                labels.push((
                    format!("label_{label}"),
                    format!("value_{}_{}", series_id % 1_000, label),
                ));
            }
            DecodedSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect(),
                samples: if histogram_series {
                    Vec::new()
                } else {
                    (0..20)
                        .map(|sample| cortexpb::Sample {
                            timestamp_ms: frame as i64 * 1_000 + sample,
                            value: (series_id + sample as usize) as f64,
                        })
                        .collect()
                },
                histograms: if histogram_series {
                    (0..20)
                        .map(|sample| cortexpb::Histogram {
                            timestamp: frame as i64 * 1_000 + sample,
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
            }
        })
        .collect();
    DecodedRequest {
        source: 0,
        series,
        metadata: Vec::new(),
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "measure") {
        measure(PathBuf::from(&args[2]), &args[3]);
        return;
    }
    let directory = fixture_dir();
    let record_count = records();
    {
        let (mut log, _) = SegmentLog::open(&directory, 0, "fixture", 0, None).unwrap();
        for frame in 0..record_count {
            log.append(
                frame as i64,
                1_000_000 + frame as i64,
                "benchmark",
                &request(frame),
            )
            .unwrap();
        }
        log.flush().unwrap();
    }
    let segment_bytes: u64 = fs::read_dir(&directory)
        .unwrap()
        .flat_map(|entry| fs::read_dir(entry.unwrap().path()).unwrap())
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    println!("variant=segment bytes={segment_bytes}");
    let executable = std::env::current_exe().unwrap();
    for name in ["segment"] {
        let status = Command::new(&executable)
            .args(["measure", directory.to_str().unwrap(), name])
            .status()
            .unwrap();
        assert!(status.success(), "measure {name} failed");
    }
    fs::remove_dir_all(directory).unwrap();
}

fn measure(directory: PathBuf, variant: &str) {
    let (before_rss, before_cpu) = process_metrics();
    let started = Instant::now();
    let store = Store::default();
    let mut count = 0;
    let log = SegmentLog::open_replaying(&directory, 0, "fixture", 0, None, 2, |record| {
        store.ingest_recovered(&record.tenant, record.request, record.ingested_ms)?;
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(log.last_offset(), Some(records() as i64 - 1));
    assert_eq!(count, records());
    let restore = started.elapsed();
    let (restore_rss, restore_cpu) = process_metrics();
    let warm = std::time::Duration::ZERO;
    let (warm_rss, warm_cpu) = (restore_rss, restore_cpu);
    let warmed = unique_records() * SERIES_PER_RECORD;
    let started = Instant::now();
    let selected = store.select_chunks("benchmark", 0, i64::MAX, &[]).unwrap();
    assert_eq!(selected.len(), warmed);
    let mut digest = crc32fast::Hasher::new();
    for series in &selected {
        digest.update(&series.encoded_labels);
        for chunk in series.chunks[series.chunk_start..series.chunk_end].iter() {
            digest.update(&chunk.wire);
        }
    }
    let query = started.elapsed();
    let (query_rss, query_cpu) = process_metrics();
    println!(
        "variant={variant} series={warmed} digest={:08x} restore_ms={:.2} warm_ms={:.2} ready_ms={:.2} query_ms={:.2} restore_cpu_ms={:.2} warm_cpu_ms={:.2} query_cpu_ms={:.2} restore_rss_delta_B={} warm_rss_delta_B={} peak_rss_B={}",
        digest.finalize(),
        restore.as_secs_f64() * 1000.,
        warm.as_secs_f64() * 1000.,
        (restore + warm).as_secs_f64() * 1000.,
        query.as_secs_f64() * 1000.,
        (restore_cpu - before_cpu) * 1000.,
        (warm_cpu - restore_cpu) * 1000.,
        (query_cpu - warm_cpu) * 1000.,
        restore_rss.saturating_sub(before_rss),
        warm_rss.saturating_sub(restore_rss),
        query_rss.max(warm_rss),
    );
}
