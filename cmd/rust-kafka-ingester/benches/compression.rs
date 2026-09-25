use std::fs;
use std::hint::black_box;
use std::time::Instant;

use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::segment::SegmentLog;

const FRAMES: usize = 2_000;
const SERIES_PER_FRAME: usize = 20;

fn request(frame: usize, histogram_heavy: bool) -> DecodedRequest {
    let series = (0..SERIES_PER_FRAME)
        .map(|index| {
            let series_id = frame * SERIES_PER_FRAME + index;
            let labels = (0..19)
                .map(|label| {
                    (
                        format!("label_{label}"),
                        format!("value_{}_{}", series_id % 1_000, label),
                    )
                })
                .chain(std::iter::once((
                    "__name__".into(),
                    format!("metric_{series_id}"),
                )))
                .collect();
            let histograms = if histogram_heavy && series_id % 10 == 0 {
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
            };
            let samples = if histograms.is_empty() {
                (0..20)
                    .map(|sample| cortexpb::Sample {
                        timestamp_ms: frame as i64 * 1_000 + sample,
                        value: (series_id + sample as usize) as f64,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            DecodedSeries {
                labels,
                samples,
                histograms,
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

fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.
}

fn measure(
    name: &str,
    frames: &[Vec<u8>],
    compress: impl Fn(&[u8]) -> Vec<u8>,
    decompress: impl Fn(&[u8]) -> Vec<u8>,
) {
    let started = Instant::now();
    let cpu_before = cpu_seconds();
    let compressed = frames
        .iter()
        .map(|frame| compress(&frame[24..]))
        .collect::<Vec<_>>();
    let compress_cpu = (cpu_seconds() - cpu_before) * 1000.;
    let compress_ms = started.elapsed().as_secs_f64() * 1000.;
    for (raw, encoded) in frames.iter().zip(&compressed) {
        assert_eq!(decompress(encoded), raw[24..]);
    }
    let started = Instant::now();
    let cpu_before = cpu_seconds();
    for encoded in &compressed {
        black_box(decompress(encoded));
    }
    let decompress_cpu = (cpu_seconds() - cpu_before) * 1000.;
    let decompress_ms = started.elapsed().as_secs_f64() * 1000.;
    let raw_bytes: usize = frames.iter().map(|frame| frame.len() - 24).sum();
    let compressed_bytes: usize = compressed.iter().map(Vec::len).sum();
    println!(
        "codec={name} frames={} raw_B={raw_bytes} compressed_B={compressed_bytes} ratio={:.3} compress_ms={compress_ms:.2} compress_cpu_ms={compress_cpu:.2} decompress_ms={decompress_ms:.2} decompress_cpu_ms={decompress_cpu:.2}",
        frames.len(),
        compressed_bytes as f64 / raw_bytes as f64,
    );
}

fn main() {
    let histogram_heavy = std::env::var_os("MIMIR_RECOVERY_HISTOGRAM_FIXTURE").is_some();
    let directory =
        std::env::temp_dir().join(format!("mimir-compression-bench-{}", std::process::id()));
    fs::create_dir_all(&directory).unwrap();
    let (log, _) = SegmentLog::open(&directory, 0, "fixture", 0, None).unwrap();
    let frames = (0..FRAMES)
        .map(|frame| {
            log.prepare(
                frame as i64,
                frame as i64 * 1_000,
                "benchmark",
                &request(frame, histogram_heavy),
            )
            .unwrap()
            .uncompressed_body()
            .to_vec()
        })
        .collect::<Vec<_>>();
    measure(
        "zstd_1",
        &frames,
        |frame| zstd::bulk::compress(frame, 1).unwrap(),
        |frame| zstd::bulk::decompress(frame, 128 * 1024 * 1024).unwrap(),
    );
    measure("lz4", &frames, lz4_flex::compress_prepend_size, |frame| {
        lz4_flex::decompress_size_prepended(frame).unwrap()
    });
    measure(
        "snappy",
        &frames,
        |frame| snap::raw::Encoder::new().compress_vec(frame).unwrap(),
        |frame| snap::raw::Decoder::new().decompress_vec(frame).unwrap(),
    );
    drop(log);
    fs::remove_dir_all(directory).unwrap();
}
