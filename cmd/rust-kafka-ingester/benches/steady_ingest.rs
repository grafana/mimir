//! CPU per sample on the steady-state Kafka path: small records for series the store already
//! has, decoded, framed for the segment log and applied in small batches, like a caught-up pod.
//!
//! `cargo bench --bench steady_ingest` (`STEADY_SERIES`, `STEADY_ROUNDS` to resize).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::decode_record_bytes;
use mimir_rust_kafka_ingester::segment::SegmentLog;
use mimir_rust_kafka_ingester::store::{IngestRecord, Store};
use prost::Message;

// Like mimir-dev-15: 41 samples per record, 19 labels per series.
const SERIES_PER_RECORD: usize = 41;
const LABELS: usize = 19;
const SCRAPE_MS: i64 = 15_000;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// User and system CPU seconds of the process.
fn cpu_seconds() -> (f64, f64) {
    // SAFETY: getrusage only writes the struct.
    let usage = unsafe {
        let mut usage = std::mem::zeroed::<libc::rusage>();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    (seconds(usage.ru_utime), seconds(usage.ru_stime))
}

fn label_pair(name: String, value: String) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: name.into_bytes().into(),
        value: value.into_bytes().into(),
    }
}

fn series_labels(id: usize) -> Vec<cortexpb::LabelPair> {
    let mut labels = vec![label_pair(
        "__name__".into(),
        format!("metric_{}", id % 400),
    )];
    for label in 1..LABELS {
        let value = match label {
            1 => format!("pod-{}", id / 400 % 2_000),
            2 => format!("namespace-{}", id % 30),
            3 => "cluster-a".into(),
            _ => format!("value_{label}_{}", id % (label * 7 + 3)),
        };
        labels.push(label_pair(format!("label_{label:02}"), value));
    }
    labels.sort_by(|a, b| a.name.cmp(&b.name));
    labels
}

/// One Kafka record value per group of series, for the scrape at `timestamp_ms`.
fn records(series: std::ops::Range<usize>, timestamp_ms: i64) -> Vec<Bytes> {
    series
        .collect::<Vec<_>>()
        .chunks(SERIES_PER_RECORD)
        .map(|ids| {
            cortexpb::WriteRequest {
                timeseries: ids
                    .iter()
                    .map(|&id| cortexpb::TimeSeries {
                        labels: series_labels(id),
                        samples: vec![cortexpb::Sample {
                            timestamp_ms,
                            value: (timestamp_ms / SCRAPE_MS) as f64 * id as f64,
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        })
        .collect()
}

/// Wall time of each step; batches below the store's parallel threshold run on this thread, so
/// it is also their CPU time.
#[derive(Default)]
struct Phases {
    decode: Duration,
    frame: Duration,
    compress: Duration,
    apply: Duration,
}

fn ingest(store: &Store, records: &[Bytes], batch: usize, offset: &mut i64, phases: &mut Phases) {
    for group in records.chunks(batch) {
        let prepared = group
            .iter()
            .map(|bytes| {
                *offset += 1;
                let started = Instant::now();
                // Like a fetched Kafka record, whose payload the decoded labels share.
                let request = decode_record_bytes(1, bytes.clone()).unwrap();
                let decoded = Instant::now();
                let frame = SegmentLog::frame(*offset, 0, "tenant", &request).unwrap();
                let framed = Instant::now();
                std::hint::black_box(frame.compress().unwrap());
                phases.decode += decoded - started;
                phases.frame += framed - decoded;
                phases.compress += framed.elapsed();
                IngestRecord {
                    tenant: "tenant".into(),
                    request,
                    ingested_ms: 0,
                    track_rate: true,
                    bytes: bytes.len(),
                }
            })
            .collect();
        let started = Instant::now();
        store.ingest_flushes(prepared).unwrap();
        phases.apply += started.elapsed();
    }
}

fn main() {
    let series = env("STEADY_SERIES", 200_000);
    let rounds = env("STEADY_ROUNDS", 4);
    let threads = std::thread::available_parallelism().map_or(4, usize::from);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start = now - (rounds as i64 + 2) * SCRAPE_MS;
    let directory = std::env::temp_dir().join(format!("steady-ingest-{}", std::process::id()));
    for batch in [1, 8] {
        let store =
            Store::with_shards(20 * 60 * 1000, None, Some(directory.clone()), 16, threads).unwrap();
        let mut offset = 0;
        // Creating the series is not what is measured.
        ingest(
            &store,
            &records(0..series, start),
            64,
            &mut offset,
            &mut Phases::default(),
        );
        let mut phases = Phases::default();
        let (mut user, mut system, mut wall) = (0.0, 0.0, 0.0);
        let group = batch * SERIES_PER_RECORD;
        for round in 1..=rounds {
            for first in (0..series).step_by(group) {
                // Records are encoded just before they are ingested, as they arrive from Kafka:
                // records encoded long before are out of the caches, and whatever reads them
                // first looks slow.
                let records = records(
                    first..(first + group).min(series),
                    start + round as i64 * SCRAPE_MS,
                );
                let (cpu_start, wall_start) = (cpu_seconds(), Instant::now());
                ingest(&store, &records, batch, &mut offset, &mut phases);
                let cpu_end = cpu_seconds();
                user += cpu_end.0 - cpu_start.0;
                system += cpu_end.1 - cpu_start.1;
                wall += wall_start.elapsed().as_secs_f64();
            }
        }
        let samples = (series * rounds) as f64;
        let per_sample = |duration: Duration| duration.as_secs_f64() * 1e9 / samples;
        println!(
            "batch={batch}: {:.0} ns CPU/sample ({:.0} user, {:.0} system), {:.2} cores; wall: decode {:.0}, frame {:.0}, compress {:.0}, apply {:.0} ns/sample",
            (user + system) * 1e9 / samples,
            user * 1e9 / samples,
            system * 1e9 / samples,
            (user + system) / wall,
            per_sample(phases.decode),
            per_sample(phases.frame),
            per_sample(phases.compress),
            per_sample(phases.apply),
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&directory);
    }
}
