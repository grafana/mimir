//! CPU per sample on the steady-state Kafka path: small records for series the store already
//! has, decoded, logged to the segment log and applied in small batches, like a caught-up pod.
//!
//! `cargo bench --bench steady_ingest` (`STEADY_SERIES`, `STEADY_ROUNDS` to resize).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::decode_record_with_label_spans;
use mimir_rust_kafka_ingester::segment::{self, SegmentLog};
use mimir_rust_kafka_ingester::store::{self, IngestRecord, Store};
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
    keys: Duration,
    encode: Duration,
    apply: Duration,
    write: Duration,
}

// Like the Kafka consumer: records are decoded and their series keys hashed first, then encoded
// for the segment log in order, applied, and written.
fn ingest(
    store: &Store,
    log: &mut SegmentLog,
    records: &[Bytes],
    batch: usize,
    offset: &mut i64,
    phases: &mut Phases,
) {
    for group in records.chunks(batch) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        log.begin_batch(now).unwrap();
        let mut frames = Vec::with_capacity(group.len());
        let prepared = group
            .iter()
            .map(|bytes| {
                *offset += 1;
                let started = Instant::now();
                // Like a fetched Kafka record, whose payload the decoded labels share.
                let (mut request, spans) =
                    decode_record_with_label_spans(1, bytes.clone()).unwrap();
                let series_hashes = Some(store::series_hashes(&mut request));
                let decoded = Instant::now();
                let keys = segment::series_keys_with_label_bytes("tenant", &request, bytes, &spans);
                let hashed = Instant::now();
                frames.push(
                    log.encode(*offset, 0, now, "tenant", &request, &keys)
                        .unwrap(),
                );
                phases.decode += decoded - started;
                phases.keys += hashed - decoded;
                phases.encode += hashed.elapsed();
                IngestRecord {
                    tenant: "tenant".into(),
                    request,
                    ingested_ms: now,
                    track_rate: true,
                    bytes: bytes.len(),
                    series_hashes,
                }
            })
            .collect();
        let started = Instant::now();
        store.ingest_flushes(prepared).unwrap();
        let applied = Instant::now();
        for frame in frames {
            log.append_compressed(frame).unwrap();
        }
        phases.apply += applied - started;
        phases.write += applied.elapsed();
    }
}

fn directory_bytes(directory: &std::path::Path) -> u64 {
    std::fs::read_dir(directory)
        .unwrap()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
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
        let (mut log, _) =
            SegmentLog::open(&directory.join("segments"), 0, "bench", 0, None).unwrap();
        let mut offset = 0;
        // Creating the series, like a replay: every sample is a new series, in full batches.
        let first = records(0..series, start);
        let mut creation = Phases::default();
        let (cpu_start, wall_start) = (cpu_seconds(), Instant::now());
        ingest(&store, &mut log, &first, 64, &mut offset, &mut creation);
        let cpu_end = cpu_seconds();
        let per_series = |duration: Duration| duration.as_secs_f64() * 1e9 / series as f64;
        println!(
            "create (64-record batches): {:.0} ns CPU/series, {:.2} cores; wall: decode {:.0}, keys {:.0}, encode {:.0}, apply {:.0}, write {:.0} ns/series",
            ((cpu_end.0 - cpu_start.0) + (cpu_end.1 - cpu_start.1)) * 1e9 / series as f64,
            ((cpu_end.0 - cpu_start.0) + (cpu_end.1 - cpu_start.1))
                / wall_start.elapsed().as_secs_f64(),
            per_series(creation.decode),
            per_series(creation.keys),
            per_series(creation.encode),
            per_series(creation.apply),
            per_series(creation.write),
        );
        let mut phases = Phases::default();
        log.flush().unwrap();
        let initial_bytes = directory_bytes(&directory.join("segments"));
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
                ingest(&store, &mut log, &records, batch, &mut offset, &mut phases);
                let cpu_end = cpu_seconds();
                user += cpu_end.0 - cpu_start.0;
                system += cpu_end.1 - cpu_start.1;
                wall += wall_start.elapsed().as_secs_f64();
            }
        }
        let samples = (series * rounds) as f64;
        drop(log);
        // What the measured scrapes added, past the first one's series definitions.
        let segment_bytes = directory_bytes(&directory.join("segments")) - initial_bytes;
        let per_sample = |duration: Duration| duration.as_secs_f64() * 1e9 / samples;
        println!(
            "batch={batch}: {:.0} ns CPU/sample ({:.0} user, {:.0} system), {:.2} cores; wall: decode {:.0}, keys {:.0}, encode {:.0}, apply {:.0}, write {:.0} ns/sample; segment log {:.1} bytes/sample",
            (user + system) * 1e9 / samples,
            user * 1e9 / samples,
            system * 1e9 / samples,
            (user + system) / wall,
            per_sample(phases.decode),
            per_sample(phases.keys),
            per_sample(phases.encode),
            per_sample(phases.apply),
            per_sample(phases.write),
            segment_bytes as f64 / samples,
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&directory);
    }
}
