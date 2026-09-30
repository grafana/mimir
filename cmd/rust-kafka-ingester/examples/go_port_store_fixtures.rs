// Writes golden query results and a head snapshot with cold blocks from the Rust store, which the
// Go port checks itself against byte for byte.
// Run: cargo run --release --example go_port_store_fixtures -- <output directory>
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use mimir_rust_kafka_ingester::proto::{cortex, cortexpb};
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::store::{IngestRecord, SnapshotOffset, Store};

const HOUR: i64 = 3_600_000;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").unwrap();
    }
    out
}

fn series(name: &str, samples: impl IntoIterator<Item = (i64, f64)>) -> DecodedSeries {
    DecodedSeries {
        labels: vec![("__name__".into(), name.into())],
        samples: samples
            .into_iter()
            .map(|(timestamp_ms, value)| cortexpb::Sample {
                timestamp_ms,
                value,
            })
            .collect(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        created_timestamp: 0,
    }
}

fn request(series: Vec<DecodedSeries>) -> DecodedRequest {
    DecodedRequest {
        source: 0,
        series,
        metadata: Vec::new(),
    }
}

fn matcher(r#type: i32, name: &str, value: &str) -> cortex::LabelMatcher {
    cortex::LabelMatcher {
        r#type,
        name: name.into(),
        value: value.into(),
    }
}

fn dump(out: &mut String, store: &Store, tenant: &str, case: &str, from: i64, to: i64, matchers: &[cortex::LabelMatcher]) {
    let selected = store.select_chunks(tenant, from, to, matchers).unwrap();
    writeln!(out, "case {case} {}", selected.len()).unwrap();
    for view in &selected {
        let wires = view.chunks[view.chunk_start..view.chunk_end]
            .iter()
            .map(|chunk| hex(&chunk.wire))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(out, "{} {wires}", hex(&view.encoded_labels)).unwrap();
    }
}

fn presence_store(directory: &Path) -> Store {
    let store = Store::new(20 * 60 * 1000, None, Some(directory.to_path_buf())).unwrap();
    let start = 10 * HOUR;
    for (name, minutes) in [("old", 0..100), ("hot", 150..300)] {
        let unrelated = if name == "old" { 300 } else { 0 };
        let mut all = vec![series("keep", minutes.clone().map(|m| (start + m * 60_000, 1.0)))];
        for n in 0..120_i64 {
            let mut s = series(name, minutes.clone().map(|m| (start + m * 60_000, n as f64)));
            s.labels.push(("job".into(), format!("job-{}", n % 5).into()));
            s.labels.push(("n".into(), n.to_string().into()));
            if n % 12 == 0 {
                s.labels.push(("agg".into(), format!("sum-{}", n % 24).into()));
            }
            s.labels.sort();
            all.push(s);
        }
        for n in 0..unrelated {
            let mut s = series("gone", minutes.clone().map(|m| (start + m * 60_000, 1.0)));
            s.labels.push(("n".into(), n.to_string().into()));
            all.push(s);
        }
        store.ingest("tenant", request(all)).unwrap();
    }
    store.head_tick(true, false);
    store.head_tick(false, false);
    store
}

fn presence_shapes() -> Vec<Vec<cortex::LabelMatcher>> {
    vec![
        vec![matcher(2, "agg", "sum-.*")],
        vec![matcher(2, "agg", ".+")],
        vec![matcher(2, "job", "job-[12]")],
        vec![matcher(0, "__name__", "hot"), matcher(3, "agg", ".+")],
        vec![matcher(0, "__name__", "old"), matcher(1, "agg", "sum-0")],
        vec![matcher(2, "__name__", "hot|old"), matcher(3, "agg", ".+")],
        vec![matcher(2, "__name__", "hot|old"), matcher(0, "agg", "")],
        vec![matcher(0, "__name__", "hot"), matcher(2, "agg", ".*")],
        vec![matcher(0, "__name__", "hot"), matcher(3, "agg", "")],
        vec![matcher(3, "job", "job-1"), matcher(2, "agg", ".+")],
        vec![matcher(2, "n", "1.*"), matcher(3, "agg", "sum-12")],
        vec![matcher(2, "missing", ".+")],
        vec![matcher(3, "missing", ".+"), matcher(0, "__name__", "old")],
        vec![matcher(0, "job", "job-2"), matcher(3, "agg", ".+")],
        vec![matcher(2, "__name__", ".+"), matcher(3, "n", "[0-5]?[0-9]")],
        vec![matcher(1, "agg", ""), matcher(0, "__name__", "hot")],
        vec![matcher(2, "job", "job-1|job-3")],
        vec![matcher(2, "__name__", "hot|gone"), matcher(2, "job", "job-(0|4)")],
        vec![matcher(2, "agg", "sum-0|sum-12|missing")],
        vec![matcher(0, "__name__", "old"), matcher(2, "n", "1|2|3|200")],
        vec![matcher(3, "job", "job-1|job-2"), matcher(2, "__name__", "old|keep")],
        vec![matcher(2, "job", "|job-1")],
        vec![matcher(2, "n", "(?i)1[0-2]")],
        vec![matcher(2, "__name__", "(?:ho|ol)[td]"), matcher(2, "job", "job-[0-2]")],
        vec![matcher(2, "agg", "nothing|none")],
        vec![matcher(3, "__name__", "hot|keep"), matcher(2, "n", "4[0-9]")],
    ]
}

fn presence(out: &mut String, store: &Store) {
    let start = 10 * HOUR;
    for (index, base) in presence_shapes().iter().enumerate() {
        for shard in [None, Some("1_of_3"), Some("3_of_3")] {
            let mut matchers = base.clone();
            if let Some(shard) = shard {
                matchers.push(matcher(0, "__query_shard__", shard));
            }
            for (range, (from, to)) in [(i64::MIN, i64::MAX), (start + 20 * 60_000, start + 40 * 60_000)]
                .into_iter()
                .enumerate()
            {
                let case = format!("presence/{index}/{}/{range}", shard.unwrap_or("none"));
                dump(out, store, "tenant", &case, from, to, &matchers);
            }
        }
    }
}

fn cold_shards(out: &mut String, directory: &Path) {
    let store = Store::new(20 * 60 * 1000, None, Some(directory.to_path_buf())).unwrap();
    let start = 10 * HOUR;
    let mut all = vec![series("long", (0..=300).map(|minute| (start + minute * 60_000, minute as f64)))];
    for (name, count, quarters) in [("old", 60, 0..400), ("middle", 20, 600..640)] {
        for n in 0..count {
            let mut s = series(name, quarters.clone().map(|quarter| (start + quarter * 15_000, f64::from(n))));
            s.labels.push(("n".into(), n.to_string().into()));
            all.push(s);
        }
    }
    store.ingest("tenant", request(all)).unwrap();
    let shard = |index: u64, count: u64| matcher(0, "__query_shard__", &format!("{index}_of_{count}"));
    let mut queries = vec![vec![matcher(0, "__name__", "old")], vec![]];
    for index in 1..=4 {
        queries.push(vec![matcher(0, "__name__", "old"), shard(index, 4)]);
        queries.push(vec![matcher(2, "n", "1.*"), shard(index, 4)]);
        queries.push(vec![matcher(0, "__name__", "middle"), matcher(0, "n", "3"), shard(index, 4)]);
    }
    for index in 1..=3 {
        queries.push(vec![shard(index, 3)]);
    }
    store.head_tick(true, false);
    for (query, matchers) in queries.iter().enumerate() {
        for (range, (from, to)) in [
            (i64::MIN, i64::MAX),
            (start + 30 * 60_000, start + 40 * 60_000),
            (start + 105 * 60_000, start + 115 * 60_000),
            (start + 2 * HOUR, start + 5 * HOUR),
        ]
        .into_iter()
        .enumerate()
        {
            dump(out, &store, "tenant", &format!("coldshards/{query}/{range}"), from, to, matchers);
        }
    }
}

fn histogram_at(timestamp: i64, buckets: u32) -> cortexpb::Histogram {
    cortexpb::Histogram {
        timestamp,
        count: Some(cortexpb::histogram::Count::CountInt(u64::from(buckets))),
        positive_spans: vec![cortexpb::BucketSpan {
            offset: 0,
            length: buckets,
        }],
        positive_deltas: (0..buckets).map(|bucket| i64::from(bucket == 0)).collect(),
        ..Default::default()
    }
}

fn mixed_records(records: i64) -> Vec<IngestRecord> {
    (0..records)
        .map(|record| IngestRecord {
            tenant: format!("tenant-{}", record % 3),
            request: request(
                (0..50)
                    .map(|series| DecodedSeries {
                        labels: vec![
                            ("__name__".into(), format!("metric_{}", series % 5).into()),
                            ("id".into(), series.to_string().into()),
                        ],
                        samples: vec![cortexpb::Sample {
                            timestamp_ms: if record % 7 == 6 { record - 5 } else { record } * 1000,
                            value: (record * series) as f64,
                        }],
                        histograms: if series % 10 == 0 {
                            vec![histogram_at(record * 1000 + 1, 3)]
                        } else {
                            Vec::new()
                        },
                        exemplars: Vec::new(),
                        created_timestamp: 0,
                    })
                    .collect(),
            ),
            ingested_ms: 0,
            track_rate: false,
            bytes: 0,
            series_hashes: None,
        })
        .collect()
}

fn mixed(out: &mut String) {
    let single = Store::with_shards(20 * 60 * 1000, None, None, 1, 1).unwrap();
    let mut records = mixed_records(400).into_iter();
    loop {
        let batch = records.by_ref().take(37).collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        single.ingest_batch(batch).unwrap();
    }
    for tenant in ["tenant-0", "tenant-1", "tenant-2"] {
        dump(out, &single, tenant, &format!("mixed/{tenant}"), i64::MIN, i64::MAX, &[]);
    }
}

// Copies a store directory, keeping only the written part of the preallocated chunk files.
fn copy_trimmed(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_trimmed(&entry.path(), &target);
            continue;
        }
        let mut bytes = fs::read(entry.path()).unwrap();
        if entry.path().parent().unwrap().file_name().unwrap().to_str().unwrap().starts_with("shard-")
            && !entry.path().to_str().unwrap().contains("/cold/")
        {
            while bytes.last() == Some(&0) {
                bytes.pop();
            }
        }
        fs::write(&target, bytes).unwrap();
    }
}

fn main() {
    let output = PathBuf::from(std::env::args().nth(1).expect("output directory"));
    fs::create_dir_all(&output).unwrap();
    let scratch = std::env::temp_dir().join(format!("storegolden-{}", std::process::id()));
    let mut out = String::new();
    let presence_dir = scratch.join("presence");
    let store = presence_store(&presence_dir);
    presence(&mut out, &store);
    store
        .write_snapshot(&[SnapshotOffset {
            offset: Some(7),
            timestamp_ms: 3,
        }])
        .unwrap();
    drop(store);
    let snapshot_dir = output.join("rust-presence-snapshot");
    let _ = fs::remove_dir_all(&snapshot_dir);
    copy_trimmed(&presence_dir, &snapshot_dir);
    cold_shards(&mut out, &scratch.join("coldshards"));
    mixed(&mut out);
    fs::write(output.join("rust-query-results.txt"), out).unwrap();
    fs::remove_dir_all(scratch).unwrap();
}
