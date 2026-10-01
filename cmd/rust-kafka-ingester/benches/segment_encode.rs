//! CPU and bytes per sample of logging real records: replays a segment file a pod wrote, then
//! encodes and writes its records again through `SegmentLog`, like the Kafka path does.
//! `SEGMENT_FILE=<path> cargo bench --bench segment_encode` (the file keeps its name, which holds its hour; `SEGMENT_REPEAT=n` logs the records
//! n times into one file, so later passes see series the file already defined).

use std::time::Duration;

use mimir_rust_kafka_ingester::segment::{SegmentLog, series_keys};

/// User and system CPU time of the process.
fn cpu_time() -> Duration {
    // SAFETY: getrusage only writes the struct.
    let usage = unsafe {
        let mut usage = std::mem::zeroed::<libc::rusage>();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let time = |value: libc::timeval| {
        Duration::from_secs(value.tv_sec as u64) + Duration::from_micros(value.tv_usec as u64)
    };
    time(usage.ru_utime) + time(usage.ru_stime)
}

fn main() {
    let path = std::env::var("SEGMENT_FILE").expect("SEGMENT_FILE");
    let repeat =
        std::env::var("SEGMENT_REPEAT").map_or(1, |repeat| repeat.parse().expect("SEGMENT_REPEAT"));
    let scratch = std::env::temp_dir().join(format!("segment-encode-{}", std::process::id()));
    let topic = "ingest";
    let directory = scratch.join("source").join(format!(
        "cluster-0-partition-5-topic-{}",
        topic
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let name = std::path::Path::new(&path).file_name().unwrap();
    // As an open file, a copy cut at any length replays up to its last whole frame.
    std::fs::copy(&path, directory.join(name).with_extension("open")).unwrap();
    let (_, records) = SegmentLog::open(&scratch.join("source"), 0, topic, 5, None).unwrap();
    let samples: usize = records
        .iter()
        .flat_map(|record| &record.request.series)
        .map(|series| series.samples.len() + series.histograms.len())
        .sum();
    let series: usize = records
        .iter()
        .map(|record| record.request.series.len())
        .sum();
    println!(
        "records={} series={series} samples={samples}",
        records.len()
    );
    {
        use prost::Message;
        let metadata: usize = records
            .iter()
            .flat_map(|record| &record.request.metadata)
            .map(|m| m.encoded_len() + 4)
            .sum();
        let with_metadata = records
            .iter()
            .filter(|record| !record.request.metadata.is_empty())
            .count();
        let histograms: usize = records
            .iter()
            .flat_map(|r| &r.request.series)
            .map(|s| s.histograms.len())
            .sum();
        let exemplars: usize = records
            .iter()
            .flat_map(|r| &r.request.series)
            .map(|s| s.exemplars.len())
            .sum();
        let created: usize = records
            .iter()
            .flat_map(|r| &r.request.series)
            .filter(|s| s.created_timestamp != 0)
            .count();
        println!(
            "metadata bytes/sample {:.1} records with metadata {with_metadata} histograms {histograms} exemplars {exemplars} created {created}",
            metadata as f64 / samples as f64
        );
    }
    let (mut log, _) = SegmentLog::open(&scratch.join("target"), 0, topic, 0, None).unwrap();
    let hour = 3_600_000;
    let mut offset = 0;
    for pass in 0..repeat {
        let (mut keys_cpu, mut encode_cpu, mut write_cpu) =
            (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        // One file: passes after the first see series it already defined, like most of an hour.
        let ingested = hour + pass as i64;
        log.begin_batch(ingested).unwrap();
        let written_before = directory_size(&scratch.join("target"));
        for record in &records {
            let started = cpu_time();
            let keys = series_keys(&record.tenant, &record.request);
            let keyed = cpu_time();
            let frame = log
                .encode(
                    offset,
                    record.kafka_timestamp_ms,
                    ingested,
                    &record.tenant,
                    &record.request,
                    &keys,
                )
                .unwrap();
            let encoded = cpu_time();
            log.append_compressed(frame).unwrap();
            write_cpu += cpu_time() - encoded;
            encode_cpu += encoded - keyed;
            keys_cpu += keyed - started;
            offset += 1;
        }
        log.flush().unwrap();
        let written = directory_size(&scratch.join("target")) - written_before;
        let per = |cpu: Duration| cpu.as_nanos() as f64 / samples as f64;
        println!(
            "pass={pass}: keys {:.0} encode {:.0} write {:.0} ns CPU/sample; {:.2} bytes/sample",
            per(keys_cpu),
            per(encode_cpu),
            per(write_cpu),
            written as f64 / samples as f64
        );
    }
    let _ = std::fs::remove_dir_all(&scratch);
}

fn directory_size(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                directory_size(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}
