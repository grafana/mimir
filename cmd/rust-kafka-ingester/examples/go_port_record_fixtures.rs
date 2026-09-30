// Writes golden data the Go port checks itself against: decoded records, and a v4 segment file.
// Run: cargo run --release --example go_port_record_fixtures -- records <path> | segment <root> | replay <root> <topic>
use std::fmt::Write as _;

use bytes::Bytes;
use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::{
    DecodedRequest, DecodedSeries, decode_record_with_label_spans,
};
use mimir_rust_kafka_ingester::segment::SegmentLog;
use prost::Message;
use prost::encoding::WireType;

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
    fn bytes(&mut self) -> Vec<u8> {
        let alphabet: [&[u8]; 6] = [b"a", b"_", b"9", "\u{e9}".as_bytes(), b"\xff", b""];
        (0..self.below(6))
            .flat_map(|_| alphabet[self.below(6) as usize].to_vec())
            .collect()
    }
}

fn put_field(record: &mut Vec<u8>, tag: u32, field: &[u8]) {
    prost::encoding::encode_key(tag, WireType::LengthDelimited, record);
    prost::encoding::encode_varint(field.len() as u64, record);
    record.extend_from_slice(field);
}

fn random_record(random: &mut Random) -> Vec<u8> {
    let mut record = Vec::new();
    for _ in 0..random.below(4) {
        let series = cortexpb::TimeSeries {
            labels: (0..random.below(5))
                .map(|_| cortexpb::LabelPair {
                    name: random.bytes().into(),
                    value: random.bytes().into(),
                })
                .collect(),
            samples: (0..random.below(3))
                .map(|_| cortexpb::Sample {
                    value: random.below(1000) as f64 - 500.5,
                    timestamp_ms: random.next() as i64,
                })
                .collect(),
            exemplars: (0..random.below(2))
                .map(|_| cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: random.bytes().into(),
                        value: random.bytes().into(),
                    }],
                    value: 1.5,
                    timestamp_ms: random.below(100) as i64,
                })
                .collect(),
            histograms: (0..random.below(2))
                .map(|_| cortexpb::Histogram {
                    sum: 2.0,
                    schema: 3,
                    positive_deltas: vec![1, -1, random.below(9) as i64],
                    timestamp: random.below(100) as i64,
                    count: Some(cortexpb::histogram::Count::CountInt(random.below(9))),
                    ..Default::default()
                })
                .collect(),
            created_timestamp: random.below(3) as i64,
        };
        let mut encoded = series.encode_to_vec();
        if random.below(4) == 0 {
            prost::encoding::encode_key(7, WireType::Varint, &mut encoded);
            prost::encoding::encode_varint(random.next(), &mut encoded);
        }
        put_field(&mut record, 1, &encoded);
    }
    if random.below(2) == 0 {
        prost::encoding::encode_key(2, WireType::Varint, &mut record);
        prost::encoding::encode_varint(random.below(3), &mut record);
    }
    for _ in 0..random.below(3) {
        let metadata = cortexpb::MetricMetadata {
            r#type: random.below(6) as i32,
            metric_family_name: ["", "up", "rpc_count", "latency_bucket"][random.below(4) as usize]
                .into(),
            help: ["", "help"][random.below(2) as usize].into(),
            unit: String::new(),
        };
        put_field(&mut record, 3, &metadata.encode_to_vec());
    }
    if random.below(4) == 0 {
        put_field(&mut record, 99, b"unknown");
    }
    record
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

// The canonical form of a decoded record, which the Go test builds the same way.
fn canonical(request: &DecodedRequest, spans: &[Option<std::ops::Range<usize>>]) -> String {
    let mut out = format!("source={}", request.source);
    for (series, span) in request.series.iter().zip(spans) {
        out.push_str(" series[");
        for (name, value) in &series.labels {
            let _ = write!(out, "l={}:{},", hex(name.as_bytes()), hex(value.as_bytes()));
        }
        for sample in &series.samples {
            let _ = write!(
                out,
                "s={}:{:016x},",
                sample.timestamp_ms,
                sample.value.to_bits()
            );
        }
        for histogram in &series.histograms {
            let _ = write!(out, "h={},", hex(&histogram.encode_to_vec()));
        }
        for exemplar in &series.exemplars {
            let _ = write!(out, "e={},", hex(&exemplar.encode_to_vec()));
        }
        let _ = write!(out, "c={},", series.created_timestamp);
        match span {
            Some(span) => {
                let _ = write!(out, "span={}-{}", span.start, span.end);
            }
            None => out.push_str("span=none"),
        }
        out.push(']');
    }
    for metadata in &request.metadata {
        let _ = write!(out, " m={}", hex(&metadata.encode_to_vec()));
    }
    out
}

fn records(path: &str) {
    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    let mut out = String::new();
    for _ in 0..200 {
        let record = random_record(&mut random);
        let mut candidates = vec![record.clone()];
        for _ in 0..3 {
            if !record.is_empty() {
                candidates.push(record[..random.below(record.len() as u64) as usize].to_vec());
                let mut corrupted = record.clone();
                let position = random.below(record.len() as u64) as usize;
                corrupted[position] ^= 1 << random.below(8);
                candidates.push(corrupted);
            }
        }
        for candidate in candidates {
            let decoded = decode_record_with_label_spans(1, Bytes::from(candidate.clone()));
            let result = match decoded {
                Ok((request, spans)) => canonical(&request, &spans),
                Err(_) => "error".into(),
            };
            let _ = writeln!(out, "{} {}", hex(&candidate), result);
        }
    }
    std::fs::write(path, out).unwrap();
}

// Small records, so the file trains its dictionary on 2000 frames without growing large.
fn tiny_request(offset: i64) -> DecodedRequest {
    DecodedRequest {
        source: (offset % 3) as i32,
        series: vec![DecodedSeries {
            labels: [
                ("__name__", format!("metric_{}", offset % 7)),
                ("job", format!("job-{}", offset % 13)),
                ("pod", format!("pod-{}", offset % 50)),
            ]
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect(),
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1_800_000_000_000 + offset * 15_000,
                value: offset as f64 / 7.0,
            }],
            histograms: if offset % 100 == 0 {
                vec![cortexpb::Histogram {
                    count: Some(cortexpb::histogram::Count::CountInt(3)),
                    sum: 4.5,
                    timestamp: offset,
                    ..Default::default()
                }]
            } else {
                Vec::new()
            },
            exemplars: Vec::new(),
            created_timestamp: 0,
        }],
        metadata: if offset % 500 == 0 {
            vec![cortexpb::MetricMetadata {
                r#type: 1,
                metric_family_name: "metric_0".into(),
                help: "help".into(),
                unit: String::new(),
            }]
        } else {
            Vec::new()
        },
    }
}

fn segment(root: &str) {
    let _ = std::fs::remove_dir_all(root);
    let (mut log, _) =
        SegmentLog::open(std::path::Path::new(root), 0, "topic", 0, None).unwrap();
    for offset in 1..=2000 {
        log.append(offset, offset, "tenant", &tiny_request(offset))
            .unwrap();
    }
    // Training runs on its own thread; frames after it finished use the dictionary.
    std::thread::sleep(std::time::Duration::from_secs(3));
    for offset in 2001..=2050 {
        log.append(offset, offset, "tenant", &tiny_request(offset))
            .unwrap();
    }
    log.flush().unwrap();
}

// Replays a segment directory another implementation wrote, printing what it holds.
fn replay(root: &str, topic: &str) {
    let (_, records) =
        SegmentLog::open(std::path::Path::new(root), 0, topic, 0, None).unwrap();
    for record in records {
        let spans = vec![None; record.request.series.len()];
        println!(
            "{} {} {} {}",
            record.offset,
            record.kafka_timestamp_ms,
            record.tenant,
            canonical(&record.request, &spans)
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "records" => records(&args[2]),
        "segment" => segment(&args[2]),
        "replay" => replay(&args[2], &args[3]),
        _ => panic!("records <path> | segment <root> | replay <root> <topic>"),
    }
}
