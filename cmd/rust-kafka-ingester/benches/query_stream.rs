use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use mimir_rust_kafka_ingester::proto::{cortex, cortexpb};
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::service::IngesterService;
use mimir_rust_kafka_ingester::store::Store;
use prost::Message;
use tonic::Request;

use cortex::ingester_server::Ingester;

const SERIES: usize = 25_000;
const RUNS: u32 = 5;

fn rss_bytes() -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8(output.stdout)
        .expect("rss UTF-8")
        .trim()
        .parse::<u64>()
        .expect("rss number")
        * 1024
}

async fn query(service: &IngesterService, matchers: Vec<cortex::LabelMatcher>) -> (usize, usize) {
    let mut request = Request::new(cortex::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 20_000,
        matchers,
        streaming_chunks_batch_size: 1024,
    });
    request
        .metadata_mut()
        .insert("x-scope-orgid", "benchmark".parse().unwrap());
    let mut stream = service.query_stream(request).await.unwrap().into_inner();
    let mut series = 0;
    let mut bytes = 0;
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        bytes += response.encoded_response.len();
        let decoded = cortex::QueryStreamResponse::decode(response.encoded_response).unwrap();
        series += decoded.streaming_series.len();
    }
    (series, bytes)
}

fn report(name: &str, cold: Duration, hot: Duration, series: usize, bytes: usize, rss: u64) {
    println!(
        "{name}: series={series} response_B={bytes} cold_ms={:.2} hot_ms={:.2} rss_B={rss}",
        cold.as_secs_f64() * 1000.,
        hot.as_secs_f64() * 1000. / RUNS as f64,
    );
}

#[tokio::main]
async fn main() {
    let store = Arc::new(Store::default());
    for index in 0..SERIES {
        let mut labels = vec![("__name__".to_owned(), format!("metric_{index}"))];
        for label in 0..19 {
            let value = if label == 9 {
                format!("group_{}", index / 100)
            } else {
                format!("value_{index}_{label}")
            };
            labels.push((format!("label_{label}"), value));
        }
        let samples = (0..20)
            .map(|sample| cortexpb::Sample {
                timestamp_ms: sample * 1000,
                value: (index + sample as usize) as f64,
            })
            .collect();
        let histograms = if index % 10 == 0 {
            vec![cortexpb::Histogram {
                timestamp: 20_000,
                count: Some(cortexpb::histogram::Count::CountInt(1)),
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                ..Default::default()
            }]
        } else {
            Vec::new()
        };
        store
            .ingest(
                "benchmark",
                DecodedRequest {
                    source: 0,
                    series: vec![DecodedSeries {
                        labels: labels
                            .into_iter()
                            .map(|(name, value)| (name.into(), value.into()))
                            .collect(),
                        samples,
                        histograms,
                        exemplars: Vec::new(),
                        created_timestamp: 0,
                    }],
                    metadata: Vec::new(),
                },
            )
            .unwrap();
    }
    let service = IngesterService::new(store);
    for (name, matchers, expected) in [
        (
            "selective",
            vec![cortex::LabelMatcher {
                r#type: 0,
                name: "label_9".into(),
                value: "group_0".into(),
            }],
            100,
        ),
        (
            "broad",
            vec![cortex::LabelMatcher {
                r#type: 2,
                name: "label_9".into(),
                value: ".+".into(),
            }],
            SERIES,
        ),
    ] {
        let before = rss_bytes();
        let started = Instant::now();
        let (series, bytes) = query(&service, matchers.clone()).await;
        let cold = started.elapsed();
        assert_eq!(series, expected);
        let mut hot = Duration::ZERO;
        for _ in 0..RUNS {
            let started = Instant::now();
            assert_eq!(query(&service, matchers.clone()).await, (series, bytes));
            hot += started.elapsed();
        }
        report(
            name,
            cold,
            hot,
            series,
            bytes,
            rss_bytes().saturating_sub(before),
        );
    }
}
