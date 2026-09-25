use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use mimir_rust_kafka_ingester::proto::{cortex, cortexpb};
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::service::IngesterService;
use mimir_rust_kafka_ingester::store::Store;
use tonic::Request;

use cortex::ingester_server::Ingester;

const SERIES: usize = 600;
const HISTOGRAMS_PER_SERIES: usize = 400;
const RUNS: usize = 3;

fn rss_bytes() -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .expect("RSS UTF-8")
        .trim()
        .parse::<u64>()
        .expect("RSS number")
        * 1024
}

async fn query(service: &IngesterService) -> (usize, usize) {
    let mut request = Request::new(cortex::QueryRequest {
        start_timestamp_ms: 100_000,
        end_timestamp_ms: 160_000,
        matchers: vec![cortex::LabelMatcher {
            r#type: 2,
            name: "__name__".into(),
            value: ".+".into(),
        }],
        streaming_chunks_batch_size: 64,
    });
    request
        .metadata_mut()
        .insert("x-scope-orgid", "benchmark".parse().unwrap());
    let mut stream = service.query_stream(request).await.unwrap().into_inner();
    let mut responses = 0;
    let mut bytes = 0;
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        responses += 1;
        bytes += response.encoded_response.len();
    }
    (responses, bytes)
}

fn histogram(timestamp: i64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        timestamp,
        count: Some(cortexpb::histogram::Count::CountInt(50)),
        positive_spans: vec![cortexpb::BucketSpan {
            offset: 0,
            length: 50,
        }],
        positive_deltas: vec![1; 50],
        ..Default::default()
    }
}

#[tokio::main]
async fn main() {
    let store = Arc::new(Store::default());
    let series = (0..SERIES)
        .map(|index| DecodedSeries {
            labels: vec![("__name__".into(), format!("metric_{index}"))],
            samples: Vec::new(),
            histograms: (0..HISTOGRAMS_PER_SERIES)
                .map(|sample| histogram(sample as i64 * 1000))
                .collect(),
            exemplars: Vec::new(),
            created_timestamp: 0,
        })
        .collect();
    store
        .ingest(
            "benchmark",
            DecodedRequest {
                source: 0,
                series,
                metadata: Vec::new(),
            },
        )
        .unwrap();
    let service = IngesterService::new(Arc::clone(&store));
    let rest_rss = rss_bytes();
    let started = Instant::now();
    let expected = query(&service).await;
    let cold = started.elapsed();
    let query_rss = rss_bytes();
    let mut hot = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let started = Instant::now();
        assert_eq!(query(&service).await, expected);
        hot.push(started.elapsed().as_secs_f64());
    }
    let update = (0..SERIES)
        .map(|index| DecodedSeries {
            labels: vec![("__name__".into(), format!("metric_{index}"))],
            samples: Vec::new(),
            histograms: vec![histogram(HISTOGRAMS_PER_SERIES as i64 * 1000)],
            exemplars: Vec::new(),
            created_timestamp: 0,
        })
        .collect();
    store
        .ingest(
            "benchmark",
            DecodedRequest {
                source: 0,
                series: update,
                metadata: Vec::new(),
            },
        )
        .unwrap();
    let started = Instant::now();
    let after_update = query(&service).await;
    let update_seconds = started.elapsed().as_secs_f64();
    assert_eq!(after_update, expected);
    println!(
        "series={SERIES} histograms={} responses={} response_bytes={} cold_s={:.3} hot_s={hot:?} after_update_s={update_seconds:.3} rest_rss_bytes={rest_rss} after_query_rss_bytes={query_rss}",
        SERIES * HISTOGRAMS_PER_SERIES,
        expected.0,
        expected.1,
        cold.as_secs_f64(),
    );
}
