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
const POINTS: usize = 3_600;

async fn query(service: &IngesterService) -> (usize, usize) {
    let mut request = Request::new(cortex::QueryRequest {
        start_timestamp_ms: 3_600_000,
        end_timestamp_ms: 3_660_000,
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

#[tokio::main]
async fn main() {
    let store = Arc::new(Store::default());
    let series = (0..SERIES)
        .map(|index| DecodedSeries {
            labels: vec![("__name__".into(), format!("metric_{index}").into())],
            samples: (0..POINTS)
                .map(|point| cortexpb::Sample {
                    timestamp_ms: point as i64 * 2_000,
                    value: ((index + point) as f64 * 0.125).sin(),
                })
                .collect(),
            histograms: Vec::new(),
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
    let started = Instant::now();
    let expected = query(&service).await;
    let cold = started.elapsed();
    let started = Instant::now();
    assert_eq!(query(&service).await, expected);
    let hot = started.elapsed();
    let update = (0..SERIES)
        .map(|index| DecodedSeries {
            labels: vec![("__name__".into(), format!("metric_{index}").into())],
            samples: vec![cortexpb::Sample {
                timestamp_ms: POINTS as i64 * 2_000,
                value: index as f64,
            }],
            histograms: Vec::new(),
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
    assert_eq!(query(&service).await, expected);
    let after_update = started.elapsed();
    println!(
        "series={SERIES} samples={} responses={} response_bytes={} cold_s={:.3} hot_s={:.3} after_update_s={:.3}",
        SERIES * POINTS,
        expected.0,
        expected.1,
        cold.as_secs_f64(),
        hot.as_secs_f64(),
        after_update.as_secs_f64(),
    );
}
