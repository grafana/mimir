//! Sharded queries over series that left the head for cold blocks, like a metric whose pods
//! churned: `cargo bench --bench cold_query` (`COLD_RUNS` repeats the query more, to profile it).

use std::time::{Duration, Instant};

use mimir_rust_kafka_ingester::proto::cortex;
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::store::Store;

const HOUR: i64 = 3_600_000;
const COLD: usize = 25_000;
const LIVE: usize = 2_500;
const RUNS: u32 = 20;

fn series(pod: String, from: i64, to: i64) -> DecodedSeries {
    DecodedSeries {
        labels: [
            ("__name__".to_owned(), "churned".to_owned()),
            ("job".to_owned(), "api".to_owned()),
            ("pod".to_owned(), pod),
        ]
        .into_iter()
        .map(|(name, value)| (name.into(), value.into()))
        .collect(),
        samples: (from..to)
            .step_by(15_000)
            .map(
                |timestamp_ms| mimir_rust_kafka_ingester::proto::cortexpb::Sample {
                    timestamp_ms,
                    value: 1.0,
                },
            )
            .collect(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        created_timestamp: 0,
    }
}

fn main() {
    let directory = std::env::temp_dir().join(format!("cold-query-bench-{}", std::process::id()));
    let store = Store::new(20 * 60 * 1000, None, Some(directory.clone())).unwrap();
    let start = 10 * HOUR;
    let ingest = |series: Vec<DecodedSeries>| {
        store
            .ingest(
                "bench",
                DecodedRequest {
                    source: 0,
                    series,
                    metadata: Vec::new(),
                },
            )
            .unwrap();
    };
    // Pods that ran for the first 2 hours, then pods that run on to 15h.
    for chunk in (0..COLD).collect::<Vec<_>>().chunks(1000) {
        ingest(
            chunk
                .iter()
                .map(|pod| series(format!("old-{pod}"), start, start + 2 * HOUR))
                .collect(),
        );
    }
    for chunk in (0..LIVE).collect::<Vec<_>>().chunks(500) {
        ingest(
            chunk
                .iter()
                .map(|pod| series(format!("live-{pod}"), start, start + 5 * HOUR))
                .collect(),
        );
    }
    store.head_tick(true, false);
    store.head_tick(false, false);
    assert_eq!(
        store.num_series("bench"),
        LIVE as u64,
        "old pods are in cold blocks"
    );
    let matchers = vec![
        cortex::LabelMatcher {
            r#type: 0,
            name: "__name__".into(),
            value: "churned".into(),
        },
        cortex::LabelMatcher {
            r#type: 0,
            name: "__query_shard__".into(),
            value: "1_of_16".into(),
        },
    ];
    let query = || {
        store
            .select_chunks_with_blocks("bench", start, start + 5 * HOUR, &matchers)
            .unwrap()
    };
    let started = Instant::now();
    let (series, blocks) = query();
    let cold = started.elapsed();
    let mut hot = Duration::ZERO;
    let runs = std::env::var("COLD_RUNS").map_or(RUNS, |runs| runs.parse().expect("COLD_RUNS"));
    for _ in 0..runs {
        let started = Instant::now();
        let (again, _) = query();
        hot += started.elapsed();
        assert_eq!(again.len(), series.len());
    }
    let chunks: usize = series
        .iter()
        .map(|view| view.chunk_end - view.chunk_start)
        .sum();
    println!(
        "cold_sharded: series={} chunks={chunks} blocks={} cold_ms={:.2} hot_ms={:.2}",
        series.len(),
        blocks.len(),
        cold.as_secs_f64() * 1000.,
        hot.as_secs_f64() * 1000. / f64::from(runs),
    );
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
