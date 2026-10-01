//! Wall time of a head tick on a store of ~1M series, like a pod of mimir-dev-15: each tick, a
//! third of the series got a sample since the last one (a 15s tick, samples every ~48s).
//! `cargo bench --bench head_tick` (`HEAD_TICK_SERIES` to resize).

use std::time::{Duration, Instant};

use mimir_rust_kafka_ingester::proto::cortexpb;
use mimir_rust_kafka_ingester::record::{DecodedRequest, DecodedSeries};
use mimir_rust_kafka_ingester::store::Store;

fn request(names: impl Iterator<Item = usize>, timestamp_ms: i64) -> DecodedRequest {
    DecodedRequest {
        source: 0,
        series: names
            .map(|index| DecodedSeries {
                labels: vec![
                    ("__name__".into(), format!("metric_{}", index % 1000).into()),
                    ("instance".into(), format!("pod-{}", index / 1000).into()),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms,
                    value: index as f64,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            })
            .collect(),
        metadata: Vec::new(),
    }
}

fn main() {
    let series = std::env::var("HEAD_TICK_SERIES").map_or(1_000_000, |series| {
        series.parse().expect("HEAD_TICK_SERIES")
    });
    let store = Store::with_shards(20 * 60_000, None, None, 16, 8).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut timestamp = now - 60 * 60_000;
    for start in (0..series).step_by(10_000) {
        store
            .ingest(
                "tenant",
                request(start..(start + 10_000).min(series), timestamp),
            )
            .unwrap();
    }
    // The first compaction sets the head's min time, which the ticks after keep.
    store.head_tick(true, true);
    let mut ticks = Vec::new();
    for round in 0..12 {
        timestamp += 15_000;
        let third = (round % 3) * series / 3;
        for start in (third..third + series / 3).step_by(10_000) {
            store
                .ingest(
                    "tenant",
                    request(start..(start + 10_000).min(third + series / 3), timestamp),
                )
                .unwrap();
        }
        let started = Instant::now();
        let reports = store.head_tick(false, true);
        ticks.push(started.elapsed());
        assert_eq!(reports[0].memory_series, series as u64);
    }
    ticks.sort();
    let median = ticks[ticks.len() / 2];
    let mean = ticks.iter().sum::<Duration>() / ticks.len() as u32;
    println!(
        "series={series}: head tick median {:.1} ms, mean {:.1} ms",
        median.as_secs_f64() * 1000.0,
        mean.as_secs_f64() * 1000.0
    );
}
