//! Merging a series' overlapping in-order and out-of-order float chunks, as queries of series with
//! out-of-order samples do: `cargo bench --bench ooo_merge`.

use std::time::Instant;

use mimir_rust_kafka_ingester::ooo_merge::{Candidate, Chunk, XOR_ENCODING, merge_overlapping};
use mimir_rust_kafka_ingester::xor;

const MERGES: u32 = 20_000;

fn chunk(samples: &[(i64, f64)]) -> Chunk {
    Chunk {
        min_time: samples[0].0,
        max_time: samples[samples.len() - 1].0,
        encoding: XOR_ENCODING,
        data: xor::encode(samples),
    }
}

fn main() {
    // A 120-sample in-order chunk scraped every 15 s, and an out-of-order chunk of late samples
    // between them, one on a timestamp the in-order chunk has.
    let in_order = chunk(
        &(0..120)
            .map(|i| (i * 15_000, (i as f64 * 1.7).sin()))
            .collect::<Vec<_>>(),
    );
    let out_of_order = chunk(
        &(0..30)
            .map(|i| (i * 60_000 + 7_500 * (i % 2), i as f64))
            .collect::<Vec<_>>(),
    );
    let candidates = || {
        vec![
            Candidate {
                chunk: in_order.clone(),
                order: (false, 1),
            },
            Candidate {
                chunk: out_of_order.clone(),
                order: (true, 1),
            },
        ]
    };
    let expected = merge_overlapping(candidates()).unwrap();
    let started = Instant::now();
    for _ in 0..MERGES {
        assert_eq!(
            merge_overlapping(candidates()).unwrap().len(),
            expected.len()
        );
    }
    println!(
        "merge: {:.2} us/series, {} chunks out",
        started.elapsed().as_secs_f64() * 1e6 / f64::from(MERGES),
        expected.len()
    );
}
