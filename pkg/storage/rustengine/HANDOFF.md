# Rust engine handoff — 2026-11-13

Branch `julienduchesne/rust-engine-v2`, on top of `julienduchesne/rust-engine`: the native engine and its Go glue
were rewritten. [README.md](README.md) describes the design.

## Goal and comparison

Target: twice better than the Go seriesstore engine (new-ingester `b889bc7cd0`) on what the engine costs: ingestion
CPU, replay time, compaction, memory, disk, and query CPU and latency. The whole process can't be twice better, since
Go keeps Kafka and gRPC, so the comparison drives an ingester through the Kafka record path and `QueryStream` without
either: `pkg/ingester/engine_compare_bench_test.go` (build tag `enginecompare`), the same file in both trees. A tenant's
records are pushed one at a time, as the consumer does. An empty query measures what a query costs the ingester
whatever the engine: about 3 µs.

Both engines run with the same settings: 1% of samples up to 10 minutes late, a one-hour out-of-order window,
compaction every 30 minutes of data, 4 workers. Numbers are from an Apple M3 Pro, one process per engine, run one
after the other.

## Results: 200k series, 13 hours, one sample a minute

Go/Rust, above 1 when Rust is better:

| | Go | Rust | Go/Rust |
|---|---|---|---|
| Replay ingestion, CPU ns/sample | 486 | 163 | 2.98 |
| Replay wall, s | 75.1 | 26.3 | 2.86 |
| Replay compaction, CPU s | 3.57 | 0.71 | 5.02 |
| Steady ingestion, CPU ns/sample | 523 | 166 | 3.16 |
| RSS growth after replay, B/series | 10,581 | 5,081 | 2.08 |
| RSS growth after queries, B/series | 10,526 | 5,202 | 2.02 |
| Disk after replay, MB | 1,054 | 1,106 | 0.95 |
| Disk after queries, MB | 1,278 | 1,106 | 1.16 |

Queries, CPU ms per query: one pod 1h 12.8×, one pod 13h 6.4×, one pod cold 1h 3.4×, one metric 13h 8.6×, a metric of
10k series over 10m 2.3×, label names 31×, label values 16.8×, the others 9–14×.

At 50k series and 4 hours, everything is at least twice better but RSS (1.46–1.65): at that point Rust has compacted
half its data to disk and Go none, while Rust's RSS still carries its memory's fixed costs.

## Open

- **Disk is about the same as Go's.** Both store XOR chunks, which noisy floats make about 7.5 bytes a sample. A
  denser cold encoding (decimal values, tighter timestamp jitter) would make it two to four times smaller, but queriers
  only take Prometheus chunk encodings, so cold reads would transcode every chunk, and long-range cold queries would
  cost more than Go's. That trade-off is the user's call.
- The final compaction of the comparison flushes a block in Rust and nothing in Go, whose schedule keeps more in
  memory; all compaction together costs Rust about 4.5 times less.
- Results are from macOS. Linux, where the earlier comparisons ran, wasn't measured yet.
- Not done: symbol collection, reusing series slots, compressed histograms, periodic head snapshots (Go's engine has
  them, so a crash replays less).
