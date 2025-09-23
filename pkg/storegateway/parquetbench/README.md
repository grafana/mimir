# Parquet Benchmark

This benchmark compares the performance of querying time series data from Parquet and TSDB storage formats in Grafana Mimir.

## Quick Start

### Option 1: Generate Data On-the-Fly (Default)

The benchmark can generate test data automatically during the test run:

```bash
# Run with Parquet storage
go test -bench=BenchmarkBucketStores_Series -benchmark-store=parquet

# Run with TSDB storage
go test -bench=BenchmarkBucketStores_Series -benchmark-store=tsdb

# Run specific test case
go test -bench=SingleMetricAllSeries -benchmark-store=parquet
```

### Option 2: Use Pre-Generated Blocks (Recommended to speed up tests and ensure consistency)

For a faster developer loop and consistent benchmarking, use pre-generated blocks with the `blockgen` tool:

```bash
# 1. Generate test blocks (see blockgen/README.md for details)
cd blockgen
go run main.go -verbose

# 2. Run benchmark with pre-generated blocks
cd ..
go test -bench=BenchmarkBucketStores_Series -benchmark-store=parquet -benchmark-tsdb-dir=./blockgen/benchmark-data
go test -bench=BenchmarkBucketStores_Series -benchmark-store=tsdb -benchmark-tsdb-dir=./blockgen/benchmark-data
```

## Benchmark Flags

| Flag                     | Default   | Description                                                                                               |
| ------------------------ | --------- | --------------------------------------------------------------------------------------------------------- |
| `-benchmark-store`       | `parquet` | Store type to benchmark: 'parquet' or 'tsdb'                                                              |
| `-benchmark-compression` | `true`    | Enable compression for parquet data                                                                       |
| `-benchmark-sort-by`     | `""`      | Comma-separated list of fields to sort by in parquet data                                                 |
| `-benchmark-tsdb-dir`    | `""`      | Path to pre-generated TSDB blocks (optional) If set the above compression and sort-by options are ignored |
