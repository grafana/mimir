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

### Option 3: Use S3/Cloud Storage Buckets

Benchmark against real data stored in S3, GCS, Azure, or other cloud storage:

```bash
# AWS S3 (standard region)
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -s3.bucket-name=my-mimir-blocks \
  -s3.region=us-west-2 \
  -s3.access-key-id=AKIA... \
  -s3.secret-access-key=xxxxx

# AWS S3 (custom endpoint)  
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -s3.bucket-name=my-blocks \
  -s3.region=eu-south-2 \
  -s3.endpoint=s3.dualstack.eu-south-2.amazonaws.com \
  -s3.access-key-id=AKIA... \
  -s3.secret-access-key=xxxxx

# MinIO/S3-compatible storage
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -s3.bucket-name=benchmark-data \
  -s3.endpoint=localhost:9000 \
  -s3.access-key-id=minioadmin \
  -s3.secret-access-key=minioadmin \
  -s3.insecure

# With explicit time range (for testing specific periods)
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -s3.bucket-name=my-blocks \
  -s3.region=us-west-2 \
  -benchmark-min-time=1640995200000 \
  -benchmark-max-time=1640998800000

# Google Cloud Storage
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -backend=gcs \
  -gcs.bucket-name=my-mimir-blocks

# Azure Blob Storage
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -backend=azure \
  -azure.container-name=mimir-blocks \
  -azure.account-name=myaccount \
  -azure.account-key=xxxxx
```

## Benchmark Flags

### Core Benchmark Options

| Flag | Default | Description |
|------|---------|-------------|
| `-benchmark-store` | `parquet` | Store type to benchmark: 'parquet' or 'tsdb' |
| `-benchmark-user` | `benchmark-user` | User ID for benchmark data (tenant ID) |
| `-benchmark-compression` | `true` | Enable compression for parquet data (ignored if using pre-generated blocks) |
| `-benchmark-sort-by` | `""` | Comma-separated list of fields to sort by in parquet data (ignored if using pre-generated blocks) |
| `-benchmark-tsdb-dir` | `""` | Path to pre-generated TSDB blocks (takes precedence over S3 configuration) |

### Time Range Override

| Flag | Default | Description |
|------|---------|-------------|
| `-benchmark-min-time` | `0` | Override min time for benchmark blocks (Unix milliseconds, 0 = auto-discover) |
| `-benchmark-max-time` | `0` | Override max time for benchmark blocks (Unix milliseconds, 0 = auto-discover) |

### S3 Configuration (Standard Mimir Bucket Flags)

| Flag | Default | Description |
|------|---------|-------------|
| `-backend` | `filesystem` | Storage backend: s3, gcs, azure, swift, filesystem |
| `-s3.bucket-name` | `""` | S3 bucket name containing blocks |
| `-s3.region` | `""` | S3 region (auto-detected if unset) |
| `-s3.endpoint` | `""` | S3 endpoint URL (for custom endpoints) |
| `-s3.access-key-id` | `""` | S3 access key ID |
| `-s3.secret-access-key` | `""` | S3 secret access key |
| `-s3.insecure` | `false` | Use HTTP instead of HTTPS |
| `-s3.native-aws-auth-enabled` | `false` | Use AWS SDK default auth chain |

### Other Storage Backend Examples

```bash
# Google Cloud Storage flags
-backend=gcs -gcs.bucket-name=my-bucket

# Azure Blob Storage flags  
-backend=azure -azure.container-name=my-container -azure.account-name=myaccount

# Swift Object Storage flags
-backend=swift -swift.container-name=my-container -swift.username=user
```

## Block Generation

The `blockgen` tool can generate realistic test data for benchmarking. When using TSDB storage, it automatically creates:

- **TSDB blocks** with index files, chunks, and metadata
- **Index-header files** for optimized queries
- **Sparse-index-header files** for memory optimization and size modeling

These files allow for accurate performance testing and size analysis of different storage formats.

For detailed information about the block generation tool, see `blockgen/README.md`.

## Data Source Priority

The benchmark uses the following priority order for data sources:

1. **S3/Cloud Storage** - If any bucket configuration is provided (e.g., `-s3.bucket-name`)
2. **Pre-generated local blocks** - If `-benchmark-tsdb-dir` is specified
3. **Generated on-the-fly** - Default behavior if no other source is configured

## Use Cases

### Development & Testing
- **On-the-fly generation**: Quick tests with small datasets
- **Pre-generated blocks**: Consistent benchmarking during development

### Production Performance Analysis
- **S3/Cloud buckets**: Benchmark against real production data
- **Time range override**: Focus on specific time periods of interest
- **Multiple storage backends**: Compare performance across different cloud providers

### Size & Compression Analysis
- **Mixed approach**: Generate test data locally, upload to S3, then benchmark
- **Different configurations**: Test various compression and sorting options

## Examples

### Benchmark Production Data
```bash
# Test specific time range from production S3 bucket
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-store=parquet \
  -s3.bucket-name=prod-mimir-blocks \
  -s3.region=us-east-1 \
  -benchmark-user=tenant-123 \
  -benchmark-min-time=1704067200000 \
  -benchmark-max-time=1704153600000
```

### Compare Local vs S3 Performance
```bash
# Local pre-generated blocks
go test -bench=BenchmarkBucketStores_Series -benchmark-tsdb-dir=./testdata

# Same data in S3
go test -bench=BenchmarkBucketStores_Series -s3.bucket-name=test-blocks
```

### Multi-Tenant Benchmarking
```bash
# Test different tenants from the same S3 bucket
go test -bench=BenchmarkBucketStores_Series \
  -s3.bucket-name=multi-tenant-blocks \
  -benchmark-user=tenant-a

go test -bench=BenchmarkBucketStores_Series \
  -s3.bucket-name=multi-tenant-blocks \
  -benchmark-user=tenant-b

# Generate data for specific tenant
cd blockgen
go run main.go -user my-custom-tenant -output ../test-data
cd ..
go test -bench=BenchmarkBucketStores_Series \
  -benchmark-tsdb-dir=./test-data \
  -benchmark-user=my-custom-tenant
```