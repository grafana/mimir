# Parquet Bucket Store Benchmarks

This is not a static benchmark. It's more of a CLI tool to allow running benchmarks for arbitrary blocks and arbitrary requests.
A BucketStore and a ParquetBucketStore are created, and the configured requests are run against both stores.
A standalone MinIO instance is used instead of an inprocess file system bucket to keep the benchmark and profiles clean and more realistic (modulo latency).


## Setup

1. Start MinIO:
```bash
docker run --name minio-benchmark -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=mimir" \
  -e "MINIO_ROOT_PASSWORD=supersecret" \
  minio/minio server /data --console-address ":9001"
```

2. Create bucket:
```bash
docker exec minio-benchmark mc alias set local http://localhost:9000 mimir supersecret
docker exec minio-benchmark mc mb local/tsdb
```

3. Setup dataset:

This benchmark does not create the dataset for you.

The benchmark expects the bucket to have what real Store Gateways would use, including parquet files. E.g.:

```
root/
├─ user1/
│   01K0AAHZRGFQXG30ASJB1NDAMB/
│     ├─ chunks/
│        ...
│     ├─ index
│     ├─ 0.chunks.parquet
│     ├─ 0.labels.parquet
│     ├─ meta.json
│     ├─ parquet-conversion-mark.json
│  ├─ 01K0ACVCGJR54NJH7XYD64S6GB/
│     ...
│  ├─ bucket-index.json.gz
├─ user2/
│    ...
```

One way to do this is running the command below, which will generate and upload converted blocks:

```bash
go run cmd/parquet-dataset-builder/*.go generate \
  -blocks-storage.backend=s3 \
  -blocks-storage.s3.endpoint=localhost:9000 \
  -blocks-storage.s3.access-key-id=mimir \
  -blocks-storage.s3.secret-access-key=supersecret \
  -blocks-storage.s3.bucket-name=your-bucket-name \
  -blocks-storage.s3.insecure=true
```

Check minio's WebUI at `http://localhost:9001` to verify the dataset is correctly uploaded.

4. Create a JSON file with requests to benchmark:

A list of requests to benchmark is provided through a JSON file like `example_requests.json`.

## Running

```bash
go test -bench=. -run=^$ -cases=example_requests.json
```

Other flags are supported, including bucket configuration. See source code for details.


## Profiles

CPU and memory profiles are generated in the `profiles/` directory for each benchmark run.