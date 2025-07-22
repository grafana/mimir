# Parquet Dataset Builder

A set of hacky tools for building datasets meant to be used with pkg/storegateway/parquetbench.


## Commands

**convert** - Convert existing TSDB blocks to parquet format for all users in a bucket.

**generate** - Generate synthetic TSDB blocks and convert them to parquet format.

**promote** - Promote labels from `target_info` series to series with matching `job` and `instance` labels.

**fake-attributes** - Add fake attributes for all series in a block, directly promoting them to labels.

## Usage

```bash
# Convert existing blocks
./mimir-parquet-dataset-builder convert --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=mybucket

# Generate new dataset
./mimir-parquet-dataset-builder generate --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=mybucket --user=test-user
```

Use `-help` with any command to see all available options.
