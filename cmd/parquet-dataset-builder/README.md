# Parquet Dataset Builder

CLI tool for building parquet datasets meant to be used with pkg/storegateway/parquetbench. It writes and reads directly to a bucket.


## Commands

**convert** - Convert existing TSDB blocks to parquet format for all users in a bucket.

**generate** - Generate synthetic TSDB blocks and convert them to parquet format.

## Usage

```bash
# Convert existing blocks
./mimir-parquet-dataset-builder convert --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=mybucket

# Generate new dataset
./mimir-parquet-dataset-builder generate --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=mybucket --user=test-user
```

Use `-help` with any command to see all available options.
