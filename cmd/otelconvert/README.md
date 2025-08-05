# otelconvert

A utility for experimenting with otel conversion of Prometheus blocks.

## Build

To build and push an image:
```shell
make build-otelconvert-image-push
```

## Prep

To list existing blocks:

### GCP
```shell
go run tools/listblocks/main.go -backend=gcs -gcs.bucket-name=dev-us-central-0-mimir-dev-11-blocks -user=9960 -show-block-size -show-stats
```

### AWS
```shell
/usr/bin/listblocks -backend=s3 -s3.access-key-id=$BLOCKS_STORAGE_S3_ACCESS_KEY_ID -s3.secret-access-key=$BLOCKS_STORAGE_S3_SECRET_ACCESS_KEY -s3.endpoint=s3.dualstack.eu-south-2.amazonaws.com -s3.bucket-name=ops-eu-south-0-mimir-ops-03-blocks -user=10428 -show-
block-size -show-stats
```

## Run

To download a block:
```shell
go run cmd/otelconvert/main.go --action download --user 9960 --block 01JSZRC7TBZ9RQZMM939H47W94 --dest /tmp/block --backend gcs --gcs.bucket-name dev-us-central-0-mimir-dev-11-blocks
```

To convert a downloaded block:
```shell
go run cmd/otelconvert/main.go --action convert --block /tmp/block --dest /tmp/converted
```

Dry-run a converted block (only print estimated final size):
```shell
go run cmd/otelconvert/main.go --action convert --block /tmp/block --count
```