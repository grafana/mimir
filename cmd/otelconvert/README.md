# otelconvert

A utility for experimenting with otel conversion of Prometheus blocks.

## Build

To build and push an image:
```shell
make build-otelconvert-image-push
```

## Prep

To list existing blocks:
```shell
go run tools/listblocks/main.go -backend=gcs -gcs.bucket-name=dev-us-central-0-mimir-dev-11-blocks -user=9960
```

## Run

To download a block:
```shell
go run cmd/otelconvert/main.go --action download --user 9960 --block 01EYYPMK842ZKSB4H2R86M878N --dest /tmp/block --backend gcs --gcs.bucket-name dev-us-central-0-mimir-dev-11-blocks
```

To convert a downloaded block:
```shell
go run cmd/otelconvert/main.go --action convert --block /tmp/block --dest /tmp/converted
```

Dry-run a converted block (only print estimated final size):
```shell
go run cmd/otelconvert/main.go --action convert --block /tmp/block --count
```