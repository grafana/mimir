## How to run it in the local development environment

Start the local development environment:

```
./development/mimir-ingest-storage/compose-up.sh
```

The build the binary and copy it to a running container:

```
GOOS=linux GOARCH=arm64 go build . && docker cp dump-kafka-records mimir-ingest-storage-mimir-backend-1-1:/mimir
```

Then run it from the container:

```
TODO
```
