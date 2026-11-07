# storegateway-query-tee

## Development

### Store-Gateway Local Request Example with `grpcurl`

All commands are run from Mimir repo root.

Build proto descriptors for consumption by `grpcurl`:

```bash
 protoc \
    -Igithub.com/grafana/mimir=. \
    -Ipkg/storegateway/storepb \
    -Ivendor/github.com/gogo/protobuf \
    --include_imports \
    --descriptor_set_out=/tmp/storegateway.protoset \
    github.com/grafana/mimir/pkg/storegateway/storegatewaypb/gateway.proto
```

Call the tee endpoint with `-v` to see extra info:

```bash
grpcurl -v -plaintext \
    -protoset /tmp/storegateway.protoset \
    -d '{}' \
    localhost:9095 gatewaypb.StoreGateway/Series
```

Output:

```console
Resolved method descriptor:
rpc Series ( .thanos.SeriesRequest ) returns ( stream .thanos.SeriesResponse );

Request metadata to send:
(empty)

Response headers received:
content-type: application/grpc

Response contents:
{}

Response trailers received:
(empty)
Sent 1 request and received 1 response
```

Decoded requests and responses will be visible in the logs.
