# grpc-tee


## Development

### Local Request Example with `grpcurl`

We can check the server functionality without any RPC backends registered -
the `dskit` server's `httpgrpc.HTTP` adapter enables requests to the server's HTTP endpoints to be made via gRPC.

The HTTP server has a health check at `/`, a readiness check at `/ready`, and the standard `/metrics` path.

Check with `grpccurl` from the Mimir repo root:
```bash
grpcurl -plaintext \
  -import-path vendor/github.com/grafana/dskit/httpgrpc \
  -import-path vendor/github.com/gogo/protobuf \
  -proto httpgrpc.proto \
  -d '{"method":"GET","url":"/"}' \
  localhost:9095 \
  httpgrpc.HTTP/Handle
```

Output:
```console
{
  "Code": 200
}
```

