---
description: Compare ingester read responses between a primary partition and a shadow partition.
menuTitle: Ingester query-tee
title: Ingester query-tee
weight: 31
---

# Ingester query-tee

Ingester query-tee is an experimental standalone gRPC proxy for comparing ingester implementations or configurations.
Deploy one tee per pair of primary and shadow ingesters that own the same Kafka partition and ingest the same data.
Each tee forwards requests to its primary ingester and mirrors sampled read requests to the corresponding shadow ingester.
Callers receive the primary response, including its streaming messages, headers, trailers, and errors.
Comparisons run asynchronously; callers do not wait for the shadow.

## Build and run

Build from the Mimir source tree:

```bash
go build -o ingester-query-tee ./cmd/ingester-query-tee
```

For a single partition pair:

```bash
./ingester-query-tee \
  -primary-address=ingester-zone-a-0.ingester-zone-a:9095 \
  -shadow-address=ingester-shadow-0.ingester-shadow:9095 \
  -server.grpc-listen-port=9095 \
  -server.http-listen-port=9900 \
  -skip-recent-samples=30s
```

Use the primary pod's read RPC address in the ring to route queries through its tee, or deploy the tee beside that pod and expose the tee port to queriers.
Keep the primary backend address pointed at the actual ingester port, so requests cannot loop back into the tee.
Repeat the pairing for every partition in the selected zone.
A load-balanced Service for an entire zone is unsuitable as a backend: it can select an ingester owning a different partition.
The tee does not register itself in the ring or discover partitions.

The HTTP server exposes `/metrics` and `/ready`.
Readiness indicates that the proxy has started; it does not verify backend health or data parity.
gRPC health requests are forwarded to the primary.

## Comparison behavior

The tee compares these ingester read RPCs:

- `QueryStream`
- `QueryExemplars`
- `LabelNames` and `LabelValues`
- `MetricsForLabelMatchers` and `MetricsMetadata`
- `LabelNamesAndValues` and `LabelValuesCardinality`
- `ActiveSeries`
- `SearchLabelNames` and `SearchLabelValues`

`QueryStream` comparisons decode chunks, identify series by their labels, and compare sample timestamps, start timestamps, and values within the requested time range.
Series order, batch sizes, chunk boundaries, and identical overlapping samples do not affect equality.
Integer and float native histograms are compared in their float representation with compacted bucket spans.
Gauge identity and conflicting known counter reset hints are compared; unknown reset hints can match either counter hint because they depend on chunk boundaries.
Float comparisons use exact bit patterns, including stale NaNs, infinities, and signed zero.

Set `-skip-recent-samples` to exclude samples newer than the arrival time of each request minus the specified duration.
This exclusion affects only the `QueryStream` comparison, leaving the forwarded request and primary response intact.
The default is `0s`, which compares all samples within the requested time range.
Even with a recent-sample exclusion, retention, out-of-order writes, ingestion lag, limits, or partition ownership changes can produce mismatches.
Both backends must enforce the same tenant limits and retain the same queryable data.

Other comparisons ignore ordering for unordered label, series, exemplar, metadata, and cardinality responses.
Search comparisons preserve result ordering and scores, while ignoring batch boundaries.
Push, user statistics, health, and other RPCs are forwarded only to the primary.
The proxy supports unary and server-streaming requests, matching the ingester API; client-streaming and bidirectional RPCs are unsupported.

Incoming gRPC metadata is forwarded to both backends, including tenant identity, read consistency, partition offsets, and the only-replica marker.
The primary request follows the caller's cancellation and deadline.
The shadow has an independent `-shadow-timeout`, defaulting to `30s`, so it can finish after the primary response completes.

## Bound shadow load

- `-sample-rate`: fraction of read requests mirrored; defaults to `1`. Set `0` to disable mirroring.
- `-max-concurrent-comparisons`: concurrent comparisons; defaults to `8`. Requests arriving at capacity use only the primary.
- `-max-response-bytes`: captured response budget per backend, including frame overhead; defaults to `16777216` (16 MiB).
- `-max-samples`: decoded samples or series per `QueryStream` response; defaults to `100000`.
- `-shadow-timeout`: maximum duration of each shadow request; defaults to `30s`.

Exceeding a comparison limit skips comparison and keeps serving the primary response.
The response budget bounds captured wire data; decoding and comparison require additional memory.
Concurrency slots remain occupied until the primary completes and the shadow is complete or times out.

## TLS

Use the server's `-server.grpc-tls-cert-path` and `-server.grpc-tls-key-path` flags for incoming TLS.
Backend TLS is configured separately using `-primary.tls-enabled` / `-shadow.tls-enabled` and the respective `tls-ca-path`, `tls-cert-path`, `tls-key-path`, and `tls-server-name` flags.
Run the binary with `-help` for the complete client and server options.

## Metrics

`ingester_query_tee_comparisons_total{method,result}` counts comparisons and skipped requests.
The `result` label is one of `match`, `mismatch`, `skipped`, `limit`, `primary_error`, `shadow_error`, or `comparison_error`.
Backend errors are counted separately from data mismatches; malformed responses are comparison errors.
No comparison is reported as a match when either backend fails or a comparison limit is exceeded.

`ingester_query_tee_backend_duration_seconds{method,backend}` measures the time to consume each backend response.
Mismatch logs contain the RPC name and outcome, without query contents, series labels, sample values, or tenant identifiers.
