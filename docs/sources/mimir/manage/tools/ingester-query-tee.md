---
description: Compare ingester read responses between a primary partition and a shadow partition.
menuTitle: Ingester query-tee
title: Ingester query-tee
weight: 31
---

# Ingester query-tee

Ingester query-tee is an experimental standalone gRPC proxy for comparing ingester implementations or configurations.
The recommended deployment is one tee sidecar in each primary ingester pod in the zone being validated.
Pair each sidecar with a shadow ingester that owns the same Kafka partition and ingests the same data.
Each tee forwards requests to its primary ingester and mirrors all supported read requests to the corresponding shadow ingester.
Callers receive the primary response, including its streaming messages, headers, trailers, and errors.
Comparisons run asynchronously; callers do not wait for the shadow.
The tee always returns only primary responses. It never falls back to the shadow, even when the primary is unavailable and the shadow succeeds.

## Build and run

Build from the Mimir source tree:

```bash
go build -o ingester-query-tee ./cmd/ingester-query-tee
```

## Recommended deployment: primary pod sidecar

Keep the primary ingester's gRPC listener on port `9095` and configure it to advertise the sidecar's port in the ring:

```text
-server.grpc-listen-port=9095
-ingester.ring.instance-port=9096
```

Run the tee in the same pod, with its own gRPC listener on port `9096`:

```bash
./ingester-query-tee \
  -primary-address=127.0.0.1:9095 \
  -shadow-address=ingester-shadow-0.ingester-shadow:9095 \
  -server.grpc-listen-port=9096 \
  -server.http-listen-port=9900 \
  -skip-recent-samples=30s
```

Queriers use the primary ingester's existing pod address in the ring, with the advertised tee port `9096`.
The tee forwards primary requests to the ingester over localhost and mirrors reads to the matching shadow pod.
Set `-sample-rate=0.01` to compare 1% of reads while mirroring all of them.
The ingester retains its ring identity, zone, and partition ownership; the sidecar does not register a separate ring instance.
Kafka ingestion continues in the primary ingester.

Repeat this setup for every primary pod in the selected zone, updating the shadow address for each partition.
Expose port `9096` in the relevant Services and NetworkPolicies, and expose port `9900` for metrics and readiness probes.
Keep the primary ingester's existing readiness probe; the tee's `/ready` endpoint checks only proxy startup.
Keep shadow ingesters isolated from normal query selection during comparison.
For deployments using gRPC TLS or mTLS, configure the tee's incoming TLS and each backend's TLS settings as described below.

A load-balanced Service for an entire zone is unsuitable as a backend: it can select an ingester owning a different partition.
Always point the primary backend at the actual ingester port `9095`, so requests cannot loop back into the tee.

When a primary pod is evicted or restarted, its tee sidecar stops with it.
Availability comes from Mimir's remaining serving zones; the tee never substitutes the shadow response.
To promote the shadow zone after validation, use the normal ingester ring and rollout process.
The tee does not discover partitions or perform the zone cutover.

Dedicated tee pods are an alternative when separate placement is required, but need explicit per-partition ring address wiring.

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
A series' advertised chunk count is not compared, because the Prometheus TSDB counts chunks before it merges overlapping ones, such as out-of-order chunks.
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

Ingesters that agree can still differ for a while on time-dependent results, such as active series between their periodic updates, or samples written late that one ingester consumed before the other.
Set `-mismatch-retries` to ask both backends again, after `-mismatch-retry-delay` (default `70s`) each time, when a comparison's items or samples differ.
The comparison is a mismatch only if some of the same items or samples are missing from the same backend, or still have different values, on every retry; otherwise it counts as `match_after_retry`.
`QueryStream` retries compare samples up to the original request's recent-sample cutoff, so samples written since don't count.
Retries run outside `-max-concurrent-comparisons`, with as many concurrent retries; mismatches arriving when all retry slots are busy are counted without retrying.
The default `0` disables retries.
Push, user statistics, health, and other RPCs are forwarded only to the primary.
The proxy supports unary and server-streaming requests, matching the ingester API; client-streaming and bidirectional RPCs are unsupported.

Incoming gRPC metadata is forwarded to both backends, including tenant identity, read consistency, partition offsets, and the only-replica marker.
The primary request follows the caller's cancellation and deadline.
The shadow has an independent `-shadow-timeout`, defaulting to `30s`, so it can finish after the primary response completes.

## Bound shadow load

- `-sample-rate`: fraction of mirrored read requests compared; defaults to `1`. Set `0.01` to compare 1%, or `0` to disable comparison while continuing to mirror reads.
- `-max-concurrent-comparisons`: concurrent comparisons; defaults to `8`. Requests arriving at capacity still mirror but skip comparison.
- `-max-response-bytes`: captured response budget per backend, including frame overhead; defaults to `16777216` (16 MiB).
- `-max-samples`: decoded samples or series per `QueryStream` response; defaults to `100000`.
- `-shadow-timeout`: maximum duration of each shadow request; defaults to `30s`.

Exceeding a comparison limit skips comparison and keeps serving the primary response.
The shadow response is always drained to completion or its timeout, including when comparison is skipped or exceeds its budget.
Uncompared responses use a reusable frame buffer without capturing or decoding the full response.
Mirroring has no concurrency cap: size the shadow and tee for the full read load. The timeout bounds shadow request lifetimes; comparison limits do not bound total mirroring concurrency or network traffic.
The response budget bounds captured wire data; decoding and comparison require additional memory.
Concurrency slots remain occupied until the primary completes and the shadow is complete or times out.

## TLS

Use the server's `-server.grpc-tls-cert-path` and `-server.grpc-tls-key-path` flags for incoming TLS.
Backend TLS is configured separately using `-primary.tls-enabled` / `-shadow.tls-enabled` and the respective `tls-ca-path`, `tls-cert-path`, `tls-key-path`, and `tls-server-name` flags.
Run the binary with `-help` for the complete client and server options.

## Metrics

`ingester_query_tee_comparisons_total{method,result}` counts comparisons and skipped requests.
The `result` label is one of `match`, `mismatch`, `skipped`, `limit`, `unavailable`, `primary_error`, `shadow_error`, or `comparison_error`.
If either backend returns gRPC `Unavailable` or `DeadlineExceeded`, the result is `unavailable`. This includes the shadow request timeout. These outcomes increment the metric without emitting comparison or RPC error logs. Other errors retain the `primary_error` or `shadow_error` classification.
Backend errors are counted separately from data mismatches; malformed responses are comparison errors.
No comparison is reported as a match when either backend fails or a comparison limit is exceeded.

`ingester_query_tee_mirrored_requests_total{method,result}` counts completed shadow attempts, including requests not compared.
The result is `success` when the full response has been consumed, `unavailable` for availability errors or timeouts, and `shadow_error` for other failures.

`ingester_query_tee_backend_duration_seconds{method,backend}` measures the time to consume each backend response.
QueryStream mismatch logs include primary/shadow series, label-pair and sample counts, missing/extra series and samples, and the number of differing sample values. Counts reflect normalized samples after the requested time range and recent-sample exclusion. Other RPCs report response item counts.
Comparison errors log the failing backend (`primary`, `shadow`, `request`, or `comparison`), the decoder/validation error, and captured frame/byte counts. A successful empty QueryStream is compared as an empty result.
Set `-log-mismatched-queries=true` to include the QueryStream request and a `replay` object in mismatch and comparison-error logs. Replay details contain the tenant ID, allowed gRPC metadata (tenant, read-consistency level/offsets/maximum delay, only-replica marker, and cluster label), primary/shadow addresses, tee hostname, request arrival time, effective comparison end time, recent-sample exclusion, and shadow timeout. Authorization and unrelated headers are never logged.

Set `-log-mismatched-samples=true` to include one deterministic first-difference example. It identifies the series labels, sample and start timestamps, missing/extra samples, value differences, or histogram reset-hint differences. Each backend includes its sample type and chunk encoding. Floats include the value and exact IEEE-754 bits; histograms include normalized count, sum, schema, zero bucket, original reset hint, and bucket/span previews. Previews contain at most eight entries per array. The example also names differing histogram fields and shows the first differing bucket even when it is beyond the preview.

Both flags default to `false` because these details can contain sensitive tenant data. Detailed payloads have a combined 16 KiB budget before logfmt escaping and are emitted at most once per second per tee. Truncated fields contain a valid JSON wrapper with a reason or prefix and have an additional truncation flag; they are not complete replay records. Oversized input is rejected before JSON serialization. `diagnostics_suppressed=true` identifies logs where the detail rate limit was reached. Comparison metrics and summary/error fields are unaffected; tenant IDs remain on summary logs when query logging is enabled, with a 256-byte cap.

To replay a QueryStream, use the original `query` object and metadata from `replay.metadata`, and call `cortex.Ingester/QueryStream` against both recorded backends. The localhost primary address refers to `replay.tee_host`: reach that pod's ingester port `9095`, not its tee port `9096`. Retain the shard matcher and streaming batch size. For comparison, apply the recorded `comparison_end_ms` instead of recomputing the recent-sample cutoff at replay time. Responses may change as ingestion, compaction, or retention progresses; replay is not an immutable snapshot of the original responses.
