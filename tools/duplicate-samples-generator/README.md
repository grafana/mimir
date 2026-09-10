# duplicate-samples-generator

Sends remote-write requests containing duplicate-timestamp samples and asserts how Mimir
accounts for them.

Mimir stores one sample per timestamp per series: the first is kept, the rest are dropped.
Some of those drops are counted, some are silent. This tool produces every duplicate shape
on demand and checks the resulting counters. See the Dropped Samples design doc for the flow
tables and why each one should be signalled.

## Run it

```
go run ./tools/duplicate-samples-generator \
  -address http://localhost:8080 \
  -ingester-address localhost:9095 \
  -metrics-url http://localhost:8080/metrics
```

Defaults run every shape, with and without a value conflict, across float samples and native
histograms, with verification on. It exits non-zero if any flow's counters disagree with what
it should have produced.

```
24 flows across float and histogram
   8 to the distributor at http://localhost:8080
  16 to the ingester at localhost:9095 over gRPC
```

Routing is per shape: a shape that keeps the duplicate inside one `TimeSeries` object is
collapsed by the distributor, so it goes to `-address`. Every other shape has to reach the
ingester, so it goes to `-ingester-address` over its `Push` RPC. Without `-ingester-address`
everything goes over remote-write, which still works today because the distributor only
collapses within-object duplicates — but once
[#15550](https://github.com/grafana/mimir/issues/15550) lands it will collapse cross-object
ones too, and the same-request flows will need `-ingester-address` to be reachable at all.

## Shapes

| `-shape` | Duplicate placement | Accounted at |
| :- | :- | :- |
| `same-object` | two samples, one timestamp, inside one `TimeSeries` object | distributor |
| `ooo-same-object` | same, at an out-of-order timestamp | distributor |
| `same-request` | two objects with identical labels in one request | ingester |
| `ooo-same-request` | same, at an out-of-order timestamp | ingester |
| `across-requests` | same series and timestamp in two separate requests | ingester |
| `ooo` | duplicate of an out-of-order sample, in a separate request | ingester |

`-conflict` gives the duplicate a different value instead of the same one; `-shape all` runs
both. Every line of output names the design-doc flow it covers.

## Flags

| Flag | Default | Meaning |
| :- | :- | :- |
| `-address` | `http://localhost:8080` | remote-write endpoint |
| `-push-path` | `/api/v1/push` | use `/api/prom/push` for a GEM gateway |
| `-ingester-address` | unset | ingester gRPC endpoint; ingester flows go here |
| `-tenant-id` | `anonymous` | `X-Scope-OrgID`, or the basic-auth username with `-auth-token` |
| `-auth-token` | unset | basic-auth token |
| `-shape` | `all` | one of the shapes above, or `all` |
| `-conflict` | `false` | give the duplicate a different value |
| `-sample-type` | `both` | `float`, `histogram`, or `both` |
| `-metric-name` | `duplicate_samples_generator` | metric name prefix |
| `-labels` | none | extra `key=value` labels; use a cost-attribution label here |
| `-metrics` | `50` | distinct metrics, and therefore series |
| `-samples-per-metric` | `2` | distinct timestamps per metric per iteration, each duplicated |
| `-count` | `5` | iterations (scrapes) |
| `-series-per-request` | `100` | maximum `TimeSeries` objects per request |
| `-interval` | `0` | pause between iterations |
| `-ooo-delay` | `5m` | how far back out-of-order samples sit; must be inside the OOO window |
| `-verify` | `true` | assert the counters; false only generates traffic |
| `-metrics-url` | derived from `-address` | `/metrics` endpoints to scrape, comma-separated |
| `-verify-delay` | `5s` | wait before the post-run scrape |
| `-replication-factor` | `1` | match the cell's `-distributor.replication-factor` |

Duplicates per flow is `-count` x `-metrics` x `-samples-per-metric`, 500 at the defaults.

## What a passing run means

Per flow, three assertions:

1. The expected reason on `cortex_discarded_samples_total` gains exactly one count per duplicate.
2. No other reason moved, so a drop cannot be miscategorised.
3. `cortex_ingester_ingested_samples_total` grows by the non-duplicate samples only.

The tool scrapes every `-metrics-url` before the flow, sends the traffic, waits
`-verify-delay`, scrapes again and compares deltas, counting only series whose `user` label
matches `-tenant-id`. Comparisons are exact, so a flow fails if anything else writes to the
same tenant during the run.

Data is never read back, so this verifies accounting, not that the stored value is the
first-seen one — `pkg/ingester/ingester_push_duplicate_test.go` covers that.

## Against a dev cell

Scrape **both** components: distributors record `sample_duplicate_timestamp`, ingesters
record the two per-value reasons and `cortex_ingester_ingested_samples_total`. Scraping only
ingesters reads the distributor flows as a silent zero.

```
kubectl -n <namespace> port-forward pod/<distributor-pod> 18010:80 &
kubectl -n <namespace> port-forward pod/ingester-zone-a-0  18001:80 &
kubectl -n <namespace> port-forward pod/ingester-zone-b-0  18002:80 &
kubectl -n <namespace> port-forward pod/ingester-zone-c-0  18003:80 &

go run ./tools/duplicate-samples-generator \
  -address https://<gateway-host> -push-path /api/prom/push \
  -tenant-id <tenant> -auth-token "$(cat token)" \
  -metrics 500 -samples-per-metric 2 -count 15 -interval 15s \
  -metrics-url http://localhost:18010/metrics,http://localhost:18001/metrics,http://localhost:18002/metrics,http://localhost:18003/metrics \
  -replication-factor 3 -verify-delay 8s
```

Pass every ingester that could own the series and set `-replication-factor` to match, or
ingester-side counts come out a multiple of what is expected. Raise `-verify-delay`: a round
trip to a real cell is seconds, not milliseconds.

## Gotchas

Out-of-order duplicate detection only looks at the current out-of-order head chunk, so a
duplicate whose original landed in an already-cut chunk is stored rather than counted. Keep
`-count` x `-samples-per-metric` at or below `-blocks-storage.tsdb.out-of-order-capacity-max`
(default 32) and put volume into `-metrics` instead — the defaults do. The tool warns if a
run would cross the boundary.

Series carry a `run` label per invocation and a `flow` label per flow, so reruns and flows
never share series.
