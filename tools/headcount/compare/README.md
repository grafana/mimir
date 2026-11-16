# compare

Compares the experimental `/prometheus/api/v1/cardinality/estimate` route with PromQL on the same selector and window, through one query-frontend, to check the estimate during a rollout.

```bash
go run ./tools/headcount/compare \
  -address http://localhost:8080 \
  -tenant <tenant> \
  -query-ingesters-within <the cell's -querier.query-ingesters-within> \
  -interval 10m -pairs-per-round 4 -request-gap 2s \
  -listen :9100
```

It runs outside the cluster through a port-forward to the query-frontend, or inside it as a workload. The tool's latency histogram is measured from the client, so it includes the network between the tool and the query-frontend; the server-side latency is in the query-frontend's query stats log lines and metrics. Both requests carry `User-Agent: mimir-cardinality-estimate-compare`, so the query-frontend's query stats log lines of this tool can be filtered, and `Cache-Control: no-store`, so neither the results cache nor the estimate cache answers them. Estimates and truths also carry `X-Read-Consistency: strong`, so on ingest storage both wait for the ingesters to catch up with what was written before them, and a window that ends near now is the same for both.

Target picking reads `/prometheus/api/v1/cardinality/label_names`, so the tenant needs `-querier.cardinality-analysis-enabled`, and the estimate needs `-querier.cardinality-estimate-enabled`. Without the first, every round fails before it sends a pair.

Both requests load the cell's read path, and the truth queries count against the tenant's fetched-series and chunk limits. Keep `-pairs-per-round` and `-request-gap` at the rate agreed with the cell's owners.

## A round

1. Pick targets from the tenant's data over the last hour: the largest metric names from the estimate's name table, each name's highest-cardinality labels and the tenant's from `/api/v1/cardinality/label_names`, and the largest values of `-matcher-label`. For each name, also pick a facet label for the matcher queries: the first of its labels that some of its series lack, found with one estimate of `{__name__="<name>", <label>=""}` per label tried, or its first label if every series has each. Then take that label's 3 largest values. This adds up to `-top-names` × (`-top-labels` + 1) small estimate requests to each round, sent `-request-gap` apart like every other request.
2. Fill each query with them on the window kinds that fit each length in `-windows` (default `1h,6h,24h`): `blocks` (ends before now minus `-query-ingesters-within`, store-gateways only), `ingesters` (inside it), `boundary` (centred on it), and `recent` (ends at now minus `-lag` and starts before the boundary: the "last N hours" a dashboard asks for, which the querier answers from both ingesters and store-gateways). A length gets an `ingesters` window when it fits inside the ingesters' range and a `recent` window otherwise, and a `boundary` window when it ends before now. With `-query-ingesters-within=6h`, 24h gets `blocks` and `recent`. Draw `-pairs-per-round` pairs at random.
3. For each pair, send the estimate and the truth, alternating which goes first, and label each `first` or `second`. The first may warm caches for the second, but neither starts cold: target picking and earlier pairs have read the same indexes. A cold index-header cost needs its own run after a store-gateway restart or a cache flush, read from the store-gateway's metrics.
4. Diff the two tables group by group, and classify each differing group. Rerun a pair with an unexplained group once; the rerun's diff is the result. A group that mismatched and no longer does is `answer_changed` when its truth changed between the runs, and `estimate_unstable` when only its estimate did. One that still mismatches stays a mismatch.

The truth is an instant query `count by (<label>) (last_over_time(<selector>[<window>]))` at the window's end minus 1 ms. The estimate counts `[start, end)` and a PromQL range at `t` selects `(t - window, t]`, so at `end - 1ms` both cover the same milliseconds. With a step, each bucket gets its own truth query with the step as the window.

## Burst

With `-burst N`, the first round and then one round every `-burst-interval` (default 1h) send N estimates at once before the round's pairs. The estimates are drawn in turn from the round's pairs, and no PromQL truth goes with them. A burst fills the count limits, which one request at a time never does: the store-gateway's `-blocks-storage.bucket-store.cardinality-estimate-max-concurrent` (8, which queues until the queue timeout) and the ingester's `-ingester.cardinality-estimate-max-concurrent` (8, which refuses at once). Use N = 16 to fill both; N is capped at 64. The burst records how each estimate was answered and how long it took, and logs one line per burst. Check during a burst that the cell's other queries stay level, on the cell's dashboards.

## Scenarios

`-scenario <file>` adds hand-written pairs: queries a sizing question needs that the drawn pairs may never pick, or a pair that mismatched, written down so it can be rerun exactly. Every round runs each scenario once, on top of the `-pairs-per-round` drawn pairs, on a window drawn from the ones it allows. A scenario no window fits is skipped for that round and logged. Its pairs carry `query="scenario:<name>"` and go through the same comparison and classes.

```yaml
- name: api_by_pod                       # required, unique
  selector: '{__name__="up", job="api"}' # required, one match[] selector
  group_by: pod                          # default __name__
  windows: [24h]                         # default -windows
  kinds: [recent]                        # default every kind: blocks, ingesters, boundary, recent
  limit: 20                              # default 0, the whole table
  steps: 0                               # default 0, no buckets
```

Keep a scenario file that names a tenant's metrics and labels outside the repository.

## Queries

| Query | `match[]` | `group_by` |
|---|---|---|
| `tenant_by_name` | `{__name__=~".+"}` | `__name__` |
| `metric_by_label` | one large metric | one of its high-cardinality labels; `group_count` gives the label's distinct values |
| `matcher_by_name` | `{<matcher-label>="<a large value>"}` | `__name__` |
| `metric_total` | one large metric | `__name__` |
| `metric_steps` | one large metric, with `step` = window / `-steps` | `__name__` |
| `tenant_by_label` | `{__name__=~".+"}` | `-matcher-label` |
| `tenant_label_vals` | `{__name__=~".+"}` | the tenant's highest-cardinality label |
| `name_regex` | `{__name__=~"<prefix>.*"}`, the prefix being a large name up to its first `_` | `__name__` |
| `metric_not_equal` | `{__name__="<name>", <facet>!="<its largest value>"}`; the series without the facet label count, as in PromQL | the facet label |
| `metric_label_regex` | `{__name__="<name>", <facet>=~"<v1>\|<v2>"}` | the facet label |
| `two_matchers` | `{__name__="<name>", <facet>="<a large value>"}` | another of the name's labels, or `__name__` |

## Outcomes and classes

Each pair and each differing group has an outcome: `pass`, `tolerated` (a class explains the difference), `flagged` (the answer's own flags allow a difference in that direction, but bound neither its size nor its cause, so it is no evidence that the estimate is right), `mismatch` (no class explains it: file a Bug issue with the logged pair), or `refusal`.

| Class | Outcome | When |
|---|---|---|
| `exact` | pass | every group equal |
| `empty` | pass | both sides have no series in the window |
| `sketch` | tolerated | the group is `estimated` and within `-sketch-sigmas` (default 3) standard errors of the HLL sketch, 1.04/√2^p at the route's precision |
| `sketch_small` | mismatch | the group is `estimated` although its truth is at or below 10,000, the route's default `sketch_above`, which it counts exactly. Checked without a step only, since a step's buckets share their group's mark |
| `incomplete` | flagged | the answer has `lower_bound` or `coverage_gap`, and the group is below the truth but not below a truth over the part of the window the answer says it counted in full. That part starts at `head_min_time` when the answer has it, since the ingesters hold everything after it; otherwise it is the longest part of the window outside every `missing_shards` range. An answer with `missing_blocks` or a `coverage_gap` and no `head_min_time` doesn't say which part it left out, and a step pair isn't probed, so those groups are flagged unbounded |
| `head_whole` | flagged | the answer counts the whole head whatever the window, and the group is above the truth but not above a truth over the last `-head-span` (default 3h, 1.5 times the TSDB block range), up to now |
| `answer_changed` | tolerated | the group mismatched, the rerun agrees, and the truth changed between the runs: compaction or late out-of-order blocks between the calls |
| `edge` | tolerated | the group is above the truth by no more than its series with a sample in the `-edge-slack` before the window and none in it, counted as `count by (<label>) (last_over_time(<selector>[<slack>] offset <window>) unless last_over_time(<selector>[<window>]))`. The estimate counts a series when one of its chunks overlaps the window, which such a series can do with its staleness marker in the window, which PromQL drops, or with a gap across the window. A series that starts after the window has no chunk in it, so the probe doesn't look there. Step pairs probe each bucket over the step. Set `-edge-slack` (default 5m) to the tenant's largest scrape interval |
| `incomplete_undercount` | mismatch | an `incomplete` group below a truth over the part of the window the answer says it counted in full |
| `head_whole_excess` | mismatch | a `head_whole` group above a truth over the last `-head-span`. The whole-head count counts a series that moved between ingesters once in each, so rule that out before filing a Bug issue |
| `estimate_unstable` | mismatch | the group mismatched and the rerun agrees, but only the estimate changed between the runs: the estimate is not deterministic |
| `window_echo` | mismatch | the route counted another window than the one asked for |
| `unclassified` | mismatch | anything else |
| `no_count_rpc` | refusal | a store-gateway or an ingester without the count RPCs answered (422, "does not support cardinality estimates") |
| `error_trailer` | refusal | the estimate's streamed table ended in an error after the 200, so its groups are not the whole answer |
| `index_bytes_limit` | refusal | the estimate refused the request on `max_index_bytes` (422) |
| `estimate_refused` | refusal | the estimate refused the request on another limit (422), or another 4xx |
| `promql_limit` | refusal | the truth hit a per-query limit (422 with an `err-mimir-max-*` error ID) |
| `bad_request` | refusal | either side answered 400: the request fails the same way every round |
| `throttled` | refusal | either side answered 429 |
| `error` | refusal | any other failure, such as a 5xx or a timeout |

With `has_more`, only the groups in the table are compared, plus `group_count` and `total` against the whole truth. The tenant-wide tables ask for `-table-limit` (default 50) groups. When the truth of a table without a step hits a per-query limit (`promql_limit`), the refusal is counted and the estimate's `-fallback-names` (default 5) largest groups are counted one at a time instead, each with an equality matcher on the group's label in place of the selector's matchers on it; `group_count`, `total` and the other groups are then not checked. A 429 or a 5xx does not start the fallback.

When the tenant had more than `-max-reference-series` (default 100,000) series over the last hour at target picking, `tenant_by_name`, `tenant_by_label` and `tenant_label_vals` skip the grouped truth, which a per-query series limit would refuse after loading the cell, and go straight to the per-group truth. Those pairs always send the estimate first, since the truth needs its groups. Set `-max-reference-series` to the tenant's max fetched series per query; 0 never skips.

## Output

One log line per pair, with the differing groups of a mismatch. With `-interval 0` the tool runs one round and exits 1 if a pair mismatched, 3 if no pair passed or was tolerated (every pair refused or flagged), and 0 otherwise. With `-listen`, `/metrics` serves:

- `cardinality_estimate_compare_pairs_total{query, window, order, outcome, class, read, source}`, where `read` and `source` are the estimate's, empty when it didn't answer
- `cardinality_estimate_compare_flags_total{query, flag}`, one per answered estimate of a pair for each flag set: `lower_bound`, `coverage_gap`, `head_whole`, `head_fallback`, `head_min_time`, `missing_blocks`, `missing_shards`, `snapped`, `has_more` or `dedup`. The log line of each pair has `read`, `source` and `flags` too
- `cardinality_estimate_compare_groups_total{query, outcome, class}`
- `cardinality_estimate_compare_refusals_total{query, side, class}`
- `cardinality_estimate_compare_request_duration_seconds{query, window, length, side, position, reference, read, source}`, client-side latency of each side of a pair that answered, once per pair: a refused or failed request is not observed, and a truth asked one value or one bucket at a time is the sum of its queries. `position` is `first` or `second`, `reference` is `grouped`, `per_value` or `per_bucket`, and `read` and `source` are the estimate's for both sides. Reruns and probes are not observed. The top bucket is above `-timeout`
- `cardinality_estimate_compare_sketch_relative_error{group_by}`, `|estimate - truth| / truth` of every `estimated` group, equal or not, with `group_by` `metric` for a table by `__name__` and `label_value` otherwise, to read a percentile of the sketch's error from. The `sketch` class is a per-group pass or fail at `-sketch-sigmas`; this histogram is what a percentile target is checked against. Step pairs are left out. Each pair's log line lists its estimated groups
- `cardinality_estimate_compare_rounds_total{result}` and `cardinality_estimate_compare_last_round_timestamp_seconds`
- `cardinality_estimate_compare_bursts_total`, `cardinality_estimate_compare_burst_requests_total{query, status}` and `cardinality_estimate_compare_burst_request_duration_seconds{status}`, where `status` is the HTTP status, `error_trailer` for a 200 whose stream ended in an error, `timeout` or `error`
