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

It runs outside the cluster through a port-forward to the query-frontend, or inside it as a workload. The tool's latency histogram is measured from the client, so it includes the network between the tool and the query-frontend; the server-side latency is in the query-frontend's query stats log lines and metrics. Both requests carry `User-Agent: mimir-cardinality-estimate-compare`, so the query-frontend's query stats log lines of this tool can be filtered, and `Cache-Control: no-store`, so neither the results cache nor the estimate cache answers them. Estimates and truths also carry `X-Read-Consistency: strong`, so on ingest storage both wait for the ingesters to catch up with what was written before them, and a window that ends near now is the same for both. With `-cluster-label`, both also carry `X-Cluster: <label>`: a query-frontend with `-server.cluster-validation.http.enabled` and soft validation off answers 511 to a client without it, unless its User-Agent is exempt.

Target picking reads `/prometheus/api/v1/cardinality/label_names`, so the tenant needs `-querier.cardinality-analysis-enabled`, and the estimate needs `-querier.cardinality-estimate-enabled`. Without the first, every round fails before it sends a pair. Target picking sends no exact tenant-wide table, which a large tenant is refused: it reads names and label values from bounds mode or from the label values cardinality endpoint (see `-target-source`).

Both requests load the cell's read path, and the truth queries count against the tenant's fetched-series and chunk limits. Keep `-pairs-per-round` and `-request-gap` at the rate agreed with the cell's owners.

## A round

1. Pick targets from the tenant's data. The metric names come from one row set, read from `-target-source`: `bounds` (default) sends a bounds request (`mode=bounds`, `group_by=__name__`, no matchers) over the `-target-window` (default 6h) that ends at the block cutoff, which is the end of the `blocks` windows, and reads the groups of the newest block range that has any, sorted by upper value; when no range answers it reads the `head` row set of the last hour. `head` reads that row set of the last hour. `label_values` reads `/prometheus/api/v1/cardinality/label_values?label_names[]=__name__` from the ingesters, which is capped by the tenant's cardinality analysis limit (`-querier.cardinality-analysis-max-results`, 500 by default): `-label-values-limit` (default 500) must not be above it, since the endpoint answers 400 then, and ranks above it are not drawn. Names are drawn by rank, at `-name-ranks` (default `5,50,500,5000`): the table's 5th largest, 50th and so on. A tenant's largest names usually have PromQL truths that exceed the chunk and series limits, and a rank past the end of the table is skipped. The tenant's series, which decides whether the tenant-wide tables skip their grouped truth, is an upper value from the same row set: the range's `upper_total`, or the heads' `in_memory_total`. Then pick each name's highest-cardinality labels and the tenant's from `/api/v1/cardinality/label_names` (read from the ingesters, so a name that is not in the heads has none), and the largest values of `-matcher-label` from the same source. For each name, also pick a facet label for the matcher queries: the first of its labels that some of its series lack, found with one estimate of `{__name__="<name>", <label>=""}` per label tried over the window the name was read from, or its first label if every series has each. Then take that label's 3 largest values. A name whose facet probe is refused gets no matcher queries that round; each such name counts in `cardinality_estimate_compare_refusals_total{query="facet_probe", side="estimate"}`. This adds up to `len(-name-ranks)` × (`-top-labels` + 1) small estimate requests to each round, sent `-request-gap` apart like every other request.
2. Fill each query with them on the window kinds that fit each length in `-windows` (default `1h,6h,24h`): `blocks` (ends before now minus `-query-ingesters-within`, store-gateways only), `ingesters` (inside it), `boundary` (centred on it), and `recent` (ends at now minus `-lag` and starts before the boundary: the "last N hours" a dashboard asks for, which the querier answers from both ingesters and store-gateways). A length gets an `ingesters` window when it fits inside the ingesters' range and a `recent` window otherwise, and a `boundary` window when it ends before now. With `-query-ingesters-within=6h`, 24h gets `blocks` and `recent`. Draw `-pairs-per-round` pairs at random.
3. For each pair, send the estimate and the truth, alternating which goes first, and label each `first` or `second`. The first may warm caches for the second, but neither starts cold: target picking and earlier pairs have read the same indexes. A cold index-header cost needs its own run after a store-gateway restart or a cache flush, read from the store-gateway's metrics.
4. Diff the two tables group by group, and classify each differing group. Rerun a pair with an unexplained group once; the rerun's diff is the result. A group that mismatched and no longer does is `answer_changed` when its truth changed between the runs, and `estimate_unstable` when only its estimate did. One that still mismatches stays a mismatch.

The truth is an instant query `count by (<label>) (last_over_time(<selector>[<window>]))` at the window's end minus 1 ms. The estimate counts `[start, end)` and a PromQL range at `t` selects `(t - window, t]`, so at `end - 1ms` both cover the same milliseconds.

## Bounds check

Bounds mode (`mode=bounds`) answers a tenant-wide table with no matchers: for each block range the window touches, a lower and an upper count per group. With `-bounds-checks N` (default 1), each round sends N bounds requests, `group_by=__name__`, each over a `blocks` or `boundary` window, and checks the newest `-bounds-max-ranges` (default 4) ranges that answered with groups in two ways. The heads' row set of a window that reaches the ingesters is not checked.

1. Against PromQL, as `query="bounds_check"`. For each range, the truth is `count by (__name__) (last_over_time({__name__=~".+"}[<range length>]))` evaluated at the range's end minus 1 ms, which counts the series with a sample in the range, so the window is the range's, not the requested one. A group passes (`within_bounds`) when `lower <= truth <= upper`. It is a `bounds_violation` (mismatch) when the truth is above `upper`, or in a group the answer lacks, unless the range has `lower_bound`, which can leave `upper` short too: then it is `flagged` as `incomplete`. A truth below `lower` can be the edge: a block's index lists a series whose last sample is just before the range and whose staleness marker is in it, which PromQL doesn't count. The check runs the `edge` probe of the pairs (see below) on the range's window, and a group whose `lower` is above the truth by no more than the probe counted for it is `tolerated` as `edge`; any other is a `bounds_violation`. A tenant above `-max-reference-series` is checked as the tenant-wide tables are: the names at `-name-ranks` of each range, one query each, and a grouped truth that hits a query limit falls back to them. A name whose own query hits a query limit is skipped, counted in `cardinality_estimate_compare_refusals_total{side="truth", class="promql_limit"}`, and the other names are still checked; the check is refused only when every name is. The relative width `(upper - lower) / lower` of every group with a `lower` above 0 is observed in `cardinality_estimate_compare_bounds_relative_width`, so the width of the answer can be read next to its correctness.
2. Against the exact route, as `query="bounds_exact_check"`, which needs no PromQL. For each name at `-name-ranks` of a range, the tool sends an exact estimate of `{__name__="<name>"}` over the range's window, in the default mode, and the group passes when `lower <= exact <= upper`. An exact answer's own flags allow a difference in their direction (`lower_bound` or `coverage_gap` below `lower`, `upper_bound` above `upper`), which is `flagged`.

Both checks count every group they compare in `cardinality_estimate_compare_groups_total`, passes included, with `pass` and `within_bounds`. A check with no range that has groups is `pass` and `empty`.

Each bounds request of a check or of target picking logs one `msg="bounds range"` line per range it answers, with `purpose` (`bounds_check` or `targets`), the range, `main_set`, `main_blocks`, `other_blocks`, `lower_bound`, `group_count`, `lower_total`, `upper_total`, `total_width` (`(upper_total - lower_total) / lower_total`), `has_more`, the request's `latency`, and `first=true` on the run's first bounds request, which reads with cold caches; it is target picking's unless `-target-source` is `label_values`. A window that reaches the ingesters adds one `msg="bounds head"` line with the heads' totals, and a window with parts no range answers adds a `msg="bounds not covered"` line.

## Burst

With `-burst N`, the first round and then one round every `-burst-interval` (default 1h) send N estimates at once before the round's pairs. The estimates are drawn in turn from the round's pairs, and no PromQL truth goes with them. A burst fills the count limits, which one request at a time never does: the store-gateway's `-blocks-storage.bucket-store.cardinality-estimate-max-concurrent` (8, which queues until the queue timeout) and the ingester's `-ingester.cardinality-estimate-max-concurrent` (8, which refuses at once). The route answers 503 `busy` when every source it asked is at its limit. Use N = 16 to fill both; N is capped at 64. The burst records how each estimate was answered and how long it took, and logs one line per burst. Check during a burst that the cell's other queries stay level, on the cell's dashboards. With `-burst-queries`, the burst draws only from the named queries: a burst of the tenant-wide tables reads every series of the tenant N times at once, so on a large tenant name the single-metric queries, such as `-burst-queries metric_total`. A burst logs one more line per status that is not 200, with the first error of that status.

With `-burst-bounds N`, each burst also sends N bounds requests at once over the round's first `blocks` window, recorded with `query="bounds_burst"`. Their head rows come from the ingesters' label values cardinality RPC, which takes no estimate count permit, so they fill only the store-gateway's count limit. N is capped at 64; 0 (default) sends none.

## Estimate only

With `-estimate-only`, each drawn and scenario pair sends only its estimate, without the PromQL truth, and nothing is compared. Every estimate, including target picking and bursts, is sent without `X-Read-Consistency: strong`, as a dashboard sends it, so its latency has no wait for the ingesters to read what was written before it; a compared run needs that wait so both sides see the same head. Use it after a compared run has shown the counts are right, for two things a compared run can't give: latency samples at a rate the PromQL load would not allow, and the estimate's own CPU, memory and bytes on the store-gateways and ingesters, which a compared run mixes with the truth's. A pair counts in `cardinality_estimate_compare_pairs_total` with `order="estimate_only"` and `outcome="answered"` or `refusal`, and its latency in `cardinality_estimate_compare_request_duration_seconds` with `position="alone"` and `reference="none"`.

## Scenarios

`-scenario <file>` adds hand-written pairs: queries a sizing question needs that the drawn pairs may never pick, or a pair that mismatched, written down so it can be rerun exactly. Every round runs each scenario once, on top of the `-pairs-per-round` drawn pairs, on a window drawn from the ones it allows. A scenario no window fits is skipped for that round and logged. Its pairs carry `query="scenario:<name>"` and go through the same comparison and classes.

```yaml
- name: api_by_pod                       # required, unique
  selector: '{__name__="up", job="api"}' # required, one match[] selector
  group_by: pod                          # default __name__
  windows: [24h]                         # default -windows
  kinds: [recent]                        # default every kind: blocks, ingesters, boundary, recent
  limit: 20                              # default 0, the whole table
  ```

Keep a scenario file that names a tenant's metrics and labels outside the repository.

## Queries

| Query | `match[]` | `group_by` |
|---|---|---|
| `tenant_by_name` | `{__name__=~".+"}` | `__name__` |
| `metric_by_label` | one large metric | one of its high-cardinality labels; `group_count` gives the label's distinct values |
| `matcher_by_name` | `{<matcher-label>="<a large value>"}` | `__name__` |
| `metric_total` | one large metric | `__name__` |
| `tenant_by_label` | `{__name__=~".+"}` | `-matcher-label` |
| `tenant_label_vals` | `{__name__=~".+"}` | the tenant's highest-cardinality label |
| `name_regex` | `{__name__=~"<prefix>.*"}`, the prefix being a large name up to its first `_` | `__name__` |
| `metric_not_equal` | `{__name__="<name>", <facet>!="<its largest value>"}`; the series without the facet label count, as in PromQL | the facet label |
| `metric_label_regex` | `{__name__="<name>", <facet>=~"<v1>\|<v2>"}` | the facet label |
| `two_matchers` | `{__name__="<name>", <facet>="<a large value>"}` | another of the name's labels, or `__name__` |

The bounds checks (`bounds_check` and `bounds_exact_check`, see above) are not drawn from `-queries` and don't count in `-pairs-per-round`.

## Outcomes and classes

Each pair and each differing group has an outcome: `pass`, `tolerated` (a class explains the difference), `flagged` (the answer's own flags allow a difference in that direction, but bound neither its size nor its cause, so it is no evidence that the estimate is right), `mismatch` (no class explains it: file a Bug issue with the logged pair), or `refusal`.

| Class | Outcome | When |
|---|---|---|
| `exact` | pass | every group equal |
| `empty` | pass | both sides have no series in the window; for a bounds check, no range answered with groups |
| `within_bounds` | pass | bounds checks only: `lower <= truth <= upper` |
| `bounds_violation` | mismatch | bounds checks only: the truth is outside the bounds, the range has no `lower_bound` that explains it above `upper`, and no edge series explains it below `lower` |
| `incomplete` | flagged | for a bounds check, the range has `lower_bound` and the truth is above `upper`. Otherwise the answer has `lower_bound` or `coverage_gap`, and the group is below the truth but not below a truth over the part of the window the answer says it counted in full. That part starts at `head_min_time` when the answer has it, since the ingesters hold everything after it; otherwise it is the longest part of the window outside every `missing_shards` range. An answer with `missing_blocks` or a `coverage_gap` and no `head_min_time` doesn't say which part it left out, so those groups are flagged unbounded |
| `upper_bound` | flagged | the answer has `upper_bound`, which the route sets when head counts of several partitions were added up so a series may count twice, and the group is above the truth. The flag allows that direction and bounds neither the size nor the cause |
| `answer_changed` | tolerated | the group mismatched, the rerun agrees, and the truth changed between the runs: compaction or late out-of-order blocks between the calls |
| `edge` | tolerated | for a bounds check, `lower` is above the truth by no more than the series the same probe counts. Otherwise the group is above the truth by no more than its series with a sample in the `-edge-slack` before the window and none in it, counted as `count by (<label>) (last_over_time(<selector>[<slack>] offset <window>) unless last_over_time(<selector>[<window>]))`. The estimate counts a series when one of its chunks overlaps the window, which such a series can do with its staleness marker in the window, which PromQL drops, or with a gap across the window. A series that starts after the window has no chunk in it, so the probe doesn't look there. Set `-edge-slack` (default 5m) to the tenant's largest scrape interval |
| `incomplete_undercount` | mismatch | an `incomplete` group below a truth over the part of the window the answer says it counted in full |
| `group_count_excess` | mismatch | a tenant-wide table truthed one group at a time has a `group_count` above the label's values over the window in `/api/v1/label/<label>/values`, plus one for the `""` group of series without the label (see below) |
| `estimate_unstable` | mismatch | the group mismatched and the rerun agrees, but only the estimate changed between the runs: the estimate is not deterministic |
| `window_echo` | mismatch | the route counted another window than the one asked for |
| `unclassified` | mismatch | anything else |
| `no_count_rpc` | refusal | a store-gateway or an ingester without the count RPCs answered (422, "does not support cardinality estimates") |
| `memory_limit` | refusal | the estimate refused the request on `-querier.cardinality-estimate-max-memory-bytes` (422), from a store-gateway, an ingester or the querier. Recognised by the error text: the flag name, "cardinality estimate memory limit" (source messages) or "amount of memory consumed by a single query" (the querier's tracker) |
| `groups_limit` | refusal | the estimate refused the request on `max_groups` (422), recognised by `max_groups` or `-querier.cardinality-estimate-max-groups` in the error text |
| `estimate_refused` | refusal | the estimate refused the request on another limit (422), or another 4xx |
| `promql_limit` | refusal | the truth hit a per-query limit (422 with an `err-mimir-max-*` error ID) |
| `bad_request` | refusal | either side answered 400: the request fails the same way every round |
| `throttled` | refusal | either side answered 429 |
| `truncated` | refusal | a PromQL answer has the warning "results truncated due to limit", or a label values answer has any warning: the querier lowers a label values request to `-querier.max-label-values-limit` with a warning and no error, so the answer holds fewer values than it was asked for. Such a table can't stand for the whole one, so it is not compared, and a label values answer is not used as the bound on `group_count` |
| `busy` | refusal | the estimate answered 503 with `errorType` `unavailable`: every store-gateway or ingester it asked was at its count limit. Safe to retry; a 503 timeout is an `error` |
| `error` | refusal | any other failure, such as a 5xx or a timeout |

A refusal is classed by its status and its error text: the `error` field of a Prometheus API error body (`status`, `errorType`, `error`), which the route sends for every error status, or the whole body otherwise.

With `has_more`, only the groups in the table are compared, plus `group_count` and `total` against the whole truth. The tenant-wide tables ask for `-table-limit` (default 50) groups. When the truth of a table hits a per-query limit (`promql_limit`), the refusal is counted and the estimate's `-fallback-names` (default 5) largest groups are counted one at a time instead, each with an equality matcher on the group's label in place of the selector's matchers on it; `total` and the other groups are then not checked, and `group_count` only for a tenant-wide table (see below). A 429 or a 5xx does not start the fallback.

When the tenant had more than `-max-reference-series` (default 100,000) series over the last hour at target picking, `tenant_by_name`, `tenant_by_label` and `tenant_label_vals` skip the grouped truth, which a per-query series limit would refuse after loading the cell, and go straight to the per-group truth. Those pairs always send the estimate first, since the truth needs its groups. Set `-max-reference-series` to the tenant's max fetched series per query; 0 never skips.

`-max-truth-series` (default 1,000,000) is the most series any PromQL truth may count, going by the estimate, which is then always sent first. Query sharding splits a truth so that each shard stays under the per-query limits, so those limits don't stop a count of millions of series from filling the queriers. A grouped truth whose estimate total is over the limit is not sent and is replaced by the per-group truth; a per-group truth leaves out the groups over the limit and takes the next largest; a bounds check leaves out the names at `-name-ranks` whose `upper` is over it. A pair with nothing left to count is a refusal of class `truth_too_large`, with no PromQL query sent. 0 sends every truth. The order of a compared pair still alternates when its metric name is known to be small: target picking read the name's series over the target window, and they are at most a quarter of `-max-truth-series`. Tenant-wide pairs, matcher pairs and scenario pairs, whose size is not known before the estimate answers, always send the estimate first.

A tenant-wide table truthed one group at a time, after the skip or the fallback, has its `group_count` checked against an upper bound: the label's values over the window from `/api/v1/label/<label>/values`, asked for with `limit` one above `group_count`. The label values API reads the values from the index of every block and head that overlaps the window, without checking samples, so every group the estimate counts is among them. A `group_count` above them is `group_count_excess`. A label values answer with a warning is not used, since the querier may have lowered its limit (`truncated`, counted in the refusals). The querier cuts a label query to the tenant's `-store.max-labels-query-length`, so keep `-windows` within it, or such a pair can be a false `group_count_excess`. An undercount of `group_count`, and `total`, are not checked: no cheap query gives a reference for them, since any PromQL count of them reads every series of the tenant over the window, which is the load the skip avoids, and splitting it into queries by name prefix spreads the same reads over more queries. Other selectors keep no check of `group_count` in the fallback, since the label values API with a selector may not read the same series.

## Output

One log line per pair, with the differing groups of a mismatch. The log line of a mismatch whose truth was one grouped query has `truth_query` and `truth_at`, the query and its evaluation time in seconds, to run it by hand and compare. With `-interval 0` the tool runs one round and exits 1 if a pair mismatched, 3 if no pair passed, was tolerated or, with `-estimate-only`, answered (every pair refused or flagged), and 0 otherwise. With `-listen`, `/metrics` serves:

- `cardinality_estimate_compare_pairs_total{query, window, order, outcome, class, read, source}`, where `read` and `source` are the estimate's, empty when it didn't answer
- `cardinality_estimate_compare_flags_total{query, flag}`, one per answered estimate of a pair for each flag set: `lower_bound`, `coverage_gap`, `head_min_time`, `missing_blocks`, `missing_shards`, `upper_bound`, `has_more` or `dedup`. The log line of each pair has `read`, `source` and `flags` too
- `cardinality_estimate_compare_groups_total{query, outcome, class}`
- `cardinality_estimate_compare_refusals_total{query, side, class}`
- `cardinality_estimate_compare_request_duration_seconds{query, window, length, side, position, reference, read, source}`, client-side latency of each side of a pair that answered, once per pair: a refused or failed request is not observed, and a truth asked one value at a time is the sum of its queries. `position` is `first`, `second`, or `alone` with `-estimate-only`, `reference` is `grouped`, `per_value`, or `none` with `-estimate-only`, and `read` and `source` are the estimate's for both sides. Reruns and probes are not observed. The top bucket is above `-timeout`
- `cardinality_estimate_compare_bounds_relative_width`, the `(upper - lower) / lower` of every group of a bounds check with a `lower` above 0. A group with a `lower` of 0 has no relative width and is left out
- `cardinality_estimate_compare_rounds_total{result}` and `cardinality_estimate_compare_last_round_timestamp_seconds`
- `cardinality_estimate_compare_bursts_total`, `cardinality_estimate_compare_burst_requests_total{query, status}` and `cardinality_estimate_compare_burst_request_duration_seconds{status}`, where `query` is the pair's query or `bounds_burst`, `status` is the HTTP status, `busy` for a 503 whose sources were all at their count limit, `timeout` or `error`
