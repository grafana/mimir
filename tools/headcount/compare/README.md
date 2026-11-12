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

It runs outside the cluster through a port-forward to the query-frontend, or inside it as a workload. Where it runs doesn't change the latency numbers, which come from the query-frontend's logs and metrics. Both requests carry `User-Agent: mimir-cardinality-estimate-compare`, so the query-frontend's query stats log lines of this tool can be filtered, and `Cache-Control: no-store`, so neither the results cache nor the estimate cache answers them.

Target picking reads `/prometheus/api/v1/cardinality/label_names`, so the tenant needs `-querier.cardinality-analysis-enabled`, and the estimate needs `-querier.cardinality-estimate-enabled`. Without the first, every round fails before it sends a pair.

Both requests load the cell's read path, and the truth queries count against the tenant's fetched-series and chunk limits. Keep `-pairs-per-round` and `-request-gap` at the rate agreed with the cell's owners.

## A round

1. Pick targets from the tenant's data over the last hour: the largest metric names from the estimate's name table, each name's highest-cardinality labels and the tenant's from `/api/v1/cardinality/label_names`, and the largest values of `-matcher-label`.
2. Fill each query with them on three window kinds for each length in `-windows`: `blocks` (ends before now minus `-query-ingesters-within`, store-gateways only), `ingesters` (inside it), and `boundary` (across it). Draw `-pairs-per-round` pairs at random.
3. For each pair, send the estimate and the truth, alternating which goes first. The first one is labelled `cold`, since it warms the index-headers and postings cache for the second, which is `warm`.
4. Diff the two tables group by group, and classify each differing group. Rerun a pair with an unexplained group once; the rerun's diff is the result. A group that mismatched and no longer does is `answer_changed`, and one that still mismatches stays a mismatch.

The truth is an instant query `count by (<label>) (last_over_time(<selector>[<window>]))` at the window's end minus 1 ms. The estimate counts `[start, end)` and a PromQL range at `t` selects `(t - window, t]`, so at `end - 1ms` both cover the same milliseconds. With a step, each bucket gets its own truth query with the step as the window.

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

## Outcomes and classes

Each pair and each differing group has an outcome: `pass`, `tolerated` (a class explains the difference), `mismatch` (no class does: file a Bug issue with the logged pair), or `refusal`.

| Class | Outcome | When |
|---|---|---|
| `exact` | pass | every group equal |
| `empty` | pass | both sides have no series in the window |
| `sketch` | tolerated | the group is `estimated` and within `-sketch-sigmas` (default 3) standard errors of the HLL sketch, 1.04/√2^p at the route's precision |
| `incomplete` | tolerated | the answer has `lower_bound`, `coverage_gap` or an error trailer, and the group is below the truth |
| `head_whole` | tolerated | the answer counts the whole head whatever the window, and the group is above the truth |
| `answer_changed` | tolerated | the group mismatched, and the rerun agrees: compaction or late out-of-order blocks between the calls |
| `edge` | tolerated | the group is above the truth, and not above a truth over the window widened by `-edge-slack` on both sides: samples just outside the window, or staleness markers inside it, which PromQL drops |
| `window_echo` | mismatch | the route counted another window than the one asked for |
| `unclassified` | mismatch | anything else |
| `no_count_rpc` | refusal | a store-gateway or an ingester without the count RPCs answered (422, "does not support cardinality estimates") |
| `estimate_refused` | refusal | the estimate refused the request (4xx) |
| `promql_limit` | refusal | the truth hit a query limit (422) |
| `error` | refusal | any other failure |

With `has_more`, only the groups in the table are compared, plus `group_count` and `total` against the whole truth. When the truth of a table by metric name hits a limit, the refusal is counted and the `-fallback-names` largest names are counted one at a time instead.

## Output

One log line per pair, with the differing groups of a mismatch. With `-interval 0` the tool runs one round and exits non-zero if a pair mismatched. With `-listen`, `/metrics` serves:

- `cardinality_estimate_compare_pairs_total{query, window, order, outcome, class}`
- `cardinality_estimate_compare_groups_total{query, outcome, class}`
- `cardinality_estimate_compare_refusals_total{query, side, class}`
- `cardinality_estimate_compare_request_duration_seconds{query, side, cache}`, end to end per request
- `cardinality_estimate_compare_rounds_total{result}` and `cardinality_estimate_compare_last_round_timestamp_seconds`
