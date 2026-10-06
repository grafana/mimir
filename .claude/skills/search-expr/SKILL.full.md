---
name: search-expr
description: Construct and run queries against Mimir's experimental fuzzy search API (search[] terms and search_expr boolean expressions) on /api/v1/search/metric_names, label_names, and label_values, to discover a metric, label, or label value without knowing its exact spelling, and on /api/v1/search/metadata, to find a metric from words in its name or HELP text. Use when composing a search_expr with AND/OR/NOT, choosing a fuzz_alg or fuzz_threshold, or testing this repo's search-filter-composition changes against a running Mimir.
---

# Mimir search API — quick reference

This page is enough for most tasks. Read `reference.md` in this directory
only if a call returns an error or an answer looks wrong.

## Cost rule

Each tool call costs one full agent turn, and each turn re-sends the whole
conversation. Response size is a small cost. Plan the one call that answers
the task, run it, then answer. Target: one HTTP call per task, or fewer
when one call can answer several tasks (see "Several lookups in one
session").

## Pick the call

Base URL: `http://<host>/prometheus/api/v1`. Send `X-Scope-OrgID: <tenant>`.

If the call has no `search[]` and no `search_expr` (you want all results),
use the legacy `/labels` or `/label/<name>/values` API. Do not use a
`/search/*` endpoint for it.

| Task | Call |
|---|---|
| Does exact name `X` exist? | `GET /label/__name__/values?match[]={__name__="X"}` (exact `=`, not `=~`) |
| Name partly known, or only descriptive words | `/search/metric_names` with `search_expr=w1 and w2 and w3`, `fuzz_alg=substring_left` |
| Name may contain a typo | AND the correctly spelled parts: `keda and scaled and errors`. If that returns 0: `fuzz_alg=jarowinkler&fuzz_threshold=30&sort_by=score&include_score=true` on the full term |
| Metric described by what it measures, in words unlikely to be in its name | `/search/metadata` with `search_expr=w1 and w2`. See "Searching by description" |
| Plain-language question ("how many queries are in flight on the query-frontend?") | AND the component, OR the synonyms: `query_frontend and (inflight or in_progress or queue)`. Add `include_metadata=true` and choose by the help text |
| Keep to one product | add `and cortex and not loki and not tempo` |
| Remove recording rules | add `and not :` |
| Remove histogram series (plain count, rate or gauge questions only) | add `and not _bucket`. Do not add `not _count` or `not _sum` to a whole query: they also remove real metrics such as `memberlist_client_cluster_members_count` |
| Latency, duration, "how long", percentile (p50, p95, p99) or distribution questions | keep the histogram series: do not use `not _bucket`, `not _count` or `not _sum`. The answer is the `_bucket` series |
| Label names on a metric | `GET /labels?match[]={__name__="M"}` (legacy API). `/search/label_names` needs a `search[]` or `search_expr` and returns HTTP 400 without one |
| All values of a label | `GET /label/L/values?match[]={__name__="M"}` (legacy API) |
| Values matching text | same, plus `search_expr=<text>&case_sensitive=false` (values are often upper case, for example `ACTIVE`) |
| Rate or ratio questions (hit rate, error rate, failure ratio) | also find the matching total: OR `requests` or `total` into the group, for example `frontend and cache and (hit or requests)`. Report both metrics |
| Complete list or exact count | `/search/metric_names`, `limit=0`, one call, with only the terms the task states: do not add `not _bucket` or `not :` (they change the count), and do not count with a legacy `=~` regex (its default window is longer than 1h). Read the count from the trailer's `returned` field; do not count results yourself. Follow `next_cursor` only if `has_more` is `true`, and add up `returned` across pages. A `limit=0` call always carries an "enforced: N" warning; ignore it unless `returned` equals N |
| Does anything like `X` exist (no count needed) | `limit=5`, stop at the first page |

## Writing the expression

- Use short word stems that appear in metric names (`inflight`, not
  `cortex_distributor_inflight_push_requests`).
- Use the singular stem: `block` matches `block_cleanup` and `blocks_cleaned`;
  `blocks` misses `cortex_compactor_block_cleanup_failed_total`.
- Use only words that appear in metric names. `tenant`, `user`,
  `namespace` and `pod` are usually label names, so do not require them.
- Some Mimir components are named differently from their metrics: the
  store-gateway's metrics are `cortex_bucket_store_*` and
  `cortex_bucket_stores_*` (search `bucket_store`, not `store_gateway`),
  and per-tenant limits and overrides are `cortex_limits_*`.
- Every OR branch must contain a positive term. An expression made only of
  exclusions (for example `not active`) is rejected.
- If a search returns 0 or 1 metrics, loosen it once: drop the least
  important AND term, or add a synonym with OR. Then search again.
- Check parameter names. An unknown parameter (for example
  `search_expression` instead of `search_expr`) is ignored.
  `/search/metric_names` and `/search/label_names` then have no search
  parameter and return HTTP 400. `/search/label_values` returns unfiltered
  results with no warning. To get unfiltered results, use the legacy API.

## Several lookups in one session

When you have several metric-name lookups to do, put them in one
`/search/metric_names` call as OR groups, then match each result to its
task. One call costs one turn; six calls cost six.

```
search_expr=cortex and not _bucket and not : and
  ((ingester and ingested and samples) or (alertmanager and notifications and failed)
   or (compactor and block and cleaned) or (query_scheduler and queue and length))
```

Set `limit=0`. A `search_expr` takes at most 32 terms, so split larger sets
into more calls. Label-name and label-value lookups are separate endpoints;
group those by metric and label instead. Exact-name existence checks can
share one call too: `(name_a or name_b or name_c)`. A name not in the
result had no series in the window; before you report it as absent,
widen the window (see "Time window").

## If you have a shell

Run all the lookups you can plan in one Bash call, and print only what
you need. Each Bash call costs a turn; output you filter away costs
nothing.

```bash
B='http://<host>/prometheus/api/v1'; H='X-Scope-OrgID: <tenant>'
q() { curl -s -G -H "$H" --data-urlencode "search_expr=$2" \
  --data-urlencode case_sensitive=false --data-urlencode fuzz_alg=substring_left \
  --data-urlencode limit=0 "$B/search/metric_names" -o "out.$1"
  echo "$1: $(jq -r '.results[]?.name' "out.$1" | grep -vE '_(count|sum)$' | tr '\n' ' ')"
  echo "$1: $(jq -c 'select(.status) | {returned, has_more}' "out.$1")"; }
q A 'cortex and not _bucket and not : and ((ingester and ingested and samples) or (compactor and block and cleaned))'
q B 'cortex and query_frontend and (inflight or in_progress)'
```

Tag each search (`A`, `B`) so you can map results to tasks. Print names
only on the first pass: help text for every result makes the output so
large that it is saved to a file, and reading it back costs more turns.
When two or three candidates remain for a task, fetch their type and help
in one call (`include_metadata=true` with a `search_expr` of just those
names). The parentheses in a jq filter that prints both results and the
trailer are needed: without them the trailer line is not printed. Do the same for
`/search/label_names` and `/search/label_values` lookups (print
`.results[]?.name` or `.results[]?.value`). Prefer this over downloading
full `/api/v1/label/__name__/values` lists to grep: the search call sends
the server and the network a small fraction of the work.

## Searching by description

`/search/metric_names` matches metric names only, never help text.
`include_metadata=true` adds type and help to results, but does not search
them.

Some tasks describe what a metric does, in words that are unlikely to be in
its name (for example "blocks deleted because the maximum time limit was
exceeded" is `cortex_ingester_tsdb_time_retentions_total`). Do not guess
name words for those: in benchmarks, agents spent up to 6 searches guessing
and still missed. Use `/search/metadata`. It searches the metric name and
its HELP text together, as one text:

- Each term can match in the name or in the HELP. `ingester and ratio`
  finds a metric with `ingester` only in its name and `ratio` only in its
  HELP. A `not` term excludes the metric if it is in either.
- A term matches only at the start of a word: `ratio` matches "Ratio",
  "ratios" and `x_ratio`, but not "duration" or "operations". `_` starts a
  new word in names.
- Case is ignored by default.
- Quote a phrase: `"maximum time limit"` matches those words in that order.

Put every description task of the session in one call, as OR groups. Each
result carries `type` and `help`, so match each result to its task by its
help text:

```bash
curl -s -G -H "$H" --data-urlencode limit=0 \
  --data-urlencode 'search_expr="maximum time limit" or "closer than the configured maximum gap"' \
  "$B/search/metadata" |
  jq -c '(.results[]? | {name, help: ((.help // "")[0:80])}), (select(.status) | {returned, has_more})'
```

Do not send `fuzz_alg`, `fuzz_threshold`, `match[]`, `start`, `end`,
`include_metadata`, `include_score` or `sort_by=score` to
`/search/metadata`: each returns HTTP 400. The endpoint takes one tenant
only.

Metadata comes from ingesters only. If `/search/metadata` finds nothing for
a task, loosen it once (fewer words, or one word per OR branch). If it
still finds nothing, fall back to one full metadata download for the
session:

```bash
curl -s -H "$H" "$B/metadata" -o meta.json
jq -r --arg p '<phrase>' '.data | to_entries[] |
  select(any(.value[]; .help | test($p; "i"))) | .key' meta.json
```

The download is large (2.9 MB and 15.5 s for 19,183 metrics on
mimir-dev-13), so do it at most once per session and reuse the file.

## Always set

- `case_sensitive=false` (the default is `true`).

## Time window

- Leave `start` unset in most cases. The default is the last 1h: the
  cheapest search, and it shows what exists now. Measured on mimir-dev-13:
  querier pods 81 (1h) vs 6,270 (`start=0`, mostly pods that no longer
  exist); metric names 1,405 (1h) vs 1,435 (`start=0`, +2%).
- Widen with a bounded `start` (now-24h or now-7d, as a Unix timestamp)
  only when:
  - you are confirming that something does NOT exist (some metrics only
    appear when an event occurs), and the 1h search found nothing;
  - the task asks about a past period ("ever", "last week");
  - you must match legacy `/api/v1/label` output, whose default window is
    much longer.
- Do not use `start=0`. It is clamped to the tenant's
  `-store.max-labels-query-length`, queries the store-gateways for that
  whole range, and was the slowest window measured (up to 9.6 s here).

## Template

```bash
curl -s -G -H 'X-Scope-OrgID: <tenant>' \
  --data-urlencode 'search_expr=ingest and storage and reader and offset' \
  --data-urlencode 'case_sensitive=false' \
  --data-urlencode 'fuzz_alg=substring_left' \
  'http://<host>/prometheus/api/v1/search/metric_names'
```

The response is NDJSON: one or more `{"results":[{"name":...}]}` lines,
then a trailer `{"status":"success","has_more":<bool>,"returned":<count>,"next_cursor":...}`.
Older Mimir builds have no `returned` field. Then each result line holds
`batch_size` results (default 100), except the last.
To get the next page, send `cursor=<next_cursor>` as the only parameter.

## Do not

- Do not try several `fuzz_alg` values to compare. Use `substring_left`,
  and change only if it returns 0 results. The only valid values are
  `substring_left`, `substring`, `subsequence` and `jarowinkler`; any other
  value (for example `prefix`) returns HTTP 400.
- Do not use a `match[]` regex (`=~`) to search names. It does a full series
  scan and was about 1.6x slower than `search_expr` on a broad query.
- Do not download the full `__name__` list to filter it yourself.
- Do not send `search[]` and `search_expr` together (HTTP 400).
- Do not use `fuzz_alg=jarowinkler` with `fuzz_threshold=0`. It is then a
  plain substring match.
