---
name: search-expr
description: Construct and run queries against Mimir's experimental fuzzy search API (search[] terms and search_expr boolean expressions) on /api/v1/search/metric_names, label_names, and label_values, to discover a metric, label, or label value without knowing its exact spelling. Use when composing a search_expr with AND/OR/NOT, choosing a fuzz_alg or fuzz_threshold, or testing this repo's search-filter-composition changes against a running Mimir.
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

| Task | Call |
|---|---|
| Does exact name `X` exist? | `GET /label/__name__/values?match[]={__name__="X"}` (exact `=`, not `=~`) |
| Name partly known, or only descriptive words | `/search/metric_names` with `search_expr=w1 and w2 and w3`, `fuzz_alg=substring_left` |
| Name may contain a typo | AND the correctly spelled parts: `keda and scaled and errors`. If that returns 0: `fuzz_alg=jarowinkler&fuzz_threshold=30&sort_by=score&include_score=true` on the full term |
| Plain-language question ("how many queries are in flight on the query-frontend?") | AND the component, OR the synonyms: `query_frontend and (inflight or in_progress or queue)`. Add `include_metadata=true` and choose by the help text |
| Remove other products, histograms, recording rules | add `and cortex and not loki and not _bucket and not :`. Do not add `not _count` or `not _sum` to a whole query: they also remove real metrics such as `memberlist_client_cluster_members_count`. Put them only in the group for a histogram metric |
| Label names on a metric | `/search/label_names?match[]={__name__="M"}` |
| All values of a label | `/search/label_values?label=L&match[]={__name__="M"}`, no `search_expr` |
| Values matching text | same, plus `search_expr=<text>&case_sensitive=false` (values are often upper case, for example `ACTIVE`) |
| Complete list or exact count | `limit=0`, one call. Read the count from the trailer's `returned` field; do not count results yourself. Follow `next_cursor` only if `has_more` is `true`, and add up `returned` across pages. A `limit=0` call always carries an "enforced: N" warning; ignore it unless `returned` equals N |
| Does anything like `X` exist (no count needed) | `limit=5`, stop at the first page |

## Several lookups in one session

When you have several metric-name lookups to do, put them in one
`/search/metric_names` call as OR groups, then match each result to its
task. One call costs one turn; six calls cost six.

```
search_expr=cortex and not _bucket and not : and
  ((ingester and ingested and samples) or (alertmanager and notifications and failed)
   or (compactor and blocks and cleaned) or (query_scheduler and queue and length))
```

Set `limit=0`. A `search_expr` takes at most 32 terms, so split larger sets
into more calls. Label-name and label-value lookups are separate endpoints;
group those by metric and label instead. Exact-name existence checks can
share one call too: `(name_a or name_b or name_c)`. A name not in the
result had no series in the window; before you report it as absent,
widen the window (see "Time window").

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
  and change only if it returns 0 results.
- Do not use a `match[]` regex (`=~`) to search names. It does a full series
  scan and was about 1.6x slower than `search_expr` on a broad query.
- Do not download the full `__name__` list to filter it yourself.
- Do not send `search[]` and `search_expr` together (HTTP 400).
- Do not use `fuzz_alg=jarowinkler` with `fuzz_threshold=0`. It is then a
  plain substring match.
