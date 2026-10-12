---
name: search-expr
description: Find Mimir metrics, labels and label values without the exact spelling, with the experimental /api/v1/search/* API (search_expr AND/OR/NOT, fuzz_alg) and /api/v1/search/metadata (name and HELP text).
---

# Mimir search API — quick reference

Read `reference.md` in this directory only if a call returns an error or an
answer looks wrong.

**Cost rule:** each tool call is one agent turn, and each turn re-sends the
whole conversation, so one call costs one turn and six calls cost six.
Response size is a small cost. Use 2 Bash calls in total, then answer.
In one benchmark round, a session with 2 Bash calls used 230K tokens and
one with 5 used 450K. Do not use a separate call to set up, to look at
saved files, or to check one task:

1. One Bash call with every lookup you can plan: label lookups, one
   `/search/metadata` call, and the metric-name lookups as OR groups,
   4 to 8 tasks per `search_expr` (at most 32 terms). Do not send one
   search per task: in benchmarks 54 single-task searches took 70 s of
   HTTP time, and 15 grouped searches took 28 s.
2. One Bash call that fixes every weak task at once: split `TRUNCATED`
   searches, loosen searches that found 0 or 1 metrics, and get type and
   help for tasks with 2 or 3 candidates. This is the last HTTP call.
3. Answer. Never leave a task blank: report the best candidate.

## Pick the call

Base URL `http://<host>/prometheus/api/v1`, header `X-Scope-OrgID: <tenant>`.
Metric names are lower case: send `case_sensitive=true` with lower-case
terms, as `q` and `c` do. Send `case_sensitive=false` only when the stored
case is unknown or mixed, as for label values (`ACTIVE`, CamelCase
`alertname`): `v` and `vc` do this. With no search term (all results), use
the legacy `/labels` or `/label/<name>/values` API, not `/search/*`.

| Task | Call |
|---|---|
| Does exact name `X` exist? | `/label/__name__/values?match[]={__name__="X"}` (exact `=`) |
| Name partly known, or descriptive words | `/search/metric_names`, `search_expr=w1 and w2`, `fuzz_alg=substring_left` |
| Name may contain a typo | AND the correctly spelled parts (`keda and scaled and errors`). If 0 results: `fuzz_alg=jarowinkler&fuzz_threshold=30&sort_by=score&include_score=true` on the full term |
| Described by what it measures, in words unlikely to be in its name | `/search/metadata`. See "Searching by description" |
| Plain-language question | AND the component, OR the synonyms: `query_frontend and (inflight or in_progress or queue)` |
| Keep to one product | add `and cortex and not loki and not tempo` |
| Remove recording rules | add `and not :` |
| Remove histogram series (plain count, rate or gauge questions only) | add `and not _bucket`. Never `not _count` or `not _sum`: they remove real metrics such as `memberlist_client_cluster_members_count`. Do not use `not _bucket` in a search for store-gateway metrics: it also removes `cortex_bucket_store_*` |
| Latency, duration, percentile or distribution | keep histogram series (no `not _bucket/_count/_sum`). The answer is the `_bucket` series |
| Rate or ratio (hit rate, error rate) | also find the total: `frontend and cache and (hit or requests)`. Report both |
| Exact count or complete list | `/search/metric_names`, `limit=0`, only the terms the task states: no `not _bucket` or `not :`, and no legacy `=~` count (its window is longer than 1h). Read the trailer's `returned`. If `has_more` is `true`, send `cursor=<next_cursor>` and add up `returned`. Ignore the "enforced: N" warning unless `returned` equals N |
| Does anything like `X` exist | `limit=5` |
| Task names a past period ("last 7 days", "ever", "has it ever failed") | widen that search in pass 1: `q K 'partition and lifecycler and reconcil' --data-urlencode start=$(( $(date +%s) - 604800 ))`. Failure and error counters often have no series in the last 1h, so the default window misses them |
| Metric in a given namespace, job or pod | the legacy call with `match[]={__name__="M",namespace="N"}`. If it returns no series, the answer is not found, even though `M` exists elsewhere |
| Label names on a metric | legacy `/labels?match[]={__name__="M"}` |
| Values of a label that match text (pod, container_id, alertname, namespace, any label) | `/search/label_values?label=L&search_expr=<text>`, with `case_sensitive=false` and `fuzz_alg=substring_left`. No `match[]` is needed. Values are often upper case (`ACTIVE`). See "Label values" |
| All values of a label on one metric, namespace or cluster (list or count) | `/search/label_values?label=L` with `match[]={__name__="M",namespace="N"}` and no `search_expr`, for example `v U user '' --data-urlencode 'match[]={__name__="M",namespace="N"}'`. Its default window is the last 1h. Legacy `/label/L/values` has a longer default window, so its lists and counts can be larger; use it only with `start` |

## Writing the expression

- Use short stems that appear in metric names (`inflight`).
- Use the singular stem: `block` matches `block_cleanup` and
  `blocks_cleaned`; `blocks` misses `cortex_compactor_block_cleanup_failed_total`.
- `tenant`, `user`, `namespace` and `pod` are label names: do not require them.
- Store-gateway metrics are `cortex_bucket_store_*` and
  `cortex_bucket_stores_*` (search `bucket_store`). Put these tasks in a
  search with no `not _bucket`, which removes every `cortex_bucket_store_*`
  name. Querier metrics about
  the store-gateway use `storegateway` (`cortex_querier_query_storegateway_*`),
  so search `(storegateway or store_gateway)`. Per-tenant limits are
  `cortex_limits_*`.
- Every OR branch needs a positive term (`not active` alone is rejected).
- 0 or 1 results: loosen once (drop an AND term or OR a synonym), then search again.
- A misspelled parameter (`search_expression`) is ignored: `/search/metric_names`
  then returns HTTP 400, `/search/label_values` returns unfiltered results.
- Valid `fuzz_alg`: `substring_left`, `substring`, `subsequence`,
  `jarowinkler`. Use `substring_left`; do not try others to compare.
  `jarowinkler` needs `fuzz_threshold` above 0.
- At most 32 terms per `search_expr`.

## Run many lookups in one Bash call

Put 4 to 8 tasks in each search as OR groups, tag each search, and print
names only. Match each result to its task by name. Keep every Bash output
under about 20 KB: output larger than that is saved to a file, and reading
it back costs extra turns. `q` prints the count and at most 100 names per
search. A `TRUNCATED` line means answers may be hidden: in pass 2, split
that search into smaller groups and search again. Put a branch that matches
many names (for example `throttl` or `restart`) in a search with fewer other
tasks. For a count task, use `c`: it prints only `returned`, never the names.

Copy the helper functions below exactly. Add your own curl options after
the tag and the expression. Do not rewrite them with `${var:+...}`: the
shell can be zsh, which passes such an expansion as one argument, and every
call then fails.

```bash
B='http://<host>/prometheus/api/v1'; H='X-Scope-OrgID: <tenant>'
q() { curl -s -G -H "$H" --data-urlencode "search_expr=$2" \
  --data-urlencode case_sensitive=true --data-urlencode fuzz_alg=substring_left \
  --data-urlencode limit=0 "${@:3}" "$B/search/metric_names" -o "out.$1"
  echo "$1 [$(jq -r 'select(.status) | "\(.returned) more=\(.has_more)"' "out.$1")]: $(jq -r '.results[]?.name' "out.$1" | grep -vE '_(count|sum)$' | head -100 | tr '\n' ' ')"
  n=$(jq -r 'select(.status).returned' "out.$1"); if [ "${n:-0}" -gt 100 ]; then echo "$1 TRUNCATED: $n names, 100 shown. A name not shown may still exist: split this search."; fi; }
c() { curl -s -G -H "$H" --data-urlencode "search_expr=$2" \
  --data-urlencode case_sensitive=true --data-urlencode fuzz_alg=substring_left \
  --data-urlencode limit=0 "${@:3}" "$B/search/metric_names" -o "out.$1"
  echo "$1 count: $(jq -r 'select(.status).returned' "out.$1")"; }
q A 'cortex and not : and ((ingester and ingested and samples) or (compactor and block and cleaned))'
q B 'cortex and query_frontend and (inflight or in_progress)'
c K 'cortex_ingester_'
```

When 2 or 3 candidates remain, get their type and help in one call:
`include_metadata=true` with a `search_expr` of those names. Do not download
the full `__name__` list to grep it.

If a task names a type (summary, histogram, counter, gauge) or asks for the
type, get the types in pass 1. Do not wait for 2 or 3 candidates. Run the
search with `include_metadata=true` and print the type of each name:
`jq -r '.results[]? | "\(.name) \(.type)"'`. Report the `type` field
exactly as returned. `unknown` is a valid type.

## Label values

Never send `search_expr` to the legacy `/label/L/values`: it is ignored, and
the call returns every value of the label. On mimir-dev-13 that was 7 MB in
67 s for `pod`, and 16 MB for `container_id` before the connection dropped.
Use `/search/label_values`, which filters on the server:

```bash
v() { curl -s -G -H "$H" --data-urlencode "label=$2" --data-urlencode "search_expr=$3" \
  --data-urlencode case_sensitive=false --data-urlencode fuzz_alg=substring_left \
  --data-urlencode limit=0 "${@:4}" "$B/search/label_values" -o "out.$1"
  n=$(jq -r 'select(.status).returned' "out.$1")
  echo "$1 [$n]: $(jq -r '.results[]?.value' "out.$1" | head -100 | tr '\n' ' ')"
  if [ "${n:-0}" -gt 100 ]; then echo "$1 TRUNCATED: $n values, 100 shown. A value not shown may still exist: search for it."; fi; }
vc() { curl -s -G -H "$H" --data-urlencode "label=$2" --data-urlencode "search_expr=$3" \
  --data-urlencode case_sensitive=false --data-urlencode fuzz_alg=substring_left \
  --data-urlencode limit=0 "${@:4}" "$B/search/label_values" -o "out.$1"
  echo "$1 count: $(jq -r 'select(.status).returned' "out.$1")"; }
v A alertname 'memcached or (inter and az)'
vc P pod 'ingester-zone-a'
v E user '9960' --data-urlencode 'match[]={__name__="M",namespace="N"}'
```

- One search per label. Put several narrow lookups on the same label in one
  `search_expr` as OR groups, and match the values to tasks. Keep count
  tasks and broad terms (for example `mimir` on `alertname`) out of those
  groups: run each as its own `vc` call.
- Use `match[]` only to reduce the scope across other labels, for example
  `v T tenant 'acme' --data-urlencode 'match[]={__name__="M",cluster="c"}'`.
  Do not put a matcher on the searched label in `match[]` (for `label=tenant`,
  no `tenant=~"…"`): put that text in `search_expr`. A regex on the searched
  label runs over all its values, twice when other matchers are present.
- For a count, use `vc`. It prints only `returned`. Do not count printed values.
- To check one exact value, search for it, with the same `match[]` scope as
  the task (`v E` above): 0 results means not found. Do this even when
  another task lists the same label. Do not look for it in a printed list:
  the helper prints at most 100 values, so the value can be cut off.
- `returned` stops at 10,000 (`has_more` is then `true`). For a label with
  more values than that (`pod`, `container_id`, `interface`, `uid`), always
  send a `search_expr`.

## Searching by description

`/search/metric_names` matches names only. `/search/metadata` searches the
name and HELP text together: each term can match in either, a term matches
at the start of a word (`ratio` matches `x_ratio`, not "duration"), and a
quoted phrase matches words in order. Put all description tasks in one call:

```bash
curl -s -G -H "$H" --data-urlencode limit=0 \
  --data-urlencode 'search_expr="maximum time limit" or "closer than the configured maximum gap"' \
  "$B/search/metadata" |
  jq -c '(.results[]? | {name, type, help: ((.help // "")[0:80])}), (select(.status) | {returned, has_more})'
```

Do not send `fuzz_alg`, `fuzz_threshold`, `match[]`, `start`, `end`,
`include_metadata`, `include_score` or `sort_by=score` to it (HTTP 400).
If it finds nothing after one loosening, download `$B/metadata` once
(2.9 MB) and search the help text with `jq`.

## Time window

- Leave `start` unset: the default last 1h is the cheapest and shows what
  exists now.
- Use a bounded `start` (now-24h or now-7d) only to confirm something does
  NOT exist, or when the task asks about a past period.
- Never `start=0`: it queries the store-gateways for the whole retention
  and was the slowest window measured.

## Regex matchers in `match[]`

Put name and value text in `search_expr`. If a call still needs a regex
matcher, such as a scope `pod=~"ingester-.*"` or a legacy fallback:

- Write contains-only alternations: `.*(?:raft).*|.*(?:metastore).*`.
  Legacy runs these at about the same speed as search.
- Do not write ordered terms such as `.*(?:warpstream.*produce).*`. With 3
  or more alternations, legacy was 5 to 11 times slower than search on
  mimir-dev-13. Match one term on the server, then check the order on the
  client: `grep 'warpstream.*produce'`.
- Do not use `(?i)`. It was 1.7 to 8.7 times slower at any alternation
  count. Write the term in its stored case.
- In `search_expr`, write ordered terms as AND (`warpstream and produce`).
  AND does not keep the order, so filter on the client if order matters.

## Response format

NDJSON: `{"results":[...]}` lines, then a trailer
`{"status":"success","has_more":<bool>,"returned":<count>,"next_cursor":...}`.
For the next page, send `cursor=<next_cursor>` as the only parameter.
Do not send `search[]` and `search_expr` together (HTTP 400). Do not use a
`match[]` `=~` regex to search names: it scans all series.
