# Streaming Search API Notes

## Endpoints

| Endpoint | Handler | Description |
|---|---|---|
| `GET/POST /api/v1/search/metric_names` | `SearchMetricNamesHandler` | Searches `__name__` label values |
| `GET/POST /api/v1/search/label_names` | `SearchLabelNamesHandler` | Searches label names |
| `GET/POST /api/v1/search/label_values` | `SearchLabelValuesHandler` | Searches values for a specific label |

## Response Format (NDJSON)

Each batch line: `{"results": [{"Value": "...", "Score": 0.0}, ...]}`
Final line (success): `{"status": "success", "has_more": true|false}`
Error line: `{"status": "error", "error": "..."}`

## Limits

The streaming search API reuses the same per-tenant limit settings as the old
`/api/v1/labels` and `/api/v1/label/{name}/values` endpoints.

### Time Range: `MaxLabelsQueryLength`

- **Flag**: `-store.max-labels-query-length`
- **YAML**: `max_labels_query_length`
- **Default**: 0 (unlimited)
- **Behaviour**: Clamps `minT` so the query window does not exceed the configured
  duration. Does **not** fail the request — silently narrows the time range.

**Current status in search path**: The clamping calls are commented out in
`pkg/querier/blocks_store_queryable_search.go` (lines ~222-226 and ~297-300) with
a "TODO - removed for testing" note. The helper `clampMinT()` is implemented at
line ~608 and ready to use. Re-enabling is a one-liner in each of
`SearchLabelNames` and `SearchLabelValues`.

The clamping is applied at the `blocksStoreQuerier` level (before the background
goroutine starts), not at the `multiQuerier` level.

### Result Count: `MaxLabelNamesLimit` / `MaxLabelValuesLimit`

- **Flags**: `-querier.max-label-names-limit`, `-querier.max-label-values-limit`
- **YAML**: `max_label_names_limit`, `max_label_values_limit`
- **Default**: 0 (unlimited)
- **Behaviour**: Clamps `SearchHints.Limit` to the per-tenant maximum before the
  fan-out. Does **not** fail the request. Unlike the old API, no warning
  annotation is added (the `SearchLabelNames/Values` path has no
  `annotations.Annotations` return).

**Where it's enforced**: `multiQuerier.SearchLabelNames` (`querier.go` ~line 713)
and `multiQuerier.SearchLabelValues` (~line 752), via `clampToMaxLimit()`.

The clamped limit is passed down through `SearchHints.Limit` → `fanOutSearch` →
sub-searchers:
- **Unsorted path** (`hints.Compare == nil`): `dedupSink.add()` enforces the
  limit eagerly and cancels the background goroutine. Also pushed to the
  store-gateway as `SearchLabelNamesRequest.Limit` / `SearchLabelValuesRequest.Limit`.
- **Sorted path** (`hints.Compare != nil`): limit is NOT pushed to sub-searchers
  (stripped in `fanOutSearch`); applied only after global sort in
  `labelSearchStream.sortAndTruncate()`.

### Difference from old API

| Behaviour | Old `/api/v1/labels` | New search API |
|---|---|---|
| `MaxLabelsQueryLength` | Applied (clamps `minT`) | **Disabled / TODO** |
| Result count limit | Applied + warning annotation | Applied, no warning |
| Annotation warnings | Yes, if limit clamped | No |
