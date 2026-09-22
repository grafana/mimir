---
name: search-expr
description: Construct and run queries against Mimir's experimental fuzzy search API (search[] terms and search_expr boolean expressions) on /api/v1/search/metric_names, label_names, and label_values, to discover a metric, label, or label value without knowing its exact spelling. Use when composing a search_expr with AND/OR/NOT, choosing a fuzz_alg or fuzz_threshold, or testing this repo's search-filter-composition changes against a running Mimir.
---

# Mimir search_expr / search[] API

Construct and run queries against Mimir's experimental fuzzy search API to
find a metric name, label name, or label value when the exact spelling is
unknown, without pulling every candidate and filtering client-side.

This API is under active development in this repo. Treat this skill as a
usage guide, not the spec — the grammar, limits, and defaults are defined in
code and can change while the feature is experimental. Before relying on a
detail here, check it against:

- `pkg/querier/search_handler.go` — HTTP parameter parsing, defaults, and
  the endpoint handlers (`SearchMetricNamesHandler`,
  `SearchLabelNamesHandler`, `SearchLabelValuesHandler`).
- `pkg/streaminglabelvalues/params.go` — `Params`, `FuzzAlg` values, and
  validation rules.
- `pkg/streaminglabelvalues/filters.go` — how each `fuzz_alg` and
  `case_sensitive` actually behave (`BuildFilter`, `buildPerTermFilter`).
- `pkg/streaminglabelvalues/internal/searchexpr/` — the `search_expr`
  parser and compiler, including the "every accepting path needs a positive
  term" rule (`Validate` in `compiler.go`).
- `docs/sources/mimir/references/http-api/_index.md` — the published
  parameter table (search for "search/label_names").

## When to Use

- Testing or reviewing a change to `search_expr` parsing, `fuzz_alg`
  scoring, or the search HTTP handlers in this repo.
- An investigation needs a metric, label, or label value and the exact name
  is unknown.
- The task needs to exclude recording rules (`:`) or classic-histogram
  suffixes (`_bucket`, `_count`, `_sum`) from metric-name results.
- The task needs a ranked, scored shortlist instead of raw alphabetical
  output from the legacy `/api/v1/label/*` endpoints.

Do not use this skill to run an actual PromQL query, or to search container
logs — this covers only the `/api/v1/search/*` name/value discovery
endpoints.

## Prerequisites

- A running Mimir with the experimental search API enabled:
  `-querier.experimental-search-api-enabled=true` (or
  `experimental_search_api_enabled: true` in YAML) on the querier config.
  Without it, every `/api/v1/search/*` call returns HTTP 404 with
  `{"status":"error","errorType":"feature_not_enabled",...}`.
- The local dev cluster already has this enabled. From the repo root:

  ```bash
  development/mimir-microservices-mode/compose-up.sh -d
  ```

  This exposes the querier's HTTP API through nginx on
  `http://localhost:8080`, with `/prometheus` as the Prometheus HTTP prefix
  and `multitenancy_enabled: false` — no `X-Scope-OrgID` header needed
  locally.
- Against a multi-tenant target, set `X-Scope-OrgID: <tenant>` on every
  request.

## Inputs

| Param | Meaning | Notes |
|---|---|---|
| `search[]` | One or more fuzzy terms, OR'd together. Max 32. | Mutually exclusive with `search_expr`; sending both is an HTTP 400. |
| `search_expr` | A boolean expression: `AND`/`OR`/`NOT` (case-insensitive keywords), `(`/`)` for grouping. `NOT` binds tightest, then `AND`, then `OR`. | Quote a term with `"` when it has whitespace or is literally `AND`, `OR`, or `NOT`; `\` escapes the next byte inside quotes. Every accepting path needs at least one positive (non-negated) term — an exclusion-only expression such as `not deprecated` is rejected. Max 4096 bytes, 32 terms, 16 nesting levels. |
| `case_sensitive` | bool, **defaults to `true`** | Default `true` means a term must match the candidate's exact letter case. Metric names and label names are conventionally all-lowercase, but label **values** are not: e.g. Mimir ring member states are `ACTIVE`, `JOINING`, `LEAVING`, `PENDING`, `Unhealthy` — a lowercase guess like `active` matches nothing under the default. Pass `case_sensitive=false` unless a case-sensitive check is deliberate. |
| `fuzz_alg` | `subsequence` (default), `jarowinkler`, `substring_left`, `substring` | See "Choosing fuzz_alg" and "Setting fuzz_threshold" below — the two parameters interact and neither is safe to set without the other. |
| `fuzz_threshold` | 0-100 minimum score | Meaning is **algorithm-dependent** — see "Setting fuzz_threshold" below. Do not reuse the same number across algorithms. |
| `match[]` | PromQL series selector(s), OR'd across repeats | Scopes candidates to series that actually exist. Cheap — see step 5. |
| `sort_by` | `alpha` (default) or `score` | `score` requires `search[]` or `search_expr`, and is rejected with `fuzz_alg=substring` because every match ties at `1.0`. |
| `include_score` | bool | Needed to see the numeric score, not just the ordering. |
| `include_metadata` | bool, default `false` | **Only affects `/search/metric_names`**; silently ignored on the other two endpoints. Adds `type`/`help`/`unit` per result, sourced from the ingesters via an extra fan-out call per result batch (`FetchMetricMetadata` in `pkg/querier/distributor_queryable_search.go`) — best-effort, so a fetch error just leaves results un-enriched rather than failing the request. Worth the extra round trip when deciding between several same-named-ish candidates by their real semantics; skip it on a first broad exploratory pass. |
| `limit`, `batch_size` | result cap / NDJSON batch framing | `limit=0` means unlimited, still capped by the tenant's `-querier.max-label-{names,values}-limit`. Default `limit` is 100 with default `sort_by=alpha` — see step 7. |
| `start`, `end` | search time range | Defaults to the last 1h — narrower than the legacy label API's default (the tenant's `max_labels_query_length`, often weeks). Set both explicitly before comparing timing against the legacy endpoint. |
| `label` | label name | Required by `/search/label_values` only. The fuzzy filter (`search[]`/`search_expr`) matches against the **value** strings for that label, never against the label name itself — see step 2. |

## Steps

1. **Confirm the feature is enabled** on the target (see Prerequisites). A
   404 with `feature_not_enabled` means the flag is off — set it, or fall
   back to the legacy `/api/v1/label/*` endpoints.
2. **Pick the endpoint** for what is being discovered:
   - Metric name → `GET /prometheus/api/v1/search/metric_names`
   - Label name → `GET /prometheus/api/v1/search/label_names`
   - Label value → `GET /prometheus/api/v1/search/label_values` (also needs
     `label=<name>`). If the label name is already known and the goal is
     just to see its values, a plain call with **no** `search[]`/
     `search_expr` at all is often the right move — it lists every value
     for that label. A fuzzy term here filters the *value* strings, not
     the label name; searching `label_values?label=state&search_expr=state`
     returns zero results, because none of that label's actual values
     (`ACTIVE`, `JOINING`, ...) contain the substring `"state"` — the word
     "state" only appears in the label name, which the query doesn't
     search. Confirmed by testing.
3. **Write a search expression**, not a bag of `search[]` terms, whenever
   the task needs composition:
   - Positive terms narrow the set: `ingester and fail`.
   - Exclude recording rules with `and not :` — recording-rule names always
     contain `:`.
   - Exclude classic-histogram suffixes when the base series is wanted:
     `and not _bucket and not _count and not _sum`.
   - Group with parentheses when mixing AND/OR:
     `ingester and (fail or error) and not :`.
   - Prefer three specific AND'ed tokens (`ingester and wal and corrupt`)
     over one broad, generic term. In testing, a single broad term
     (`ring`, or `wal and corrupt` without a third scoping token) reliably
     let 3-4 unrelated names through even after adding `not :`; three
     specific AND'ed tokens with `fuzz_alg=substring_left` went straight to
     the one correct answer with no noise, every time it was tried.
4. **Set `case_sensitive=false`** unless there's a specific reason to
   require exact case — see the Inputs table. This matters most for label
   *values*, which have no casing convention, unlike metric/label names.
5. **Choose `fuzz_alg` and `fuzz_threshold` together** — see "Choosing
   fuzz_alg" and "Setting fuzz_threshold" below. Never leave
   `fuzz_threshold` at its default `0` when using `fuzz_alg=jarowinkler`:
   at `0` it is not fuzzy at all.
6. **Add `match[]`** as soon as a rough scope is known (namespace, job,
   cluster). This is the largest precision lever available and is not the
   expensive part of the call.
7. **Set `sort_by=score&include_score=true`** to check whether scores are
   actually separated or clustered flat. A tight cluster (all scores within
   roughly 0.001 of each other) means the score is not doing the ranking —
   the expression's AND/OR/NOT composition filtered the list, not the fuzzy
   score.
8. **Check `has_more` on the trailer, and raise `limit` if the expected
   match seems to be missing.** There is no continuation cursor — the only
   way to see more results is to raise `limit` and re-run, not to page.
   This matters most with the default `sort_by=alpha`: a broad term can
   match more than the default `limit=100`, and the one candidate actually
   wanted can sort alphabetically past the cutoff and simply never appear,
   with no error to explain why. If a plausible term returns nothing (or
   not the expected metric) and `has_more` is `true`, raise `limit` before
   concluding the metric doesn't exist. Narrowing the expression (step 3)
   avoids the problem in the first place.
9. **Iterate.** If results are too broad, add another `and not <term>` or a
   `match[]`. If too narrow or empty, loosen with `or`, drop a `not`, widen
   `fuzz_threshold`, or switch `fuzz_alg`.

### Choosing fuzz_alg

| Algorithm | Behavior | Use when |
|---|---|---|
| `subsequence` (default) | Greedy subsequence match: accepts a candidate if the term's letters appear *in order* somewhere in it, tolerant of missing letters | The exact spelling is genuinely unknown, or the term is an abbreviation |
| `jarowinkler` | Edit-distance-style similarity | Checking for likely typos of an already-known name — but only once `fuzz_threshold` is set above `0`, see below |
| `substring_left` | Plain substring match, scores higher when the match starts further left | The exact token is known and multiple exact terms need to be AND'ed without subsequence false positives |
| `substring` | Plain substring match, every hit scores `1.0` | Only presence/absence matters, not ranking — pair with `sort_by=alpha` |

`subsequence`'s "letters in order" rule has a real blind spot: it cannot
tolerate a typo that **transposes** two letters, even when every other
letter is correct. In testing, the term `compactoin` (a transposed typo of
`compactions`) failed to match `cortex_ingester_tsdb_compactions_failed_total`
under `subsequence`, because after matching `...compact`, the pattern still
needs an `o` then an `i` in order, but `compactions` has `i` before `o` at
that point — no valid subsequence exists, so it silently doesn't match, with
no error to explain why. `subsequence` still matched *other*, unrelated
candidates whose text happened to supply an `o` before an `i` somewhere
after "compact" (e.g. one containing "...in_progress"). If a term that
should obviously match returns nothing under `subsequence`, suspect a
transposition in the term and try `jarowinkler` instead.

`sort_by=score` with a fuzzy term and no `match[]` scoping can be
misleading on its own: an unrelated but coincidentally-matching name can
outrank the metric actually wanted. Scope with `match[]`, and prefer
`substring`/`substring_left` for exact tokens, before trusting the score
ordering.

### Setting fuzz_threshold

`fuzz_threshold` (0-100, divided by 100 internally) means something
different for each algorithm — check `pkg/streaminglabelvalues/filters.go`
(`buildPerTermFilter`) before assuming a number carries over from one
algorithm to another:

- **`subsequence`**: the minimum subsequence-match score to accept. The
  default `0` accepts *any* nonzero subsequence match, however weak — this
  is why a short or generic term under `subsequence` reliably lets
  unrelated candidates through (see step 3). Raising the threshold can
  help, but composing a more specific expression (more AND'ed terms, or
  switching to `substring_left`) is usually a more predictable fix than
  hunting for the right cutoff.
- **`jarowinkler`**: **at the default `0`, the fuzzy matcher is not used at
  all** — `buildPerTermFilter` omits the Jaro-Winkler filter entirely when
  `fuzz_threshold==0` and falls back to a plain leftmost-substring match
  (confirmed by the test `TestBuildFilterJaroWinklerThresholdZeroIsSubstringOnly`
  in `filters_test.go`). A misspelled term that isn't a literal substring of
  anything will return zero results at `fuzz_threshold=0`, which looks
  exactly like "no such metric" — always set an explicit `fuzz_threshold>0`
  when using `jarowinkler` for typo tolerance. There is no single threshold
  that works everywhere: Jaro-Winkler similarity is computed against the
  *entire* candidate string, so a short term compared against Mimir's
  typically long metric names (30-50+ characters) scores much lower for the
  same edit distance than the same typo would against a short candidate.
  This repo's own filter tests use `80` as an example threshold, but those
  test cases compare near-equal-length strings (`"metric"` vs `"metricc"`)
  — that number does not transfer to a short search term against a real,
  long Mimir metric name. In testing, a 10-character typo of a ~35-character
  metric name needed `fuzz_threshold=30` to match at all; `70` returned
  zero results for the same term. If `jarowinkler` returns nothing, try
  lowering the threshold substantially (e.g. by 20-30 points) before
  concluding the candidate doesn't exist, and expect to iterate rather than
  guess correctly on the first try.
- **`substring_left`**: `fuzz_threshold` has **no effect at all**.
  `NewFilterContains` (what `substring_left` compiles to) takes no
  threshold argument — any value passed is silently accepted by the HTTP
  layer and then ignored.
- **`substring`**: only `100` (its own default) is valid; every other
  explicit value is rejected with an HTTP 400, because every match already
  scores `1.0`.

## Verification

Run one call and check both the NDJSON body and the trailer:

```bash
curl -s -G \
  --data-urlencode 'search_expr=ingester and fail and not : and not _bucket and not _count and not _sum' \
  --data-urlencode 'case_sensitive=false' \
  --data-urlencode 'sort_by=score' \
  --data-urlencode 'include_score=true' \
  --data-urlencode 'limit=10' \
  'http://localhost:8080/prometheus/api/v1/search/metric_names'
```

Expected shape — one or more `{"results":[...]}` batch lines, then a
trailer line:

```json
{"results":[{"name":"cortex_ingester_ingested_samples_failures_total","score":0.9666742021276595}]}
{"status":"success","has_more":false}
```

- `has_more:true` means raise `limit` and re-run to see the rest — there is
  no cursor.
- A `warnings` array on the trailer means the tenant's
  `-querier.max-label-{names,values}-limit` clamped the result; the
  shortfall is real, not a bug in the expression.
- An immediate HTTP 400 with `errorType: "bad_data"` almost always means one
  of: both `search[]` and `search_expr` were set, `sort_by=score` was used
  with no search terms at all, `sort_by=score` was combined with
  `fuzz_alg=substring`, or the expression has no positive term.

## Error Handling

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 404, `feature_not_enabled` | `-querier.experimental-search-api-enabled` is not set on this target | Set the flag, or use the legacy `/api/v1/label/*` endpoints |
| HTTP 400, "search terms and search expression are mutually exclusive" | Both `search[]` and `search_expr` were sent | Send only one |
| HTTP 400, "every accepting path must require a positive term" | Expression is exclusion-only, e.g. `not deprecated` | Add a positive anchor term, e.g. `metric_prefix and not deprecated` |
| HTTP 400, "sort_by=score requires search[] or search_expr" | Sorting by score with no search terms at all | Add a term, or use `sort_by=alpha` |
| HTTP 400, "sort_by=score is not supported with fuzz_alg=substring" | `substring` scores every match `1.0` | Use `sort_by=alpha`, or switch algorithm |
| `label_values` returns zero results even though the label clearly has values | The fuzzy term matched against the label *name* (or something not present in any value string), not the values themselves | Drop the search filter and list all values for that `label=`, or search for text that actually appears in a value |
| A term you're sure should match returns nothing under `jarowinkler` | `fuzz_threshold` was left at the default `0`, which disables fuzziness entirely and falls back to plain substring | Set `fuzz_threshold` explicitly (start well below 80 — see "Setting fuzz_threshold") |
| A term you're sure should match returns nothing under `subsequence` | The term transposes two letters relative to the real candidate, breaking the required in-order letter sequence | Try `fuzz_alg=jarowinkler` with a tuned threshold, or fix the letter order |
| A guessed value (e.g. a status/state string) never matches | `case_sensitive` defaults to `true`, and label values have no casing convention (e.g. ring states are `ACTIVE`, `JOINING`, `Unhealthy`) | Pass `case_sensitive=false` |
| Result set has unrelated hits | `subsequence` default matched an unrelated name through a loose subsequence, especially with short generic terms | Switch to `substring_left`/`substring`, add `match[]`, or add another `and` term |
| Scores are all within about 0.001 of each other | The score carries little relevance signal for this expression or term length | Trust the AND/OR/NOT composition and `not` exclusions, not the score ordering; add `match[]` for real precision |
| The expected metric doesn't appear, but the term seems right | Default `sort_by=alpha` with default `limit=100` can truncate before reaching an alphabetically-late match when the term is broad | Check `has_more`; raise `limit`, or narrow the expression with more AND'ed terms |
| Search feels slower than expected | A legacy `match[]` regex on `/api/v1/label/<name>/values` triggers a full series scan on some backends; `search_expr` does not share that cost profile | Prefer `search_expr` over a legacy `match[]` regex when the goal is text filtering, not series existence |
