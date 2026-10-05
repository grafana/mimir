# Mimir search_expr / search[] API — full reference

This is the detailed reference for the `search-expr` skill. `SKILL.md` in
this directory is the quick reference. Read this file only when a call
returns an error, or when an answer looks wrong.

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

## Efficiency: minimize round trips, not response bytes

Measured on mimir-dev-13 across 11 real alert-derived cases (see
`docs/superpowers/search-api-benchmark/` if present, or the benchmark that
produced these numbers): agents that swept multiple `fuzz_alg`/
`fuzz_threshold` combinations, or followed `next_cursor` to full exhaustion
by default, ended up spending **more** total tokens than agents that pulled
the entire legacy `__name__` listing in one call — even though the search
API's own response bodies were 12x-4931x smaller in bytes than that legacy
dump. The reason: every additional tool call re-sends the growing
conversation back to the model, and that per-round-trip cost dominates over
payload size. In the same benchmark, a single well-chosen search call beat
both the legacy full-dump approach *and* a multi-call fuzzy-search
exploration on total tokens. Small response, many round trips is not a win.
Treat call count, not byte count, as the cost you're optimizing.

Concretely:

- **Compose before you call.** Decide the tightest `search_expr`/`match[]`
  you can from the task description before issuing the first request. Don't
  run a broad exploratory call "to see what's there" and then narrow in a
  second call — use the Steps below to pick the expression up front.
- **Pick one `fuzz_alg`, don't sweep.** Default to a single
  `fuzz_alg=substring_left` call when the token's spelling is known or
  described, even approximately — see "Choosing fuzz_alg" below for when to
  deviate. Only issue a *second* call (typically `subsequence`, or
  `jarowinkler` with an explicit non-zero threshold) if the first call
  returns zero results and you have a specific reason to suspect a typo or
  abbreviation. Don't run several algorithms "just to compare" when the
  first one already answered the question.
- **Stop at the first page unless the task needs every item.** Only follow
  `next_cursor` to exhaustion when the task genuinely requires a complete
  enumeration (a full inventory, a count that must be exact). For an
  existence check ("does X exist", "roughly how many Y are there"), the
  first page plus the `has_more` flag is enough — a 15-page cursor walk to
  confirm something that a 1-page check would have confirmed is a real cost,
  not free precision.
- **One call with several AND'ed terms beats several calls with one term
  each.** If three descriptive words are known, put all three in one
  `search_expr` rather than trying them one at a time.

## Don't reach for a legacy `match[]` regex either — verified slower server-side

It's tempting, when the exact name is unknown, to skip `search_expr`
entirely and instead guess a `match[]` regex against the legacy
`/api/v1/label/__name__/values` endpoint (e.g.
`match[]={__name__=~"keda_scaled_obj.*errors_total"}` to bridge an unknown
middle section). This can return a small, cheap-looking response — but it
is doing a full series scan under the hood, and is measurably slower than
the equivalent `search_expr` call, especially as the candidate set grows.
Measured directly against mimir-dev-13 (5 repeated calls each, `curl -w
'%{time_total}'`, same underlying candidate set both ways):

| Query | Legacy `match[]` regex | `search_expr` | Result |
|---|---|---|---|
| Narrow regex (~179 candidates) | median 0.83s (spikes to 1.7s) | median 0.85s | roughly tied on median, but legacy has real tail-latency spikes `search_expr` didn't show |
| Broad prefix (~1420 candidates) | median 1.35s (cold-start spike to 3.8s) | median 0.83s | `search_expr` **~1.6x faster** at steady state, ~4.6x faster than legacy's worst case |

The gap grows with candidate-set size, consistent with `match[]` regex
triggering a full series scan rather than the indexed-ish path `search_expr`
uses. A hand-crafted regex can also look cheap in isolation but doesn't
generalize — it only worked above because a wildcard happened to be placed
exactly where the ambiguity was; a different typo position breaks it
silently, with no error. Prefer composing a `search_expr` (next section)
over guessing a `match[]` regex, even when a regex seems easy to write.

### Composing search_expr for cases that would tempt a match[] regex or a raw dump

The goal is to never need "pull everything and grep" or "guess a regex
wildcard." A `search_expr` can express everything those two tricks were
trying to do, without their downsides:

- **Unknown middle segment** (what a `.*` wildcard is usually for): AND the
  parts you DO know instead of wildcarding the part you don't —
  `keda and scaled and errors` finds `keda_scaled_object_errors_total`
  without needing to know the word is "object" at all, and without the
  fragility of a wildcard placed at the wrong spot.
- **Unknown suffix/prefix family**: AND the stem with a distinguishing
  second term rather than a bare prefix — `cortex and ingest and storage
  and reader and offset` narrows a broad `cortex_*` prefix down to the
  ~20 relevant results directly, instead of listing everything with that
  prefix and scanning by eye.
- **"One of several possible spellings"**: use `OR` inside parentheses
  rather than a regex alternation — `(compaction or compact) and fail`.
- **"Definitely not this family"**: `and not <term>` composes cleanly
  with any of the above, the same way a negative-lookahead-style regex
  would, but without the regex syntax risk.

If a case still seems to need a regex-only trick after composing a
`search_expr` this way, that's a signal the term breakdown needs another
AND'ed word, not a signal to fall back to `match[]`.

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
| `search[]` | One or more fuzzy terms, OR'd together. Max 32. | Mutually exclusive with `search_expr`; sending both is an HTTP 400. `/search/metric_names` and `/search/label_names` need `search[]` or `search_expr`; sending neither is an HTTP 400. Optional on `/search/label_values`. |
| `search_expr` | A boolean expression: `AND`/`OR`/`NOT` (case-insensitive keywords), `(`/`)` for grouping. `NOT` binds tightest, then `AND`, then `OR`. | Quote a term with `"` when it has whitespace or is literally `AND`, `OR`, or `NOT`; `\` escapes the next byte inside quotes. Every accepting path needs at least one positive (non-negated) term — an exclusion-only expression such as `not deprecated` is rejected. Max 4096 bytes, 32 terms, 16 nesting levels. |
| `case_sensitive` | bool, **defaults to `true`** | Default `true` means a term must match the candidate's exact letter case. Metric names and label names are conventionally all-lowercase, but label **values** are not: e.g. Mimir ring member states are `ACTIVE`, `JOINING`, `LEAVING`, `PENDING`, `Unhealthy` — a lowercase guess like `active` matches nothing under the default. Pass `case_sensitive=false` unless a case-sensitive check is deliberate. |
| `fuzz_alg` | `subsequence` (default), `jarowinkler`, `substring_left`, `substring` | See "Choosing fuzz_alg" and "Setting fuzz_threshold" below — the two parameters interact and neither is safe to set without the other. |
| `fuzz_threshold` | 0-100 minimum score | Meaning is **algorithm-dependent** — see "Setting fuzz_threshold" below. Do not reuse the same number across algorithms. |
| `match[]` | PromQL series selector(s), OR'd across repeats | Scopes candidates to series that actually exist. Cheap — see step 5. |
| `sort_by` | `alpha` (default) or `score` | `score` requires `search[]` or `search_expr`, and is rejected with `fuzz_alg=substring` because every match ties at `1.0`. |
| `cursor` | Opaque token from a previous response's `next_cursor` trailer field | Works for both `sort_by=alpha` and `sort_by=score` (score-ordered cursors carry an extra internal `score_after` alongside the resume value). Still rejected with `fuzz_alg=substring`, same as `sort_by=score` itself. When `cursor` is set, it must be the **only** query parameter; every other parameter (terms, `fuzz_alg`, `match[]`, `limit`, ...) travels inside the opaque token from the original request. See "Paginating with cursor" below. |
| `include_score` | bool | Needed to see the numeric score, not just the ordering. |
| `include_metadata` | bool, default `false` | **Only affects `/search/metric_names`**; silently ignored on `/search/label_names` and `/search/label_values`, and an HTTP 400 on `/search/metadata` (which always returns metadata). Adds `type`/`help`/`unit` per result, sourced from the ingesters via an extra fan-out call per result batch (`FetchMetricMetadata` in `pkg/querier/distributor_queryable_search.go`) — best-effort, so a fetch error just leaves results un-enriched rather than failing the request. Worth the extra round trip when deciding between several same-named-ish candidates by their real semantics; skip it on a first broad exploratory pass. |
| `limit`, `batch_size` | result cap / NDJSON batch framing | `limit=0` means unlimited, still capped by the tenant's `-querier.max-label-{names,values}-limit`. Default `limit` is 100 with default `sort_by=alpha` — see step 7. |
| `start`, `end` | search time range | Defaults to the last 1h — narrower than the legacy label API's default (the tenant's `max_labels_query_length`, often weeks). Set both explicitly before comparing timing against the legacy endpoint. **This truncation is silent — no warning, unlike a `limit` clamp.** Verified: with the default window, `search_expr=cortex` returned 630 names; widening `start` to `0` surfaced 2 more (real metrics whose trigger conditions hadn't fired in the last hour). Widen `start` to a bounded window (for example now-24h or now-7d) when the task asks about a past period, or before reporting something as absent. Do not use `start=0`: it is clamped to the tenant's `-store.max-labels-query-length`, queries the store-gateways for that whole range, and for high-churn values (such as `pod`) returns mostly entities that no longer exist (measured: 81 querier pods in 1h vs 6,270 with `start=0`). See "Time window" in `SKILL.md`. |
| `label` | label name | Required by `/search/label_values` only. The fuzzy filter (`search[]`/`search_expr`) matches against the **value** strings for that label, never against the label name itself — see step 2. |

## Steps

1. **Confirm the feature is enabled** on the target (see Prerequisites). A
   404 with `feature_not_enabled` means the flag is off — set it, or fall
   back to the legacy `/api/v1/label/*` endpoints.
2. **Pick the endpoint** for what is being discovered:
   - Metric name → `GET /prometheus/api/v1/search/metric_names`
   - Label name → `GET /prometheus/api/v1/search/label_names`. This needs
     a `search[]` or `search_expr`. To list every label name of a metric,
     use the legacy `GET /prometheus/api/v1/labels?match[]={__name__="M"}`.
   - Metric by its name and HELP text → `GET /prometheus/api/v1/search/metadata`.
     Needs `search[]` or `search_expr`. Name and HELP are searched as one
     text, so `ingester and ratio` matches `ingester` in the name and
     `ratio` in the HELP. A term matches at the start of a word (`ratio`
     matches "Ratio", "ratios" and `x_ratio`, not "duration"), and case is
     ignored by default. Do not send `match[]`, `start`, `end`,
     `include_metadata`, `include_score`, `fuzz_alg`, `fuzz_threshold` or
     `sort_by=score`: each is an HTTP 400. Each result carries `type`,
     `help` and `unit`.
   - Label value → `GET /prometheus/api/v1/search/label_values` (also needs
     `label=<name>`). If the label name is already known and the goal is
     to see all its values, do not use the search API. Use the legacy
     `GET /prometheus/api/v1/label/<name>/values?match[]=...`. Use the
     legacy API for every call that has no `search[]` and no
     `search_expr`. A fuzzy term here filters the *value* strings, not
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
     `and not _bucket and not _count and not _sum`. Use `not _count` and
     `not _sum` only in a group that targets a histogram: they also remove
     real metrics whose names contain those words, such as
     `memberlist_client_cluster_members_count`. For latency, duration,
     percentile or distribution questions, do not exclude these suffixes
     at all: the answer is the histogram's `_bucket` series.
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
8. **Check `has_more` on the trailer if the expected match seems to be
   missing.** With the default `sort_by=alpha`, a broad term can match more
   than the default `limit=100`, and the one candidate actually wanted can
   sort alphabetically past the cutoff and simply never appear, with no
   error to explain why. If `has_more` is `true`:
   Under both `sort_by=alpha` and `sort_by=score`, follow `next_cursor`
   (see "Paginating with cursor" below) rather than guessing a bigger
   `limit` — it walks every remaining result exactly once instead of
   re-running a wider query. `fuzz_alg=substring` is the one exception:
   it can't use `sort_by=score` at all (every match ties at `1.0`), so
   there raise `limit` under `sort_by=alpha` instead.
   Narrowing the expression (step 3) avoids the problem in the first place,
   and is usually cheaper than paging through a broad one.
9. **Iterate.** If results are too broad, add another `and not <term>` or a
   `match[]`. If too narrow or empty, loosen with `or`, drop a `not`, widen
   `fuzz_threshold`, or switch `fuzz_alg`.

### Choosing fuzz_alg

| Algorithm | Behavior | Use when |
|---|---|---|
| `substring_left` | Plain substring match, scores higher when the match starts further left | **Default first choice.** Use whenever the token's spelling is known or described, even approximately — measured to tie or beat `subsequence` on precision in 8 of 9 non-typo cases in the mimir-dev-13 benchmark, with no false-positive noise. See "Efficiency" above: pick this and commit, don't sweep. |
| `subsequence` (API default if `fuzz_alg` is omitted) | Greedy subsequence match: accepts a candidate if the term's letters appear *in order* somewhere in it, tolerant of missing letters | Only as a **fallback** after `substring_left` returns zero and the spelling is genuinely unknown or abbreviated — not a co-equal first try. Reliably lets unrelated names through on short/generic terms (see benchmark notes below); pair with more AND'ed terms or `match[]` if used. |
| `jarowinkler` | Edit-distance-style similarity | Only as a further fallback when a specific typo is suspected — but only once `fuzz_threshold` is set above `0`, see below. **Always pair with `sort_by=score&include_score=true`.** Confirmed on mimir-dev-13: `fuzz_alg=jarowinkler&fuzz_threshold=30` on the literal misspelled term `keda_scaled_objcet_errors_total` found the correct `keda_scaled_object_errors_total` cleanly at the top (score 0.994, next candidate at 0.934) — but an earlier attempt with the same algorithm and threshold under the default `sort_by=alpha` looked like a failure, because the correct match was likely sorted somewhere past the default `limit=100` alongside unrelated low-score noise instead of surfacing at the top. If `jarowinkler` "isn't working," check `sort_by` before concluding the threshold is wrong. |
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

### Paginating with cursor

Use `cursor` when the task genuinely needs *every* matching result, not
just a bigger top-N: enumerating every value of a high-cardinality label
that matches a term,
walking a broad metric-name expression to completion without missing an
alphabetically-late match, or walking a large ranked result set under
`sort_by=score` past the first page. Works for both orderings; the one
exception is `fuzz_alg=substring` under `sort_by=score`, which is rejected
outright (every match ties at `1.0`, so there's nothing to rank or resume).

**Default rule, decide fast, don't deliberate:** if the task doesn't say
"every", "exact count", "complete list", or similar, assume it does NOT
need exhaustive pagination. Report the first page plus the `has_more` flag
("at least 100, more available" is a true and sufficient answer for an
existence check or a rough-size question) and stop — don't spend extra
reasoning turns weighing whether to paginate. Re-reading this decision
back and forth costs more in agent turns than the pagination itself would;
make the call once, from the task wording, and move on. Only walk the
full cursor chain when the task explicitly requires a complete enumeration.

**When the task does need a complete list, try `limit=0` in one call
first.** `limit=0` returns everything up to the tenant's
`-querier.max-label-{names,values}-limit` in a single response. Measured
on mimir-dev-13 (5,175 pod values, tenant limit 10,000): one legacy call
returned the full list for ~150K agent tokens, while an agent that used
`limit=1000` and followed `next_cursor` made 7 calls and spent ~903K
tokens. Follow `next_cursor` only when `has_more` is `true` after the
`limit=0` call. Every `limit=0` call carries the warning "results may be
truncated due to querier.max-label-values-limit (requested limit: 0,
enforced: N)", even when only a few values come back (confirmed: 5 ring
state values returned with that warning). The warning means the result
was really cut off only when the result count equals N.

How it works, confirmed live against the local dev cluster:

1. Run the initial search as normal (`sort_by=alpha`, the default). If the
   trailer has `has_more:true`, it also carries `next_cursor`, an opaque
   base64 token.
2. Fetch the next page with `cursor=<next_cursor>` as the **only** query
   parameter:

   ```bash
   curl -s -G --data-urlencode 'cursor=<next_cursor value>' \
     'http://localhost:8080/prometheus/api/v1/search/metric_names'
   ```

   Adding any other parameter alongside `cursor` (even repeating `limit`
   or `search_expr` with the same value) is an HTTP 400 — the token already
   encodes every original parameter (terms/expression, `case_sensitive`,
   `fuzz_alg`, `fuzz_threshold`, `sort_dir`, `limit`, `batch_size`,
   `include_score`, `include_metadata`, `label`, `start`/`end`, `match[]`)
   plus the resume position. Just pass the token back unchanged.
3. Keep following `next_cursor` until a trailer has `has_more:false` (no
   `next_cursor`) — that page is the last one.

Things that matter operationally, not just mechanically:

- **A cursor is a resume position, not a consistent snapshot.** It resumes
  strictly after the last value seen, by value comparison — not an offset.
  A result added between two pages, sorting before the resume point, is
  never seen; a result deleted between pages is just naturally skipped.
  Don't treat a completed walk as a point-in-time listing if the underlying
  data can change mid-walk.
- **A cursor can go stale across a Mimir upgrade.** It's versioned
  (`searchCursorVersion` in `pkg/querier/search_handler.go`); a version bump
  makes an old in-flight cursor fail clearly (`"unsupported version"`)
  rather than being misinterpreted. Don't persist a cursor across a
  deployment.
- **During a rolling upgrade**, if the backend that served a page doesn't
  yet understand cursor-based resume, the trailer's `warnings` says so and
  that page's results may be incomplete — don't treat a clean `has_more`
  walk as guaranteed-complete if that warning appears on any page.

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
{"status":"success","has_more":false,"returned":1}
```

- `returned` is the number of results in this response. Use it for "how
  many" questions instead of counting result lines. Builds before this
  field was added omit it.
- `has_more:true` means there's more to see: the trailer also carries
  `next_cursor` — follow it (see "Paginating with cursor") under either
  `sort_by=alpha` or `sort_by=score`. The one case with no cursor at all is
  `fuzz_alg=substring` (which can't use `sort_by=score` in the first
  place); raise `limit` and re-run there instead.
- A `warnings` array on the trailer means the tenant's
  `-querier.max-label-{names,values}-limit` clamped the result; the
  shortfall is real, not a bug in the expression.
- An immediate HTTP 400 with `errorType: "bad_data"` almost always means one
  of: neither `search[]` nor `search_expr` was set on `/search/metric_names`
  or `/search/label_names`, both `search[]` and `search_expr` were set,
  `sort_by=score` was used with no search terms at all, `sort_by=score`
  was combined with
  `fuzz_alg=substring`, or the expression has no positive term.

## Error Handling

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 404, `feature_not_enabled` | `-querier.experimental-search-api-enabled` is not set on this target | Set the flag, or use the legacy `/api/v1/label/*` endpoints |
| HTTP 400, "a search[] or search_expr parameter is required" | `/search/metric_names` or `/search/label_names` got neither parameter, often because a parameter name is misspelt (for example `search_expression`) | Correct the parameter name. To get all results with no term, use the legacy `/label/__name__/values` or `/labels` API |
| HTTP 400, "search terms and search expression are mutually exclusive" | Both `search[]` and `search_expr` were sent | Send only one |
| HTTP 400, "every accepting path must require a positive term" | Expression is exclusion-only, e.g. `not deprecated` | Add a positive anchor term, e.g. `metric_prefix and not deprecated` |
| HTTP 400, "sort_by=score requires search[] or search_expr" | Sorting by score with no search terms at all | Add a term, or use `sort_by=alpha` |
| HTTP 400, "sort_by=score is not supported with fuzz_alg=substring" | `substring` scores every match `1.0` | Use `sort_by=alpha`, or switch algorithm |
| `label_values` returns zero results even though the label clearly has values | The fuzzy term matched against the label *name* (or something not present in any value string), not the values themselves | List all values with the legacy `/label/<name>/values` API, or search for text that actually appears in a value |
| A term you're sure should match returns nothing under `jarowinkler` | `fuzz_threshold` was left at the default `0` (disables fuzziness, falls back to plain substring), OR `sort_by=alpha` buried the real match past `limit=100` behind unrelated low-score noise | Set `fuzz_threshold` explicitly (start well below 80 — see "Setting fuzz_threshold") AND set `sort_by=score&include_score=true` so the real match surfaces at the top instead of getting lost alphabetically — confirmed to matter in practice, not just in theory |
| A term you're sure should match returns nothing under `subsequence` | The term transposes two letters relative to the real candidate, breaking the required in-order letter sequence | Try `fuzz_alg=jarowinkler` with a tuned threshold, or fix the letter order |
| A guessed value (e.g. a status/state string) never matches | `case_sensitive` defaults to `true`, and label values have no casing convention (e.g. ring states are `ACTIVE`, `JOINING`, `Unhealthy`) | Pass `case_sensitive=false` |
| Result set has unrelated hits | `subsequence` default matched an unrelated name through a loose subsequence, especially with short generic terms | Switch to `substring_left`/`substring`, add `match[]`, or add another `and` term |
| Scores are all within about 0.001 of each other | The score carries little relevance signal for this expression or term length | Trust the AND/OR/NOT composition and `not` exclusions, not the score ordering; add `match[]` for real precision |
| The expected metric doesn't appear, but the term seems right | Default `limit=100` can truncate before reaching a later match (alphabetically, under `sort_by=alpha`; lower-ranked, under `sort_by=score`) when the term is broad | Check `has_more`; follow `next_cursor` (see "Paginating with cursor"), or narrow the expression with more AND'ed terms |
| Count is short by a small, unexplained amount, with no `has_more`/`warnings` signal at all | The default 1h `start`/`end` window silently excluded a real metric that hasn't fired recently — this truncation carries no warning, unlike a `limit` clamp | If the task is about a past period, set a bounded `start` (for example now-7d). Do not use `start=0` (see "Time window" in `SKILL.md`) |
| Raising `limit` doesn't return more results, and every page repeats the same warning | The tenant's `-querier.max-label-{names,values}-limit` is set below what you're asking for — the trailer's `warnings` says "enforced: N"; `limit` cannot override this | `next_cursor` is the only way past this cap — see "Paginating with cursor". Raising `limit` further has no effect once this warning appears. |
| HTTP 400, "cursor and other search parameters are mutually exclusive" | A parameter (even a repeat of one already inside the cursor) was sent alongside `cursor` | Send `cursor` alone; every other parameter is already encoded in it |
| HTTP 400, "invalid cursor: unsupported version" | The cursor was generated by a different (older or newer) Mimir binary version and its payload shape changed | Restart the walk from a fresh, cursor-less request rather than resuming an old one across an upgrade |
| A `has_more` walk via cursor looks complete but data seems missing | A backend source mid-rolling-upgrade doesn't yet honor the cursor's resume position and returns its same fixed first-N window every page | Check the trailer's `warnings` for the "may not yet support cursor-based resume" text; treat that page as possibly incomplete, not as ground truth |
| Search feels slower than expected | A legacy `match[]` regex on `/api/v1/label/<name>/values` triggers a full series scan on some backends; `search_expr` does not share that cost profile | Prefer `search_expr` over a legacy `match[]` regex when the goal is text filtering, not series existence |
