# query-failure-domain-analysis

Offline analysis tool that estimates how much the query failure domain (number
of partitions a query has to fan out to) would shrink if Mimir routed series
to partitions by metric name rather than by all labels.

This tool was built to evaluate the hypothesis that a
large portion of complex queries — e.g. `cpu_usage / cpu_limit`,
`memory_used / memory_total` — would touch a small subset of partitions if
metrics were laid out lexicographically by name.

[design-doc]: ../../../

## What it does

For each PromQL query in the input, the tool:

1. Parses the expression with the Prometheus parser.
2. Walks the AST to extract every `__name__` matcher.
3. Simulates how many distinct partitions the query would touch under each
   selected routing scheme:

   | Scheme         | Model                                                                                                           |
   | -------------- | --------------------------------------------------------------------------------------------------------------- |
   | `all-labels`   | Status quo: query fans out to every partition in the shuffle shard.                                             |
   | `hash-by-name` | Cortex-style: `partition = xxhash(name) % N` per metric.                                                        |
   | `lex-sort`     | `partition = floor(lexDistance(name) * N)` — static base-26 positional encoding over `a..z`, universe-free.     |

Queries with no `__name__` constraint (e.g. `count({namespace="prod"})`) or
with negative name matchers are recorded as "fullShard" because no name-based
scheme can localise them. Queries whose only `__name__` constraint is a regex
are also treated as fullShard unless a metric-name universe is supplied via
`-metric-names` (so the regex candidates can be enumerated).

### Static lexicographic bucketing (for `lex-sort`)

The `lex-sort` scheme maps each metric name to a `[0, 1]` value using a
base-26 positional encoding over the lowercase alphabet:

```
lexDistance(s) = sum_i alphabetIndex(s[i]) / 26^(i+1)
```

where `alphabetIndex(c) ∈ [0, 25]` is `c − 'a'` for lowercase letters (with
case-folding for uppercase), and `0` for digits/`_`/`:`/other bytes. The
partition is then `floor(lexDistance(name) * N)`, so adjacent-prefix metric
names tend to share a bucket.

This is the right primitive for partition assignment, but as a "how similar
are these names" score it suffers from geometric decay — two names that
share a 4-char prefix differ by less than `26⁻⁴ ≈ 2 × 10⁻⁶` in the encoding,
which rounds to zero when displayed. So we use a different metric for the
per-query score.

### Name distance score

A per-query value in `[0, 1)` defined as

```
lexFraction(maxName) − lexFraction(minName)
```

where `min`/`max` are taken by byte-wise string ordering and `lexFraction`
places each name on the byte-positional lex spectrum:

```
lexFraction(s) = sum_i byte(s[i]) / 256^(i+1)
```

i.e. every possible byte string maps to a unique position in `[0, 1)`, with
byte-lex order preserved. The score directly says "how far apart on the
spectrum of all possible byte strings do the query's most extreme metric
names sit".

The encoding decays geometrically — each successive byte contributes 256×
less than the previous — so prefix-sharing names produce very small but
distinct scores. The report renders them in scientific notation and bins
them into log-spaced buckets at cutoffs of `256⁻ᵏ` so the orders of
magnitude are visible (default depth: `k = 16`, configurable via the
`maxPrefixDepth` constant in `report.go`). A score in `(256⁻⁵, 256⁻⁴]`
means "the names share their first 4 bytes"; a score in `(256⁻¹, 1)`
means "the names differ at
their very first byte".

Concrete examples:

| Pair                                          | Score (approx)   | Meaning                  |
| --------------------------------------------- | ---------------- | ------------------------ |
| single name                                   | `0`              |                          |
| `cpu_usage / cpu_limit`                       | `8.2e-12`        | first 4 bytes shared     |
| `disk_total_bytes / disk_used_bytes`          | `3.6e-15`        | first 5+ bytes shared    |
| `memory_limit / memory_usage`                 | `8.7e-20`        | first 5+ bytes shared    |
| `http_request_duration_seconds_count / _sum`  | `≈ 3e-74`        | first 5+ bytes shared    |
| `node_load1 / cpu_usage`                      | `0.043`          | first byte differs       |
| `aaaaaa / zzzzzz`                             | `0.098`          | first byte differs       |

Implementation note: the difference is computed directly via per-byte
summation rather than as `lexFraction(a) − lexFraction(b)`, so prefix-shared
names that would otherwise collapse to `0` under float64 catastrophic
cancellation are preserved.

The score is universe-independent; you do not need to supply a metric-name
file to compute it.

## Typical pipeline

The tool reads logfmt-formatted query-frontend "query stats" log lines from
stdin, one record per line. It extracts the `param_query` field (the PromQL
expression) and the `user` field (the Mimir tenant / `org_id`). Any leading
non-`key=value` prefix that gcx prepends (typically a UI timestamp + level)
is automatically skipped, so you don't need to pre-process the output.

Point it at the query-frontend logs of the cell you want to analyse:

```bash
gcx logs query --from "now-24h" --to "now" --limit 0 \
  '{namespace=~"cortex-prod-04",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read"' \
  | go run ./tools/query-failure-domain-analysis \
      -partitions=385 \
      -schemes=all-labels,hash-by-name,lex-sort
```

Things to customise for your cell:

- `namespace=~"..."` — the namespace of the Mimir deployment you're analysing.
- `name=~"query-frontend.*"` — matches the query-frontend pods. Adjust if
  your deployment uses a different label/name convention.
- The `!= "/api/v1/read"` filter excludes remote-read calls (those have a
  different request shape and aren't useful for this analysis).

If your queries match metrics by regex on `__name__` (e.g.
`{__name__=~"cpu_(usage|limit)"}`), supply the universe of metric names the
cell ingests so those queries can be resolved instead of falling back to
fullShard:

```bash
gcx metrics label-values __name__ \
    -d <prom-uid> --from now-1h --to now --org-id <tenant> \
  | jq -r '.[]' > metric-names.txt

gcx logs query ... | go run ./tools/query-failure-domain-analysis \
    -partitions=385 \
    -metric-names=metric-names.txt
```

The universe file is only consulted for regex resolution — equality matchers
and the `lex-sort`/name-distance computations use the universe-free static
encoding.

### Focusing on multi-metric queries

Single-metric queries (e.g. `cpu_usage`) and constraint-free queries
(`count({namespace="prod"})`) can't benefit from name-based sharding — the
former already hit one partition under any name-based scheme, the latter
have to hit them all. To zoom in on the queries the design doc is actually
trying to optimise (combinations across multiple metric names like
`cpu_usage / cpu_limit`), pass `-min-names=2`:

```bash
gcx logs query ... | go run ./tools/query-failure-domain-analysis \
    -partitions=385 \
    -min-names=2
```

The report's "Inputs" section reports how many queries were excluded so the
numbers stay traceable back to the raw log volume.

### Spot-checking the buckets

If you're trying to sanity-check what kinds of queries fall into each
"distinct metric names" bucket, add `-samples=N`. The "Query complexity"
section will then print up to `N` example PromQL strings underneath each
bucket count:

```bash
gcx logs query ... | go run ./tools/query-failure-domain-analysis \
    -partitions=385 \
    -samples=3
```

```
Query complexity (distinct metric names per query)
  buckets                 queries
  fullShard (0)               8214
      | count({namespace="prod"})
      | sum(rate({job="grafana"}[5m]))
      | absent({cluster="prod-us-central-0"})
  1                          43210
      | up
      | node_load1
      | http_requests_total
  2-5                        38201
      | cpu_usage / cpu_limit
      | rate(http_request_duration_seconds_sum[5m]) / rate(http_request_duration_seconds_count[5m])
      | sum(memory_used_bytes) / sum(memory_total_bytes)
  ...
```

Samples are the first N queries seen per bucket (deterministic for a given
input). They respect `-min-names`, so filtered queries don't appear.

### CDF view of the name-distance distribution

The default name-distance histogram tells you the *modal* prefix-sharing
depth across queries. If instead you want a one-glance answer to "what
fraction of queries would benefit from a name-based sharding scheme that
groups together names sharing at least `k` leading bytes?", pass `-cdf`:

```bash
gcx logs query ... | go run ./tools/query-failure-domain-analysis \
    -partitions=385 \
    -cdf
```

```
Name distance score (lexFraction(max) - lexFraction(min) over each query's matched names)
  ...
  distribution (CDF; fraction with score <= 256^-k = fraction with >= k bytes shared):
    score == 0 (single-name queries only)            42891   45.79%
    score <= 256^-16 (>= 16 bytes shared)            47995   51.24%
    score <= 256^-15 (>= 15 bytes shared)            48305   51.57%
    ...
    score <= 256^-5  (>= 5 bytes shared)             62408   66.63%
    score <= 256^-4  (>= 4 bytes shared)             73811   78.81%
    score <= 256^-3  (>= 3 bytes shared)             77212   82.44%
    score <= 256^-2  (>= 2 bytes shared)             78113   83.40%
    score <= 256^-1  (>= 1 byte shared)              78195   83.49%
    score <= 1 (all scored queries)                  93660  100.00%
```

Each row is "fraction of scored queries whose lex-min/max metric names
share at least `k` bytes". The numbers are monotonically increasing and
the final row is always `100.00%`.

### Comparing across namespaces

The Mimir query-frontend log line doesn't normally carry the Kubernetes
namespace, since namespace is a Loki *stream* label. To make the namespace
visible to this tool you can append it to each line via a LogQL
`line_format`:

```bash
gcx logs query -d <loki-uid> --from "now-48h" --to "now" --limit 0 \
  '{namespace=~"mimir-dedicated-(48|49|50)",name=~".*query-frontend.*"} \
    |= "query stats" != "/api/v1/read" \
    | logfmt | line_format "{{.__line__}} namespace={{.namespace}}"' \
  | go run ./tools/query-failure-domain-analysis \
      -schemes=lex-sort -min-names=2 -by-namespace
```

With `-by-namespace`, the tool emits the global report and then one
additional report per namespace, ordered by query count.

If you want the result as a chartable table — e.g. to drop into Google
Sheets and visualise the per-namespace prefix-sharing depth side-by-side
— pass `-csv` (optionally with `-cdf`) and redirect to a file:

```bash
gcx logs query ... | go run ./tools/query-failure-domain-analysis \
    -schemes=lex-sort -min-names=2 -cdf -csv > namespaces.csv
```

The CSV has the shape:

```
bytes_shared,mimir-dedicated-48,mimir-dedicated-49,mimir-dedicated-50
0,1.50,2.30,1.10
1,7.20,9.10,8.40
...
15,1.30,1.00,1.20
16,51.24,55.00,53.10
```

The first column (`bytes_shared`) is just the integer `k` — Google
Sheets picks this up as the x-axis label, with one series per namespace
column. The semantics depend on the mode:

- Default (per-bucket histogram): `k` means "first `k` bytes shared".
  `k` runs `0..16`; the `k = 16` row is open-ended (16 or more bytes
  shared). The "single-name queries" bucket sits off this axis, so
  it's dropped from the CSV (look at the text report for that
  number).
- `-cdf`: `k` means "at least `k` bytes shared". `k` runs `1..16`
  (the tautological `k = 0` anchor, which would always be 100%, is
  dropped). Higher `k` ⇒ stricter prefix-sharing requirement ⇒ lower
  percentage. Useful for "what fraction of queries would benefit from
  a depth-`k` lexicographic split?"

Open the file in Google Sheets and pick `Insert → Chart → Line` (or
`Bar`) with the namespace columns as series — one line per cell, all
sharing the same `bytes_shared` x-axis.

Lines that *don't* have a `namespace=` field (e.g. a partially-injected
LogQL filter or queries from outside the `line_format` scope) land in a
`(unspecified)` column.

## Flags

| Flag            | Description                                                                                                                                                                  |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-partitions`   | Number of partitions to simulate. Approximate the target cell's ingestion shuffle shard size. Default `256`.                                                                 |
| `-metric-names` | Optional path to a file with one metric name per line. Used only to resolve `__name__` regex matchers in the input; not needed otherwise.                                    |
| `-schemes`      | Comma-separated subset of `all-labels,hash-by-name,lex-sort`. Default: all three.                                                                                            |
| `-by-tenant`    | Emit an additional report per tenant (`org_id`), largest first.                                                                                                              |
| `-by-namespace` | Emit an additional report per Kubernetes namespace, largest first. Requires that each log line carries a `namespace=` field — see the "Comparing across namespaces" section below for the LogQL recipe.                                                                                                                                                                                                       |
| `-csv`          | Emit the name-distance distribution as CSV with one column per namespace plus a leading `bytes_shared` column (the integer `k`), suitable for charting in Google Sheets. Suppresses the text report. Combine with `-cdf` to emit cumulative percentages instead of per-bucket percentages.                                                                                                                    |
| `-min-names`    | If `> 0`, only analyse queries that target at least this many distinct metric names (after regex resolution). Set `-min-names=2` to focus on the multi-metric queries — like `cpu_usage / cpu_limit` — that name-based sharding is supposed to help. `fullShard` queries (no usable `__name__` constraint, or regex without a universe) are excluded by any value `> 0`. Default `0` (no filter).             |
| `-samples`      | If `> 0`, print up to this many example PromQL queries under each "Query complexity" bucket so you can spot-check what each class actually looks like. The first N queries seen per bucket are kept (deterministic). Default `0` (no samples).                                                                                                                                                              |
| `-cdf`          | Render the name-distance distribution as a CDF (`P(score ≤ 256⁻ᵏ)` at each cutoff, equivalent to "fraction of queries whose lex-min/max metric names share at least `k` bytes") instead of the default per-bucket histogram. Easier to read when the question is "how many queries would name-based sharding help?".                                                                                       |
| `-progress`     | Print parse progress to stderr every N lines (0 to disable). Default `50_000`.                                                                                               |

## Output

```
Inputs
  total log lines read:    102345
  queries parsed:          101874
  promql parse failures:   471
  fullShard queries:       8214  (no __name__ constraint, negative name matcher, or unresolved regex)
  metric-name universe:    not provided (regex-on-__name__ queries fall back to fullShard)
  simulated partitions:    385

Failure domain per scheme (partitions touched per query)
  scheme                         mean      p50      p90      p99      max  full-shard%
  all-labels (status quo)      385.00      385      385      385      385      100.00%
  hash-by-name                   2.83        2        4       12      385        8.06%
  lex-sort                       2.21        1        3        7      385        8.06%

Fraction of queries with failure domain <= K partitions
  scheme                         <=1        <=4    <=38   <=96   <=192   <=385
  all-labels (status quo)       0.00%      0.00%   0.00%  0.00%   0.00%  100.00%
  hash-by-name                 32.45%     74.18%  91.40%  93.84%  93.84%  100.00%
  lex-sort                     54.10%     85.71%  91.40%  93.84%  93.84%  100.00%

Name distance score (lexFraction(max) - lexFraction(min) over each query's matched names)
  scored queries:                   93660  (skipped 8214 fullShard / unresolved)
  single-name queries (score==0):   42891  (45.79%)
         mean         p50         p75         p90         p99         max
    1.842e-02   2.418e-15   1.107e-11   3.910e-02   6.881e-02   9.804e-02
  distribution (log-spaced; cutoffs at 256^-k mean 'first k bytes shared'):
    score == 0 (single name)                          42891   45.79%
    (0, 256^-16]           first 16+ bytes shared      5104    5.45%
    (256^-16, 256^-15]     first 15 bytes shared        310    0.33%
    ...                                                   (depths 14..6 omitted)
    (256^-6, 256^-5]       first 5 bytes shared        7012    7.49%
    (256^-5, 256^-4]       first 4 bytes shared       11403   12.18%
    (256^-4, 256^-3]       first 3 bytes shared        3401    3.63%
    (256^-3, 256^-2]       first 2 bytes shared         901    0.96%
    (256^-2, 256^-1]       first 1 byte shared           82    0.09%
    (256^-1, 1)            first byte differs          6878    7.34%

Query complexity (distinct metric names per query)
  buckets                  queries
  fullShard (0)               8214
  1                          43210
  2-5                        38201
  6-20                       10942
  21-100                      1102
  101+                         205
```

## Notes & limitations

- `lex-sort` uses a static base-26 positional encoding for bucketing
  (universe-free). The encoding folds non-letters (`_`, digits, `:`, etc.)
  to position 0, so e.g. `cpu0` and `cpua` map to the same bucket. Good
  enough for this kind of failure-domain estimation, but not exact byte
  ordering.
- The per-query name-distance score uses byte-positional `lexFraction`
  (base-256, no clamping). It is computed as a direct per-byte difference
  so prefix-shared names that would otherwise hit float64 catastrophic
  cancellation still produce meaningful scores.
- The bucketing used by `lex-sort` and the encoding used by the name
  distance score are deliberately different functions: `lexDistance` for
  the scheme (so bucket utilisation is balanced), `lexFraction` for the
  score (so the lex spectrum is byte-faithful).
- The simulation assumes evenly-sized partitions (uniform per-name
  distribution for `hash-by-name`, uniform `lexDistance` distribution for
  `lex-sort`). Real-world ingestion imbalance — which the rebalancer in the
  design doc is supposed to fix — is out of scope.
- A tenant's effective shuffle shard size is not extracted automatically.
  Pick `-partitions` to reflect the tenant(s) you care about. For mixed
  cells, re-run with `-by-tenant` to see per-tenant numbers.
- Regex matchers on `__name__` are only handled when `-metric-names` is
  provided. Without it those queries fall back to fullShard, which is
  conservative (it under-credits `lex-sort` / `hash-by-name` for any cell
  that has many regex-heavy queries).
- Series-count weighting (e.g. one metric containing 80% of a tenant's
  series) is not modelled here; this tool answers "what's the read-side
  failure domain", not "what's the resulting write balance".
