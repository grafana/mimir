# Read-Tee

Read-tee is a reverse proxy that sits in front of a cell's query-frontend and amplifies Prometheus read (query) traffic to it. It is the read-path sibling of write-tee: it replays each incoming query N times against the amplified `_amp{N}` series that write-tee creates, to load-test the read path.

## Features

### Core Functionality

- **Single Endpoint** (`-backend.endpoint`): all reads are forwarded to one backend (the query-frontend). HTTP only.
- **Synchronous Original**: the original, unmodified query is sent synchronously and its response returned to the caller — this is replica 1, and it hits the base (unsuffixed) series. Amplification never slows or alters the caller's query.
- **Read Amplification**: when `-backend.amplification-factor` > 1, `N-1` rewritten copies are sent to the same endpoint fire-and-forget.
  - Factor 1.0: passthrough — only the original query is sent.
  - Factor N: original + copies with matchers suffixed `_amp1` … `_amp{N-1}` (N total queries). Integer factors only; a fractional part is truncated.
- **Per-Query Rewriting**: each query is parsed and rewritten individually, so any query shape/filter set is handled automatically (see below).
- **Amplified Routes**: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/{name}/values` (and their `/prometheus`-prefixed forms). Any other path is passed through unamplified.
- **Fire-and-Forget**: amplified copies are dispatched asynchronously, bounded by `-backend.async-max-in-flight`; excess copies are dropped and counted.

## Example Usage

Amplify a cell's read traffic 10x onto its own query-frontend:

```bash
read-tee \
  -backend.endpoint=http://query-frontend:8080 \
  -backend.amplification-factor=10.0 \
  -server.http-listen-port=8080
```

The query-frontend receives the original query synchronously (response returned immediately) plus 9 rewritten copies (`_amp1`..`_amp9`) fire-and-forget, for 10x read load.

## How Query Rewriting Works

Read-tee does **not** use a fixed query template. For *each* incoming request it parses the PromQL expression (or each `match[]` selector), walks the AST, and — for copy `k` — suffixes **label values except `__name__`**. Positive matchers get `_amp{k}`, mirroring how write-tee suffixes *series* labels, so copy `k`'s matchers select exactly the `_amp{k}` series write-tee produced. Negative matchers are handled separately (see below). Because it operates on the parsed query, it adapts to any shape or set of filters.

Per-matcher rules (copy `k`, default negative-matcher handling):

| Matcher            | Rewritten                        |
| ------------------ | -------------------------------- |
| `l="v"`            | `l="v_amp{k}"`                   |
| `l=~"re"`          | `l=~"(?:re)_amp{k}"`             |
| `l!="v"`           | `l!~"v(?:_amp[0-9]+)?"`          |
| `l!~"re"`          | `l!~"(?:re)(?:_amp[0-9]+)?"`     |
| `__name__="…"`     | unchanged (metric names are not suffixed) |
| `l=""` (absence)   | unchanged                        |

Grouping (`by`/`without`/`on`), functions, range/offset/`@`, and subqueries are left untouched — only leaf selector matchers change. Positive regexes become `(?:re)_amp{k}` because PromQL regexes are fully anchored, so the group-plus-suffix matches `<value>_amp{k}` exactly.

### Negative matchers

Positive matchers (`=`, `=~`) restrict a copy to a single replica by suffixing the value `_amp{k}`. **Negative matchers** (`!=`, `!~`) are controlled by `-backend.negative-matchers-exclude-all-amp-values`:

- **Enabled (default):** a negative matcher excludes the value in all its forms — the base value and every `_amp{N}` variant (`(?:_amp[0-9]+)?` — the value optionally followed by any `_amp{N}`). A `!=` becomes a `!~` (its literal value is regex-quoted) because a single `!=` can only exclude one exact string. The digit class (rather than `.*`) keeps the exclusion tight to genuine amplified variants and avoids dropping unrelated values that merely share the prefix. The rationale: suffixing a negative matcher with only `_amp{k}` (see below) excludes just the amp-`k` form, so a copy that isn't otherwise pinned to a single replica would still match the base series and every *other* replica.
- **Disabled:** negative matchers are suffixed `_amp{k}` like positive matchers.

> **Downside of the default:** converting `!=` to `!~` replaces an exact-value exclusion with a regex one. At query time `!~` is generally heavier than `!=` — instead of subtracting the postings for a single value, the engine must find which label values match the regex. The literal-prefix optimization in Prometheus's matcher (only regex-testing values that start with the literal) limits this, but on a high-cardinality label — especially since the amplified data multiplies distinct values — the amplified copy can be somewhat heavier than a faithful replay of the original `!=`. Disable the option to avoid it.

Examples (factor 3 → original + `_amp1`, `_amp2`):

```
sum by(job) (rate(http_requests_total{cluster="c1", job="api"}[5m]))
  → sum by(job) (rate(http_requests_total{cluster="c1_amp1", job="api_amp1"}[5m]))   (copy 1)
  → sum by(job) (rate(http_requests_total{cluster="c1_amp2", job="api_amp2"}[5m]))   (copy 2)

count(up{namespace="mimir", pod=~"ingester.*"})
  → count(up{namespace="mimir_amp1", pod=~"(?:ingester.*)_amp1"})                    (copy 1)
```

Two things to know:

- **Queries with no non-`__name__` matcher** (bare `up`, or `{__name__="up"}`) have nothing to suffix, so their copies equal the original and hit the base series — effectively not amplified.
- If a query fails to parse/rewrite, that copy is skipped and counted (`cortex_readtee_rewrite_errors_total`); the original is always still served.

### Matching all replicas (`amp.*`-mode)

Per-replica copies each read one replica's worth of series, which can make individual copies lighter than heavy production queries. `-backend.amplify-all-replicas-fraction` (0.0–1.0, default 0) samples that fraction of incoming reads and, for each one, sends a **single** copy whose matchers target the base series plus every replica at once instead of the `N-1` per-replica copies: every matcher matches the value optionally followed by any `_amp{N}` (`l="v"` → `l=~"v(?:_amp[0-9]+)?"`, `l=~"re"` → `l=~"(?:re)(?:_amp[0-9]+)?"`, and negatives likewise). This raises samples-per-query to resemble heavier production queries. These copies are counted in `cortex_readtee_amplify_all_replicas_total`. Requires `amplification-factor > 1`.

### Strong read consistency

By default amplified copies inherit the caller's read consistency (usually eventual). `-backend.strong-consistency-instant-fraction` (0.0–1.0, default 0) samples, **independently per copy**, that fraction of copies of **instant queries** (`/api/v1/query`) and sends them with the `X-Read-Consistency: strong` header. This exercises the strong-consistency read path, mirroring the ruler (which runs instant queries). Only copies are affected — the original client request is never altered. Range/series/label endpoints are never given the header. Counted in `cortex_readtee_strong_consistency_copies_total`.

## Architecture

### Request Flow

```
Gateway → Read-Tee → [Query-Frontend] (original query, synchronous) ← Response returned immediately
                   ↘ [Query-Frontend] (rewritten copies _amp1.._amp{N-1}, fire-and-forget, bounded)
```

1. **Receive**: the gateway sends a read request to read-tee (it is wired in at the gateway's query endpoint).
2. **Dispatch Copies**: when factor > 1, read-tee rewrites the query for replicas `_amp1`..`_amp{N-1}` and dispatches each to the endpoint asynchronously (fire-and-forget, bounded by max-in-flight).
3. **Send Original**: the original query is sent synchronously to the endpoint.
4. **Return**: the endpoint's response to the original is returned to the caller immediately (does not wait for copies). Copy responses are discarded.

Copies flow through the full read path (query-frontend → querier → ingesters/store-gateways) against distinct `_amp{k}` series, so they do real work with distinct results-cache keys (no dedup).

## Configuration

```
-backend.endpoint             The query-frontend endpoint to forward reads to. Required.
-backend.amplification-factor Factor N (default 1 = passthrough). Integer part = original + (N-1) copies.
-backend.negative-matchers-exclude-all-amp-values  Default true. Negative matchers (!=, !~) exclude the value in all forms (base + every _amp{N}); != becomes a regex-quoted !~. Set false to suffix negative matchers with _amp{k} like positive matchers.
-backend.amplify-all-replicas-fraction  Fraction (0.0-1.0, default 0) of reads sent as a single heavy copy matching base + all replicas instead of N-1 per-replica copies. Requires amplification-factor > 1.
-backend.strong-consistency-instant-fraction  Fraction (0.0-1.0, default 0) of instant-query copies sent with X-Read-Consistency: strong (per-copy, copies only). Mirrors the ruler.
-backend.async-max-in-flight  Max concurrent in-flight amplified copies; excess dropped and counted.
-backend.read-timeout         Timeout reading the backend response; set >= the query path's querier.timeout.
-backend.skip-tls-verify      Skip TLS verification on the backend.
-server.http-listen-port      HTTP listen port.
-server.http-write-timeout    Set >= read-timeout so long/range query responses aren't cut off.
```

## Coordinating With Write-Tee

Amplified copies only do real work if the `_amp{k}` series exist. Read-tee at factor `R` queries `_amp1`..`_amp{R-1}`; write-tee at factor `W` creates `_amp1`..`_amp{W-1}`. **Keep `R ≤ W`** — otherwise copies hit `_amp` series that were never written and return empty results (wasted load). The ksonnet library enforces this with an assertion.
