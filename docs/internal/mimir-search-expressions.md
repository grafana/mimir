# Search expressions for Prometheus and Mimir

- Status: Proposed
- Last updated: 2026-09-16
- Research snapshot: Prometheus `3fab29b0da5f31f98893b73bcc2b20fcf79d0e1e`; Mimir `49ccc1325cfdb61c4e68c8cd1fe8272da3430a3d`
- Canonical repository source: `docs/internal/mimir-search-expressions.md`

This is the single design document for the Mimir and Prometheus search-expression
project. It supersedes the earlier Google Docs **Search Expressions in Mimir**
(the Expr-library proposal) and **Mimir Search Expressions** (the initial
Mimir-only staged plan). Those documents are historical input, not parallel
specifications.

## Current implementation status

| Area | Status in this working tree |
| --- | --- |
| Mimir filter composition | PR #16592 implements AND acceptance, minimum-child scoring, rejection short-circuiting, and fail-closed empty AND behavior. |
| Mimir parser and AST | PR #16592 implements terms, quoted terms, recursive `NOT`, `AND`, `OR`, parentheses, `NOT > AND > OR` precedence, quoted keyword literals, malformed-input tests, and fuzz coverage. |
| Mimir expression compilation and evaluation | Implemented in this working tree. `searchexpr.Compile` lowers NOT with double-negation elimination and De Morgan's laws, keeps negative predicates score-neutral, and adapts the result to `storage.Filter`. `streaminglabelvalues.BuildFilter` executes an optional expression while preserving legacy repeated-term OR behavior. Unit, race, integration, and fuzz tests cover the execution path. |
| Mimir API and RPC | Not implemented. The HTTP layer does not accept `search_expr`, and the search protobuf messages do not carry an expression. Consequently, the evaluator is available to in-process callers but is not yet reachable through the public search endpoints. |
| Prometheus | No expression implementation exists in the researched tree. The existing `storage.Filter` and `storage.SearchHints.Filter` boundary is the proposed integration contract. |

The HTTP parameter, protobuf fields, rollout, and upstream Prometheus changes
described below remain proposed design. The parser/compiler and in-process
Mimir execution behavior are implemented in the local working tree for PR
#16592 but have not yet been wired to either public or internal search APIs.

## Decision summary

Add an optional `search_expr` parameter to the experimental metric-name,
label-name, and label-value search endpoints. The expression language supports
terms, quoted terms, `NOT`, `AND`, `OR`, and parentheses, with precedence
`NOT > AND > OR`.

For Prometheus, compile the expression in `web/api/v1` to the existing
`storage.Filter` contract and continue passing it through
`storage.SearchHints.Filter`. Do not add the expression AST to `storage`, and
do not change `storage.Searcher`.

For Mimir, use the same parser, semantics, and compiled-filter behavior. Carry
the expression string over the existing search RPCs, validate it at the HTTP
edge, and compile it independently in ingesters and store-gateways. Gate the
HTTP parameter until all leaf components in a deployment understand the new
wire field.

The existing `search[]` parameter remains unchanged and retains OR semantics.
`search[]` and `search_expr` are mutually exclusive.

## Why this is the recommended contract

Prometheus already separates request syntax from storage execution:

```text
HTTP search parameters
  -> web/api/v1 builds storage.SearchHints
  -> storage.Searcher
  -> TSDB evaluates SearchHints.Filter for each candidate
  -> storage merges, deduplicates, orders, and limits SearchResult values
```

The relevant contract is:

```go
type Filter interface {
	Accept(value string) (accepted bool, score float64)
}

type SearchHints struct {
	Filter  Filter
	Limit   int
	OrderBy Ordering
}
```

This boundary is sufficient for storage implementations: they do not need to
know whether a filter came from one term, repeated `search[]` parameters, or a
boolean expression. Keeping the grammar out of `storage` also avoids coupling
TSDB and remote implementations to an HTTP language that may evolve while the
Search API is experimental.

The upstream implementation points at the research snapshot are:

| Responsibility | Prometheus location |
| --- | --- |
| Storage filter and search contracts | `storage/interface.go` (`Filter`, `SearchHints`, `Searcher`) |
| Filtering, ordering, and top-K execution | `storage/generic.go` (`ApplySearchHints`) |
| HTTP parsing and hint construction | `web/api/v1/search.go` (`parseSearchParams`, `newSearchRequest`) |
| Leaf filters and current OR composition | `web/api/v1/search_filters.go` |
| TSDB implementation | `tsdb/querier.go` (`SearchLabelNames`, `SearchLabelValues`) |
| Multi-source merge contract | `storage/generic.go` (`MergeSearchResultSets`) |

### Why not use `ChainFilter` as the expression API

Prometheus currently exports `v1.ChainFilter`, but it is only referenced by its
tests. It is not a suitable expression contract as written:

- It implements AND acceptance but returns the **maximum** child score.
- An empty chain accepts every candidate with score `1.0`.
- It cannot represent OR or score-neutral negation.
- Its location in `web/api/v1` makes it an implementation helper rather than a
  cross-storage abstraction.

Mimir's current branch intentionally implements AND with the **minimum** child
score and rejects an empty AND. That prevents a candidate with one weak
required term from ranking as a perfect match merely because another required
term matched perfectly, and it fails closed if a compiler bug creates an empty
node.

The expression implementation should therefore use a dedicated internal tree
evaluator and expose only its root as `storage.Filter`. `ChainFilter` can remain
unchanged for compatibility; the expression feature does not need it.

## Goals

- Let callers narrow discovery results with boolean composition in one request.
- Preserve the existing Search API's filtering, scoring, ordering, streaming,
  deduplication, and limit behavior.
- Give Prometheus, Mimir, and other `storage.Searcher` implementations one
  execution contract.
- Keep selector scope and candidate-text filtering distinct: `match[]` chooses
  eligible series; `search_expr` filters each resulting name or value.
- Bound parser and evaluator work for untrusted input.
- Preserve source and wire compatibility for clients that use `search[]`.

## Non-goals

- Replacing PromQL or extending PromQL's expression grammar.
- Field-specific predicates, regular-expression literals, arbitrary functions,
  semantic/vector search, or an OpenSearch query DSL in the first version.
- Changing the NDJSON response format.
- Making `storage.Filter` serializable.
- Pagination or cursor support.

## User-facing API

Add one optional form/query parameter to all three endpoints:

```text
search_expr=<expression>
```

It is supported for GET query parameters and POST form data, like the existing
search parameters.

Examples:

```text
search_expr=cortex AND rule_evaluation_failures OR loki
search_expr=(cortex OR loki) AND NOT deprecated
search_expr="AND" OR "rule evaluation"
```

The other search parameters continue to configure every positive term leaf:

- `case_sensitive`
- `fuzz_alg`
- `fuzz_threshold`
- `sort_by`
- `sort_dir`
- `include_score`
- `limit`
- `batch_size`
- `match[]`

### Compatibility rules

1. Omitting `search_expr` preserves current behavior exactly.
2. Repeated `search[]` parameters retain their current OR-max behavior.
3. Supplying both `search[]` and `search_expr` returns HTTP 400. There is no
   implicit rule for combining two different syntaxes.
4. Supplying an empty or whitespace-only `search_expr` returns HTTP 400. To
   request an unfiltered search, omit both search parameters.
5. `sort_by=score` remains valid with an expression. Results accepted only by
   negative predicates have score `0` and use the existing value-ascending
   tie-break.
6. Ordering is applied before limiting. This is part of the API contract and
   was clarified by Prometheus issue #19673 and fixed by PR #19694.

## Grammar

The initial grammar is deliberately small:

```ebnf
expression = or_expression ;
or_expression = and_expression, { OR, and_expression } ;
and_expression = unary_expression, { AND, unary_expression } ;
unary_expression = NOT, unary_expression | primary ;
primary = term | "(", expression, ")" ;
term = bare_term | quoted_term ;
```

Rules:

- Operators are case-insensitive and require token boundaries.
- `NOT` binds more tightly than `AND`; `AND` binds more tightly than `OR`.
- `NOT` is recursive, so `NOT NOT foo` is valid and normalizes to `foo`.
- Binary operators associate left to right.
- Adjacent terms do not imply AND. `foo bar` is invalid; use `foo AND bar`.
- `AND`, `OR`, and `NOT` are reserved as bare terms. Quote them to search for
  the literal text.
- Double-quoted terms may contain whitespace, parentheses, or keywords.
- A backslash escapes the next character inside a quoted term.
- Empty terms and empty groups are invalid.

Mimir's parser on PR #16592 already implements these grammar and precedence
rules and has table tests, malformed-input tests, and a fuzz target. It should
be the behavioral reference for the upstream parser, subject to the resource
limits below.

## Evaluation and scoring

Acceptance and relevance are separate. Boolean negation affects acceptance but
must not accidentally make a result more relevant.

| Node | Acceptance | Score contribution |
| --- | --- | --- |
| Term | Leaf filter accepts | Leaf filter score; the existing substring, subsequence, or Jaro-Winkler behavior is unchanged |
| AND | Every child accepts | Minimum score among accepted positive children |
| OR | At least one child accepts | Maximum score among accepted positive branches |
| NOT | Child rejects | No positive score contribution |

Examples:

| Expression | Candidate state | Result |
| --- | --- | --- |
| `foo AND bar` | `foo=1.0`, `bar=0.4` | Accept, score `0.4` |
| `foo OR bar` | `foo=0.7`, `bar` rejects | Accept, score `0.7` |
| `foo AND NOT old` | `foo=0.8`, `old` rejects | Accept, score `0.8` |
| `foo OR NOT old` | `foo` rejects, `old` rejects | Accept, score `0` |
| `NOT old` | `old` rejects | Accept, score `0` |

### Internal evaluation result

`storage.Filter` has no way to distinguish an accepted zero-score result from
a boolean predicate that deliberately contributes no score. A single numeric
neutral value also cannot work for both operators: AND needs a neutral value of
`1`, while OR needs `0`.

Use a richer result only inside the expression evaluator:

```go
type evalResult struct {
	accepted bool
	score    float64
	scored   bool
}
```

The root adapter implements `storage.Filter`. If the root accepts but no
positive term contributed a score, it returns `(true, 0)`.

During compilation, carry a negation flag down the AST, eliminate double
negation, and swap AND/OR under negation according to De Morgan's laws. This
preserves the score of `NOT NOT foo`, avoids context-dependent numeric
sentinels, and makes every negative leaf explicitly unscored without allocating
a second normalized AST.

### Case folding and memoization

Keep Prometheus's current wrapper order:

1. Construct all leaf matchers against normalized query terms.
2. Compile the boolean tree.
3. Wrap the whole tree once for case folding when case-insensitive.
4. Wrap the whole tree once with the bounded memoizer when it contains an
   expensive scorer.

This ensures each candidate is lowercased and memoized once, regardless of the
number of leaves.

## Prometheus implementation design

### Package ownership

Add parser and evaluator files under `web/api/v1`, or an internal subpackage
owned by it:

```text
web/api/v1/internal/searchexpr/parser.go
web/api/v1/internal/searchexpr/eval.go
```

The parser owns the user-facing syntax. The evaluator accepts a term-filter
factory so it can reuse the existing substring/fuzzy construction without
moving HTTP-specific algorithms into `storage`.

Conceptually:

```go
func Parse(input string, limits Limits) (Expr, error)

func Compile(
	expr Expr,
	newTermFilter func(string) storage.Filter,
) storage.Filter
```

Refactor `buildSearchFilter` into:

- a leaf-filter factory for one term;
- the current legacy `search[]` OR composition;
- expression parsing and compilation;
- the existing case-folding and memoization wrappers.

### Request flow

```text
GET/POST search request
  -> parse form and common search options
  -> reject search[] + search_expr
  -> parse and validate search_expr
  -> compile to storage.Filter
  -> acquire storage.Querier
  -> pass filter in storage.SearchHints
  -> existing Searcher / TSDB / merge path
  -> existing NDJSON response
```

Parse and compile before acquiring a querier so malformed input fails cheaply
and cannot exercise storage setup.

No changes are required to:

- `storage.Filter`;
- `storage.SearchHints`;
- `storage.Searcher`;
- `storage.SearchResultSet`;
- TSDB search method signatures;
- merge, deduplication, streaming, or response types.

### Resource limits

Apply limits during tokenization/parsing, before allocating an unbounded AST:

| Limit | Initial value | Rationale |
| --- | ---: | --- |
| Expression length | 4096 bytes | Generous for interactive discovery while bounding request and error-processing work |
| Term leaves | 32 | Matches the current `maxSearchTermsPerRequest` contract |
| Parenthesis / AST depth | 16 | Prevents recursive-parser and evaluator abuse |
| Empty terms | Rejected | Avoids accidental match-all leaves |

The limit error should identify the exceeded limit without echoing the entire
expression. Parser error offsets are byte offsets, matching Go string slicing
and the current Mimir parser.

The existing memoizer limits of 100,000 entries and 10 MiB continue to bound
per-request scoring cache growth. Expression evaluation is otherwise
`O(candidate count * evaluated leaves)`, with AND rejection and perfect-score
OR short-circuiting where correct.

### Files expected to change upstream

| File | Change |
| --- | --- |
| `web/api/v1/search.go` | Parse `search_expr`, enforce exclusivity and limits, build the expression filter before opening the querier |
| `web/api/v1/search_filters.go` | Extract one-term construction and add/wire the internal expression evaluator |
| `web/api/v1/openapi_paths.go` | Document the query parameter |
| `web/api/v1/openapi_schemas.go` | Document POST form support |
| `web/api/v1/openapi_examples.go` | Add one expression example |
| `docs/querying/api.md` | Document syntax, precedence, compatibility, and score behavior |
| OpenAPI golden files | Regenerate expected specifications |

The initial upstream PR should avoid changing UI behavior. UI adoption can
follow once the API semantics are reviewed.

## Mimir implementation design

Mimir has the same logical execution contract but must cross process
boundaries. Its current search requests encode repeated terms and fuzzy
settings in `SearchFilter` protobuf messages for ingesters and store-gateways.

### In-process contract

Extend `streaminglabelvalues.Params` with an expression field while preserving
legacy terms:

```go
type Params struct {
	Terms      []string
	Expression string
	// Existing case and fuzzy fields remain unchanged.
}
```

Validation rejects simultaneous `Terms` and `Expression`. `BuildFilter`
parses/compiles `Expression` when present and otherwise preserves its current
legacy OR behavior.

### Wire contract

Add an expression string to both existing `SearchFilter` protobuf messages:

```proto
message SearchFilter {
  repeated string terms = 1;
  bool case_insensitive = 2;
  FuzzAlg fuzz_alg = 3;
  int32 fuzz_threshold = 4;
  string expression = 5;
}
```

Prefer the source expression over a recursive AST protobuf:

- the grammar remains the canonical versioned representation;
- the protobuf does not freeze internal AST node shapes;
- the expression is small and is parsed once per leaf RPC, not once per
  candidate;
- ingesters and store-gateways already compile their wire filter locally.

The querier parses the expression first to return a deterministic HTTP 400.
Leaf components parse it again before scanning, treating any failure as an
invalid-argument error. This is intentional defense in depth for internal RPC
callers and mixed-version mistakes.

### Rolling-upgrade safety

An old protobuf receiver ignores unknown field 5. If it receives an
expression with no legacy terms, it can interpret the filter as empty and run
an unbounded search. Therefore:

1. Land parser/compiler and protobuf receiver support in ingesters and
   store-gateways first.
2. Keep the HTTP `search_expr` parameter behind the existing experimental
   search feature gate plus a dedicated rollout toggle.
3. Roll all leaf components to the supporting version.
4. Enable expression forwarding in queriers only after the leaf rollout is
   complete.
5. Do not silently downgrade an expression to an empty legacy filter.

A future capability handshake could remove the operational rollout step, but
is not required for the first experimental version.

## Test plan

### Parser

- Precedence and associativity for `NOT`, `AND`, and `OR`.
- Nested parentheses and redundant parentheses.
- Case-insensitive operators.
- Quoted keywords, whitespace, parentheses, quote escapes, and backslashes.
- Every malformed boundary: empty input, missing operands, adjacent terms,
  empty groups, unmatched parentheses, and unterminated quotes.
- Each configured resource limit at, below, and above the boundary.
- Fuzz test asserting no panic and only valid, fully populated AST nodes.

### Evaluation

- AND rejection and short-circuiting.
- AND weakest-positive-term score.
- OR maximum-positive-branch score and perfect-match short-circuiting.
- NOT acceptance and score neutrality under both AND and OR.
- Double-negation and De Morgan normalization.
- Negative-only expression score `0`.
- Empty internal AND/OR nodes fail closed and trigger invariant diagnostics.
- Case folding occurs once per candidate for a composite expression.
- Memoization wraps the entire tree and respects existing entry/byte caps.

### API and storage integration

- GET and POST accept identical expressions.
- `search[]` plus `search_expr` returns 400.
- Existing `search[]` golden behavior is unchanged.
- All three endpoints apply expressions.
- `match[]` scopes series before candidate filtering.
- Score ordering, alphabetical tie-breaking, deduplication, `has_more`, and
  limit-after-ordering remain correct.
- Malformed expressions do not acquire a querier.
- Mimir ingester and store-gateway translations produce identical filters.
- Mimir mixed-version protection never turns an expression into match-all.

### Corpus scenario

Use the existing GCX-derived scenario from the Mimir work:

```text
cortex AND rule_evaluation_failures OR loki
```

It parses as:

```text
OR(AND(cortex, rule_evaluation_failures), loki)
```

| Candidate | Expected |
| --- | --- |
| `cortex_prometheus_rule_evaluation_failures_total` | Accept via AND branch |
| `loki_prometheus_rule_evaluation_failures_total` | Accept via OR branch |
| `envoy_cache_shard_00004_evaluation_failures_total` | Reject |

## Delivery plan

1. **Semantics PR:** agree on grammar, AND scoring, NOT score neutrality, HTTP
   parameter name, and limits. This document is the review artifact.
2. **Prometheus parser/compiler PR:** internal parser, normalization, evaluator,
   and unit/fuzz tests; no HTTP exposure.
3. **Prometheus API PR:** `search_expr`, validation, OpenAPI, docs, and endpoint
   tests.
4. **Mimir leaf-support PR:** land the completed in-process compiler integration,
   then add the protobuf field and receiver support in ingesters/store-gateways;
   still do not accept expressions at HTTP.
5. **Mimir API PR:** HTTP parsing and forwarding behind the rollout toggle,
   followed by end-to-end and mixed-version tests.
6. **Client/UI adoption:** add expression-aware search only after both servers'
   API behavior is available and documented.

## Alternatives considered

### Put the AST in `storage.SearchHints`

Rejected. Storage only needs per-candidate acceptance and score. An AST in
`storage` would make every Searcher implementation depend on HTTP grammar and
would force downstream systems to track AST evolution.

### Change `storage.Filter.Accept`

Rejected. The existing interface is already implemented throughout the search
path and is adequate at the boundary. The `scored` distinction is needed only
inside boolean evaluation and can be erased by the root adapter.

### Reinterpret multiple `search[]` values as AND

Rejected. The accepted proposal and shipped API define repeated values as OR.
Changing them would silently break clients.

### Allow `search[]` and `search_expr` together

Rejected for the first version. Choosing implicit AND or OR would be surprising
and would create two equivalent ways to express the same tree. Callers can put
all terms in `search_expr`.

### Serialize the AST in Mimir protobufs

Rejected initially. It freezes an internal representation into two wire
contracts without avoiding the mixed-version rollout hazard.

### Reuse PromQL parsing

Rejected. PromQL operators act on vectors and have different syntax, types,
and semantics. Search expressions act on one candidate string.

## Prior work and upstream discussion

| Work | Relevance |
| --- | --- |
| [Prometheus proposal #74](https://github.com/prometheus/proposals/pull/74) | Accepted the experimental search endpoints and proposed the `Filter`, `SearchHints`, and `Searcher` separation now present upstream. Discussion explicitly considered feasibility for distributed systems such as Mimir, Cortex, and Thanos. |
| [Proposal #74 extensibility comment](https://github.com/prometheus/proposals/pull/74#discussion_r3185955229) | Asked that the API remain extensible beyond current fuzzy query parameters, including semantic search and external services such as OpenSearch. Search expressions fit this direction without widening the storage boundary. |
| [Prometheus issue #18404](https://github.com/prometheus/prometheus/issues/18404) | Tracks the Search API implementation. It links the algorithm, storage, API, and future UI work. |
| [Prometheus PR #18402](https://github.com/prometheus/prometheus/pull/18402) | Added subsequence scoring. |
| [Prometheus PR #18405](https://github.com/prometheus/prometheus/pull/18405) | Added Jaro-Winkler scoring. |
| [Prometheus PR #18497](https://github.com/prometheus/prometheus/pull/18497) | Added `storage.Filter`, `SearchHints`, `Searcher`, result iteration, merge, and TSDB implementations. Review emphasized deterministic scoring across Searcher implementations and streaming/early-limit behavior. |
| [Prometheus PR #18573](https://github.com/prometheus/prometheus/pull/18573) | Added the HTTP endpoints, leaf filters, OR composition, case folding, bounded memoization, OpenAPI, and documentation. Its review capped repeated terms at 32 as a DoS defense. |
| [Prometheus PR #18573 scoring discussion](https://github.com/prometheus/prometheus/pull/18573#discussion_r3141028915) | Established the current `ChainFilter` choice of maximum score for its AND-like helper. Search-expression weakest-term scoring is intentionally different and needs explicit maintainer agreement. |
| [Prometheus issue #19673](https://github.com/prometheus/prometheus/issues/19673) and [PR #19694](https://github.com/prometheus/prometheus/pull/19694) | Clarified and fixed the contract that ordering happens before limiting. Expression support must preserve this behavior. |
| [Mimir PR #16592](https://github.com/grafana/mimir/pull/16592) | The local working tree for this PR implements the AND-min combinator, boolean parser/AST, and in-process compiler/evaluator with score-neutral NOT, double-negation, De Morgan, malformed-input, integration, and fuzz coverage. HTTP and protobuf wiring remain outstanding. It is the immediate implementation evidence for this design. |

Repository and GitHub searches performed on 2026-09-16 found no existing
Prometheus issue or PR that implements this boolean search-expression grammar.
The closest explicit upstream request is the proposal #74 extensibility comment
above. The new work should therefore begin with a semantics/design review rather
than assuming that the Mimir grammar and score rules are already an upstream
contract.

## Open decisions for upstream review

Only three decisions need maintainer agreement before implementation:

1. Is `search_expr` the preferred parameter name?
2. Should AND use weakest-term scoring as proposed, or preserve
   `ChainFilter`'s strongest-term scoring?
3. Are score-neutral negative predicates acceptable, with negative-only
   matches receiving score `0`?

The storage boundary, request flow, compatibility behavior, and implementation
location do not require new public interfaces.
