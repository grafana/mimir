# Bugbot review guidance

## Antithesis assertions (advisory, never blocking)

Mimir vendors the Antithesis Go SDK (`github.com/antithesishq/antithesis-sdk-go/assert`) in
`default-no-op` mode. The assertions compile to empty functions unless the binary is built with
the `enable_antithesis_sdk` build tag, so they are inert in released builds.

### Rule 1: bug-only paths

If a changed non-test `.go` file under `pkg/` or `cmd/` adds a line matching
`/panic\(|this is a bug|should never happen/`, and the enclosing function contains no line matching
`/assert\.(Unreachable|AlwaysOrUnreachable|Always|Sometimes|Reachable)\(/`, then:

- Add a non-blocking Bug titled "Consider an Antithesis assertion"
- Body: "This branch is reachable only because of a bug, so Antithesis can watch for it. Consider
  `assert.Unreachable(\"<short message>\", map[string]any{...})` immediately before the existing
  panic, error return, or warning log; it augments that handling rather than replacing it. Optional
  instrumentation, not a defect, so feel free to decline."

### Rule 2: properties that should hold

If a changed non-test `.go` file under `pkg/` or `cmd/` states a property that the code never
verifies, and the enclosing function contains no line matching
`/assert\.(Unreachable|AlwaysOrUnreachable|Always|Sometimes|Reachable)\(/`, then:

- Add a non-blocking Bug titled "Consider an Antithesis assertion"
- Body: "<the property, in one sentence>. Consider `assert.Always(cond, \"<short message>\",
  map[string]any{...})` where the property is established. Use `AlwaysOrUnreachable` instead if a
  request timeout or a cancelled context can skip that line, or `Sometimes` if this is a state the
  system should merely reach at least once. Optional instrumentation, not a defect, so feel free to
  decline."

A property is stated when the diff adds either of these, and nothing in the code checks it:

- A comment matching `/assumes|invariant|caller must|must always|must never|guaranteed to/`, which
  records an expectation in prose only.
- A value the surrounding code then relies on without verifying: a `len(`/`cap(` relationship, a
  bound or clamp, an ordering, or a counter that only ever increases.

Both rules: add one Bug per location, anchored to those lines. Judge each candidate on its own, so
an assertion added elsewhere in the pull request is no reason to stay quiet about the rest. Never
request changes and never block a pull request over either rule. Ignore `_test.go` files, `vendor/`,
`integration/`, `tools/`, `operations/`, and generated code. A path reachable through normal
operation, invalid user input, or an unavailable dependency is an ordinary error rather than a bug,
so never apply rule 1 there, though it may still carry a property worth rule 2.

Which assertion type to suggest:

| SDK function | Semantics | Fits |
| --- | --- | --- |
| `assert.Unreachable(msg, details)` | The path must never be reached | "Impossible" branches that are a bug by construction |
| `assert.AlwaysOrUnreachable(cond, msg, details)` | Like `Always`, but also passes if never reached | A condition that must hold whenever it is evaluated, on a path faults may stop us from reaching |
| `assert.Always(cond, msg, details)` | Must hold every time it is evaluated | Invariants on a path reached regardless of faults |
| `assert.Sometimes(cond, msg, details)` | Must hold at least once across all branches of history | Coverage probes that an interesting state is really reached, such as a retry succeeding after an earlier attempt failed |
| `assert.Reachable(msg, details)` | The path must be reached at least once | Confirming a phase actually executes |

- Between `Always` and `AlwaysOrUnreachable`: use `Always` when the path is reached regardless of
  faults, and `AlwaysOrUnreachable` when the check guards a positive outcome that fault injection
  may prevent from happening at all, such as a check on a result that only exists once the
  operation runs to completion and that a request timeout or a cancelled context can cut short.
  Choosing `Always` there produces false failures, because never reaching the assertion is a
  legitimate outcome under faults.
- Every assertion in the repo today is an `Unreachable`. Treat that as a gap to close rather than
  as the house default.
- All five functions work for code in `pkg/`. `Always`, `Sometimes`, and `Reachable` do require
  the `antithesis-go-instrumentor` pass, which this repo never runs itself, but
  `grafana/backend-enterprise` vendors Mimir and instruments the whole tree, `vendor/` included,
  before building the Antithesis image with the `enable_antithesis_sdk` tag.
- `Reachable` belongs in Antithesis workload drivers far more often than in production code.

House style. An `Unreachable` goes immediately before the existing error return, panic, or log,
augmenting that handling rather than replacing it:

```go
if len(node.Args) < 1 {
    assert.Unreachable("expected at least one argument in call", map[string]any{
        "function":      node.Function,
        "num_arguments": len(node.Args),
    })
    return false, fmt.Errorf("%w: expected at least one argument in call to %s, got %d (this is a bug)", ErrInvalidFunctionArgs, node.Function, len(node.Args))
}
```

An `Always` or `Sometimes` goes wherever the property is established, and states the condition:

```go
assert.Always(consumed <= limit, "memory consumption stays within the query limit", map[string]any{
    "consumed": consumed,
    "limit":    limit,
})
```

- Keep the condition cheap and free of side effects. The SDK compiles to an empty function without
  the build tag, but the arguments are still evaluated at the call site, so a disabled assertion is
  not necessarily free. Avoid conditions that are expensive to compute on hot paths.
- The message must be a short string literal and stay stable, because Antithesis derives one test
  property per unique message. Distinct assertions need distinct messages.
- Pass the values needed to debug the failure in the details map, using `snake_case` keys.

See `pkg/streamingpromql/optimize/plan/remove_statically_empty_expressions.go` and
`pkg/util/limiter/memory_consumption.go` for existing examples, and the
[Antithesis assertions reference](https://antithesis.com/docs/properties_assertions/assertions/)
for the full semantics.
