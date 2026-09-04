# Bugbot review guidance

## Antithesis assertions (advisory, never blocking)

Mimir vendors the Antithesis Go SDK (`github.com/antithesishq/antithesis-sdk-go/assert`) in
`default-no-op` mode. The assertions compile to empty functions unless the binary is built with
the `enable_antithesis_sdk` build tag, so they are inert in released builds.

Look for two kinds of opportunity in a pull request:

1. **Properties that should hold.** New code usually relies on some invariant the author has in
   their head but never wrote down: a value that must always be within a range, an ordering that
   must always be respected, a state the system should be able to reach at least once. These are
   what Antithesis explores hardest, and the repo has none of them today, so raising these is the
   priority. Suggest `Always`, `AlwaysOrUnreachable`, or `Sometimes`.
2. **Bug-only paths.** A branch reachable only because of a bug, typically one that already ends
   in an error saying "this is a bug", a `panic`, or a "this should never happen" warning log.
   Suggest `Unreachable`.

How to raise it:

- Post a separate inline comment on each place worth instrumenting, anchored to those lines, the
  way a human reviewer would. Every one of them is a non-blocking suggestion: never report a
  missing assertion as a bug, and never request changes over it. The author is free to decline
  any or all of them.
- Do not stay silent just because the pull request already adds an Antithesis assertion somewhere.
  Still point out every remaining candidate of either kind, whether an unstated property or an
  uninstrumented bug-only path, and still say so when a different assertion from the SDK would fit
  a case better than the one that was used.
- A path reachable through normal operation, invalid user input, or an unavailable dependency is
  an ordinary error, not a bug. Never suggest `Unreachable` there. It may still carry a property
  worth asserting, so consider the first category instead of staying silent.
- Only consider non-test Go files under `pkg/` and `cmd/`. Ignore `_test.go` files, `vendor/`,
  `integration/`, `tools/`, `operations/`, and generated code.

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
