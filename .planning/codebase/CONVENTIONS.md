# Coding Conventions

**Analysis Date:** 2026-04-22

Mimir is a mature, multi-tenant Go codebase with strict conventions. Most of these are enforced by CI via `make check-*` targets, so following them locally saves a round trip.

## Reference Documents

Before adding new code, skim:
- `CONTRIBUTING.md` (stub pointing to `docs/internal/contributing/`)
- `docs/internal/contributing/README.md` — workflow, formatting, CHANGELOG, errors catalog, **"Unsafe memory tricks"** section
- `docs/internal/contributing/design-patterns-and-conventions.md` — Go style, import style, no globals, Prometheus metrics, config/flag naming
- `docs/internal/contributing/how-to-convert-config-to-per-tenant-limit.md`
- `docs/internal/contributing/pooling-buffers-for-grpc-messages.md`
- `pkg/CLAUDE.md` — condensed version of design patterns for in-tree reference
- Root `CLAUDE.md` — the authoritative source for the `yoloString` / `unsafeMutableString` rules

## Go Code Style

**Baseline:**
- Follow [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) and Peter Bourgon's [Go: Best Practices for Production Environments](https://peter.bourgon.org/go-in-production/).
- Comment sentences begin with the name of the thing being described and end with a period.
- No global variables. Everything (metrics, caches, clients) is passed via constructors.

**Source-level patterns you will see everywhere:**
- Constructors of the form `func New(cfg Config, deps..., reg prometheus.Registerer, log log.Logger) (*T, error)` — see `pkg/distributor/distributor.go:504` (`func New(cfg Config, clientConfig ingester_client.Config, limits *validation.Overrides, ...)`).
- `Config` structs with a `RegisterFlags(f *flag.FlagSet, ...)` method (dskit-style). Example: `pkg/distributor/distributor.go:256` defines `type Config struct` and `pkg/distributor/distributor.go:358` registers its flags. Sub-configs register their own flags via `cfg.HATrackerConfig.RegisterFlags(f, logger)` etc.
- `Config.Validate()` methods return typed package-level errors. Example: `pkg/distributor/push.go:77` validates `RetryConfig`.
- Service constructors wire dskit `services.Service` lifecycles — starting/stopping goroutines is never done in `New`.

## Naming Patterns

**Files:** `snake_case.go` (`push.go`, `ha_dedupe_per_sample.go`, `validate_test.go`).

**Packages:** short, lowercase, no underscores (`distributor`, `ingester`, `mimirpb`, `globalerror`).

**Types / funcs:** Go standard. Exported = `PascalCase`, internal = `camelCase`. Constructors: `NewX`. Error types end in `Err` or `Error` (`distributorMaxWriteMessageSizeErr` in `pkg/distributor/push.go:118`).

**CLI flags and YAML keys** (from `pkg/CLAUDE.md`):
- YAML: lowercase, `_` separated — `memcached_client`, `max_series_per_user`.
- CLI flags: lowercase, `-` separated — `-memcached-client`, `-ingester.max-global-series-per-user`.
- When a similar option already exists, reuse its name (`addresses` for endpoint lists).
- Flags referenced in docs/changelog are always prefixed with a single `-`.

## Formatting

Run the relevant formatter before every commit. These match check targets in CI.

| File type | Command | Source |
|---|---|---|
| Go | `make format` (invokes `gofmt -s -w` + `goimports -w -local github.com/grafana/mimir`) | `Makefile:471-473` |
| PromQL tests | `make format-promql-tests` (runs `tools/format-promql-test.sh`) | `Makefile:505-509` |
| Protobuf | `make format-protobuf` (`buf format`) | `Makefile:511-517` |
| Mixin (`operations/mimir-mixin/`) | `make format-mixin` then `make build-mixin` | `Makefile:597`, see `docs/internal/contributing/README.md:53` |
| Other jsonnet (`operations/mimir`, `operations/mimir-tests`, `development/`) | `make format-jsonnet-manifests` | `Makefile:750` |
| Makefiles | `make format-makefiles` | `Makefile:651-653` |

**IDE setup:** use `docs/internal/contributing/vscode-goimports-settings.json` or configure goimports-on-save. A formatting-only commit is a red flag in review.

## Import Organization

Three groups separated by a single blank line, sorted lexicographically within each group. No extra blank lines inside a group.

1. **Stdlib**
2. **Third-party** (`github.com/...`, `go.uber.org/...`, etc.)
3. **Mimir internal** (`github.com/grafana/mimir/...`)

`goimports -local github.com/grafana/mimir` enforces the split. Real example from `pkg/distributor/push.go:1-36`:

```go
import (
	"context"
	"errors"
	"flag"
	// ...stdlib...

	"github.com/go-kit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	// ...3rd party...

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)
```

**Import aliases** with underscores are common and intentional (`utillog`, `util_log`, `asmodel`, `mimir_storage`, `promcfg`). If `goimports` resolves to a same-named vendored package (e.g. otel `exemplar` vs Prometheus `exemplar`), fix the alias manually — this is called out in `.claude/skills/split-file/SKILL.md`.

## Error Handling

**User-visible errors get stable IDs via `pkg/util/globalerror`.**

- Each error ID is a constant of type `globalerror.ID` declared in `pkg/util/globalerror/user.go` (IDs like `MaxSeriesPerUser`, `SampleTooFarInFuture`, `DistributorMaxInflightPushRequests`). These IDs are **immutable once released** — never rename them.
- When an error is presented to a user, format the message through one of the `ID` helper methods so the message gets the `(err-mimir-<id>)` suffix and a remediation hint. See `pkg/util/globalerror/user.go:110-128`:
  - `id.Message(msg)` — plain form.
  - `id.MessageWithPerInstanceLimitConfig(msg, flag, addFlags...)` — instance-level limit.
  - `id.MessageWithPerTenantLimitConfig(msg, flag, addFlags...)` — per-tenant limit.
  - `id.MessageWithStrategyAndPerTenantLimitConfig(msg, strategy, flag, addFlags...)` — limit + mitigation.
- If the same error is returned from multiple places, define a single helper so the message string lives in one location and is tested once. Example: `labelNameTooLongMsgFormat` in `pkg/distributor/validate.go:78` is built once at package init.
- Every new user-facing error also needs a runbook entry under `docs/sources/mimir/manage/mimir-runbooks/_index.md`.

**gRPC error wrapping.**
- Use `globalerror.WrapErrorWithGRPCStatus(err, code, details)` (`pkg/util/globalerror/grpc.go:57`) to attach a typed `mimirpb.ErrorDetails` and preserve the underlying error via `ErrorWithStatus`.
- Use `globalerror.WrapGRPCErrorWithContextError(ctx, err)` (`pkg/util/globalerror/grpc.go:24`) when forwarding gRPC errors across a cancelled/deadlined context so the original ctx error semantics are preserved.

**General Go error handling:**
- Wrap with `fmt.Errorf("...: %w", err)` — do not `%s` errors.
- Keep package-level sentinel errors (`errNonPositiveMinBackoffDuration` in `pkg/distributor/push.go:52-54`) lowercase and unexported unless they are part of the API.
- `errors.Is` / `errors.As` for comparisons — don't string-match on `err.Error()`.

## Prometheus Metrics

From `pkg/CLAUDE.md`:
- **Never** use a global `promauto.New*` registration. Always create with `promauto.With(reg).NewXxx(...)`.
- Internal components take a `prometheus.Registerer` as a constructor argument (`NewComponent(reg prometheus.Registerer)`), never fall back to `prometheus.DefaultRegisterer`.
- Examples: `pkg/distributor/distributor.go:414-436` — `promauto.With(reg).NewCounterVec`, `promauto.With(reg).NewHistogramVec`.
- Metric names follow the `cortex_` prefix for historical compatibility (see CHANGELOG entries renaming between `cortex_*` and new names).

**Testing metrics:** use `testutil.GatherAndCompare(reg, strings.NewReader(expected), names...)` from Prometheus client_golang. See `pkg/distributor/distributor_test.go:318` and `:468`.

## Limits, Overrides, Runtime Config

- Global defaults live on `validation.Limits` in `pkg/util/validation/limits.go` (≈2000 lines). Each field has both YAML and CLI flag bindings via dskit's `flagext`.
- Runtime-tunable per-tenant limits are accessed through `*validation.Overrides`. Every per-tenant query pattern is a method like `(o *Overrides) IngestionRate(userID string) float64` (`pkg/util/validation/limits.go:785`). Do not read fields directly — go through the accessor so the runtime-config overlay is respected.
- Runtime overrides are loaded by `dskit/runtimeconfig` and wired in `pkg/mimir/mimir.go:143` (`RuntimeConfig runtimeconfig.Config`).
- To add a per-tenant limit, follow `docs/internal/contributing/how-to-convert-config-to-per-tenant-limit.md`.
- For tests, build a mock via `validation.MockOverrides(customize)` (`pkg/util/validation/limits_mock.go:33`) or `validation.NewMockTenantLimits(map)`.

## Functional Options Pattern

Mimir prefers explicit `Config` structs over functional options, but options are used in client constructors with many optional knobs. Example:

```go
// pkg/storage/ingest/writer_client.go:22
type KafkaWriterClientOption func(*kafkaWriterClientOptions)
type kafkaWriterClientOptions struct { /* ... */ }
```

When you need a similar pattern, mirror that structure — a public function type, a private options struct, and `WithX` constructors.

## Middleware / Decorator Pattern

Push path (`pkg/distributor/push.go:38-39`):

```go
type PushFunc func(ctx context.Context, req *Request) error
```

Middlewares wrap a `PushFunc` and return a new one:

```go
// pkg/distributor/distributor.go:333
func WithCleanup(next PushFunc, pushWrapper func(next PushFunc, ctx context.Context, pushReq *Request) error) PushFunc
```

Prefer decorating `PushFunc`s rather than adding flags/branches to the distributor.

## CHANGELOG

**Every user-visible change requires an entry at the top of `CHANGELOG.md` under `## main / unreleased`.**

Format: one bullet per change, with a scope tag and a PR number suffix.

```
* [CHANGE|FEATURE|ENHANCEMENT|BUGFIX] <Component>: short description. #<PR>
```

Scopes (in this order within a release):
- **[CHANGE]** — behaviour change, renames, deprecations. No new feature and no bug fix.
- **[FEATURE]** — new functionality.
- **[ENHANCEMENT]** — improves existing functionality (perf, ergonomics).
- **[BUGFIX]** — realigns behaviour to expectations.

**PR number workflow:**
1. Before opening the PR, reserve the next number with `./tools/github-next-pr-number.sh`.
2. Append `#<number>` to the changelog line.
3. Open the PR; the number must match.

Example from current `CHANGELOG.md:7`:
```
* [CHANGE] Query-frontend: Renamed `minimum_step_size` filter in `blocked_queries` configuration to `step_size_shorter_than`... #15081
```

Component names are consistent: `Distributor`, `Ingester`, `Querier`, `Query-frontend`, `Ruler`, `Store-gateway`, `Compactor`, `Alertmanager`, `All`, `Hash ring`, `Cache`, `Limits`.

## Config / Flag Documentation Regeneration

After any change to a flag or YAML config field, regenerate the reference docs and commit the result:

```bash
make reference-help doc
```

This rebuilds `cmd/mimir/help.txt.tmpl` (and `.txt.tmpl` variants) plus the generated Markdown under `docs/sources/mimir/references/configuration-parameters/`. The `check-reference-help` and `check-doc` CI targets (`Makefile:13`) will fail if you skip this.

For a no-op visual sanity check, inspect the doc generator: `tools/doc-generator/`.

## Unsafe Memory Tricks — `yoloString` / `unsafeMutableString`

**This is a correctness-critical convention. Read the root `CLAUDE.md` "⚠️ Unsafe memory tricks" section in full.**

**What it is:**
- `pkg/mimirpb/timeseries.go:478` defines `func yoloString(buf []byte) string` which uses `unsafe.String(unsafe.SliceData(buf), len(buf))` to build a `string` that aliases the underlying bytes **without copying**.
- The decompression buffers and protobuf byte slices are taken from pools (`pkg/mimirpb/timeseries_pools.go:23`, `yoloSlicePool`). The buffer is returned to the pool after the handler finishes, at which point another request may reuse the bytes.
- `PreallocWriteRequest` (`pkg/mimirpb/timeseries.go:51`) holds label `Name`/`Value` fields populated by the custom `LabelAdapter.Unmarshal` that uses `yoloString` (`pkg/mimirpb/timeseries.go:420, 451`).
- Some fields are typed `unsafeMutableString` (an alias of `string`) and labels are `unsafeMutableLabel` to flag the hazard at the type level, but the annotation is not exhaustive.

**The rule:**
- Any string value read from an unmarshalled request **must not outlive the request-handling window** (between buffer lease and buffer release back to the pool).
- If you need the string to outlive that window — for error messages, metric labels, trace attributes, cache keys, async goroutines, anything — deep-copy it with `strings.Clone` before storing it.
- `slices.Clone` on `[]string`, struct copy, and array copy **do not help** — they clone the header/reference, not the underlying bytes.

**Critical area: the distributor push path.**
- `Handler` (`pkg/distributor/push.go:88`) leases a request buffer, unmarshals into `PreallocWriteRequest`, then calls the `PushFunc` (`pkg/distributor/push.go:38`). After `PushFunc` returns, the buffer is returned to the pool.
- Any string from the `PreallocWriteRequest` that must outlive `PushFunc` is cross-tenant unsafe. Existing examples that get this right:
  - `pkg/distributor/validate.go:187-188` — `strings.Clone(unsafeMetric)`, `strings.Clone(unsafeLabel)` before storing.
  - `pkg/distributor/validate.go:600` — `labels.Label{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)}` when retained.
  - `pkg/distributor/distributor.go:1399` — `attribute.String("metric", strings.Clone(unsafeMetricName))` for span attributes.
  - `pkg/distributor/ha_dedupe_per_sample.go:224, 281-282` — clones `cluster` and `replica` because they become metric labels.
  - `pkg/distributor/ha_dedupe_per_request.go:36` — `cluster, replica = strings.Clone(cluster), strings.Clone(replica)`.

**Why this matters:** Mimir is multi-tenant. A live reference to a pooled buffer after release is a **cross-tenant data leak vector**. CI has no linter for this; reviews and tests are the guardrail.

**When in doubt, clone.** The cost of `strings.Clone` on a label value is negligible versus the cost of a security incident.

Related internal doc: `docs/internal/contributing/pooling-buffers-for-grpc-messages.md`.

## Comments

- Package-level exported types and functions should have doc comments starting with the identifier name.
- Add `// nolint:<linter>` with a comment only when there is no clean alternative. The `yoloString` function in `pkg/mimirpb/timeseries.go:479` is a legitimate `// nolint:gosec`.
- `// SPDX-License-Identifier: AGPL-3.0-only` at the top of every new Go file. For files derived from Cortex add the `Provenance-includes-*` block.

## Function and Module Design

- Keep exported API surface small. `pkg/distributor/distributor.go` is 3578 lines — decomposition into sibling files in the same package (e.g. `ha_dedupe.go`, `ha_dedupe_per_sample.go`, `validate.go`, `push.go`) is preferred over extracting subpackages.
- When a file grows unwieldy, use `.claude/skills/split-file/SKILL.md` to split it while preserving `git log --follow` history.

## Dependency Management

- Dependencies are vendored. After `go get` + `go mod tidy`, **always** run `go mod vendor` and commit `go.mod`, `go.sum`, and `vendor/` together.
- `faillint` is used to ban specific imports (see `Makefile:454`, `:458`). A new ban goes in the `lint` target.

---

*Convention analysis: 2026-04-22*
