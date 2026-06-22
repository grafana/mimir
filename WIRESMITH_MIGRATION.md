# Wiresmith migration status

Migration of Grafana Mimir protos from gogoproto (`protoc` + `gogoslick`) to
the [wiresmith](https://github.com/grafana/wiresmith) compiler.

Toolchain: wiresmith pinned to the public pseudo-version
`github.com/grafana/wiresmith v0.0.0-20260618101418-7b3348950083`
(`grafana/wiresmith` @ `7b33489`, which adds der5 uniform pointer getters +
7m6 `google.protobuf.Any` support; supersedes the earlier
`854b4c6`/`UnmarshalNoPrescan` pin). The module is public and go-installable —
no `replace`, `GOPRIVATE`, or `url.insteadOf` needed. Install the compiler with
`go install github.com/grafana/wiresmith/cmd/wiresmith@v0.0.0-20260618101418-7b3348950083`
(use `GOPROXY=direct` if the module proxy lags). Generated code depends on
`protohelpers.SkipValue` / `protohelpers.MaxUnmarshalDepth` and (for Any-using
protos) `types/known/anypb`, vendored from that release.

> **der5 + 7m6 validation branch (`wiresmith-der5-7m6-validate`).** This branch
> validates and adopts two compiler changes pinned above (`7b33489`): **der5** —
> singular message-field getters are now uniformly `*T` in *every* presence mode
> (no_presence previously emitted value `T` getters); and **7m6** —
> `google.protobuf.Any` is supported, resolving to wiresmith's shipped
> `github.com/grafana/wiresmith/types/known/anypb` (struct `{TypeUrl, Value}` +
> wiresmith wire methods + `MarshalFrom`/`UnmarshalTo`/`UnmarshalNew`/`TypeName`
> helpers; no gogo-registry registration — `ProtoReflect` delegates to the
> official Any descriptor). The pin is the published pseudo-version with **no
> `replace`** (the vendored `anypb` is byte-identical to the `7b33489` worktree).
> Regenerating `mimir`/`ha_tracker`/`stats` against `7b33489` produced only:
> (a) two der5 getters on `mimir.proto` (`VectorHistogram.GetHistogram`,
> `TimeSeriesRW2.GetMetadata`) flipping `T → *T` (no call-site fallout — neither
> getter is called as a value getter anywhere in the repo; `go build ./...`
> clean), and (b) an unrelated marshal codegen improvement (single-byte
> length-prefix fast-path: `if len <= 0x7F { dAtA[i-1] = uint8(len); i-- } else
> { EncodeVarint }`) on every length-delimited field. The mimir expdiff still
> reverse-applies cleanly onto fresh `7b33489` output, reproducing the committed
> `mimir.pb.go` byte-for-byte (the expdiff's `@@` hunk line numbers drift ~225
> lines from the der5/fast-path shifts, but the patch body is unchanged and git
> apply's context matching absorbs the drift — verified round-trip).

**Pin status:** the go.mod pin (`v0.0.0-20260618101418-7b3348950083`, `7b33489`)
matches the checked-in generated code: `mimir`/`ha_tracker`/`stats`/`rules`
regenerate against it and (for mimir) the expdiff reverse-applies to reproduce
`mimir.pb.go` byte-for-byte, so `make check-protos` round-trips against the
pinned compiler. No `replace` remains; the vendored runtime
(`protohelpers` + `types/known/anypb`) is consistent with the pin.

## Status

| Proto                              | Status                                 | Expdiff                                                                                     |
| ---------------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------- |
| `pkg/mimirpb/mimir.proto`          | migrated (phase 1+2+3+DB-18+NoPrescan) | 1081 lines (1084 w/ guard hunk, 1094 pre-4f41063, 1087 phase 3, 1103 phase 2, 2144 phase 1) |
| `pkg/distributor/ha_tracker.proto` | migrated (phase 2)                     | none                                                                                        |
| `pkg/querier/stats/stats.proto`    | migrated (phase 2)                     | none (one hand-written `GoString` shim, kept while querymiddleware/querierpb are gogo)       |
| `pkg/ruler/rulespb/rules.proto`    | migrated (7m6/Any, validate branch)    | none (no shim needed)                                                                        |
| `pkg/streamingpromql/**` (9 protos) | migrated (cqa.1)                       | none (2 `GoString` shim methods retired in cqa.2)                                          |
| `pkg/querier/querierpb/querier.proto` | migrated (cqa.2)                     | none (gogoproto_registry.go for gogo interop)                                               |
| `pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto` | migrated (cqa.2) | none                                         |
| `pkg/scheduler/schedulerpb/scheduler.proto`   | migrated (cqa.3)                     | none                                                                 |
| `pkg/ruler/ruler.proto`                       | migrated (cqa.3)                     | none                                                                 |
| `pkg/blockbuilder/schedulerpb/scheduler.proto` | migrated (cqa.3)                    | none                                                                 |
| `pkg/compactor/scheduler/compactorschedulerpb/compactorscheduler.proto` | migrated (cqa.3) | none                                              |
| `pkg/frontend/v2/frontendv2pb/frontend.proto` | migrated (cqa.3)                     | none (wiresmith_compat.go: FreeBuffer/Buffer/SetBuffer for gRPC frame retention)             |
| `pkg/util/httpgrpcpb/httpgrpcpb.proto`        | new (cqa.3 bridge)                   | none (dskit httpgrpc local-copy bridge with conversion helpers + HeadersCarrier)             |
| `pkg/frontend/querymiddleware/model.proto`    | **deferred** — gogo Any registry dependency  | —                                                                    |
| all other protos (~5)              | still gogoproto                        | —                                                                                           |

The streamingpromql cluster (cqa.1): `types/types.proto`,
`planning/plan.proto`, `planning/core/core.proto`, and the optimize/plan node
protos `commonsubexpressionelimination/node.proto`,
`rangevectorsplitting/{node,functions}.proto`, `multiaggregation/node.proto`,
`splitandcache/node.proto`, `remoteexec/node.proto`. See the
"streamingpromql cluster" section below. **Deferred (DB-2):**
`rangevectorsplitting/cache/cache.proto` uses `querierpb.SeriesMetadata` /
`querierpb.Annotations` value message fields, so a wiresmith `cache.proto` would
call `UnmarshalWithDepth`/`EqualWiresmith`/`CompareWiresmith` on gogo-generated
querierpb types — it can only migrate once querierpb (cqa.2) does. Listed in
the cqa.1 bead but moved to cqa.2 for this reason.

Full repo builds; pkg/mimirpb, pkg/distributor, pkg/ingester,
pkg/storage/ingest, pkg/querier(+stats+worker), pkg/frontend/v2, pkg/ruler,
pkg/scheduler, pkg/blockbuilder, pkg/compactor/scheduler test suites green.

Regen reproducibility (against the pinned `databases` @ `854b4c6` compiler,
2026-06-12): `ha_tracker.{pb,_compare,_equal,_reflect}.go`, `stats.{...}.go`,
and `mimir_{compare,equal,reflect}.pb.go` regenerate byte-for-byte identical to
the committed files. `mimir.pb.go` was regenerated with `854b4c6`
to pick up the new `UnmarshalNoPrescan(dAtA []byte) error` emission (top-level
pre-scan is skipped via a depth-sentinel of −1; nested pre-scans preserved; the
generated pre-scan guard is now `if l >= 256 && depth >= 0`). `make protos`
(`854b4c6` regen + `tools/apply-expected-diffs.sh` reverse-apply) reproduces the
committed `mimir.pb.go` byte-for-byte, and the rebuilt expdiff round-trips in
both directions. The expdiff was re-derived (16 hunks → 16 hunks, but the
WriteRequest pre-scan-guard change was _dropped_: see below). Workaround review:
the **RW2 pre-scan guard is no longer a hand patch** — the hand-written
`&& !m.unmarshalFromRW2` guard is replaced by routing the RW2 path through the
generated `UnmarshalNoPrescan` (generalizing the hand fix into supported API).
The `GoString` shim, the gogo registry, and the varint helpers are still kept
(deliberate / no compiler equivalent).

## Resolved wiresmith blockers (phase 1 → phase 2)

1. **RESOLVED — presence bitmap**: `(wiresmith.options.no_presence_all) = true`
   (file) / `(wiresmith.options.no_presence)` (message) drop the
   `XXX_fieldsPresent` bitmap. mimir.proto, ha_tracker.proto and stats.proto
   all set the file option: structs are declared-fields-only again, the
   unsafe casts to Prometheus types (`[]Sample ↔ []promql.FPoint`,
   `[]BucketSpan ↔ []histogram.Span`, `FloatHistogram ↔
histogram.FloatHistogram`, `SampleHistogram ↔ model.SampleHistogram`) are
   layout-safe natively, and `require.Equal(literal, unmarshalled)` tests
   pass without shims. The phase-1 bitmap-strip expdiff hunks (~1000 lines)
   and the bitmap-only test shims (`pkg/util/test/shape.go`,
   `timeseries_test.go` unexported-field skips) were all removed/reverted.
2. **RESOLVED — pre-scan preallocation clobbering pooled slices**: the
   generated pre-count pass reserves only when the target slice is empty —
   `if len(m.X) == 0 && cap(m.X) < c { m.X = make([]T, 0, c) }` — so populated
   (pooled / merge-target) slices fall back to amortized append; the phase-1
   pool-guard expdiff hunks are gone. (This is the #134 O(n²)-pre-scan fix.
   The mimir branch was previously pinned to `a6d80cb`, which predated #134 and
   emitted a grow-and-copy variant; re-pinning to `4f41063` picked up the fix,
   which is the only generated-code change from the re-pin — see the regen note
   below.)

## mimir.proto specifics

### Annotations

- `(wiresmith.options.customtype) = "PreallocTimeseries"` on
  `WriteRequest.timeseries` and `"UnsafeMutableLabel"` on
  `TimeSeries.labels` / `Metric.labels` / `Exemplar.labels` — preserves the
  zero-copy (yolo-string) unmarshal path. Adapter methods
  (`SizeWiresmith`/`MarshalWiresmith`/`UnmarshalWiresmith`/`EqualWiresmith`/
  `CompareWiresmith`) live in `pkg/mimirpb/wiresmith_adapters.go`, delegating
  to the existing gogo-style implementations.
- `(wiresmith.options.casttype)` on `FloatHistogram.counter_reset_hint` →
  `histogram.CounterResetHint` (defined over `byte`, wire kind uint32).
- `(wiresmith.options.pointer) = true` where gogo nullability produced
  pointer shapes: `WriteRequest.metadata`, `FloatHistogramPair.histogram`,
  `SampleHistogram.buckets`, `SampleHistogramPair.histogram`.
- `(wiresmith.options.no_presence_all) = true` file-wide.
- `(wiresmith.options.enum_no_prefix_all) = true` file-wide — emits
  unprefixed enum value constants (`UNKNOWN`, `COUNTER`, `API`,
  `ERROR_CAUSE_*`, `METRIC_TYPE_*`, ...) matching gogo's
  `goproto_enum_prefix=false` output, so the previous const-alias shims in
  `gogoproto_compat.go` are gone. `Histogram.ResetHint` opts back in with
  `(wiresmith.options.enum_no_prefix) = false` (it had explicit
  `goproto_enum_prefix=true` under gogo; bare `UNKNOWN`/`GAUGE` would collide
  with `MetricMetadata.MetricType` at package scope).

### Hand-written support files (pkg/mimirpb/)

- `wiresmith_adapters.go` — customtype adapter methods.
- `gogoproto_compat.go` — `sovMimir`/`encodeVarintMimir`/`skipMimir` helpers
  used by the hand-written marshalling code (the unprefixed enum constant
  aliases that used to live here are gone; see `enum_no_prefix_all` above).
- `gogoproto_registry.go` — gogo-registry registrations;
  `github.com/gogo/status` (via dskit `grpcutil.ErrorToStatus`) resolves
  `google.protobuf.Any` error details through the gogo registry
  (caught by `TestIsClientError`).
- `unmarshal_rw2.go` — RW2→RW1 direct unmarshalling functions, copied from
  the previously patched gogo output.

### The expected-diff (expdiff)

`tools/apply-expected-diffs.sh` (from `make protos`) applies
`pkg/mimirpb/mimir.pb.go.expdiff` in reverse onto fresh output. The gogo-era
diff is retained as `mimir.pb.go.expdiff.legacy-gogoproto`.

Expdiff: **1081 lines, 16 hunks** (DB-NoPrescan: 1084/16 with the guard hunk;
pre-4f41063: 1094/16; phase 3: 1087/16; phase 2: 1103/17; phase 1: 2144). The
`854b4c6` regen re-derived the expdiff: the WriteRequest hand-patch that gated
the pre-scan on `&& !m.unmarshalFromRW2` was **dropped** — the committed guard
is now the generator's own `if l >= 256 && depth >= 0`, so that change vanishes
from the diff (the hunk that carried it shrank from `-15 +12` to `-9 +6`, now
only removing the RW2 state-var declarations). The compiler features
(`UnmarshalNoPrescan` methods, the `&& depth >= 0` guard suffix on every
pre-scan-bearing message) are emitted by the generator and so appear on the
generated side, not as hand patches. ~790 of the expdiff
lines are one hunk: the deleted generated bodies of the
`TimeSeriesRW2`/`ExemplarRW2`/`MetadataRW2` unmarshallers, replaced by
`return errorInternalRW2` stubs (the generator now also emits a
`TimeSeriesRW2.UnmarshalNoPrescan`, likewise removed by that hunk since these
messages are never standalone-unmarshalled in Mimir). The true hand-patch
surface (~307 lines):

1. Extra struct fields: `WriteRequest` (`BufferHolder`,
   `sourceBufferHolders`, `skipUnmarshalingExemplars`,
   `skipNormalizeMetadataMetricName`, `skipDeduplicateMetadata`,
   `unmarshalFromRW2`, `rw2symbols`), `TimeSeries`
   (`SkipUnmarshalingExemplars`).
2. RW2 dispatch in `WriteRequest.unmarshal`: RW1/RW2 field guards, yolo
   symbols into paged storage, RW2 series decoded straight into
   `[]PreallocTimeseries`, metadata flush; plus an RW2-aware redirect of the
   pre-count preallocation (field 5 counts preallocate `m.Timeseries`, the
   generated `SymbolsRW2`/`TimeseriesRW2` preallocations are dropped). The
   top-level pre-scan walk is **no longer** gated in the generated file: the
   RW2 path is now entered through the generated `WriteRequest.UnmarshalNoPrescan`
   (dispatched from `PreallocWriteRequest.Unmarshal` in `timeseries.go`), which
   passes a depth sentinel of −1 so the generator's own
   `if l >= 256 && depth >= 0` guard skips the walk. See the RW2 pre-scan
   section.
3. Exemplar skipping in `TimeSeries.unmarshal` (case 3 + prealloc gating).
4. RW2 unmarshal stubs (see above).

Rebuild procedure: regenerate (pristine), re-apply the patches, then
`git diff --no-index <patched> <pristine>` with paths rewritten to
`a/pkg/mimirpb/mimir.pb.go` / `b/pkg/mimirpb/mimir.pb.go`; verify with
`git apply -R` round-trip.

## ha_tracker.proto (pkg/distributor)

Zero expdiff, zero shims: `ReplicaDesc` is plain data; dskit's memberlist
`codec.NewProtoCodec` marshals through gogo interfaces that wiresmith's
method set satisfies (`Marshal`/`Unmarshal`/`Reset`/`String`/`ProtoMessage`).
Because `pkg/distributor` contains other still-gogo protos and wiresmith,
when invoked with no positional files, eagerly compiles every `.proto` under
`--proto_path`, the Makefile rule passes the target proto as a positional
argument: wiresmith then emits only that file (and resolves its transitive
imports against the `--proto_path` walk), so the still-gogo siblings are
ignored. The flat single-file layout routes output under the proto package
name (`distributor`), which lines up with the target directory when `--out`
points one level up. (This replaces the earlier scratch-dir staging recipe.)

## stats.proto (pkg/querier/stats)

- Exercises `(wiresmith.options.stdduration)`: value `time.Duration` shape
  matches gogo `stdduration+nullable=false`; the hand-written atomic
  field accesses (`atomic.AddInt64((*int64)(&s.WallTime), ...)`) work
  unchanged.
- **Cross-toolchain imports**: stats.proto is imported by three still-gogo
  protos (`querierpb/querier.proto`, `querymiddleware/model.proto`,
  `frontendv2pb/frontend.proto`), including as a gogo `customtype`
  (`SafeStats`). Their committed gogo-generated code compiles against the
  wiresmith output because the method surface matches — except gogoslick's
  `GoString`, provided as a one-method shim in
  `pkg/querier/stats/wiresmith_compat.go`. **KEPT**: as of `4f41063` the
  compiler still emits no `GoString` and exposes no option for it (verified
  against the released module), so the shim is still required while gogo
  importers remain.
- So protoc can still regenerate those importers, wiresmith's
  `options.proto` is checked in at `proto-include/wiresmith/options.proto`
  and `./proto-include` was added to the protoc `-I` path (this also
  un-breaks regeneration of gogo protos importing `pkg/mimirpb/mimir.proto`,
  latent since phase 1). `proto-include` is pruned from `PROTO_DEFS`.

## rules.proto (pkg/ruler/rulespb) — 7m6/Any migration

First proto migrated for `google.protobuf.Any` (7m6) and first to import another
**wiresmith** proto by Go module path. Chosen as the cleanest Any vehicle: its
non-WKT imports are already migrated (`mimir.proto`) and the Any field is pure
opaque passthrough.

- **Any field**: `RuleGroupDesc.options` (`repeated google.protobuf.Any`)
  resolves to `[]*anypb.Any` (`github.com/grafana/wiresmith/types/known/anypb`).
  `(wiresmith.options.pointer) = true` keeps the gogo `[]*types.Any` shape. The
  options field is an extension point for downstream `ManagerOpts`; mimir never
  populates or reads it, so no Any helper call sites needed bridging. The one
  test literal (`ruler_test.go`: `Options: []*types.Any{}`) was retargeted to
  `[]*anypb.Any{}` and the now-unused `github.com/gogo/protobuf/types` import
  dropped.
- **Annotations**: `interval`/`for`/`keep_firing_for`/`evaluationDelay`/
  `queryOffset` → `(wiresmith.options.stdduration)` (value `time.Duration`,
  matching gogo `stdduration+nullable=false`); `labels`/`annotations` →
  `(wiresmith.options.customtype) = ".../mimirpb.LabelAdapter"` (reuses the
  existing `LabelAdapter` `*Wiresmith` adapter methods in mimirpb); `rules` →
  `(wiresmith.options.pointer)` (gogo had no `nullable=false`, so `[]*RuleDesc`);
  `no_presence_all` file-wide.
- **No shims required.** `pkg/ruler/ruler.proto` is still gogo and embeds
  `*rulespb.RuleGroupDesc`/`*RuleDesc`; the gogo-generated `ruler.pb.go` compiles
  against the wiresmith output because the method surface matches
  (`Marshal`/`Unmarshal`/`Size`/`Equal`/`Reset`/`String`/`ProtoMessage`). Its
  `fmt.Sprintf("%#v", ...)` calls do not require a `GoStringer` (fall back to
  default formatting), and no test asserts on `GoString` output, so — unlike
  stats.proto — **no `GoString` shim is needed**. Persisted rule groups are
  written via gogo `proto.Marshal(group)` (bucketclient), which dispatches to the
  generated `Marshal()` method → wire-identical bytes.
- **Generator invocation**: rules.proto imports
  `github.com/grafana/mimir/pkg/mimirpb/mimir.proto` by module path, and
  wiresmith resolves imports by import-statement path under `--proto_path`. The
  repo does not place mimir.proto at that path, so the Makefile rule stages a
  temporary tree (`.rules-stage/github.com/grafana/mimir/pkg/mimirpb/mimir.proto`
  + `.rules-stage/rulespb/rules.proto`), runs wiresmith with
  `-M github.com/grafana/mimir/pkg/mimirpb/mimir.proto=github.com/grafana/mimir/pkg/mimirpb`,
  and copies the four outputs back. Regen is byte-for-byte reproducible.

## streamingpromql cluster (cqa.1)

Nine protos forming an internal dependency cluster, migrated leaf-first
(`types` → `planning/plan` → `planning/core` → `optimize/plan/*` nodes). All
adopt `(wiresmith.options.no_presence_all) = true` (gogo `nullable=false`
layout parity — the cluster is wire-serialized through the `EncodedNode.details`
bytes via gogo `proto.Marshal`/`Unmarshal`, which dispatch to wiresmith's
`Marshal()`/`Unmarshal()` methods). None needed an expdiff (all plain messages).

### Per-proto annotations

- `types/types.proto` — plain int64/bool messages; `no_presence_all` only.
- `planning/plan.proto` — `enum_no_prefix_all` (gogoslick emitted unprefixed
  `NODE_TYPE_*`); `version` field: `casttype = "QueryPlanVersion"` +
  `jsontag = "version"` (suppresses `,omitempty`); `lookbackDelta`:
  `stdduration`; `nodes`: `pointer` (gogo `[]*EncodedNode`).
- `planning/core/core.proto` — `enum_no_prefix_all` (4 enums, distinct prefixes,
  no bare-constant collisions); `casttype` on `PositionRange.{start,end}`
  (`posrange.Pos`), `VectorMatching.card`, `LabelMatcher.type`; `customtype =
  "...mimirpb.LabelAdapter"` on `FunctionCallDetails.absentLabels` (reuses the
  mimirpb adapter); `stdtime` on the three `timestamp` fields; `stdduration` on
  the offset/range/step fields; `pointer` on all `matchers` (`[]*LabelMatcher`)
  and on `BinaryExpressionDetails.{vectorMatching,hints}` (gogo `*T`).
- optimize/plan nodes — `commonsubexpressionelimination` and `multiaggregation`
  import core and use `pointer` on `core.LabelMatcher`/`AggregateExpressionDetails`
  fields; `rangevectorsplitting/functions.proto` uses `pointer` on its singular
  `cortexpb.Histogram`/`Sample` fields (gogo `*mimirpb.*`); `splitandcache` uses
  `stdduration`; `remoteexec` is plain.

### Call-site changes (legitimate adaptation, not workarounds)

- **stdtime is value-only.** gogo `stdtime + nullable` produced `*time.Time` on
  the `VectorSelectorDetails`/`MatrixSelectorDetails`/`SubqueryDetails.Timestamp`
  fields; wiresmith stdtime is value `time.Time`. The "unset" sentinel moved
  from `nil` to the zero `time.Time` (`IsZero()`), gogoproto-compatible on the
  wire. `core.TimeFromTimestamp`/`TimestampFromTime` and `describeSelector` were
  retyped to value `time.Time`; the nil-checks in `matrix_selector.go`,
  `vector_selector.go`, `subquery.go`, `info.go` and the two `timestampOf` test
  helpers were updated.
- **`LabelMatcher.Equal`**: gogo's `equal_all = false` suppressed generation, so
  `matchers.go` hand-wrote `Equal(*LabelMatcher)`. wiresmith always generates
  `Equal(interface{})` (compares Type/Name/Value identically), so the
  hand-written method was removed to avoid the duplicate-method collision; the
  one caller (`matchersEqual`) passes `*LabelMatcher`, which the generated
  signature accepts.

### GoString shims (retired in cqa.2)

`pkg/streamingpromql/{types,planning}/wiresmith_compat.go` previously added
`GoString()` to `types.EncodedQueryTimeRange`, `types.EncodedOperatorEvaluationStats`,
and `planning.EncodedQueryPlan` — the still-gogo `pkg/querier/querierpb` and
`.../rangevectorsplitting/cache` importers called `GoString()` on embedded values.
Both files were **deleted in cqa.2**: once querierpb and cache are wiresmith they
no longer call `GoString()` on these types. The `pkg/querier/stats/wiresmith_compat.go`
shim is kept because gogo frontendv2pb still embeds `Stats` and calls
`fmt.Sprintf("%#v", this.Stats)` in its generated `GoString()`.

### Generator invocation

The cluster's protos import each other by Go module path. `make protos` runs
`tools/wiresmith-streamingpromql.sh`, which stages the nine emitted protos plus
the two imported-but-not-emitted protos (`mimir.proto`, already wiresmith;
`operators/functions/functions.proto`, an enum-only gogo leaf with no options)
into a `.streamingpromql-stage` tree mirroring the module-path layout, then runs
a single wiresmith invocation with `-M` pinning every staged proto to its real
Go import path and the nine emitted protos passed positionally. Regen is
byte-for-byte reproducible. The gogo importers regenerate via protoc with
`./proto-include` supplying `wiresmith/options.proto`.

### Benchmark

`BenchmarkPlanEncodingAndDecoding` (pkg/streamingpromql) — the plan
encode/decode path that marshals/unmarshals every migrated type through the
`EncodedNode.details` bytes. See the Benchmarks section below.

## querier/frontend stats-importer cluster (cqa.2)

Four protos; two migrated, two deferred.

### Migrated: `pkg/querier/querierpb/querier.proto`

`EvaluateQueryRequest`, `EvaluationNode`, `EvaluateQueryResponse` (streaming
oneof), `SeriesMetadata`, `InstantVectorSeriesData`, `Error`, `Annotations`, etc.
Messages only — no gRPC service in this file (gRPC is on the frontendv2pb side).

- `no_presence_all` file-wide.
- `(wiresmith.options.customtype) = "github.com/grafana/mimir/pkg/mimirpb.LabelAdapter"`
  on `SeriesMetadata.labels` (same adapter as mimirpb + rules).
- **`SafeStats` customtype on `EvaluateQueryResponseEvaluationCompleted.stats`**:
  `stats.SafeStats` is a struct that embeds the wiresmith-generated `stats.Stats`
  with atomic-safe methods. Adapters added to `pkg/querier/stats/wiresmith_adapters.go`:
  `SizeWiresmith()` delegates to `Size()`; `MarshalWiresmith(buf)` delegates to
  `MarshalToSizedBuffer(buf)`; `UnmarshalWiresmith(buf)` delegates to `Unmarshal(buf)`;
  `EqualWiresmith` / `CompareWiresmith` unwrap the inner `Stats` (same pattern as
  `PreallocTimeseries` in mimirpb). The customtype value shape is `SafeStats`
  (not pointer), matching the existing gogo field layout.
- **`gogoproto_registry.go`**: `dispatcher.go` calls `proto.MessageName(&EvaluateQueryRequest{})`
  using gogo's registry. Wiresmith does not register with gogo's registry, so
  `pkg/querier/querierpb/gogoproto_registry.go` calls `proto.RegisterType()` for
  every querierpb message type in its `init()`. The wiresmith-generated types satisfy
  gogo's `proto.Message` interface (`Reset`/`String`/`ProtoMessage`/`Unmarshal`).
- **Oneof wrapper API**: wiresmith oneof wrappers hold the inner type by VALUE,
  not pointer. All call sites in `dispatcher.go`, `dispatcher_test.go`,
  `scheduler_processor_test.go`, `frontend_test.go`, and `remoteexec_test.go` were
  updated: `SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{...}` →
  `SeriesMetadata: querierpb.EvaluateQueryResponseSeriesMetadata{...}` (and similarly
  for all other wrapper fields). The `EvaluateQueryResponse` itself is embedded as
  `*querierpb.EvaluateQueryResponse` in the still-gogo `frontendv2pb` message — that
  pointer is unchanged.

### Migrated: `pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto`

Plain messages (`CachedSeries`, etc.) that import querierpb types. Deferred from
cqa.1 because wiresmith-generated cache.pb.go would call `UnmarshalWithDepth` /
`EqualWiresmith` / `CompareWiresmith` on gogo-generated querierpb types (which lack
those methods). Migrates cleanly now that querierpb is wiresmith. No expdiff, no
shims.

### Generator invocation

Script: `tools/wiresmith-cqa2.sh`. Stages the emitted protos plus all transitive
imports (mimir.proto, stats.proto, plan.proto, types.proto) into `.cqa2-stage/`
mirroring the module-path layout, runs wiresmith with `-M` flags, copies outputs back.

### Deferred: `pkg/frontend/v2/frontendv2pb/frontend.proto`

Blocker: `frontend.proto` imports `dskit/httpgrpc/httpgrpc.proto`, which is a
gogo-annotated proto. The staged `sed '/gogoproto/d'` strip solves the proto-parsing
issue, but wiresmith's generated code calls `UnmarshalWithDepth(...)`, `Clone()`, and
`Compare(...)` on embedded `*httpgrpc.HTTPResponse` and `httpgrpc.Header` fields,
assuming those types are wiresmith-generated. Since httpgrpc is gogo-generated and
lacks these methods, the output does not compile. No available workaround that
preserves the `*httpgrpc.HTTPResponse` field type at call sites. Deferred until
`dskit/httpgrpc` migrates to wiresmith or wiresmith gains a "fallback to gogo methods"
mode for cross-toolchain embedded fields.

The original gogo `frontend.proto` and its generated files are restored unchanged.
The `pkg/querier/stats/wiresmith_compat.go` `GoString` shim is kept because gogo
frontendv2pb still calls `GoString` on the embedded `Stats` field.

### Deferred: `pkg/frontend/querymiddleware/model.proto`

Blocker: `Extent.response` is `google.protobuf.Any`, and `results_cache.go` uses
`types.MarshalAny` / `types.UnmarshalAny` / `types.EmptyAny` from gogo's registry
system. Wiresmith's `anypb.Any` (`github.com/grafana/wiresmith/types/known/anypb`)
does not register with the gogo registry, so the Any helpers cannot resolve wiresmith
types. Deferred until the querymiddleware layer is decoupled from the gogo Any
registry (the same class of blocker as storepb/rpc.proto).

### Test results (cqa.2, all `-count=1`)

| Package                                                            | Result |
| ------------------------------------------------------------------ | ------ |
| `./pkg/querier/...`                                                | ok     |
| `./pkg/frontend/...`                                               | ok     |
| `./pkg/streamingpromql/optimize/...`                               | ok     |
| `go build ./pkg/querier/... ./pkg/frontend/... ./pkg/streamingpromql/optimize/...` | clean  |

## Deferred Any-using protos (7m6)

The other three Any-using protos are deferred — each is a separate dependency
cluster matching a later migration phase, not a blocker:

- **`pkg/scheduler/schedulerpb/scheduler.proto`** (`SchedulerToFrontend.payload`
  Any): imports dskit `httpgrpc.proto` (gogo-annotated) — needs the staged
  `sed '/gogoproto/d'` vendored-copy treatment (Tempo's pattern) plus two
  streaming services. Cluster-sized; deferred to the scheduler phase.
- **`pkg/frontend/querymiddleware/model.proto`** (`PrometheusResponse` Any):
  imports already-migrated `mimir.proto` + `stats.proto`, so the same staged
  module-path regen as rules.proto applies — but it is JSON-heavy
  (`jsontag` everywhere, jsonpb), needing JSON golden pins first. Deferred to the
  querymiddleware phase (it is one of the stats.proto importers listed in the
  ordering notes).
- **`pkg/storegateway/storepb/rpc.proto`** (six `hints` Any fields on DEPRECATED
  fields): **not reserved-out** — the deprecated `hints` fields are still read on
  the backward-compat path (`pkg/storegateway/bucket.go`:
  `if req.Hints != nil { types.UnmarshalAny(req.Hints, ...) }`), so removing them
  would break compatibility with older clients. rpc.proto also imports its gogo
  sibling `types.proto`, so it can only migrate as part of the whole storepb
  3-proto cluster (phase 3). Deferred.

## Test results (phase 2, all `-count=1`)

| Package                                | Result                                          |
| -------------------------------------- | ----------------------------------------------- |
| `./pkg/mimirpb/...`                    | ok                                              |
| `./pkg/distributor/...`                | ok                                              |
| `./pkg/ingester/...`                   | ok                                              |
| `./pkg/storage/ingest/...`             | ok                                              |
| `./pkg/querier/stats`, `./pkg/querier` | ok                                              |
| `./pkg/frontend/...`                   | ok                                              |
| `./pkg/util/test`                      | ok                                              |
| `go build ./...`, `go vet`             | clean (modulo pre-existing `Seek` vet warnings) |

## Test results (cqa.2, all `-count=1`)

| Package                               | Result |
| ------------------------------------- | ------ |
| `./pkg/querier/...`                   | ok     |
| `./pkg/frontend/...`                  | ok     |
| `./pkg/streamingpromql/optimize/...`  | ok     |

## Benchmarks (Apple M4 Pro, benchstat-grade — streamingpromql cqa.1, 2026-06-18)

`BenchmarkPlanEncodingAndDecoding` (pkg/streamingpromql) — gogo baseline at the
cqa.1 branch fork point (`wiresmith` @ `eccdd5b`) vs wiresmith after. Method:
two `go test -c` binaries, **alternated** 20 rounds; `-benchtime=1s -benchmem`,
`benchstat` n=20. This benchmark marshals/unmarshals the whole `EncodedQueryPlan`
(every migrated Details type, via the `EncodedNode.details` bytes) across 18
PromQL expressions, each with an `encode` and a `decode` sub-benchmark.

Geomean across all 36 sub-benchmarks:

| Metric    | gogo     | wiresmith | Δ                |
| --------- | -------- | --------- | ---------------- |
| sec/op    | 657.1n   | 610.4n    | **−7.11%**       |
| B (wire)  | 145.4    | 140.4     | **−3.44%**       |
| B/op      | 1.389Ki  | 1.298Ki   | **−6.52%**       |
| allocs/op | 26.98    | 25.50     | **−5.49%**       |

**No regression on any sub-benchmark.** Every `decode` is a significant win
(−5% to −20% wall, −5% to −25% B/op, −5% to −15% allocs) from the no_presence
struct layout plus wiresmith unmarshal; `encode` is −1% to −7% wall and emits
**fewer wire bytes** (wiresmith's single-byte length-prefix fast-path). The
migration is wire-neutral: the generated marshal field tags are byte-identical
to gogo (verified by diffing the emitted tag bytes, modulo cosmetic hex
zero-padding in the source). Encode B/op is flat on a few small expressions
(`123`, `sum(rate(foo[5m]))`) — those are the noise floor.

## Benchmarks (Apple M4 Pro, benchstat-grade — UnmarshalNoPrescan, 2026-06-12)

`BenchmarkUnMarshal` (pkg/mimirpb), gogo baseline (cb6dac78a3) vs wiresmith
`wiresmith` branch with the RW2 path routed through the generated
`UnmarshalNoPrescan` (compiler `databases` @ `854b4c6`). Method: two
`go test -c` binaries, **alternated** 20 rounds (gogo, wiresmith, gogo, …) so
thermal drift cancels across the pair; `-benchtime=2s -benchmem`, `benchstat`
n=20.

| Bench                    | gogo sec/op | wiresmith sec/op | Δ time               | B/op Δ      | allocs Δ |
| ------------------------ | ----------- | ---------------- | -------------------- | ----------- | -------- |
| Marshal/RW1              | 11.53m      | 10.38m           | **−9.93%** (p=0.000) | ~           | ~        |
| Marshal/RW2              | 5.080m      | 4.778m           | **−5.95%** (p=0.000) | ~           | ~        |
| Unmarshal/RW1 skip=true  | 8.891m      | 9.162m           | ~ (p=0.127)          | **−45.34%** | −16.58%  |
| Unmarshal/RW1 skip=false | 10.57m      | 10.08m           | **−4.60%** (p=0.000) | **−47.00%** | −39.85%  |
| Unmarshal/RW2 skip=true  | 9.923m      | 9.892m           | ~ (p=0.461)          | ~           | ~        |
| Unmarshal/RW2 skip=false | 10.72m      | 10.64m           | ~ (p=0.242)          | ~           | ~        |

Takeaways:

- **Marshal is ~6–10% _faster_** under wiresmith.
- **RW1 unmarshal is a net win**: skip=false −4.6% wall _and_ −47% bytes / −40%
  allocs; skip=true statistically flat wall (p=0.127) for −45% bytes / −17%
  allocs.
- **RW2 unmarshal is at parity** (both cases statistically indistinguishable
  from gogo: p=0.461 / p=0.242, at byte and alloc parity), down from the +15%
  regression recorded against the older `a6d80cb` pin. Routing the RW2 path
  through the generated `UnmarshalNoPrescan` skips the top-level `if l >= 256`
  walk exactly as the old `&& !m.unmarshalFromRW2` hand guard did — so the win
  is preserved while the hand patch is gone. No remaining regressions to flag.

These match or improve on the prior hand-guard run (Marshal −9.2%/−6.6%; RW1
skip=true +3.8%, skip=false −4.0%; RW2 +2.4%/+2.6%): RW2 did not regress and is
now noise-level rather than a small positive delta.

### RW2 pre-scan: why the skip exists (closed via UnmarshalNoPrescan)

`WriteRequest.unmarshal` (mimir.pb.go) runs a full extra linear pass over the
payload counting fields 1/3/4/5 to preallocate slices whenever `len >= 256`.
On RW2 the bulk of the bytes are field 4 (symbols) — and `field4count` is
explicitly discarded (`_ = field4count`) because symbols go to `m.rw2symbols`
paged storage, not a preallocated slice. So an unguarded RW2 path would pay for
a full scan of its largest section and get nothing back; only `field5count`
(timeseries) yields a small preallocation. Against the older `a6d80cb` pin this
cost RW2 ~+15% wall (a line-level CPU profile attributed ~180ms — ~5% of total,
~35% of `unmarshal`'s own flat time — to the pre-scan loop on the RW2 path).

**Original fix (DB-18 / wiresmith-bobw, commit becca9c7):** the mimir-side
expdiff gated the pre-scan with `if l >= 256 && !m.unmarshalFromRW2`, a
runtime-flag-driven hand patch in the generated file.

**Current fix (UnmarshalNoPrescan, `databases` @ `854b4c6`):** the compiler now
emits `UnmarshalNoPrescan(dAtA []byte) error` on every pre-scan-bearing message;
it calls `m.unmarshal(dAtA, -1)`, and the generated guard
`if l >= 256 && depth >= 0` skips _only_ the top-level pre-scan (the −1
sentinel; nested messages
recurse with `depth+1 >= 0`, keeping their own pre-scans; `UnmarshalWithDepth`
clamps any externally supplied `depth < 0` to 0). `PreallocWriteRequest.Unmarshal`
(`timeseries.go`) routes the RW2 path through `WriteRequest.UnmarshalNoPrescan`
and the RW1 path through plain `Unmarshal`. This generalizes the hand guard into
supported API: the `&& !m.unmarshalFromRW2` patch is dropped from the expdiff,
RW2 stays at parity, and RW1 keeps the −47%-bytes win. The `unmarshalFromRW2`
runtime flag is **kept** — it still drives `ProtocolVersion()` and the entire
RW2→RW1 decode dispatch (RW1-rejection in RW2 mode, paged-symbol handling,
field-5 redirect, metadata flush); only its pre-scan-gating role moved to the
NoPrescan entry point.

## Known limitations

### cqa.3: bounded permanent leak in QueryResultStreamRequest buffer retention

`pkg/frontend/v2/frontendv2pb/wiresmith_compat.go` implements `SetBuffer` /
`FreeBuffer` / `Buffer` on `QueryResultStreamRequest` via a global `sync.Map`
keyed by `*QueryResultStreamRequest`. The mechanism is needed because
`mimirpb.LabelAdapter.UnmarshalWiresmith` (see `pkg/mimirpb/timeseries.go`)
aliases `Name` and `Value` directly into the gRPC receive-frame buffer using
`yoloString`; that buffer must be kept alive until the caller is done with the
label strings.

**The leak:** `ProtobufResponseStream` passes decoded messages through a
1-element buffered channel. If a consumer calls `Close()` while a message is
already committed to that channel buffer, Go's runtime may select the
`notifyClosed` branch in `Next()`'s select rather than the `messages` branch.
The abandoned message is never read, `FreeBuffer` is never called, and the
`sync.Map` entry — holding strong references to both the
`*QueryResultStreamRequest` key and the `mem.Buffer` value — is never removed
and cannot be GC'd.

The leak is **bounded**: at most one entry per early-closed stream, so the
total retained memory is proportional to the number of streams abandoned with a
buffered message at any moment.

**Proper fix:** wiresmith bead **wiresmith-egvq** (P1) will add `unique`-interned
buffer-independent strings, eliminating `yoloString` frame-aliasing in
`LabelAdapter.UnmarshalWiresmith` entirely. Once that ships, the
`wiresmith_compat.go` `sync.Map` mechanism and the `SetBuffer`/`FreeBuffer`
call sites can all be removed. This bead is a prerequisite for the mimir
migration's upstream merge.

## Remaining wiresmith blockers / friction (ranked)

1. **RESOLVED (mostly) — eager whole-tree compilation under a single
   `--proto_path`.** Without positional files, wiresmith compiles every
   `.proto` under `--proto_path`, so a sibling with unresolvable imports
   (e.g. `distributorpb/distributor.proto` importing `gogoproto/gogo.proto`)
   failed the whole run. The `databases` build now compiles only the
   positional files plus their transitive imports; the Makefile rules pass
   the target proto positionally and dropped the scratch-staging recipe.
   Remaining friction: still a single `--proto_path` (no multiple roots /
   exclusion globs), and imports must be reachable under that one root.
2. **RESOLVED — RW2 unmarshal wall-clock vs gogo**: the always-on pre-scan
   pass for payloads ≥ 256B was pure overhead on RW2. Closed by the generated
   `UnmarshalNoPrescan` (`databases` @ `854b4c6`) plus the #134 pre-scan codegen
   fix; RW2 is now at parity (see benchmarks). RW1 keeps the pre-scan (net win
   there). The earlier `&& !m.unmarshalFromRW2` hand guard is gone — the
   compiler now provides the per-call top-level-skip knob (depth-sentinel −1),
   so the guard is out of the expdiff.
3. **RESOLVED — `goproto_enum_prefix=false` equivalent**:
   `(wiresmith.options.enum_no_prefix_all) = true` (file) /
   `(wiresmith.options.enum_no_prefix)` (per-enum override) emit unprefixed
   value constants. mimir.proto adopts it file-wide (with
   `Histogram.ResetHint` overriding back to `false`); the const-alias shims
   in `gogoproto_compat.go` were removed.
4. **No unmarshal context-threading / parent hook for customtype** — the RW2
   dispatch and exemplar skipping still require expdiff patching of
   `WriteRequest.unmarshal` (gogo had the same limitation; a hook would
   remove most of the remaining mimir expdiff).
5. **No `GoString`** (gogoslick parity) — still-gogo importers of a migrated
   proto call it on embedded messages; one-line shim per package. An emit
   option would remove the shim.
6. Cosmetic API churn (documented, handled at call sites): oneof wrappers
   hold values; `QueryResponse_String`/`String` naming (no trailing
   underscore); exported oneof interface names; no `Equal` on oneof wrapper
   types; singular message getters return `*T` in every presence mode
   (der5, `databases` @ `7b33489` — previously no_presence emitted value `T`
   getters; the change is now uniform and chained-call compatible, and caused
   zero call-site fallout in mimir).

## Migration ordering notes / next targets

Multiple `.proto` files per Go package now work in wiresmith, and a migrated
proto can keep gogo importers (method surface compatible + `GoString` shim +
`proto-include` for protoc). Recommended order:

1. **DONE (cqa.2)**: `pkg/querier/querierpb/querier.proto`,
   `pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache/cache.proto`.
   **Deferred (cqa.2)**: `pkg/frontend/v2/frontendv2pb/frontend.proto` (httpgrpc
   gogo incompatibility — needs dskit httpgrpc migration or wiresmith cross-toolchain
   embedded field support); `pkg/frontend/querymiddleware/model.proto` (gogo Any
   registry dependency).
2. `pkg/ruler/rulespb/rules.proto` — **DONE (validate branch)**. Next:
   `pkg/scheduler/schedulerpb/scheduler.proto` (streaming services),
   `pkg/alertmanager/alertspb/alerts.proto`.
3. `pkg/storegateway/storepb/*` — three protos in one Go package
   (now supported), cross-package hintspb imports, heavy custom code.
4. `pkg/ingester/client/ingester.proto` — large, imports mimir.proto,
   streaming service, hand-patched code similar to mimirpb.

Before merging: `go.mod` is pinned to the public `databases` pseudo-version
`v0.0.0-20260612130815-854b4c6268c2` (no `replace` — the local `replace` was
removed earlier), which matches the checked-in `mimir.pb.go` (both `854b4c6`,
`UnmarshalNoPrescan`), so `make check-protos` round-trips. Remaining: bump to
the `wiresmith` main pseudo-version once `databases` merges to main (a squash
orphans the `854b4c6` commit, but the module proxy keeps the current pin
fetchable in the meantime); run integration tests; benchstat-grade write-path
benchmarks (unit-bench deltas recorded above).
