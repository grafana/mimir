# Wiresmith migration status

Migration of Grafana Mimir protos from gogoproto (`protoc` + `gogoslick`) to
the [wiresmith](https://github.com/grafana/wiresmith) compiler.

Toolchain: wiresmith pinned to the public release
`github.com/grafana/wiresmith v0.0.0-20260611164808-4f41063d76a2`
(`origin/main` @ `4f41063`). The module is public and go-installable — no
`replace`, `GOPRIVATE`, or `url.insteadOf` needed. Install the compiler with
`go install github.com/grafana/wiresmith@4f41063d76a26c3580bf341ca1e2a8e82aaa6467`.
Generated code depends on `protohelpers.SkipValue` /
`protohelpers.MaxUnmarshalDepth`, vendored from that release.

## Status

| Proto                              | Status                       | Expdiff                                               |
| ---------------------------------- | ---------------------------- | ----------------------------------------------------- |
| `pkg/mimirpb/mimir.proto`          | migrated (phase 1+2+3+DB-18) | 1084 lines (1094 pre-4f41063, 1087 phase 3, 1103 phase 2, 2144 phase 1) |
| `pkg/distributor/ha_tracker.proto` | migrated (phase 2)           | none                                                  |
| `pkg/querier/stats/stats.proto`    | migrated (phase 2)           | none (one hand-written `GoString` shim)               |
| all other protos (~22)             | still gogoproto              | —                                                     |

Full repo builds; pkg/mimirpb, pkg/distributor, pkg/ingester,
pkg/storage/ingest, pkg/querier(+stats), pkg/frontend/... test suites green.

Regen reproducibility (against the public `4f41063` compiler, 2026-06-12):
`ha_tracker.{pb,_compare,_equal,_reflect}.go`, `stats.{...}.go`, and
`mimir_{compare,equal,reflect}.pb.go` regenerate byte-for-byte identical to the
committed files. Only `mimir.pb.go` changed, and solely because the previous
pin (`a6d80cb`) predated #134: the pre-scan prealloc codegen moved to the
amortized empty-slice form. The expdiff was re-derived (16 hunks preserved);
`tools/apply-expected-diffs.sh` round-trips. Workaround review: no shim was
made removable by a now-shipped compiler feature — the customtype / casttype /
stdduration / pointer / enum_no_prefix features were already adopted in the
protos, and the alias shims were already removed in phase 3. The `GoString`
shim, the gogo registry, the varint helpers, and the RW2 pre-scan guard are
all kept (deliberate / runtime-flag / no compiler equivalent).

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

Expdiff: **1084 lines, 16 hunks** (pre-4f41063: 1094/16; phase 3: 1087/16;
phase 2: 1103/17; phase 1: 2144). The re-pin to `4f41063` re-derived the
expdiff: the auto-generated pre-scan prealloc blocks moved from the
grow-and-copy form to the shorter empty-slice-only form (#134), shrinking the
diff by 10 lines; the hand-patch surface (struct fields, RW2 dispatch, exemplar
skipping, RW2 stubs, the `&& !m.unmarshalFromRW2` pre-scan guard) is unchanged.
~790 of those
lines are one hunk: the deleted generated bodies of the
`TimeSeriesRW2`/`ExemplarRW2`/`MetadataRW2` unmarshallers, replaced by
`return errorInternalRW2` stubs. The true hand-patch surface (~310 lines):

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
   whole `if l >= 256` pre-scan is now gated on `&& !m.unmarshalFromRW2`: in
   RW2 mode the counts are discarded (symbols paged, field5 prealloc ~0
   benefit), so the walk is skipped to fix the +15% RW2 unmarshal wall-clock
   regression (DB-18 / wiresmith-bobw); RW1 keeps the −47%-bytes win.
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

## Benchmarks (Apple M4 Pro, benchstat-grade — re-pinned to 4f41063, 2026-06-12)

`BenchmarkUnMarshal` (pkg/mimirpb), gogo baseline (cb6dac78a3) vs wiresmith
`wiresmith` branch @ wiresmith `4f41063`. Method: two `go test -c` binaries,
**alternated** 20 rounds (gogo, wiresmith, gogo, …) so thermal drift cancels
across the pair; `-benchtime=2s -benchmem`, `benchstat` n=20, every wall-clock
delta p≤0.009.

| Bench                    | gogo sec/op | wiresmith sec/op | Δ time     | B/op Δ      | allocs Δ |
| ------------------------ | ----------- | ---------------- | ---------- | ----------- | -------- |
| Marshal/RW1              | 12.18m      | 11.02m           | **−9.55%** | ~           | ~        |
| Marshal/RW2              | 5.277m      | 4.861m           | **−7.87%** | ~           | ~        |
| Unmarshal/RW1 skip=true  | 9.201m      | 9.735m           | +5.80%     | **−45.34%** | −16.58%  |
| Unmarshal/RW1 skip=false | 10.89m      | 10.62m           | **−2.50%** | **−47.00%** | −39.85%  |
| Unmarshal/RW2 skip=true  | 10.25m      | 10.42m           | +1.63%     | ~           | ~        |
| Unmarshal/RW2 skip=false | 11.00m      | 11.32m           | +2.99%     | +0.01%      | ~        |

Takeaways:

- **Marshal is ~8–10% _faster_** under wiresmith.
- **RW1 unmarshal is a net win**: skip=false −2.5% wall _and_ −47% bytes / −40%
  allocs; skip=true +5.8% wall for −45% bytes / −17% allocs.
- **RW2 unmarshal is now ~parity** (+1.6% / +3.0% wall, p≤0.003), down from the
  +15% regression recorded against the older `a6d80cb` pin. The #134 pre-scan
  codegen fix plus the `&& !m.unmarshalFromRW2` guard (DB-18) together remove
  the RW2 pre-scan penalty: in RW2 mode the `if l >= 256` walk is skipped
  entirely. There are no remaining regressions to flag.

### RW2 pre-scan: why the guard exists (now closed via DB-18)

`WriteRequest.unmarshal` (mimir.pb.go) runs a full extra linear pass over the
payload counting fields 1/3/4/5 to preallocate slices whenever `len >= 256`.
On RW2 the bulk of the bytes are field 4 (symbols) — and `field4count` is
explicitly discarded (`_ = field4count`) because symbols go to `m.rw2symbols`
paged storage, not a preallocated slice. So an unguarded RW2 path would pay for
a full scan of its largest section and get nothing back; only `field5count`
(timeseries) yields a small preallocation. Against the older `a6d80cb` pin this
cost RW2 ~+15% wall (a line-level CPU profile attributed ~180ms — ~5% of total,
~35% of `unmarshal`'s own flat time — to the pre-scan loop on the RW2 path).

**Fix (DB-18 / wiresmith-bobw, commit becca9c7):** the mimir-side expdiff gates
the pre-scan with `if l >= 256 && !m.unmarshalFromRW2`, so RW2 skips the walk
entirely while RW1 keeps the −47%-bytes win. With the `4f41063` re-pin this
brings RW2 to **~parity with gogo** (+1.6% / +3.0% wall — see the bench table),
closing the regression. This is a runtime-flag-driven decision the compiler
can't make generically, so the guard stays in the expdiff.

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
   pass for payloads ≥ 256B was pure overhead on RW2. Closed by the DB-18
   `&& !m.unmarshalFromRW2` guard plus the #134 pre-scan codegen fix; RW2 is
   now ~parity (see benchmarks). RW1 keeps the pre-scan (net win there). A
   wiresmith-native knob/heuristic to skip the counting pass when its counts
   don't drive a preallocation would let the guard move out of the expdiff,
   but is no longer blocking.
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
   types; getters for singular value message fields return `*T`.

## Migration ordering notes / next targets

Multiple `.proto` files per Go package now work in wiresmith, and a migrated
proto can keep gogo importers (method surface compatible + `GoString` shim +
`proto-include` for protoc). Recommended order:

1. `pkg/querier/querierpb/querier.proto` + `pkg/frontend/querymiddleware/model.proto`
   - `pkg/frontend/v2/frontendv2pb/frontend.proto` — the stats.proto
     importers; note `frontend.proto` uses gogo `customtype = SafeStats` on a
     message field, which wiresmith customtype can express (needs
     `SizeWiresmith`-family adapters on `SafeStats`, same recipe as
     `PreallocTimeseries`). querier.proto declares a service → exercises
     wiresmith's grpc emission. They also import mimir.proto — wiresmith
     compiles imports by module path only if present under `--proto_path`;
     staging must include the imported protos (or symlinks).
2. `pkg/ruler/rulespb/rules.proto`, `pkg/scheduler/schedulerpb/scheduler.proto`
   (streaming services), `pkg/alertmanager/alertspb/alerts.proto`.
3. `pkg/storegateway/storepb/*` — three protos in one Go package
   (now supported), cross-package hintspb imports, heavy custom code.
4. `pkg/ingester/client/ingester.proto` — large, imports mimir.proto,
   streaming service, hand-patched code similar to mimirpb.

Before merging: `go.mod` is already pinned to the public release
`v0.0.0-20260611164808-4f41063d76a2` (no `replace`). Remaining: run integration
tests; benchstat-grade write-path benchmarks (unit-bench deltas recorded above).
