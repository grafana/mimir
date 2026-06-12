# Wiresmith migration status

Migration of Grafana Mimir protos from gogoproto (`protoc` + `gogoslick`) to
the [wiresmith](https://github.com/grafana/wiresmith) compiler.

Toolchain: wiresmith pinned to the public `databases`-branch pseudo-version
`github.com/grafana/wiresmith v0.0.0-20260612130815-854b4c6268c2`
(`databases` @ `854b4c6`, which emits `UnmarshalNoPrescan`). The module is
public and go-installable — no `replace`, `GOPRIVATE`, or `url.insteadOf`
needed. Install the compiler with
`go install github.com/grafana/wiresmith/cmd/wiresmith@v0.0.0-20260612130815-854b4c6268c2`.
Generated code depends on `protohelpers.SkipValue` /
`protohelpers.MaxUnmarshalDepth`, vendored from that release (protohelpers is
unchanged from the prior `4f41063` pin, so the vendored runtime is identical).

**Pin status:** the go.mod pin now matches the checked-in `mimir.pb.go`. Both
were produced with `databases` @ `854b4c6` (the compiler that emits
`UnmarshalNoPrescan` — see the RW2 pre-scan section below), so
`make check-protos` round-trips against the pinned compiler. Remaining step:
bump to the corresponding `wiresmith` main pseudo-version once `databases`
merges to main. A squash-merge orphans the `854b4c6` commit, but the Go module
proxy keeps the pseudo-version fetchable, so the pin stays installable in the
interim.

## Status

| Proto                              | Status                       | Expdiff                                               |
| ---------------------------------- | ---------------------------- | ----------------------------------------------------- |
| `pkg/mimirpb/mimir.proto`          | migrated (phase 1+2+3+DB-18+NoPrescan) | 1081 lines (1084 w/ guard hunk, 1094 pre-4f41063, 1087 phase 3, 1103 phase 2, 2144 phase 1) |
| `pkg/distributor/ha_tracker.proto` | migrated (phase 2)           | none                                                  |
| `pkg/querier/stats/stats.proto`    | migrated (phase 2)           | none (one hand-written `GoString` shim)               |
| all other protos (~22)             | still gogoproto              | —                                                     |

Full repo builds; pkg/mimirpb, pkg/distributor, pkg/ingester,
pkg/storage/ingest, pkg/querier(+stats), pkg/frontend/... test suites green.

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
WriteRequest pre-scan-guard change was *dropped*: see below). Workaround review:
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
   passes a depth sentinel of −1 so the generator's own `if l >= 256 && depth
   >= 0` guard skips the walk. See the RW2 pre-scan section.
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

## Benchmarks (Apple M4 Pro, benchstat-grade — UnmarshalNoPrescan, 2026-06-12)

`BenchmarkUnMarshal` (pkg/mimirpb), gogo baseline (cb6dac78a3) vs wiresmith
`wiresmith` branch with the RW2 path routed through the generated
`UnmarshalNoPrescan` (compiler `databases` @ `854b4c6`). Method: two
`go test -c` binaries, **alternated** 20 rounds (gogo, wiresmith, gogo, …) so
thermal drift cancels across the pair; `-benchtime=2s -benchmem`, `benchstat`
n=20.

| Bench                    | gogo sec/op | wiresmith sec/op | Δ time     | B/op Δ      | allocs Δ |
| ------------------------ | ----------- | ---------------- | ---------- | ----------- | -------- |
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
it calls `m.unmarshal(dAtA, -1)`, and the generated guard `if l >= 256 && depth
>= 0` skips *only* the top-level pre-scan (the −1 sentinel; nested messages
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

Before merging: `go.mod` is pinned to the public `databases` pseudo-version
`v0.0.0-20260612130815-854b4c6268c2` (no `replace` — the local `replace` was
removed earlier), which matches the checked-in `mimir.pb.go` (both `854b4c6`,
`UnmarshalNoPrescan`), so `make check-protos` round-trips. Remaining: bump to
the `wiresmith` main pseudo-version once `databases` merges to main (a squash
orphans the `854b4c6` commit, but the module proxy keeps the current pin
fetchable in the meantime); run integration tests; benchstat-grade write-path
benchmarks (unit-bench deltas recorded above).
