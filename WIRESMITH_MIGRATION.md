# Wiresmith migration status

Migration of `pkg/mimirpb/mimir.proto` from gogoproto (`protoc` + `gogoslick`)
to the [wiresmith](https://github.com/grafana/wiresmith) compiler.

## Status: pkg/mimirpb migrated, full repo builds, all touched + write-path tests green

### What migrated

- `pkg/mimirpb/mimir.proto` is compiled by wiresmith (see the Makefile grouped
  target for `pkg/mimirpb/mimir*.pb.go`). gogoproto annotations were rewritten
  to `wiresmith/options.proto` options:
  - `(wiresmith.options.customtype) = "PreallocTimeseries"` on
    `WriteRequest.timeseries` and `"UnsafeMutableLabel"` on
    `TimeSeries.labels` / `Metric.labels` / `Exemplar.labels`. These preserve
    Mimir's zero-copy (yolo-string) unmarshal path. The wiresmith
    `SizeWiresmith`/`MarshalWiresmith`/`UnmarshalWiresmith`/`EqualWiresmith`/
    `CompareWiresmith` contract is implemented in
    `pkg/mimirpb/wiresmith_adapters.go` by delegating to the existing
    gogo-style methods.
  - `(wiresmith.options.casttype) =
    ".../model/histogram.CounterResetHint"` on
    `FloatHistogram.counter_reset_hint`. Works even though
    `CounterResetHint` is a defined type over `byte` and the wire kind is
    `uint32` (Go conversions truncate as gogo did).
  - `(wiresmith.options.pointer) = true` where gogo nullability produced
    pointer shapes that hand-written code (and unsafe casts to
    `model.SampleHistogram*`) depend on: `WriteRequest.metadata`,
    `FloatHistogramPair.histogram`, `SampleHistogram.buckets`,
    `SampleHistogramPair.histogram`.
- New hand-written support files in `pkg/mimirpb/`:
  - `wiresmith_adapters.go` — customtype adapter methods.
  - `gogoproto_compat.go` — `sovMimir`/`encodeVarintMimir`/`skipMimir`
    helpers (used by hand-written marshalling code) and unprefixed enum
    constant aliases (`UNKNOWN`, `COUNTER`, `API`, `ERROR_CAUSE_*`,
    `METRIC_TYPE_*`, ...) matching gogo's `goproto_enum_prefix=false` output.
  - `gogoproto_registry.go` — gogo-registry `proto.RegisterType/Enum` calls.
    `github.com/gogo/status` (used via dskit `grpcutil.ErrorToStatus`)
    resolves `google.protobuf.Any` error details through the gogo registry;
    without these registrations `mimirpb.ErrorDetails` in gRPC statuses
    silently stops round-tripping (caught by `TestIsClientError`).
  - `unmarshal_rw2.go` — the RW2→RW1 direct unmarshalling functions
    (`TimeSeries.UnmarshalRW2`, `Exemplar.UnmarshalRW2`,
    `MetricMetadataUnmarshalRW2`), copied verbatim from the previously
    patched gogo `mimir.pb.go`. Under gogo these lived inside the generated
    file via the expected diff; they are plain hand-written code now.

### The expected-diff (expdiff) mechanism

`tools/apply-expected-diffs.sh` (invoked from `make protos`) applies
`pkg/mimirpb/mimir.pb.go.expdiff` in reverse onto freshly generated output.
The old gogo expdiff is retained as
`pkg/mimirpb/mimir.pb.go.expdiff.legacy-gogoproto` for reference.

The new expdiff against wiresmith output (~2100 lines) patches:

1. **Extra struct fields**: `WriteRequest` gains `BufferHolder`,
   `sourceBufferHolders`, `skipUnmarshalingExemplars`,
   `skipNormalizeMetadataMetricName`, `skipDeduplicateMetadata`,
   `unmarshalFromRW2`, `rw2symbols`; `TimeSeries` gains
   `SkipUnmarshalingExemplars`. (Same as under gogo.)
2. **RW2 dispatch in `WriteRequest.unmarshal`**: in RW2 mode, field 4
   (symbols) goes into paged yolo-string storage, field 5 (RW2 timeseries)
   is decoded straight into `[]PreallocTimeseries` with symbol resolution,
   RW1 fields are rejected, and collected metadata is flushed at the end.
   (Same as under gogo.)
3. **Exemplar skipping** in `TimeSeries.unmarshal` (same as under gogo).
4. **Pool-aware preallocation**: wiresmith emits a pre-count pass that
   unconditionally `make()`s repeated-field slices for payloads >= 256 bytes;
   patched to respect existing (pooled) capacity for
   `WriteRequest.Timeseries` and `TimeSeries.Labels/Samples/Exemplars/Histograms`.
5. **RW2 unmarshal stubs**: `TimeSeriesRW2`/`ExemplarRW2`/`MetadataRW2`
   `unmarshal` return `errorInternalRW2` (defensive guard, same as gogo).
6. **fieldsPresent bitmap removal** (wiresmith-specific, see blockers): the
   presence bitmap field, all `Has*` accessors, bitmap reads/writes are
   removed from every message.

To regenerate: `make protos` (or run the wiresmith invocation from the
Makefile, then `git apply -R pkg/mimirpb/mimir.pb.go.expdiff`). When the
generated output changes shape, recreate the expdiff by diffing the desired
(patched) file against pristine output:
`git diff --no-index <patched> <generated>` with paths rewritten to
`a/pkg/mimirpb/mimir.pb.go` / `b/pkg/mimirpb/mimir.pb.go`.

### Consumer adaptations

- `QueryResponse` oneof wrappers hold values, and the `string` variant is
  `QueryResponse_String`/`.String` (gogo: `QueryResponse_String_`/`.String_`):
  adapted `pkg/api/protobuf_codec.go`,
  `pkg/frontend/querymiddleware/codec_protobuf.go`,
  `pkg/ruler/remotequerier_decoder.go` and their tests.
- `pkg/continuoustest`: gogo emitted `Equal` on oneof wrapper types; added
  local `histogramCountEqual`/`histogramZeroCountEqual` helpers.
- `pkg/mimirpb/timeseries.go`: oneof interface names are exported
  (`Histogram_Count` instead of gogo's `isHistogram_Count`).
- `pkg/util/test/shape.go`: shape trees skip unexported (codegen-internal)
  fields under the same flag that skips gogo `XXX_` fields.

### Test results (all with `-count=1`)

| Package | Result |
|---|---|
| `./pkg/mimirpb/...` | ok |
| `./pkg/util/test/...` | ok |
| `./pkg/continuoustest/...` | ok |
| `./pkg/api/...` | ok |
| `./pkg/frontend/querymiddleware/...` | ok |
| `./pkg/ruler/...` | ok |
| `./pkg/distributor/...` | ok |
| `./pkg/ingester/...` | ok |
| `./pkg/storage/ingest/...` | ok (after expected-error-text fix; one kafka-based flake under load) |
| `./pkg/...` (full sweep) | 131 packages ok; pkg/ingester, pkg/compactor, pkg/storage/ingest failed under full-suite parallel load but pass in isolation (load flakes); the only real failure was TestPusherConsumer's expected unmarshal error text, fixed |

`go build ./...` and `go vet ./...` are clean (modulo pre-existing `Seek`
signature vet warnings unrelated to this migration).

### Benchmarks (Apple M4 Pro, single run each — NOT benchstat-grade)

`BenchmarkUnMarshal` (pkg/mimirpb, large multi-series requests), wiresmith
vs gogo baseline (commit cb6dac78a3 in a separate worktree):

| Bench | gogo ns/op | wiresmith ns/op | gogo B/op | wiresmith B/op | gogo allocs | wiresmith allocs |
|---|---|---|---|---|---|---|
| Marshal/RW1 | 11.79ms | 12.08ms (+2%) | 26.8MB | 26.8MB | 2 | 2 |
| Marshal/RW2 | 5.26ms | 5.48ms (+4%) | 18.4MB | 18.4MB | 2 | 2 |
| Unmarshal/RW1 skip=true | 9.24ms | 11.86ms (+28%) | 57.1MB | 31.2MB (−45%) | 60426 | 50408 (−17%) |
| Unmarshal/RW2 skip=true | 10.57ms | 12.94ms (+22%) | 32.8MB | 31.3MB | 50025 | 50015 |
| Unmarshal/RW1 skip=false | 10.96ms | 12.26ms (+12%) | 61.9MB | 32.8MB (−47%) | 100426 | 60408 (−40%) |
| Unmarshal/RW2 skip=false | 11.41ms | 13.72ms (+20%) | 34.4MB | 33.0MB | 60025 | 60016 |

Takeaway: marshal is at parity; unmarshal is ~12–28% slower wall-clock in
this single-run microbench while allocating roughly half the bytes on RW1
(wiresmith's pre-count pass produces exactly-sized slices). The slowdown is
plausibly the extra pre-scan pass over the payload (always active for these
large requests); worth a proper benchstat investigation and possibly a
wiresmith tuning knob before declaring perf parity on the write path.

## Remaining work

- The `replace github.com/grafana/wiresmith => /Users/oleg-kozlyuk/Projects/wiresmith/wiresmith`
  in `go.mod` must be replaced with a published version before this can merge;
  `vendor/github.com/grafana/wiresmith` is currently vendored from that path.
- Only `pkg/mimirpb/mimir.proto` is migrated. All other `.proto` files
  (ingester client, store-gateway, frontend, alertmanager, ...) still use
  gogoproto and `make protos`'s `%.pb.go: %.proto` protoc rule.
- `make protos` end-to-end (including protoc for the unmigrated files) not
  exercised here; the wiresmith rule + expdiff application were verified
  manually.
- Integration tests (`integration/`) not run.
- Wire-format note: for `nullable=false` singular message fields gogo always
  emitted the submessage (empty ⇒ `tag, len=0`); wiresmith (with the bitmap
  stripped) omits empty submessages. Decoded values are identical; only the
  encoded bytes differ for empty-submessage edge cases.

## Wiresmith blockers / feature requests (ranked)

1. **No way to disable per-field presence tracking (`fieldsPresent` bitmap).**
   - Where: every message in `pkg/mimirpb/mimir.proto`; worst for `Sample`,
     `BucketSpan`, `FloatHistogram`, `FloatHistogramPair`, `SampleHistogram`,
     `HistogramBucket`.
   - gogo equivalent: gogo simply has no presence for proto3 non-optional
     fields.
   - Why it blocks: (a) the bitmap changes struct memory layout, breaking
     Mimir's unsafe casts to Prometheus types (`[]Sample ↔ []promql.FPoint`,
     `[]BucketSpan ↔ []histogram.Span`, `FloatHistogram ↔
     histogram.FloatHistogram`, `SampleHistogram ↔ model.SampleHistogram`) —
     slice casts corrupt memory because the element stride differs; (b) every
     `require.Equal(literal, unmarshalled)` test in the repo fails because
     presence bits are set only on the unmarshalled side; (c) 8 bytes per
     message of dead weight for an API (`Has*`) Mimir never calls.
   - Today this is ~90% of the new expdiff (struct fields, 39 `Has*` methods,
     32 unmarshal bitmap writes, marshal/size/get branches).
   - Suggested feature: file- or message-level option, e.g.
     `(wiresmith.options.no_presence) = true`, that drops the bitmap, `Has*`
     accessors and presence-conditional emission (presence = non-zero value,
     gogo semantics).

2. **Pre-count preallocation pass clobbers caller-provided slices.**
   - Where: generated `unmarshal` of `WriteRequest`, `TimeSeries` (any
     message with repeated fields, payload >= 256B).
   - Why it blocks: Mimir pools `[]PreallocTimeseries` and the
     `TimeSeries.Labels/Samples/Exemplars/Histograms` slices
     (`PreallocWriteRequest.Unmarshal`, `TimeseriesFromPool`); wiresmith's
     unconditional `m.X = make(...)` discards the pooled backing arrays.
   - Patched in the expdiff to `if cap(m.X) < c { m.X = make(...) }`.
   - Suggested feature: emit the cap-guard by default — it is semantics-neutral
     and makes generated code pooling-friendly.

3. **No `goproto_enum_prefix=false` equivalent.**
   - Where: `ErrorCause`, `QueryStatus`, `QueryErrorType`,
     `WriteRequest.SourceEnum`, `MetricMetadata.MetricType`,
     `MetadataRW2.MetricType` — old API exposed `UNKNOWN`, `COUNTER`, `API`,
     `ERROR_CAUSE_*`, etc., used by dozens of packages.
   - Workaround: const aliases in `gogoproto_compat.go` (clean, but the alias
     file must track the proto by hand).
   - Suggested feature: enum-level option to emit unprefixed constants.

4. **No unmarshal context-threading / parent-hook for customtype.**
   - Where: `WriteRequest.timeseries`: elements need
     `skipUnmarshalingExemplars` and (for RW2) the symbols table and metadata
     set from the parent message before `UnmarshalWiresmith` runs.
   - gogo had the same limitation (this part of the expdiff is parity, not a
     regression), so this is a "would remove the need for patching" item: a
     hook like `UnmarshalWiresmithInContext(buf []byte, parent any)` or a
     generated per-message unmarshal-interceptor option would let the whole
     RW2 dispatch live in hand-written code and shrink the expdiff to just
     the extra struct fields.

5. **Minor API-shape differences requiring one-off call-site churn**
   (not blockers, recorded for completeness):
   - Oneof wrapper for a field named `string` is `QueryResponse_String` with
     field `String` (gogo: trailing-underscore names).
   - Oneof wrapper types have no `Equal` methods.
   - Oneof message variants hold values, not pointers
     (`&QueryResponse_Matrix{Matrix: data}` vs `{Matrix: &data}`).
   - Oneof interface type is exported (`Histogram_Count`) instead of gogo's
     unexported `isHistogram_Count`.
   - Getters for singular value-type message fields return `*T` (gogo
     `nullable=false` returned `T`).

### Wiresmith capabilities verified working

- `customtype` on repeated message fields with pooled/yolo custom types
  (zero-copy parity with gogo, including the marshalled-data cache in
  `PreallocTimeseries`).
- `casttype` uint32 → external defined-over-`byte` type.
- `pointer` on singular and repeated message fields.
- Wire format binary-compatible with the gogo encoder for everything tests
  cover (distributor/ingester suites round-trip through both hand-written
  and generated paths).
- Generated method surface (`Marshal`/`MarshalTo`/`MarshalToSizedBuffer`/
  `Size`/`Unmarshal`/`Reset`/`String`/`Equal`/`Compare`) is name-compatible
  with gogo, so nearly all hand-written mimirpb code compiled unchanged.
- google.golang.org/protobuf registry integration (Any round-trip through
  grpc-go status) works out of the box; the *gogo* registry needed the manual
  shim above.
