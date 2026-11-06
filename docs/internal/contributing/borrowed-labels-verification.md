# Borrowed label API verification

Local verification used Go 1.27.3 on darwin/arm64 (Apple M5 Pro). The change
preserves pooled, zero-copy decoding; it does not replace Go's string runtime
representation or provide lifetime checking.

## Correctness and integration

The following checks passed during implementation:

- Compilation of all package, command, tool, and integration tests with
  `go test ./pkg/... ./tools/... ./cmd/... ./integration/... -run '^$'`.
- Production command/tool builds with default labels and `-tags=slicelabels`.
- Linux/amd64 production cross-build with `CGO_ENABLED=0` and
  `-tags=netgo,stringlabels`, across packages, commands, tools and integration helpers.
- Full `pkg/mimirpb` tests with default, `slicelabels`, `nopools`, and combined
  `slicelabels,nopools` configurations, plus its race tests.
- Full default distributor, ingester, query-middleware, store-gateway, ruler,
  storage-ingestion, blockbuilder, cost-attribution, continuous-test, extraction,
  validation, and affected command-line tool suites.
- Full `slicelabels` distributor, ingester, and query-middleware suites, plus
  extraction, cost-attribution, validation, storage-ingestion and Kafka-tool tests.
- Focused race tests for HA ownership, validation after buffer release, query
  metadata ownership, ingester pushes, and restricted-string operations.
- Buffer-holder linter and its tests; changed-code lint across `pkg` and `tools`;
  `make BUILD_IN_CONTAINER=false format`; whitespace checks.
- Traffic-dump tests with `-tags=requires_libpcap`.

Regression tests cover wire compatibility against Prometheus protobuf labels,
existing JSON representation, independent copies after source-buffer mutation,
layout-dependent conversions, Remote Write 1 and 2, exemplar interoperability,
HA canonical map-key ownership, and query metadata retained after RPC release.

Generated `mimir.pb.go` was reproduced into a temporary directory with protoc
36.2 and `protoc-gen-gogoslick` v1.3.2. Applying the repository's expected diff in
reverse produced a byte-for-byte match. The pinned build image could not be
pulled because local Google Cloud credentials required reauthentication.

### Limits and environmental failures

The entire repository's behavioral suite and Docker-backed end-to-end tests
were not executed. Repository-wide `slicelabels` test compilation has an existing
failure in `pkg/util/limiter`: `requireSameLabels` is undefined in that variant.
The same error was reproduced in the original workspace. This unrelated test
configuration was not changed.

One heavily concurrent distributor rerun exhausted local TCP addresses
(`can't assign requested address`); the full suite passed again with
`GOMAXPROCS=4 go test ./pkg/distributor -parallel=2 -count=1`. An idle-TSDB timing
failure in an earlier variant run passed its focused rerun and the subsequent
full variant suite. Earlier fixture-type failures were corrected and rerun.

The language-server tool was scoped to the original workspace and rejected the
isolated worktree. Compiler, tests, exact textual edits, and source reviews were
used instead.

## Performance

Eight alternating baseline/change runs of the existing protobuf benchmarks used
`GOMAXPROCS=2`, a 250 ms benchmark duration, and precompiled test binaries.
Baseline and changed binaries were alternated in both orders. Raw local outputs
were saved under `/tmp/mimir-borrowed-paired-{baseline,after}.bench`, with benchstat
output in `/tmp/mimir-borrowed-paired-benchstat.txt`; these temporary files are not
repository artifacts.

- Remote Write 1 and 2 marshal/decode showed no statistically significant timing
  change in that comparison, with or without exemplar decoding.
- Allocation counts were unchanged across those benchmarks; allocated bytes were
  essentially unchanged (one RW2 case differed by less than 0.1%).
- Direct label hashing measured 48.20 ns to 49.05 ns (+1.76%, p=0.013). It remained
  allocation-free. Earlier runs varied in direction; do not claim exact zero cost.
- Converting to Prometheus labels and hashing measured 70.64 ns to 70.21 ns,
  without a significant change, retaining its existing one allocation.
- Basic wrapper equality measured approximately 1.6 ns, with zero allocations.
  The compiler reports the small inspection operations as inlineable.

There is a deliberate cost outside decoding: protobuf query-response encoding
now owns ordinary-string label fields. A shared owned string buffer limits this
to **one additional allocation per nonempty series**, rather than one allocation
per label name and value. Sample vector cases changed from 24 to 25 allocations
per response; a string response changed from 22 to 23. Timing was noisy, so no
precise throughput claim is made for query-response encoding. This ownership
copy is not part of the zero-copy ingestion path.

The HA grouping benchmark processes 1,000 series with one distinct pair at about
12 microseconds and 5 allocations; 100 distinct pairs take about 36 microseconds
and 311 allocations. Copies scale with distinct pairs, not series count.

### Reproduction

In a clean baseline checkout and the changed checkout, respectively, run:

```sh
GOMAXPROCS=2 go test ./pkg/mimirpb -run '^$' \
  -bench 'Benchmark(UnMarshal|NonStableHash)' -benchmem -count=8

GOMAXPROCS=2 go test ./pkg/frontend/querymiddleware -run '^$' \
  -bench 'BenchmarkProtobufFormat_EncodeResponse/(successful_string_response|successful_vector_response_with_single_series_with_one_label|successful_vector_response_with_single_series_with_many_labels)$' \
  -benchmem -benchtime=200ms -count=6

go test ./pkg/distributor -run '^$' \
  -bench BenchmarkHAReplicaGroupingOwnership -benchmem
```

Use benchstat to compare output files, and alternate binaries to reduce drift.
The broad existing frontend encoding benchmark can spend excessive time
calibrating an empty-response case; the restricted selection above avoids it.
