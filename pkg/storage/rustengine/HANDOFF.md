# Rust engine handoff — 2026-11-13

Work stopped at the user's request. All benchmark runners, profilers, Kubernetes
monitors and port-forwards were stopped. Do not restart jobs merely by reading
this file.

## Branch and scope

- Branch: `julienduchesne/rust-engine`; validated implementation: `6fec35a1f3`.
- Keep the ingester process in Go. Rust implements the tenant storage engine
  behind the engine interface using CGO and a static library. This is **not** the
  whole-ingester rewrite from `new-ingester-rs`.
- Go owns APIs, Kafka, offsets, ring, limits, active-series tracking and scheduling.
  Rust owns storage, transactions, postings, WAL/checkpoints, cold files and retention.
- Ingest and query streaming are implemented. See [README.md](README.md) for
  architecture, build commands, ownership rules, recovery and current limitations.
- Experimental only: no Prometheus block shipping, per-tenant write serialization,
  experimental disk format, JSON head checkpoints, no native histogram codec.
- No PR was opened. The user authorized this branch push.

## Performance: no parity claim

The Go binary and benchmark driver are frozen at new-ingester commit
`efd29f584834f127934aca2cb40f0c1b5961e8f4`. Local measurements use native Linux
ARM64 in Docker; Kubernetes uses AMD64. Do not compare architectures directly.

Completed steady-state local run, Rust `02bba6aa80`:

- About 30k series / 100 tenants / 13h; 21m warmup, 10m reads, 5m ingestion;
  query scale 5, four CPUs per engine, 6GiB limits.
- **Diagnostic lateness override: 60s**, versus the original 600s distribution.
- Rust read CPU **+6.22%**, ingestion CPU **−18.83%**; RSS about **36–37% lower**.
- All four read-window quarters were slower. A favorable 30s profile did not
  establish parity with the complete 10m counters.
- 168,052 QueryStream comparisons, zero mismatches; all nine APIs had zero
  final differences/errors in measured and retry buckets. Warmup had transient differences.

Completed dense replay, Rust `6fec35a1f3`:

- 100k slots / one tenant / 13h, original **600s lateness**, four CPUs per engine.
  Kafka buffering limited to 64MB of uncompressed bytes for both engines after
  an earlier diagnostic exceeded both engines' 6GiB limits. No native profiler
  or local compilation ran during this replay.
- Go replay **92.08s**; Rust **258.22s** (2.80× elapsed).
- Final process CPU Go **93.75s**, Rust **350.96s** (3.74×). These are cumulative
  process counters including startup, live ingestion and report collection,
  not isolated replay-window CPU counters.
- Final RSS Go **1,063,376KiB**, Rust **2,386,892KiB** (2.24×).
- No query workload or steady-state comparison in this diagnostic.
- The report's reused display label says `11f7`; `config.version_b` confirms
  actual binary `6fec35a1f3`. Source/image provenance records the correct revision.

**The small-run memory advantage does not generalize to dense replay.**

## Stopped runs

- Full Kubernetes job `ingester-bench-go-rust-julien-11f7dac126-full`, context
  `dev-us-central-0`, namespace `mimir-dev-14`, UID
  `e79fee6e-ccdb-491e-9731-cfc651ee4a09`: stopped while catching up with live writes.
  Go replay completed in 731s. Rust had passed the backfill offset but had not
  caught up when stopped. **No full steady-state report.** Replay RSS was higher
  for Rust; do not claim a full-size memory saving.
- Its Job and ConfigMap were deleted after logs/status/profile preservation;
  matching pods and ephemeral PVCs were verified absent. The earlier full
  `2b5e738bbf` job had already been removed.
- Local `linux-settled-pooled-late600-6fec35a1f3`: stopped during warmup before
  the timed read window; **no valid steady-state result**. Containers/volumes removed.
- Uploaded approved full-run image remains in the private registry:
  `us-docker.pkg.dev/grafanalabs-dev/docker-backend-enterprise-dev/ingester-bench:rust-engine-11f7dac126`
  at digest `sha256:122436a1ce206f388b8ba69e6e1d158ea855af1d2a12b5b1f8109606d8906b93`.
  It predates the latest commits. No experimental RW2 image was uploaded.

## Recent validated changes

- `c989c29df9`: indexed capped exemplar eviction instead of scanning/sorting every series.
- `7270f07c91`: shared label strings across series and posting dictionaries.
- `02bba6aa80`: leave unaffected compressed series intact during compaction.
- `11f7dac126`: compaction returns deletion deltas rather than full Go label snapshots.
- `6d9606aa0e`: immutable cold indexes shared across state snapshots.
- `4c179b4f8f`: known-series frame keys borrowed during synchronous native decoding.
- `0912669e0c`: hashed reference lookup and shared series rows with copy-on-write mutations.
- `6fec35a1f3`: ported new-ingester Kafka decompression pooling and response buffer pooling.

Latest combined native validation: **47 tests**, Clippy with warnings denied,
Go race tests for native client/ingester, and four differential traces with
**44 / 44 / 18 / 17** matching snapshots. Frontend `mimirpb` and `storage/ingest`
race suites also passed. The Rust package already contains regression coverage
for streaming, errors, rollback, crash recovery, ownership, cold reads and selectors.

## Unapplied experiments

The two patches in [experiments](experiments/) are **alternatives**, both based on
`6fec35a1f3`; apply one, not both. Both pass `git apply --check`. They are preserved
for continuation, not enabled in the branch's implementation.

1. `record-batching-and-direct-dispatch.patch`: use the existing ordered Kafka
   record path with Go decoding; replace per-sample native Request/Response
   construction with a scalar helper. Native tests/Clippy/race/four traces passed;
   decoded-versus-record Go integration passed. Scalar component fixture improved
   about 24% after the first iteration; **no whole-engine performance measurement**.
   A local ARM64 image exists as `mimir-rust-engine-bench:6fec-records-direct-arm64`.
2. `rw2-symbol-framing.patch`: includes the above and adapts new-ingester's pooled
   RW2 reader. Known floats frame directly from symbol references; unsupported/new
   series use Go decoding. Native storage remains Rust. Race/streaming tests,
   mixed-record data/errors/metrics comparison, buffer-mutation ownership test and
   exact wire-frame comparison passed using an overlay. Differential fuzzing
   passed **344,153 executions / 31.46s** after fixing first-symbol validation.
   **Not benchmarked or fully hardened**: review malformed unsupported-field
   fallback, nested metadata, rare wire forms and partial-decoding errors before
   applying. No image was built for this second patch.

The full native profile supports investigating these paths: Engine.call 15.90%
flat, Go RW2 unmarshal 7.23%, native BTree reference lookup 4.83%, Go TimeSeries.Size
4.80%, State.apply 3.84%, shard maps/hashing/reuse additional time. This profile
targets older `11f7`; map/cold-index/input changes came later. `runtime.cgocall`
contains native computation and must not be described as pure FFI overhead.

## Artifacts and reproduction cautions

Local working artifacts: `/private/tmp/rust-ingester-compare-20261112`.
Persistent results:
`/Users/julienduchesne/.codex/visualizations/2026/11/12/01a1d0d1-6bd5-7270-bbb6-3ac232655960/ingester-bench-comparison`.
These local artifacts are not uploaded by the branch push.

- `linux-settled-shared-02bba6aa80/report.html`: last completed steady-state report.
- `linux-dense-replay-v2-6fec35a1f3/report.html`: completed dense replay report.
- `kubernetes-full-11f7dac126/INCOMPLETE.json`: stopped full-run provenance/logs.
  Its `native-replay-profile` has the 30s / 99Hz user-CPU profile, 9,671 samples,
  zero lost, plus resolved flat/children reports. Additional sampling affects
  full replay timing; it did not run in any steady-state measurement window.
- `RUN_STATE.json`: pointers and stop state. Some older nested history is stale;
  this handoff and current source take precedence.
- Working directory also retains frozen source trees, image Dockerfiles/commands,
  benchmark runners, prototype overlays and the four differential oracle fixtures.

Build the static archive before Go and change a CGO macro whenever it changes;
Go's cache does **not** track `.a` contents. Use README commands. The last validated
root archive SHA was `718dce219b2dbf5f58024b6fc829e8d87fb941909a02da1665aa08d7f4032827`.
Use a **unique CARGO_TARGET_DIR per external prototype**: reusing a target directory
between same-name packages previously reused the wrong candidate's test binary.
Differential fixtures write shared output files, so run the four oracles serially.
Do not run builds/profilers alongside timed benchmark windows. Keep original
lateness and record UTC compaction phase; older 60s-lateness results are not a
like-for-like improvement claim against 600s runs.
