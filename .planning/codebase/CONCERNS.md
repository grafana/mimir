# Codebase Concerns

**Analysis Date:** 2026-04-22

This document catalogs the areas of Grafana Mimir where a contributor is most likely to trip over something subtle and cause production impact. Mimir is a multi-tenant time-series database; mistakes here have outsized consequences (cross-tenant data leakage, silent data loss, ring partitions, runaway cardinality, query correctness regressions).

Always cross-reference the root `CLAUDE.md` file (the `⚠️ Unsafe memory tricks` section) before touching the write path.

---

## Tech Debt

### Legacy Cortex heritage throughout
- Issue: Many files still carry `Provenance-includes-location: https://github.com/cortexproject/cortex/...` headers. Large swaths of the ingester, distributor, querier and alertmanager were forked from Cortex, which means code style, error types and ring semantics sometimes reflect older design decisions.
- Files: `pkg/distributor/push.go` (line 2), `pkg/util/validation/limits.go` (line 2), `pkg/alertmanager/state_replication.go`, `pkg/mimir/runtime_config.go`, many others.
- Impact: New contributors assume idioms they see are current best practice when they may be legacy. Some flags still say `cortex_` in metric names even in current code (see `pkg/alertmanager/state_replication.go:93` `alertmanager_state_replication_total`, plus a forest of `cortex_*` metric names across the codebase).
- Fix approach: When touching a file with a Cortex provenance header, check git log to see what has actually evolved before assuming patterns are current.

### Monolithic "mega-files"
- Issue: Several files have grown into thousand-plus-line monoliths that mix construction, lifecycle, push path, query path, HTTP handlers and metrics.
- Files:
  - `pkg/distributor/distributor.go` (3,578 lines)
  - `pkg/storegateway/bucket.go` (2,237 lines)
  - `pkg/util/validation/limits.go` (2,030 lines)
  - `pkg/ruler/ruler.go` (1,674 lines)
  - `pkg/mimir/modules.go` (1,579 lines)
  - `pkg/querier/blocks_store_queryable.go` (1,473 lines)
  - `pkg/frontend/querymiddleware/codec.go` (1,447 lines)
  - `pkg/storegateway/series_refs.go` (1,410 lines)
  - `pkg/alertmanager/multitenant.go` (1,395 lines)
- Impact: Hard to navigate, high review cost, easy to miss side-effects. The project has `.claude/skills/split-file/SKILL.md` explicitly because this pattern recurs.
- Fix approach: Use the `split-file` skill to break these up by logical concern (push path / query path / lifecycle / HTTP) while preserving `git log --follow` history.

### `cortex_*` metric naming frozen for compatibility
- Issue: Metrics still use the `cortex_` prefix for backward compatibility with dashboards and alerts. Newly added metrics in `pkg/distributor/push.go` (e.g. `cortex_distributor_request_body_compression_ratio`, CHANGELOG #14232) follow the same convention.
- Impact: Counterintuitive for newcomers. Renaming is a breaking change for operators.
- Fix approach: Do not rename. Add new metrics under the existing prefix scheme.

---

## Security Considerations

### ⚠️ Cross-tenant data leakage via `yoloString` / `UnsafeMutableString`

**This is the single largest correctness hazard in the codebase.** See the root `CLAUDE.md` "⚠️ Unsafe memory tricks" section.

- Risk: Mimir decompresses incoming gRPC/HTTP write requests into buffers drawn from a shared pool. Unmarshaling constructs strings (label names, label values, metric names, symbols, exemplar labels) that are **not** independent allocations — they point directly into the pooled buffer. Once the buffer is returned to the pool and reused by a different request (likely a different tenant), any string still holding a reference now reads the new tenant's data. This is **cross-tenant data leakage**.
- Core mechanics:
  - `pkg/mimirpb/timeseries.go:522-531` defines `UnsafeMutableString = string` as a documented alias that callers must treat specially. `// DO NOT SHALLOW-COPY`.
  - `pkg/mimirpb/timeseries.go:514-520` defines `UnsafeMutableLabel = LabelAdapter` with the same warning.
  - `pkg/mimirpb/timeseries.go:361-478` is a hand-rolled `Unmarshal` variant that replaces byte copying with `yoloString`. Labels, values, exemplar labels and symbols are all built this way.
  - `pkg/mimirpb/timeseries_pools.go:23` — `yoloSlicePool` backs the yolo strings.
  - `pkg/mimirpb/ref_leaks_test.go` is the integration test that proves a leak panics the process (via `debug.SetPanicOnFault`) to catch regressions in dev.
- Where to be paranoid:
  - **Distributor `PushFunc`** (`pkg/distributor/push.go`, `pkg/distributor/distributor.go`): strings from `*mimirpb.PreallocWriteRequest` must not outlive the `PushFunc` call. The `Request` type in `pkg/distributor/request.go:19-113` manages cleanup with a slice of cleanup callbacks invoked via `CleanUp()`. Adding a new middleware that retains labels requires `strings.Clone`.
  - **HA tracker** deep-copies explicitly before retaining values for dedupe bookkeeping: `pkg/distributor/ha_dedupe_per_sample.go:224,281-282` and `pkg/distributor/ha_dedupe_per_request.go:36` use `strings.Clone`.
  - **Validation summaries** that outlive the request already use `strings.Clone`: `pkg/distributor/validate.go:187-188`, `:600` and in `distributor.go:1399` for tracing attributes.
  - **Blake2b hashing** works on the unsafe bytes directly via `unsafeMutableStringToBytes` (`pkg/distributor/validate.go:630-645`) — a known-safe usage pattern; the result is copied by the hash function.
  - **Influx parser** (`pkg/distributor/influxpush/parser.go:124-196`) intentionally constructs `yoloString`s from the parser buffer. Those strings must also not escape the push handler.
  - **Store-gateway** uses `yoloString` on index-header buffers: `pkg/storage/indexheader/index/symbols.go:238`, `:150`, `postings.go:225`. These alias mmap'd index-header files and are safe as long as the index header isn't unmapped while strings are alive.
- Current mitigation:
  - `UnsafeMutableString` / `UnsafeMutableLabel` type aliases document the hazard at the declaration site.
  - `pkg/mimirpb/ref_leaks_test.go` instrument panics on reuse-after-free.
  - Convention: deep-copy with `strings.Clone` for anything that escapes the handler.
- Recommendations:
  - Treat any label name/value coming out of `PreallocWriteRequest` as toxic until proven copied.
  - `slices.Clone` on a slice of labels does **NOT** suffice — it copies the header and `LabelAdapter` structs but not the underlying string data. You must `strings.Clone` each `Name` and `Value` individually (or copy into a fresh `labels.Label`).
  - There are no guardrails beyond the tests. Reviewers must actively look for this in write-path PRs.

### Environment-based secrets
- `.env`-style files are not used directly. Config comes from YAML plus CLI flags. Cloud credentials are discovered via standard SDK chains (AWS, GCS, Azure) configured under `pkg/storage/bucket/`.
- Risk: Bucket configs in `pkg/storage/bucket/{s3,gcs,azure,swift}/` accept inline secret access keys. Logging/operator runbooks must be careful not to echo them.

### Alertmanager multi-tenant state persistence
- Risk: `pkg/alertmanager/state_replication.go` replicates silences and notification logs across the AM ring. A bug here can cause tenant A's silences to apply to tenant B.
- Files: `pkg/alertmanager/state_replication.go`, `pkg/alertmanager/state_persister.go`, `pkg/alertmanager/multitenant.go`.
- Current mitigation: Tenant IDs are carried through replication payloads; `multitenant.go:925` gates replication on `ring.ReplicationFactor() > 1`.

---

## Fragile Areas

### Hash ring / memberlist membership
- Files: `pkg/ingester/ingester_ring.go`, `pkg/ingester/ingester_ring_lifecycler.go`, `pkg/distributor/distributor_ring.go`, `pkg/storegateway/gateway_ring.go`, `pkg/compactor/compactor_ring.go`, `pkg/ruler/ring.go`, `pkg/alertmanager/alertmanager_ring.go`.
- Why fragile:
  - KV backend choice (`consul`, `etcd`, `memberlist`) affects correctness and propagation latency. The ingester ring defaults to memberlist (`pkg/ingester/ingester_ring.go:124`), while some components still default differently.
  - **Zone-aware replication** (`ZoneAwarenessEnabled`) must be consistent across components that share a ring. Asymmetry breaks quorum math.
  - **Unregister on shutdown** semantics differ across ingester/store-gateway. Ingester lifecycler supports runtime toggling via HTTP endpoint `/ingester/unregister-on-shutdown` (`pkg/api/api.go:349`, `pkg/ingester/ingester_http.go:244-302`). Store-gateway supports a similar flag (`pkg/storegateway/gateway_ring.go:108-185`). Getting this wrong causes data availability gaps during restarts vs. ring churn storms.
  - CHANGELOG #13104 / #13142 removed the ability to disable heartbeats — users upgrading from very old configs will see hard failures.
  - CHANGELOG #13129 / #13651 / #13664 added experimental memberlist zone-aware routing; still maturing.
- Safe modification:
  - Never change ring key layout without a migration path.
  - Any change touching replication quorum must be reviewed alongside zone-awareness and shard size semantics.
  - Integration tests in `integration/` exercise multi-zone setups; run them.

### TSDB head / WAL correctness
- Files: `pkg/ingester/ingester.go`, `pkg/ingester/ingester_tsdb.go`, `pkg/ingester/ingester_push.go`, `pkg/storage/tsdb/config.go`, `pkg/storage/soft_append_error_processor.go`.
- Why fragile:
  - **Out-of-order window** and **past grace period** interact in non-obvious ways. See `pkg/util/validation/limits.go:57-58, 166-167, 371-372`. Recent commit `d4be66c510` added per-tenant `enforce_out_of_order_window_on_distributor` (CHANGELOG #15090) so the distributor can reject too-old samples without setting `past_grace_period > 0`. Getting the interaction wrong silently drops samples (soft error) or returns 5xx (hard error).
  - **WAL replay concurrency** (`pkg/storage/tsdb/config.go:200, 325`): setting `wal_replay_concurrency = 0` means "use all CPU cores", which can starve scheduling on dense nodes.
  - **Shipping** blocks to object storage runs in a per-tenant goroutine (`pkg/ingester/ingester.go:298-384`, `cortex_ingester_oldest_unshipped_block_timestamp_seconds`). If it falls behind, data can be lost on disk eviction.
  - **Native histograms**: `validation.max-native-histogram-buckets` plus `reduce-native-histogram-over-max-buckets` (see `pkg/util/validation/limits.go:54-55`) either reject or downsample. Changing defaults changes tenant-visible behavior.
  - **Active series tracking** has a 1 MiB cap: `pkg/ingester/active_series.go:26` `activeSeriesMaxSizeBytes = 1 * 1024 * 1024`. Responses over this threshold are truncated; callers must be ready.
  - **Soft vs. hard errors** are centralized in `pkg/storage/soft_append_error_processor.go`. Soft errors (out-of-bounds, out-of-order, duplicate, max-series) let the ingester continue with the rest of the request; hard errors abort it. Misclassifying a new error kind here can turn every push 5xx.
- Safe modification: Add new soft-error cases to `ProcessErr` in `soft_append_error_processor.go:62-95` and mirror them in `pkg/ingester/errors.go:65-73` (`softError` marker interface). Do not quietly convert a hard error to soft.

### Blocks storage consistency (compactor ↔ store-gateway ↔ queriers)
- Files: `pkg/compactor/compactor.go`, `pkg/compactor/bucket_compactor.go`, `pkg/compactor/blocks_cleaner.go`, `pkg/storage/tsdb/bucketindex/updater.go`, `pkg/storage/tsdb/bucketindex/loader.go`, `pkg/querier/blocks_consistency_checker.go`, `pkg/storage/tsdb/block/global_markers_test.go`.
- Why fragile:
  - The compactor writes the `bucket-index.json.gz`. Store-gateways and queriers read it. Out-of-band uploads or deletion-mark authoring must stay compatible with the updater.
  - **Deletion marks** (`<id>-deletion-mark.json`) and **no-compact marks** (`<id>-no-compact-mark.json`) live in `markers/` and change block visibility. Missing the mark path format (see `pkg/storage/tsdb/block/global_markers_test.go`) causes silent divergence.
  - **Blocks consistency checker** in `pkg/querier/blocks_consistency_checker.go` tracks which blocks the querier expected vs. received from store-gateway. Grace-period logic around freshly uploaded blocks is subtle.
  - **Compactor block-upload API** (`pkg/compactor/block_upload.go`) is gated per tenant (`pkg/util/validation/limits.go:283-288`: `compactor_block_upload_enabled`, `compactor_block_upload_validation_enabled`, `compactor_block_upload_verify_chunks`). A client can upload a malformed block; validation is the only safety net.
  - CHANGELOG #13815 / #13824 require index v2 for uploads; v1 index headers trigger warnings (#13834).
- Safe modification: Any change to the bucket-index schema requires coordinated deploy of compactor → store-gateway → queriers. Compactor typically goes last (writer follows readers).

### Query-frontend / scheduler / splitting / caching
- Files: `pkg/frontend/querymiddleware/roundtrip.go`, `pkg/frontend/querymiddleware/split_and_cache.go`, `pkg/frontend/querymiddleware/cardinality.go`, `pkg/frontend/querymiddleware/query_blocker.go`, `pkg/frontend/querymiddleware/sharded_queryable.go` and `pkg/scheduler/`.
- Why fragile:
  - Time-splitting (`split_and_cache.go`) and sharding (`astmapper/sharding.go`) rewrite queries. A regression here silently changes query results. Tests in `pkg/frontend/querymiddleware/` rely on compat fixtures.
  - Results caching has separate TTLs for OOO window data (`query-frontend.results-cache-ttl-for-out-of-order-time-window`, `pkg/util/validation/limits.go:76`). A new cache key must include all semantically relevant inputs or you get cross-tenant or stale results.
  - CHANGELOG #15081 renamed a `blocked_queries` runtime-config key; users with old config will hit validation failures.
  - Query memory limit now spans time-split sub-queries with MQE enabled (CHANGELOG #14980); behavior differs between engines.
  - Cardinality estimation: `pkg/frontend/querymiddleware/cardinality.go:25-36` uses week-long TTLs and 2h buckets. Stale estimates can mis-plan sharding.
- Safe modification: Any change to middleware ordering in `roundtrip.go` must be regression-tested against the fixtures.

### Streaming PromQL engine (MQE)
- Files: `pkg/streamingpromql/`.
- Why fragile:
  - MQE is a Mimir-specific PromQL engine that coexists with Prometheus's engine. Query engine selection is per-tenant (`pkg/frontend/config.go:37`, `QueryEngine` experimental). Behavior differences between engines are a known source of incident tickets.
  - Optimization passes (`pkg/streamingpromql/optimize/plan/`) rewrite plans; bugs there produce wrong results, not errors.
  - Many `FIXME` comments cluster here (see hotspots below), reflecting active maturation.
- Safe modification: Use `streamingpromql/engine_test.go` as a regression bed. Do not change an optimization without a planning test.

### Remote-write v2 / OTLP ingestion
- Files: `pkg/distributor/push.go`, `pkg/distributor/otel.go` (898 lines), `pkg/distributor/otlpappender/mimir_appender.go`, `pkg/distributor/influxpush/parser.go`.
- Why fragile:
  - `push.go:168-230` branches on `isRW2` for PRW2 vs PRW1 handling. A new code path that forgets the branch silently mis-parses.
  - OTLP translation has config flags for underscore sanitization and translation strategy (CHANGELOG #14782, #13133). Headers `X-Mimir-OTLP-AddSuffixes`, `X-Mimir-OTLP-TranslationStrategy` allow per-request override when `-api.otlp-translation-headers-enabled` is on.
  - `EnforceOOOWindowOnDistributor` is off by default; users who expect ingester-side rejection to happen distributor-side won't get it unless they enable it (CHANGELOG #15090).
  - Uncompressed body size accounting: `pkg/distributor/request.go:40-45` and `pkg/distributor/distributor.go:2337` note that post-conversion byte counts are not wire-accurate. Relying on them for rate-limiting math is wrong.
- Safe modification: Always add a test that covers both RW1 and RW2 paths. OTLP tests live in `otel_test.go` (2,642 lines) — use existing fixtures.

### Ruler
- Files: `pkg/ruler/ruler.go` (1,674 lines), `pkg/ruler/manager.go`, `pkg/ruler/remotequerier.go`, `pkg/ruler/notifier.go`.
- Why fragile:
  - Ruler evaluates rules by querying a remote queryable (`remotequerier.go`). Transient queryable outages can produce a burst of `unknown`-state alerts (CHANGELOG #13060).
  - Ruler `TODO` at `pkg/ruler/ruler.go:508`: the ruler does not wait for its queryable to finish starting, so early evaluations can fail.
  - Tenant federation (`ruler.go:150`) allows cross-tenant rule evaluation; careful with isolation.
  - Experimental outbound/inbound sync queue polling, rule concurrency limits, rule-evaluation-write (`ruler.go:152-166`).
  - Filter for missing rule groups is not a hard error (`ruler.go:706`), meaning typos in tenant rule config silently drop rules.
- Safe modification: Never remove a rule-state transition without coordinating with `/api/v1/alerts` and `/api/v1/rules` contracts.

### Alertmanager sharding and state replication
- Files: `pkg/alertmanager/multitenant.go`, `pkg/alertmanager/state_replication.go`, `pkg/alertmanager/alertmanager_ring.go`, `pkg/alertmanager/state_persister.go`.
- Why fragile:
  - Replication factor gates state-replication behavior (`multitenant.go:902,925`).
  - A `TODO` at `pkg/alertmanager/alertmanager.go:319` flags that the AM instance may accept requests before it is fully settled — a restart race.
  - Strict initialization grace period was renamed (CHANGELOG #14960: `alertmanager-grafana-alertmanager-idle-grace-period` → `alertmanager-strict-initialization-idle-grace-period`).
- Safe modification: Any change to state-replication merge logic requires a multi-instance test; silences and nflogs are tenant-scoped and must stay that way.

### Long-running background operations / graceful shutdown
- Files: `pkg/ingester/ingester.go` (shipper loop, head compaction), `pkg/compactor/compactor.go`, `pkg/compactor/blocks_cleaner.go`, `pkg/storegateway/bucket_stores.go` (bucket-index loader), `pkg/alertmanager/multitenant.go`.
- Why fragile:
  - Shutdown must drain in-flight pushes/queries before unregistering from the ring. Premature unregister = lost writes and failed reads.
  - Store-gateway prepare-shutdown endpoint: `pkg/storegateway/gateway_prepare_shutdown_http.go` toggles `UnregisterOnShutdown` at runtime.
  - Ingester circuit breakers (`pkg/ingester/circuitbreaker.go`) wrap both push and read paths with separate timeouts (`circuitBreakerDefaultPushTimeout = 2s`, `circuitBreakerDefaultReadTimeout = 30s`). An open breaker surfaces as 5xx to upstream clients; breaker metrics are in `cortex_ingester_circuit_breaker_*`.
  - Reactive limiter (`pkg/util/reactivelimiter/reactivelimiter.go`) is experimental and gates distributor/ingester load. Recent changes: CHANGELOG #14007.
- Safe modification: Never add a background loop without hooking into the services manager (`pkg/mimir/modules.go` lifecycle) so it is gracefully stopped.

### Per-tenant limits and runtime-config reload
- Files: `pkg/util/validation/limits.go` (2,030 lines), `pkg/mimir/runtime_config.go`, `pkg/mimir/modules.go`.
- Why fragile:
  - Runtime-config is loaded with YAML `KnownFields(true)` (`pkg/mimir/runtime_config.go:78`), so an unknown key in the runtime override file fails the reload — but a reload failure keeps serving the previous config. Operators can think they've changed behavior when they haven't.
  - Multi-document YAML is rejected (`errMultipleDocuments`).
  - CHANGELOG #15052 added support for loading runtime config from `http://` / `https://` URLs with a 30s default timeout. Transient HTTP failures mean stale limits, not an outage.
  - CHANGELOG #15081: a rename in `blocked_queries` schema will silently break queries blocked by the old key name; the new validation surfaces this at load.
  - Limits values like `max_global_series_per_user`, `ingestion_rate`, `ingestion_burst_size`, `out_of_order_time_window`, `past_grace_period` are hot-reloadable but components cache them. Whether a change takes effect immediately depends on the component.
- Safe modification: When adding a limit, add it with a sensible default, wire it through `validation.Overrides`, and document hot-reload semantics.

---

## Performance Bottlenecks

### Store-gateway serialized fetches
- Problem: `pkg/storegateway/bucket.go:816` — "TODO: can we send this in parallel while we start fetching the chunks below?" Current implementation serializes operations that could overlap.
- Impact: Extra query latency under high-block fan-out.
- Improvement path: Parallelize as the TODO notes; be careful of back-pressure on the bucket.

### Postings-for-matchers cache force
- Files: `pkg/storage/tsdb/config.go:244, 336-338`.
- Problem: `HeadPostingsForMatchersCacheForce` bypasses the concurrency gate that normally avoids cache churn.
- Impact: Forcing can either accelerate recurring queries or thrash the cache.
- Current state: Off by default; only flip with benchmarking.

### Index-header lazy vs. eager loading
- Files: `pkg/storage/indexheader/`.
- CHANGELOG #13126 removed the ability to disable eager startup; eager loading is now always enabled when lazy loading is enabled. Net: startup memory footprint is higher than it used to be.

### Bucket-index loader
- File: `pkg/storage/tsdb/bucketindex/loader.go`.
- Problem: One bucket-index per tenant, loaded on demand, refreshed on a timer. Large deployments have many tenants; loading is a measurable startup cost and ongoing IO.
- Metric: `cortex_bucket_index_loads_total`, `cortex_bucket_index_load_failures_total`.

### Streaming PromQL engine hot paths
- `pkg/streamingpromql/types/fpoint_ring_buffer.go:350`, `hpoint_ring_buffer.go:225`: FIXMEs note that `ForEach` overhead forced exposing internal state. Don't regress this without measuring.
- `pkg/streamingpromql/operators/binops/*.go`: several FIXMEs propose bitmaps over `[]bool` for presence tracking — unaddressed.

---

## Known TODO/FIXME Hotspots

Representative pointers. Grep confirms dozens more; these are the ones a contributor is most likely to bump into.

1. `pkg/ruler/ruler.go:508` — "ideally, ruler would wait until its queryable is finished starting." Restart races produce transient query failures.
2. `pkg/blockbuilder/blockbuilder.go:268` — block-builder unmarshals exemplars even though TSDB never persists them into blocks. Wasted CPU.
3. `pkg/blockbuilder/blockbuilder.go:531` — block counts metric is not tracked.
4. `pkg/blockbuilder/scheduler/scheduler.go:744` — flushing happens even when state is not dirty.
5. `pkg/blockbuilder/scheduler/scheduler.go:1013` — scheduler job rejection currently has no structured reason.
6. `pkg/storegateway/bucket_index_reader.go:111-112` — promise cancellation fails all waiters even when more callers are queued (`grafana/mimir#331`).
7. `pkg/storegateway/bucket.go:816` — serial where parallel would help (see above).
8. `pkg/querier/blocks_store_queryable.go:828` — hints are not passed down to the storage layer.
9. `pkg/frontend/querymiddleware/stats.go:212` — unclear whether `lookbackDelta` needs to be an argument.
10. `pkg/frontend/querymiddleware/astmapper/sharding.go:79` — analyzer is re-created instead of reused during shard planning.
11. `pkg/util/log/log.go:23` — global logger anti-pattern acknowledged; migration unfinished.
12. `pkg/storage/ingest/fetcher.go:654` — Kafka decompressor could use a pool.
13. Many `FIXME`s concentrated in `pkg/streamingpromql/` (common subexpression elimination, binops) — see `optimize/plan/commonsubexpressionelimination/optimization_pass.go:383,648`, `binops/and_unless_binary_operation.go:347`, `binops/or_binary_operation.go:416`.

Auto-generated `.pb.go` files contain many `XXX_` methods from gogoproto; those are not real TODOs, ignore them.

---

## Experimental Features

Mimir marks experimental config via struct tag `category:"experimental"`. There are **234+** such fields across `pkg/` (`grep -rn 'category:"experimental"' pkg/ | wc -l`). Representative high-blast-radius ones:

- **Block-builder**: `pkg/blockbuilder/config.go:25, 27` — `apply_max_global_series_per_user_below`, `generate_sparse_index_headers`. The block-builder/partitioned ingest storage pipeline itself is relatively new.
- **Ingest storage (Kafka)**: `pkg/storage/ingest/config.go`, `DESIGN.md`, `INTERNALS.md`. The whole pipeline is still stabilizing; many recent CHANGELOG entries around Kafka (#14307, #14344, #14550, #14674, #14780, #14898, #14903).
- **Remote execution / query planning in frontend**: `pkg/frontend/querymiddleware/roundtrip.go:70-77` (`enable_remote_execution`, `use_mimir_query_engine_for_sharding`, `rewrite_histogram_queries`, `rewrite_propagate_matchers`, `shard_active_series_queries`, `use_active_series_decoder`) and `pkg/frontend/v2/frontend.go:77-78` (batch sizes).
- **TSDB out-of-order advanced settings**: `pkg/storage/tsdb/config.go:206, 219` — `bigger_out_of_order_blocks_for_old_samples`, `out_of_order_capacity_max`.
- **Distributor enforce OOO window on distributor**: `pkg/util/validation/limits.go:167` — `enforce_out_of_order_window_on_distributor` (CHANGELOG #15090, commit `d4be66c510`). Off by default.
- **Per-sample HA dedupe**: `ha_tracker_per_sample_dedupe` (CHANGELOG #15064). Off by default.
- **Ingester**:
  - Early head compaction: `pkg/ingester/ingester_early_compaction_test.go`, plus limits `early_head_compaction_owned_series_threshold`, `early_head_compaction_min_estimated_series_reduction_percentage` (CHANGELOG #13980, #15056).
  - `num_tokens = 0` for tokenless ingesters with ingest storage (CHANGELOG #14024).
- **Ruler**: experimental queue-poll intervals, rule concurrency limits, rule-evaluation-write, health-check grace (`pkg/ruler/ruler.go:152-166`).
- **Store-gateway**: `tenant-shard-size-per-zone` (CHANGELOG #13835), `sharding-ring.excluded-zones` (#14120).
- **Reactive limiter**: `pkg/util/reactivelimiter/reactivelimiter.go:24`, `prioritizer.go:17` — experimental cross-component load shedding.
- **Memberlist zone-aware routing**: CHANGELOG #13129, #13651, #13664.
- **MQE extended selectors and new functions**: `smoothed`, `anchored` (CHANGELOG #13398), `info` (#13443).
- **Cost attribution**: `pkg/costattribution/`, enabled via `cost_attribution_labels_structured` (old `cost_attribution_labels` removed, CHANGELOG #13286).

Never rely on experimental behavior without an explicit flag in your config. Assume they can change or be removed in any release.

---

## Deprecations

These are config fields carrying `category:"deprecated"` — contributors should avoid adding new uses and watch for removal PRs:

- `pkg/frontend/querymiddleware/roundtrip.go:98` — `cache_samples_processed_stats` (CHANGELOG #13582, #14843 context).
- `pkg/ruler/ruler.go:119, 129` — `alertmanager_url` and `alertmanager_client` at ruler level; use `limits.ruler_alertmanager_client_config` instead.
- `pkg/storegateway/gateway_ring.go:88` — `auto_forget_enabled`.
- `pkg/storage/ingest/config.go:101` — `write_clients`, slated for removal in Mimir 3.3.
- `pkg/storage/tsdb/config.go:238, 254` — `head_postings_for_matchers_cache_size`, `block_postings_for_matchers_cache_size`.
- `pkg/querier/querier.go:74` — `filter_queryables_enabled`, to be removed in Mimir 3.3 (CHANGELOG #14843).

CHANGELOG `[CHANGE]` entries are the authoritative source of deprecation and behavior changes. When in doubt, search there first.

---

## Scaling Limits

### Active-series response size
- File: `pkg/ingester/active_series.go:26`.
- Limit: 1 MiB per response.
- Behavior: Responses over the threshold are truncated.
- Scaling path: Streaming of active series responses is now always on (CHANGELOG #14095 / #14114) — use `ShardActiveSeriesQueries` to split wide queries.

### Ring sizes and memberlist
- Very large rings (thousands of instances) strain memberlist gossip.
- Experimental zone-aware routing is the active mitigation (CHANGELOG #13129).

### Per-tenant limits
- `-validation.max-active-series-additional-custom-trackers` default `0` (CHANGELOG #14226) to prevent runaway custom-tracker cardinality per tenant.
- `-validation.max-native-histogram-buckets` guards memory blowup per histogram.

---

## Test Coverage Gaps

### yoloString memory safety
- What's not tested exhaustively: every middleware in the push path for correct deep-copying.
- Files: `pkg/distributor/*.go` middlewares plus `pkg/mimirpb/ref_leaks_test.go`.
- Risk: A new middleware that retains labels leaks cross-tenant data; current tests catch explicit `FreeBuffer` misuse but not all middlewares retain-without-clone scenarios.
- Priority: High.

### MQE vs. Prometheus engine parity
- What's not tested: every exotic query construct across both engines.
- Files: `pkg/streamingpromql/engine_test.go`, `pkg/querier/`.
- Risk: Query-result divergence between engines; users flipping `query_engine` see different answers.
- Priority: High.

### Ring membership edge cases
- What's not tested: arbitrary ordering of zone-aware rollouts with replication factor changes.
- Files: integration tests under `integration/`.
- Risk: Quorum violations during rollouts.
- Priority: Medium.

### OTLP translation strategies
- What's not tested: every combination of `add-suffixes`, translation strategy, underscore sanitization, and name validation scheme.
- Files: `pkg/distributor/otel_test.go` (already 2,642 lines — the problem is combinatorial explosion, not absence).
- Priority: Medium.

---

## Dependencies at Risk

Mimir vendors dependencies (`vendor/` directory, go modules). Critical upstream surfaces:

- **Prometheus** (`github.com/prometheus/prometheus`): TSDB changes here ripple into ingester/querier. Mimir tracks Prometheus closely.
- **Thanos-derived code**: blocks store, store-gateway internals retain Thanos heritage comments (`Provenance-includes-location: https://github.com/thanos-io/thanos/...` in `pkg/storage/tsdb/block/index.go` and elsewhere).
- **dskit** (`github.com/grafana/dskit`): ring, memberlist, KV, runtimeconfig, services — the foundation. dskit upgrades have caused subtle ring-behavior changes in the past.
- **failsafe-go** (`github.com/failsafe-go/failsafe-go`): circuit breakers in `pkg/ingester/circuitbreaker.go`. Semantics change can alter request rejection behavior.
- **franz-go** (implicit via ingest storage) for Kafka: `pkg/storage/ingest/fetcher.go:654` still has a TODO for decompressor pooling.

---

## Pointers for New Contributors

1. Read `CLAUDE.md` (root) end-to-end before touching the write path. The yoloString section is not optional.
2. Before editing the distributor/ingester, skim `pkg/distributor/request.go` to understand the cleanup-callback model.
3. For ingester push-side error handling, `pkg/storage/soft_append_error_processor.go` is the central switchyard.
4. For any ring/KV change, use the `integration/` tests; unit tests do not cover ring quorum edge cases.
5. When adding a config flag, mirror the convention in `pkg/util/validation/limits.go` and tag `category:"experimental"` unless you are ready to support it in production.
6. Use the `.claude/skills/split-file` skill when a file crosses ~1,500 lines; do not let monoliths keep growing.
7. CHANGELOG is part of the review. `[CHANGE]` entries are load-bearing for operators upgrading.

---

*Concerns audit: 2026-04-22*
