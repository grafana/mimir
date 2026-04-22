# Architecture

**Analysis Date:** 2026-04-22

## Pattern Overview

**Overall:** Horizontally scalable, shared-nothing microservices built around hash rings and object storage. Same binary runs in monolithic, read-write, or microservices mode via a module target list.

**Key Characteristics:**
- Single Go binary (`cmd/mimir`) compiles all services. At runtime, `-target=<modules>` selects which components run in-process. Defaults to `-target=all` (monolithic).
- Services coordinate via the dskit **module manager** (`github.com/grafana/dskit/modules`). A dependency graph in `pkg/mimir/modules.go` `setupModuleManager` wires lifecycle: each service is a `services.Service` with `starting`/`running`/`stopping` phases.
- Services discover each other through **hash rings** stored in a KV store (typically memberlist gossip; consul/etcd also supported via dskit).
- **Shared-nothing**: ingesters own series shards, store-gateways own block shards, compactors own tenants. Ownership is derived from consistent hashing against the ring.
- **Object storage is the durable store**. Local disk on ingesters is a write-through buffer, local disk on store-gateways is a cache of block indexes.
- **Per-tenant isolation** via `X-Scope-OrgID` header on all HTTP requests and `org-id` on gRPC metadata. Tenant ID is propagated in `context.Context` and used to shard rings, partition TSDBs, and scope object storage paths.

## Layers

**Root binary / module wiring:**
- Purpose: Parse config, instantiate services in dependency order, expose admin HTTP server.
- Location: `cmd/mimir/main.go`, `pkg/mimir/mimir.go`, `pkg/mimir/modules.go`
- Contains: `Mimir` struct holds all service handles; `Config` holds all per-service configs.
- Depends on: `github.com/grafana/dskit/{services,modules,server,ring,kv/memberlist}`
- Used by: nothing; this is the entry point.

**Write path (distributor):**
- Purpose: Receive HTTP writes (Prometheus remote-write, OTLP, Influx), validate, replicate to ingesters.
- Location: `pkg/distributor/`
- Entry points: `pkg/distributor/push.go:Handler` (remote write), `pkg/distributor/otel.go:OTLPHandler`, `pkg/distributor/influx.go` (Influx).
- Core type: `Distributor` in `pkg/distributor/distributor.go`. `Push(ctx, *mimirpb.WriteRequest)` is the gRPC + internal entry (line 2008).
- Depends on: ingester ring (`pkg/ingester/ingester_ring.go`), partition ring (`pkg/ingester/ingester_partition_ring.go`), overrides, validation, HA tracker, cost attribution.
- Used by: `pkg/api/api.go:RegisterDistributor` mounts routes `/api/v1/push`, `/otlp/v1/metrics`, `/api/v1/push/influx/write`.

**Ingest (ingester):**
- Purpose: Keep recent data in an in-process Prometheus TSDB per tenant, serve queries over recent window, flush blocks to object storage.
- Location: `pkg/ingester/`
- Core type: `Ingester` in `pkg/ingester/ingester.go`. Push path: `pkg/ingester/ingester_push.go:PushWithCleanup`. Query path: `pkg/ingester/ingester_query.go:QueryStream`. Per-tenant TSDB wrapping: `pkg/ingester/ingester_tsdb.go`. Compaction to blocks: `pkg/ingester/ingester_compaction.go`.
- Ring: `pkg/ingester/ingester_ring.go` (instance ring, key `"ring"`), `pkg/ingester/ingester_partition_ring.go` (partition ring for ingest-storage mode, key `"ingester-partitions"`).
- Active series tracking: `pkg/ingester/activeseries/`.

**Alternative ingest (Kafka-based):**
- Purpose: When `-ingest-storage.enabled=true`, distributors write records to Kafka partitions (instead of / alongside ingesters) and ingesters consume those partitions.
- Location: `pkg/storage/ingest/` (Kafka producer/consumer library), `pkg/blockbuilder/` (offline builder that reads Kafka and uploads TSDB blocks).
- Reader: `pkg/storage/ingest/reader.go`. Writer: `pkg/storage/ingest/writer.go`.

**Read path (querier + store-gateway):**
- Querier orchestrates fanout between ingesters (recent data) and store-gateways (historical blocks).
- Querier core: `pkg/querier/querier.go` builds a Prometheus `storage.Queryable`. `pkg/querier/distributor_queryable.go` queries ingesters via distributor; `pkg/querier/blocks_store_queryable.go` queries store-gateways; `pkg/querier/distributor_queryable_streaming.go` handles streaming chunks.
- Block discovery: `pkg/querier/blocks_finder_bucket_index.go` reads the bucket index written by compactor to know which blocks exist per tenant.
- Consistency check: `pkg/querier/blocks_consistency_checker.go` ensures all expected blocks were queried.
- Store-gateway: `pkg/storegateway/gateway.go` (`StoreGateway`), `pkg/storegateway/bucket.go` (`BucketStore` per tenant), `pkg/storegateway/bucket_stores.go` (multi-tenant registry). Sync loop: `StoreGateway.syncStores` (`gateway.go:341`).
- Query engine: Mimir Query Engine (MQE) at `pkg/streamingpromql/engine.go` is the streaming PromQL engine; Prometheus engine is a fallback.

**Query frontend / scheduler (read fan-out and queueing):**
- Purpose: Split, shard, cache, and queue queries before they hit queriers.
- Query frontend v2: `pkg/frontend/v2/frontend.go` enqueues to `pkg/scheduler/scheduler.go` via gRPC.
- Query middleware pipeline: `pkg/frontend/querymiddleware/` (split-by-interval, sharding, caching, retries, codec).
- Query scheduler: `pkg/scheduler/scheduler.go`. FIFO per-tenant queue with fairness: `pkg/scheduler/queue/queue.go`.
- Querier worker: `pkg/querier/worker/` pulls work from scheduler / frontend.

**Ruler (rule evaluation):**
- Purpose: Evaluate recording and alerting rules per tenant, send results back into the write path.
- Core: `pkg/ruler/ruler.go` (`Ruler`). Rule storage: `pkg/ruler/rulestore/` (backed by object storage). Rule eval manager: `pkg/ruler/manager.go` (per-tenant `MultiTenantManager`). Remote evaluation: `pkg/ruler/remotequerier.go`.
- Sharding: ring at `pkg/ruler/ruler_ring.go` assigns rule groups to ruler instances by group hash. `pkg/ruler/ruler.go:tokenForGroup` (line 531), `instanceOwnsRuleGroup` (line 544).
- Sync loop: `Ruler.run` (`ruler.go:564`) calls `syncRules` to pull rule groups this instance owns, then hands to `MultiTenantManager`. Evaluated samples are pushed back to distributor via `pkg/ruler/compat.go`.

**Alertmanager:**
- Purpose: Multi-tenant Prometheus Alertmanager.
- Core: `pkg/alertmanager/multitenant.go` (`MultitenantAlertmanager`), `pkg/alertmanager/alertmanager.go` (per-tenant). Config storage: `pkg/alertmanager/alertstore/`. Sharding ring: `pkg/alertmanager/alertmanager_ring.go`. State replication across shards: `pkg/alertmanager/state_replication.go`.

**Compactor:**
- Purpose: Download tenant blocks from object storage, apply split-merge compaction, upload back, update bucket index, clean old blocks.
- Core: `pkg/compactor/compactor.go` (`MultitenantCompactor`). Per-tenant compaction: `compactUser` (`compactor.go:850`). Split-merge strategy: `pkg/compactor/split_merge_compactor.go`, `split_merge_grouper.go`, `split_merge_planner.go`.
- Bucket index writer: `pkg/compactor/batch_caching_meta_fetcher.go` + `pkg/storage/tsdb/bucketindex/updater.go`.
- Cleaner: `pkg/compactor/blocks_cleaner.go` (deletes old blocks past retention, marks deletions).
- Sharding: `pkg/compactor/compactor_ring.go` shards tenants across compactor instances.

**Shared infrastructure:**
- API routing: `pkg/api/api.go` (mounts routes per service via `RegisterDistributor`, `RegisterIngester`, etc.). Endpoints: `pkg/api/api.go:289-291`.
- Auth / tenant extraction: `pkg/api/tenant.go` (HTTP middleware), relies on `github.com/grafana/dskit/tenant` to set/read tenant IDs on context.
- Limits & overrides: `pkg/util/validation/limits.go` (default limits). Runtime overrides loader: `pkg/mimir/runtime_config.go` watches a file and hot-reloads `validation.Overrides`.
- Memberlist KV: initialized by `initMemberlistKV` in `pkg/mimir/modules.go:1303`; shared across all rings in the process.
- Protobuf types: `pkg/mimirpb/mimir.proto` (`WriteRequest`, `PreallocWriteRequest`). Ingester client protos: `pkg/ingester/client/`.

## Data Flow

**Write path (remote write):**

1. Client POSTs compressed Snappy protobuf to `/api/v1/push` (or OTLP/Influx). Authn middleware extracts `X-Scope-OrgID` → `context.Context` tenant (`pkg/api/api.go`).
2. `pkg/distributor/push.go:Handler` decompresses into a pooled buffer, unmarshals into `mimirpb.PreallocWriteRequest` (strings reference the pooled buffer; see `CLAUDE.md` yoloString warning).
3. Per-request middleware in `Distributor.PushWithMiddlewares` applies: instance limits, inflight byte limits, HA deduplication (`pkg/distributor/ha_tracker.go`), validation (`pkg/distributor/validate.go`), relabeling, cost attribution (`pkg/costattribution/`).
4. `Distributor.push` (`distributor.go:2039`) computes hash tokens per series, looks up the tenant's shuffle-shard on the ingester ring, fan-outs via `ring.DoBatchWithOptions` with replication factor (default 3).
5. Each ingester receives `client.Push` gRPC; `pkg/ingester/ingester_push.go:PushWithCleanup` writes to the per-tenant Prometheus TSDB (memory), appends to WAL. Usage tracker increments active-series counters (`pkg/usagetracker/`).
6. Ingester `HeadCompaction` (`pkg/ingester/ingester_compaction.go`) periodically (default every 2h) builds a block on disk, uploads via `thanos-io/objstore` to the configured bucket.
7. Compactor discovers new blocks in the tenant prefix, runs split-merge compaction (`pkg/compactor/split_merge_compactor.go`), writes the result and updates the tenant bucket index (`pkg/storage/tsdb/bucketindex/`).

**Write path (ingest storage / Kafka):**

1. Distributor's `sendWriteRequestToPartitions` writes serialized records to Kafka partitions chosen from the partition ring (`pkg/ingester/ingester_partition_ring.go`). See `pkg/storage/ingest/writer.go`.
2. Ingesters consume their owned partitions (`pkg/storage/ingest/reader.go`) and apply records to TSDB via `PushToStorageAndReleaseRequest` (`pkg/ingester/ingester_push.go:821`).
3. Block-builder (`pkg/blockbuilder/blockbuilder.go`) separately consumes Kafka and uploads blocks to the bucket — ingesters do not have to.

**Read path (instant/range query):**

1. Client GETs `/prometheus/api/v1/query`. Router sends to query-frontend.
2. Frontend applies middleware: step-align, split-by-interval, cardinality estimation, sharding (`pkg/frontend/querymiddleware/`), response caching.
3. Frontend enqueues sub-query to query-scheduler gRPC `FrontendLoop` (`pkg/scheduler/scheduler.go:239`). Scheduler applies per-tenant fair queuing (`pkg/scheduler/queue/queue.go`).
4. Querier worker (`pkg/querier/worker/`) pulls work from scheduler via `QuerierLoop` (`scheduler.go:453`).
5. Querier builds a Prometheus `storage.Queryable` (`pkg/querier/querier.go`) with two children:
   - `distributor_queryable` → streams chunks from ingesters in the tenant's shuffle-shard.
   - `blocks_store_queryable` → lists blocks from bucket index, maps block IDs to store-gateway instances via the store-gateway ring, fetches chunks via gRPC `Series` (`pkg/storegateway/gateway.go:353`).
6. Mimir Query Engine (`pkg/streamingpromql/engine.go`) or Prometheus engine evaluates the PromQL AST against the combined queryable. Results stream back up to frontend, which assembles the final response.

**Ruler eval path:**

1. `Ruler.run` (`pkg/ruler/ruler.go:564`) ticks on an interval. `syncRules` lists rule groups per tenant from `rulestore`, keeps only groups this instance owns by `instanceOwnsRuleGroup`.
2. Owned groups are handed to `MultiTenantManager` (`pkg/ruler/manager.go`), which runs a Prometheus `rules.Manager` per tenant.
3. Rule evaluation queries either a local queryable (same process) or a remote querier/frontend (`pkg/ruler/remotequerier.go`) depending on `-ruler.query-frontend.address`.
4. Recording rule results are written back via a push client that re-enters the distributor's `Push` (`pkg/ruler/compat.go`). Alerts are sent to Alertmanager via Prometheus `notifier`.

**State Management:**
- Per-instance state (TSDB, WAL, local cache) on local disk, paths validated for non-overlap by `pkg/mimir/mimir.go:validateFilesystemPaths` (line 500).
- Cluster state (ring membership, tokens) in KV store, gossiped via memberlist by default.
- Durable state in object storage: blocks under `<tenant>/`, bucket index under `<tenant>/bucket-index.json.gz`, tenant deletion marks, rule configs, alertmanager configs.
- Runtime overrides hot-reloaded from a YAML file watched by `pkg/mimir/runtime_config.go`.

## Key Abstractions

**Module:**
- Purpose: A named unit registered with the dskit `modules.Manager`. Exposes a `services.Service` lifecycle and declares dependencies.
- Examples: All modules listed in `pkg/mimir/modules.go:87-132` (constants like `Distributor`, `Ingester`, `QueryFrontend`).
- Pattern: `initFoo() (services.Service, error)` methods on `*Mimir` (e.g. `initDistributorService`, `initQuerier` in `pkg/mimir/modules.go`). Dependencies declared in `setupModuleManager` deps map (`modules.go:1528`).

**Hash Ring:**
- Purpose: Consistent-hash sharding and membership. Each service instance registers itself with tokens; callers look up which instance(s) own a key.
- Examples: ingester ring (`pkg/ingester/ingester_ring.go`), store-gateway ring (`pkg/storegateway/gateway_ring.go`), compactor ring (`pkg/compactor/compactor_ring.go`), ruler ring (`pkg/ruler/ruler_ring.go`), alertmanager ring (`pkg/alertmanager/alertmanager_ring.go`), querier ring (`pkg/querier/querier_ring.go`), distributor ring (`pkg/distributor/distributor_ring.go`).
- Pattern: uses `github.com/grafana/dskit/ring`. Shuffle-sharding per tenant via `ring.ShuffleShard(tenantID, shardSize)`.

**Partition Ring:**
- Purpose: Used only in ingest-storage (Kafka) mode to map tenants to Kafka partitions.
- Examples: `pkg/ingester/ingester_partition_ring.go`, `pkg/usagetracker/` partition ring.

**Bucket Client:**
- Purpose: Vendor-agnostic object storage client (S3, GCS, Azure, Swift, filesystem).
- Location: `pkg/storage/bucket/client.go`, backends under `pkg/storage/bucket/{s3,gcs,azure,swift,filesystem}/`.
- Wraps `github.com/thanos-io/objstore`. Tenant-scoped via `pkg/storage/bucket/user_bucket_client.go`.

**Queryable:**
- Purpose: Prometheus `storage.Queryable` interface that plugs into PromQL engine.
- Examples: `pkg/querier/distributor_queryable.go`, `pkg/querier/blocks_store_queryable.go`. Composed in `pkg/querier/querier.go`.

**Overrides / Limits:**
- Purpose: Per-tenant configuration for rate limits, retention, series limits, etc.
- Location: `pkg/util/validation/limits.go` defines fields; `pkg/util/validation/limits_mock.go` for tests. Runtime overrides loaded by `pkg/mimir/runtime_config.go`, accessed as `*validation.Overrides`.

## Entry Points

**`main`:**
- Location: `cmd/mimir/main.go`
- Triggers: binary execution.
- Responsibilities: parse `-config.file`, call `cfg.RegisterFlags`, build `*mimir.Mimir` via `mimir.New(cfg)` (`pkg/mimir/mimir.go`), start the module manager which starts all services in dep order.

**HTTP/gRPC server:**
- Location: dskit `server.Server` constructed in `initServer` (`pkg/mimir/modules.go`). Ports default: HTTP 8080, gRPC 9095.
- Triggers: per-service routes registered via `pkg/api/api.go` (`RegisterDistributor`, `RegisterIngester`, `RegisterQueryFrontend`, `RegisterRuler`, `RegisterAlertmanager`, ring status pages, etc.). gRPC services registered via `Register<Service>Server` in each service's init method.

**Key HTTP endpoints (relative to `-server.path-prefix`):**
- Writes: `/api/v1/push`, `/otlp/v1/metrics`, `/api/v1/push/influx/write` (constants at `pkg/api/api.go:289-291`).
- Queries: `<prometheus-http-prefix>/api/v1/query`, `query_range`, `query_exemplars`, `series`, `labels`, `label/<name>/values` (`pkg/api/api.go:487-504`).
- Admin: `/ready`, `/metrics`, `/config`, `/services`, `/memberlist`, `/ingester/ring`, `/store-gateway/ring`, `/compactor/ring`, `/ruler/ring`, `/alertmanager/ring`.

## Error Handling

**Strategy:**
- Errors bubble up as Go `error`; domain-specific errors in `pkg/util/globalerror/user.go` with stable string IDs (e.g. `err-mimir-sample-out-of-order`).
- HTTP/gRPC translation via `toErrorWithGRPCStatus` (`pkg/distributor/errors.go`, `pkg/ingester/errors.go`). Each service has an `errors.go`.
- Runbook entries live in `docs/sources/mimir/manage/mimir-runbooks/_index.md`, cross-referenced by the error ID.

**Patterns:**
- Input validation errors return 4xx (e.g. `validation.Limits` violations). Internal errors return 5xx. Rate limit / inflight limit errors return 429.
- Soft vs hard errors in distributor: soft errors (4xx from some ingesters) don't fail the whole write as long as quorum succeeded (see `pkg/distributor/distributor.go:sendWriteRequestToBackends`, line 2106).
- Context cancellation and deadline are preserved explicitly (`context.WithoutCancel` used when fan-out must outlive caller; see `distributor.go:2123`).

## Cross-Cutting Concerns

**Logging:**
- `github.com/go-kit/log` with structured key/value pairs. Levels via `github.com/go-kit/log/level`.
- Logger at `pkg/util/log/` (package `util_log`). Trace-aware span logger at `pkg/util/spanlogger/`.
- Rate limiting optional (`mainFlags.rateLimitedLogsEnabled` in `cmd/mimir/main.go`).

**Metrics:**
- Prometheus client, registered with `promauto.With(reg)` (convention from `pkg/CLAUDE.md` — no global registries, pass `prometheus.Registerer` to constructors).
- Metric namespace `cortex` (set in `pkg/mimir/mimir.go:163` for back-compat).

**Tracing:**
- OpenTelemetry. Each package declares a tracer: `var tracer = otel.Tracer("pkg/<component>")`.
- Spans propagated over gRPC/HTTP metadata.
- Activity tracker (`pkg/util/activitytracker/`) writes in-flight operations to a file for crash diagnostics.

**Validation:**
- Write-path validation: `pkg/distributor/validate.go` enforces label count, label length, sample value, timestamp bounds per tenant via `validation.Overrides`.
- Query-path validation: `pkg/util/validation/blocked_query.go`, `limited_query.go` enforce tenant-level query restrictions.

**Authentication:**
- `dskit/middleware.AuthenticateUser` HTTP middleware extracts `X-Scope-OrgID` and puts the tenant ID on the request context (applied in `pkg/api/api.go:newRoute` when `auth` is true).
- gRPC uses metadata key `org-id` via `dskit/user` interceptors.
- Multitenancy toggleable via `-auth.multitenancy-enabled`. When disabled, `-auth.no-auth-tenant` (default `anonymous`) is used.

**Cross-tenant safety:**
- `CLAUDE.md` (repo root) documents the yoloString hazard: strings inside `PreallocWriteRequest` alias a shared request buffer. `strings.Clone` required before a string outlives the request. Enforced by convention and tests, not by the type system.

---

*Architecture analysis: 2026-04-22*
