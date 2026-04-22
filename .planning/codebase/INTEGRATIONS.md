# External Integrations

**Analysis Date:** 2026-04-22

## Overview

Mimir is a multi-tenant, horizontally-scalable time-series database. Every integration is designed for shared-nothing scaling and tenant isolation. All inter-service coordination happens through a hash ring (backed by a pluggable KV store), and all persistent state lives in pluggable object storage.

## Ingestion (Inbound)

All write paths converge on the distributor, which then forwards to ingesters (or to Kafka for the experimental ingest-storage path).

**Prometheus remote-write (v1 + v2):**
- Endpoint constant: `PrometheusPushEndpoint = "/api/v1/push"` (`pkg/api/api.go:289`).
- Entry code: `pkg/distributor/push.go`, `pkg/distributor/request.go`, `pkg/distributor/distributor.go`.
- Payload is Snappy-compressed protobuf (`github.com/golang/snappy`, `pkg/mimirpb/`).

**OTLP metrics (HTTP/protobuf, HTTP/JSON):**
- Endpoint constant: `OTLPPushEndpoint = "/otlp/v1/metrics"` (`pkg/api/api.go:290`).
- Flag: `-api.otlp-translation-headers-enabled` (`pkg/api/api.go:84`), enables per-request `X-Mimir-OTLP-AddSuffixes` / `X-Mimir-OTLP-TranslationStrategy` overrides.
- Handler: `pkg/distributor/otel*.go`. Uses `go.opentelemetry.io/collector/pdata` for decoding and `github.com/grafana/mimir-otlptranslator` (replaced at `go.mod:378`) for metric translation.

**InfluxDB line protocol:**
- Endpoint constant: `InfluxPushEndpoint = "/api/v1/push/influx/write"` (`pkg/api/api.go:291`).
- Parser: `pkg/distributor/influxpush/parser.go`.
- Pulls in `github.com/influxdata/influxdb/v2` (`go.mod:23`).

**High-availability deduplication:**
- HA tracker at `pkg/distributor/ha_tracker_http.go` and `pkg/distributor/ha_tracker.go` (Prometheus HA pair deduplication by external labels).
- Backed by a KV store (see KV Stores section).

**Experimental Kafka ingest path:**
- Optional mode where distributors write to Kafka and ingesters consume from Kafka partitions. Code: `pkg/storage/ingest/` (see `pkg/storage/ingest/DESIGN.md` and `INTERNALS.md`).
- Client: `github.com/twmb/franz-go` v1.20.7 with `kadm`, `kfake`, `kmsg`, `kotel`, `kprom` plugins.
- Dev environment: `development/mimir-ingest-storage/`.

## Querying (Inbound)

- PromQL and range/instant query endpoints routed through the query-frontend (`pkg/frontend/`), then to the query-scheduler (`pkg/scheduler/`), then to queriers.
- Query middleware stack: `pkg/frontend/querymiddleware/` (splitting, sharding, caching, step alignment, codecs).
- Wire codecs: JSON (`codec_json.go`) and protobuf (`codec_protobuf.go`) with protobuf-native content type via `pkg/api/protobuf_codec.go`.
- Cardinality APIs: `pkg/distributor/`, `pkg/cardinality/`, `pkg/frontend/querymiddleware/cardinality.go`.
- Streaming engine: `pkg/streamingpromql/` (Mimir's streaming PromQL engine built on top of Prometheus PromQL).
- Remote querier (ruler -> frontend): `pkg/ruler/remotequerier.go`.

## Object Storage (Blocks, Rules, Alertmanager Configs, Bucket Index)

Abstracted through `github.com/thanos-io/objstore`. The Mimir-level client lives in `pkg/storage/bucket/client.go`.

**Supported backends (`pkg/storage/bucket/client.go:33-62`):**

| Backend     | Constant     | Client package                                | SDK / driver                                              |
|-------------|--------------|-----------------------------------------------|----------------------------------------------------------|
| AWS S3      | `"s3"`       | `pkg/storage/bucket/s3/`                      | `github.com/minio/minio-go/v7` and `aws-sdk-go-v2/s3`    |
| Google GCS  | `"gcs"`      | `pkg/storage/bucket/gcs/`                     | `cloud.google.com/go/storage`                            |
| Azure Blob  | `"azure"`    | `pkg/storage/bucket/azure/`                   | `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`   |
| OpenStack Swift | `"swift"` | `pkg/storage/bucket/swift/`                  | `github.com/ncw/swift`                                   |
| Filesystem  | `"filesystem"` | `pkg/storage/bucket/filesystem/`            | Local FS (dev/test only)                                 |

**What lives in object storage:**
- TSDB blocks per tenant (ingesters upload, store-gateway reads, compactor compacts) under the tenant prefix.
- Mimir internal metadata under prefix `__mimir_cluster` (const `MimirInternalsPrefix`, `pkg/storage/bucket/client.go:54`).
- Bucket index: `pkg/storage/tsdb/bucketindex/` - listing cache for store-gateway/querier.
- Bucket cache: `pkg/storage/tsdb/bucketcache/` - in-memory/memcached caching layer.
- Ruler rule groups: `pkg/ruler/rulestore/bucketclient/`.
- Alertmanager configs: `pkg/alertmanager/alertstore/bucketclient/`.
- Tenant deletion markers: `pkg/storage/tsdb/tenant_deletion_mark.go`.

**Backend-agnostic features:**
- Server-side encryption overlay: `pkg/storage/bucket/sse_bucket_client.go`.
- Prefixing per tenant/purpose: `pkg/storage/bucket/prefixed_bucket_client.go`, `pkg/storage/bucket/user_bucket_client.go`.
- Delay/fault-injection wrappers: `pkg/storage/bucket/delayed_bucket_client.go`, `pkg/storage/bucket/error_injected_bucket_client.go`.
- Tracing: `objstoretracing "github.com/thanos-io/objstore/tracing/opentelemetry"` wraps every bucket (`pkg/storage/bucket/client.go:21`).

## KV Stores (Hash Rings, HA Tracker, Partition Ring)

Pluggable through `github.com/grafana/dskit/kv` (see `vendor/github.com/grafana/dskit/kv/client.go:38-88`).

**Supported backends:**

| Backend      | Flag value    | Notes                                                     |
|--------------|---------------|-----------------------------------------------------------|
| In-memory    | `inmemory`    | Single-process testing only                               |
| Consul       | `consul`      | `github.com/hashicorp/consul/api` v1.33.7                 |
| etcd         | `etcd`        | `go.etcd.io/etcd/client/v3` v3.6.9                        |
| Memberlist   | `memberlist`  | Grafana fork of HashiCorp memberlist (`go.mod:357`); gossip-based, recommended default |
| Multi        | `multi`       | Shadow-write/read between two backends for migration      |

**Memberlist wiring:** `pkg/mimir/modules.go:21` imports `github.com/grafana/dskit/kv/memberlist`. `MemberlistKV` is initialized at `pkg/mimir/modules.go:1303`. Status page: `pkg/api/memberlist_status.gohtml`.

**What uses KV:** distributor ingester ring, ingester shuffle sharding, query-scheduler ring, compactor ring, store-gateway ring, ruler ring, alertmanager ring, HA tracker (distributor), partition ring (ingest-storage mode). Each component registers flags like `-<component>.ring.store` and `-<component>.ring.consul.hostname` etc.

## Alertmanager

- Multi-tenant Alertmanager at `pkg/alertmanager/multitenant.go` built on top of `github.com/prometheus/alertmanager` v0.31.1.
- Per-tenant config stored in object storage via `pkg/alertmanager/alertstore/bucketclient/` or locally via `pkg/alertmanager/alertstore/local/`.
- Configs served at `/multitenant_alertmanager/configs` (admin) and `/api/v1/alerts` (per tenant) - see `pkg/api/api.go:244-258`.
- Internal gRPC client: `pkg/alertmanager/alertmanager_client.go`, with ring-based replication.
- Merger for state replication: `pkg/alertmanager/merger/`.
- Rate-limited notifier: `pkg/alertmanager/rate_limited_notifier.go`.
- Protobuf types: `pkg/alertmanager/alertmanagerpb/`, `pkg/alertmanager/alertspb/`.
- Outbound notifications use upstream Alertmanager integrations (email/SMTP, PagerDuty, Slack, OpsGenie, VictorOps, Webhook, Pushover, WeChat, Telegram via `gopkg.in/telebot.v3` at `go.mod:224`, MSTeams, SNS via `aws-sdk-go-v2/service/sns` at `go.mod:116`).

## Ruler

- Code: `pkg/ruler/`. Rule evaluator runs rules and writes results back via remote-write or via a local pusher.
- Rule groups persisted to object storage (`pkg/ruler/rulestore/bucketclient/`) or local FS (`pkg/ruler/rulestore/local/`).
- Remote query backend: `pkg/ruler/remotequerier.go` (HTTP) and `pkg/ruler/grpc_roundtripper.go` (gRPC).
- Notifier connects to the Alertmanager cluster: `pkg/ruler/notifier.go`, `pkg/ruler/notifier/`.
- Namespace protection rules: `pkg/ruler/namespace_protection.go`.
- Mapping / tenant isolation: `pkg/ruler/mapper.go`.

## Query-Frontend & Scheduler

- Query-frontend: `pkg/frontend/` with HTTP (`pkg/frontend/transport/`) and gRPC (`pkg/frontend/v2/`) fronts.
- Query-scheduler: `pkg/scheduler/scheduler.go` with gRPC proto `pkg/scheduler/schedulerpb/`. See `pkg/scheduler/DESIGN.md` and `pkg/scheduler/README.md`.
- Scheduler discovery (DNS / ring-based): `pkg/scheduler/schedulerdiscovery/`.
- Middleware stack applies caching and result stitching - cache can be memcached or Redis via dskit cache abstractions.

## Caching

- Results cache, chunks cache, index cache, metadata cache - all configured via `pkg/storage/tsdb/caching_config.go`.
- Supported backends: Memcached (`github.com/grafana/gomemcache` at `go.mod:287`), Redis (via dskit), in-memory (`github.com/dgraph-io/ristretto` at `go.mod:259`).
- Index cache specifics: `pkg/storage/tsdb/indexcache/`.
- Bucket cache: `pkg/storage/tsdb/bucketcache/`.

## Authentication & Multi-Tenancy

**Tenant model:** Mimir does NOT implement user auth. It delegates to an upstream proxy (e.g., oauth2-proxy, Grafana Enterprise Metrics, nginx). Every request must carry a tenant ID header.

- Header: `X-Scope-OrgID` (seen throughout `pkg/api/activity_tracking_test.go:30,79,160,221,273` and used via dskit's `user.ExtractOrgIDFromHTTPRequest`).
- Extraction: `pkg/api/activity_tracking.go:79` and `pkg/api/tenant.go:26` via `github.com/grafana/dskit/tenant`.
- Multi-tenant federation: multiple tenants may be specified with a pipe-separated value (handled by `tenant.TenantIDs(ctx)`).
- Single-tenant mode: `-auth.multitenancy-enabled=false` lets Mimir auto-inject `fake` as the tenant ID.

**Vault integration (for secrets, TLS certs, etc.):**
- Code: `pkg/vault/vault.go`, `pkg/vault/auth.go`.
- Client: `github.com/hashicorp/vault/api` v1.22.0 (`go.mod:66`).
- Auth methods: AppRole (`go.mod:298`), Kubernetes (`go.mod:299`), Userpass (`go.mod:300`).

## Observability (Outbound)

**Metrics:**
- Self-instrumented with `github.com/prometheus/client_golang` (Grafana fork at `go.mod:32`). Exposed at `/metrics`.
- Every internal component accepts a `prometheus.Registerer` (no global registry - see `pkg/CLAUDE.md` conventions).

**Tracing:**
- OpenTelemetry via `go.opentelemetry.io/otel` v1.43.0 and friends. Exporters: OTLP (gRPC and HTTP), Jaeger (legacy), stdout.
- Jaeger propagator: `go.opentelemetry.io/contrib/propagators/jaeger` (`go.mod:89`).
- gRPC instrumentation: `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` (`go.mod:88`).
- HTTP instrumentation: `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` (`go.mod:336`).
- OpenTracing bridge retained via `github.com/opentracing/opentracing-go` (`go.mod:29`) for gradual migration. Fork of `opentracing-contrib/go-stdlib` pinned via replace at `go.mod:371`.

**Logging:**
- `github.com/go-kit/log` v0.2.1 (`go.mod:11`) - primary logger.
- `github.com/go-logfmt/logfmt` v0.6.1 - logfmt output format.
- `github.com/tjhop/slog-gokit` v0.1.6 - bridge from Go's `slog` into go-kit log (needed because modern deps use `slog`).

**Profiling:**
- `github.com/felixge/fgprof` v0.9.5 (`go.mod:57`) - full Goroutine profiling.
- `github.com/grafana/pyroscope-go/godeltaprof` - continuous profiling exporter.

**Usage stats / anonymous telemetry:**
- Code: `pkg/usagestats/`. Opt-out anonymous usage reporting.

## Service Discovery

- DNS-based discovery via dskit's DNS resolver for memcached, query-schedulers, store-gateway.
- Memberlist gossip also functions as a discovery mechanism.
- Systemd notify support: `github.com/okzk/sdnotify` (`go.mod:70`) for ready/stopping notification.

## CI/CD & Deployment

**Container registry:**
- Published to `grafana/mimir`, `grafana/mimir-build-image`, `grafana/mimirtool`, `grafana/metaconvert`, `grafana/query-tee` on Docker Hub.

**CI:**
- GitHub Actions in `.github/workflows/`. Primary workflow `.github/workflows/test-build-deploy.yml` (`name: ci`).
- Release tags `mimir-X.Y.Z*`; weekly branches `r[0-9]+`.

**Deployment assets (first-party):**
- Jsonnet / Tanka: `operations/mimir/` with tests in `operations/mimir-tests/`.
- Helm chart: `operations/helm/` with its own release pipeline.
- K6 load tests: `operations/k6/`.
- Mixin (dashboards, rules, alerts): `operations/mimir-mixin/` compiled to `operations/mimir-mixin-compiled/`, `-baremetal/`, `-gem/`.
- OPA policies: `operations/policies/` (Conftest).

**Local dev environments:**
- `development/mimir-microservices-mode/`, `development/mimir-monolithic-mode/`, `development/mimir-monolithic-mode-with-swift-storage/`, `development/mimir-read-write-mode/`, `development/mimir-ingest-storage/`.
- Each has a `compose-up.sh` / `compose-down.sh` pair.

## Webhooks & Callbacks

**Incoming (beyond the push endpoints above):**
- Admin endpoints: `pkg/api/api.go` registers many HTTP endpoints (e.g., `/ready`, `/config`, `/debug/pprof`, `/metrics`, ring pages, memberlist status). All non-auth endpoints are listed in `pkg/api/api.go`.
- Alertmanager admin: `/multitenant_alertmanager/status`, `/multitenant_alertmanager/configs`, `/multitenant_alertmanager/ring`, `/multitenant_alertmanager/delete_tenant_config` (`pkg/api/api.go:244-248`).

**Outgoing:**
- Alertmanager → receivers (upstream AM integrations).
- Ruler → remote querier (HTTP or gRPC).
- Ruler recording-rule writes → distributor push path (in-process or remote).
- Experimental: Kafka producer from distributor, Kafka consumer in ingester.

## Environment Configuration

**Required / common env vars:** (Mimir itself reads config from YAML + flags, so env vars mostly configure credentials consumed by SDKs)
- AWS SDK: `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE` (used when S3 backend is selected).
- GCP: `GOOGLE_APPLICATION_CREDENTIALS` (used by GCS client).
- Azure: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`, managed identity env vars (used by azblob client).
- Vault: connection/address configured in YAML `-vault.url`, token/auth method per config. See `pkg/vault/auth.go`.
- Build-time: `GOPROXY`, `GOTOOLCHAIN=local`, `GOOS`, `GOARCH` (see `Makefile` and `mimir-build-image/Dockerfile`).
- Runtime: `JAEGER_AGENT_HOST` / OTLP `OTEL_EXPORTER_OTLP_ENDPOINT` for tracing, per the OTel SDK conventions.

**Secrets location:**
- Recommended: HashiCorp Vault via `pkg/vault/`.
- Fallback: environment variables / file-based secrets. No secrets live in the repo.
- `.env*` files are NOT used by Mimir itself; docker-compose dev setups under `development/` use `.env`-style interpolation local to their directory.

## Key Files (Quick Reference)

- Push endpoints: `pkg/api/api.go:289-291`.
- Tenant extraction: `pkg/api/tenant.go`, `pkg/api/activity_tracking.go`.
- Storage backend list: `pkg/storage/bucket/client.go:33-62`.
- KV backend list: `vendor/github.com/grafana/dskit/kv/client.go:38-88`.
- Module wiring and dependency graph: `pkg/mimir/modules.go`.
- Memberlist init: `pkg/mimir/modules.go:1303`.
- Kafka ingest design: `pkg/storage/ingest/DESIGN.md`, `pkg/storage/ingest/INTERNALS.md`.
- Scheduler design: `pkg/scheduler/DESIGN.md`.

---

*Integration audit: 2026-04-22*
