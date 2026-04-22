# Technology Stack

**Analysis Date:** 2026-04-22

## Languages

**Primary:**
- Go `1.25.9` (per `go.mod` line 3 and `mimir-build-image/Dockerfile` line 8) - All service code under `cmd/` and `pkg/`. Built with `GOTOOLCHAIN=local` to pin toolchain to build image.

**Secondary:**
- Jsonnet / libsonnet - Operations tooling, deployment manifests, dashboards/alerts. Found under `operations/mimir/`, `operations/mimir-mixin/`, `operations/mimir-tests/`, and `development/*/`.
- Bash - Build and CI scripts in `tools/`, `development/*/compose-up.sh`, `mimir-build-image/build.sh`.
- Python - A handful of doc/build helpers (python3-requests installed in build image).
- Dockerfile - Per-binary images at `cmd/mimir/Dockerfile`, `cmd/metaconvert/Dockerfile`, `cmd/mimirtool/Dockerfile`, `cmd/query-tee/Dockerfile`, and the build image at `mimir-build-image/Dockerfile`.
- HTML templates - Status/index pages at `pkg/api/index.gohtml`, `pkg/api/memberlist_status.gohtml`, `pkg/mimir/status.gohtml`.
- Protobuf (`.proto`) - Wire formats under `pkg/mimirpb/`, `pkg/alertmanager/alertmanagerpb/`, `pkg/scheduler/schedulerpb/`, `pkg/ingester/client/`.

## Runtime

**Environment:**
- Go runtime (statically compiled binary). Production image is `gcr.io/distroless/static-debian12` (see `cmd/mimir/Dockerfile` line 5). Race image is `gcr.io/distroless/base-nossl-debian12` (see `Makefile` line ~120).
- Build tags `netgo stringlabels` applied globally per `.golangci.yml` lines 11-13.

**Package Manager:**
- Go modules via `go.mod` / `go.sum`. Vendored dependencies committed under `vendor/`.
- Jsonnet Bundler (`jb`) for jsonnet dependencies (see `mimir-build-image/Dockerfile` line 53).

## Frameworks

**Core application framework (no HTTP/RPC framework in the traditional sense; Mimir composes libraries):**
- `github.com/grafana/dskit` (`go.mod` line 20) - The foundational toolkit. Provides services lifecycle, ring, KV store, middleware, tracing, tenant/user extraction, spanlogger, flagext, cache, concurrency, server wiring. This is arguably THE most important dependency.
- `github.com/prometheus/prometheus` - Pinned to the Mimir fork `github.com/grafana/mimir-prometheus` via the replace directive on `go.mod` line 353. Supplies TSDB, PromQL engine, model, `remote` package, `storage` interfaces.
- `github.com/thanos-io/objstore` (`go.mod` line 76) - Object storage abstraction used by `pkg/storage/bucket/` for S3/GCS/Azure/Swift/filesystem backends.

**HTTP/gRPC:**
- `github.com/gorilla/mux` (`go.mod` line 19) - HTTP router used in `pkg/api/api.go`.
- `google.golang.org/grpc` v1.80.0 (`go.mod` line 46) - All intra-cluster RPC.
- `github.com/gogo/protobuf` (`go.mod` line 14) and `github.com/gogo/status` - gogoproto code gen for wire types.
- `google.golang.org/protobuf` v1.36.11 - Standard Google protobuf runtime.

**Testing:**
- `github.com/stretchr/testify` v1.11.1 (`go.mod` line 38) - Assertions, mocks.
- `go.uber.org/goleak` v1.3.0 (`go.mod` line 41) - Goroutine leak detection.
- `github.com/grafana/e2e` (`go.mod` line 21) - Integration test harness wrapping docker-compose.
- `github.com/efficientgo/e2e` (`go.mod` line 263) - Underlying e2e toolkit.

**Build/Dev:**
- Make - primary entry point (`Makefile`), with `Makefile.local.example` for local overrides.
- Docker build image `grafana/mimir-build-image` (`mimir-build-image/Dockerfile`) - pinned Go, protoc, `protoc-gen-gogoslick`, `goimports`, `golangci-lint` v2.8.0, `jsonnet`, `jsonnetfmt`, `jb`, `mixtool`, `helm`, `helm-docs`, `kustomize`, `yq`, `shfmt`, `prettier`, `buf`, `conftest`, `tanka`, `skopeo`.

## Key Dependencies

**Critical (the Mimir root dependencies):**
- `github.com/grafana/dskit` - Shared Grafana infra toolkit (services, ring, KV, middleware, tenant context, tracing). Used pervasively.
- `github.com/grafana/mimir-prometheus` (via replace of `github.com/prometheus/prometheus` at `go.mod` line 353) - Mimir's own fork of Prometheus for TSDB and PromQL.
- `github.com/thanos-io/objstore` - Object storage abstraction at `pkg/storage/bucket/client.go`.
- `github.com/grafana/memberlist` (fork of HashiCorp memberlist via replace at `go.mod` line 357) - Gossip KV store.
- `github.com/hashicorp/consul/api` - Consul KV client.
- `go.etcd.io/etcd/client/v3` - etcd KV client.
- `github.com/prometheus/alertmanager` v0.31.1 (`go.mod` line 31) - Multi-tenant Alertmanager machinery in `pkg/alertmanager/`.
- `github.com/twmb/franz-go` v1.20.7 and `kadm`/`kfake`/`kmsg`/`kotel`/`kprom` plugins (`go.mod` lines 78-83) - Kafka client used by the experimental ingest-storage path at `pkg/storage/ingest/`.

**Storage backends:**
- `cloud.google.com/go/storage` v1.62.1 (`go.mod` line 51) - GCS.
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` v1.6.4 - Azure Blob Storage.
- `github.com/aws/aws-sdk-go-v2/service/s3` v1.97.3 - AWS S3.
- `github.com/minio/minio-go/v7` v7.0.100 - S3-compatible storage client (default S3 backend path).
- `github.com/ncw/swift` v1.0.53 - OpenStack Swift.

**Caching:**
- `github.com/grafana/gomemcache` - Memcached client.
- `github.com/dgraph-io/ristretto` v0.2.0 - In-process cache.
- Redis access is via dskit's caching abstractions (not a direct root dep here).

**Telemetry:**
- `go.opentelemetry.io/otel` v1.43.0 and ecosystem (`go.mod` lines 87-93, 201-220) - OTel tracing/metrics SDK.
- `github.com/opentracing/opentracing-go` (`go.mod` line 29) - OpenTracing bridge (legacy).
- `github.com/uber/jaeger-client-go` (`go.mod` line 39) - Jaeger tracing client.
- `github.com/prometheus/client_golang` v1.23.3 (pinned to a Grafana fork per `go.mod` line 32) - Prometheus metrics instrumentation.

**OTLP / OpenTelemetry ingestion:**
- `go.opentelemetry.io/collector/pdata` v1.56.0 (`go.mod` line 87) - OTLP pdata types.
- `go.opentelemetry.io/proto/otlp` v1.10.0 - OTLP wire proto.
- `github.com/grafana/mimir-otlptranslator` (via replace at `go.mod` line 378) - Histogram/metric translation.

**Performance/algorithms:**
- `github.com/cespare/xxhash/v2` v2.3.0 - Fast hashing.
- `github.com/klauspost/compress` v1.18.5 - Compression (snappy, gzip, zstd).
- `github.com/golang/snappy` v1.0.0 - Prometheus remote-write payload compression.
- `github.com/edsrzf/mmap-go` - Memory-mapped files for TSDB.
- `github.com/oklog/ulid/v2` - Block ID generation.
- `github.com/tylertreat/BoomFilters` - Bloom/count-min sketches.
- `github.com/grafana/regexp` (Grafana's speedup fork via replace at `go.mod` line 365) - Faster regex.

**Auth/secrets:**
- `github.com/hashicorp/vault/api` v1.22.0 and auth providers (approle, kubernetes, userpass) at `go.mod` lines 66, 298-300. Vault integration lives in `pkg/vault/`.

**CLI/config:**
- `github.com/alecthomas/kingpin/v2` - CLI parsing for mimirtool.
- `github.com/spf13/cobra` / `spf13/pflag` - Cobra commands in select tools.
- `gopkg.in/yaml.v3` (via `go.yaml.in/yaml/v3` with Grafana fork per `go.mod` line 362) - YAML config.

**Infrastructure:**
- `github.com/grafana/e2e` - Integration test framework.
- `github.com/grafana-tools/sdk` - Grafana dashboard SDK (used by mimirtool).
- `sigs.k8s.io/kustomize/kyaml` - YAML manipulation used in tooling.
- `k8s.io/client-go` (indirect) - Pulled in via upstream Prometheus/alertmanager deps.

## Configuration

**Environment:**
- Config primarily via YAML config file passed with `-config.file` and CLI flags (see `cmd/mimir/main.go`). Flag-to-YAML mapping auto-generated.
- Config reference lives at `cmd/mimir/config-descriptor.json`, `cmd/mimir/help.txt.tmpl`, `cmd/mimir/help-all.txt.tmpl`. Run `make reference-help doc` to regenerate.
- Runtime overrides reloaded at runtime via `pkg/mimir/runtime_config.go`.

**Build:**
- `Makefile` (top-level) orchestrates all build/test/lint/doc targets. Builds run inside `grafana/mimir-build-image` by default.
- `mimir-build-image/build.sh` is the image entrypoint.
- `Makefile.local.example` demonstrates local overrides (non-committed `Makefile.local`).
- `tools/image-tag` computes image tag from git state.

## Platform Requirements

**Development:**
- Docker (or podman with `CONTAINER_MOUNT_OPTIONS=delegated` override) to run build image.
- `gnu-sed` on macOS (project `CLAUDE.md` notes this).
- Go 1.25.9 locally if building outside the container (`BUILD_IN_CONTAINER=false`).
- Optional: `goimports`, `golangci-lint` v2.8.0, `jsonnet`/`jb`/`mixtool`/`helm-docs` for working outside the build image.

**Production:**
- Linux amd64/arm64 container. Base images:
  - Default: `gcr.io/distroless/static-debian12` (`cmd/mimir/Dockerfile` line 5).
  - Race-enabled: `gcr.io/distroless/base-nossl-debian12` (Makefile) - needs glibc for race detector.
- Exposes port 8080 (HTTP) by default. Separate gRPC port configured in YAML.

## CI

- GitHub Actions under `.github/workflows/`. Primary pipeline is `.github/workflows/test-build-deploy.yml` (`name: ci`) triggered on push to `main`, weekly branches `r[0-9]+`, release tags `mimir-*`, and pull requests.
- Other workflows: `helm-ci.yml`, `helm-release.yaml`, `test-docs.yml`, `backport.yaml`, `changelog-check.yml`, `flaky-tests.yml`, `push-mimir-build-image.yml`, `update-vendored-mimir-prometheus.yml`, `stale.yaml`, plus several doc-publish and deploy-preview workflows.
- Most CI jobs run inside the `grafana/mimir-build-image` container produced by `mimir-build-image/Dockerfile`.
- Go build cache warmed per-branch via `warmup-go-build-cache-*` jobs to keep subsequent lint/test/integration jobs fast.

## Lint & Formatting

**Go lint:**
- `golangci-lint` v2.8.0 driven by `.golangci.yml`. Enabled linters: `errorlint`, `forbidigo`, `gocritic` (dupImport only), `gosec` (G103/G104/G108/G109/G112/G114/G302/G401), `loggercheck`, `misspell`, `revive`, `staticcheck`, `exhaustruct` (with `.*` excluded - effectively scaffolding). Formatters: `gci`, `gofmt`.
- `forbidigo` rule bans `CloseSend` on server-streaming gRPC; use `util.CloseAndExhaust` instead (`.golangci.yml` lines 52-54).
- `staticcheck` keeps `ST1016` enabled (receiver-name consistency).
- Extra linter: `faillint` (installed in build image) for banned import checks.

**Go format:**
- `goimports` with `-local github.com/grafana/mimir` to group Mimir imports separately (per `CLAUDE.md`). Run via `make format`.
- `gci` configured in `.golangci.yml` lines 101-116 with sections `standard`, `default`, `prefix(github.com/grafana/mimir)` in that order.

**Other formatters (all invoked via Makefile):**
- `make format-mixin` then `make build-mixin` for `operations/mimir-mixin/` (uses `mixtool`, `jsonnetfmt`).
- `make format-jsonnet-manifests` for `operations/mimir/`, `operations/mimir-tests/`, `development/`.
- `make format-makefiles` (Makefile changes).
- `make format-protobuf` (`.proto` via `buf`).
- `make format-promql-tests`.
- `misspell` and `shellcheck` are installed in the build image.
- `prettier` v2.3.2 installed in the build image for Markdown/YAML formatting.
- `helm-docs` v1.14.2 for Helm chart READMEs.

## Build Tooling / Release

- Docker images published: `grafana/mimir`, `grafana/mimir-build-image`, `grafana/mimirtool`, `grafana/metaconvert`, `grafana/query-tee` (one per `cmd/*` subdirectory containing a `Dockerfile`).
- Multi-arch builds via `push-multiarch-mimir` Makefile target (see `Makefile` line ~108). Targets linux/amd64 and linux/arm64.
- Helm chart published separately under `operations/helm/` with its own release workflow (`helm-release.yaml`, `helm-weekly-release-pr.yaml`).
- No `goreleaser` used at the repo root - releases are driven by Makefile + GitHub Actions.
- `packaging/` holds deb/rpm package configuration.

## Key Paths

- Binary entrypoint: `cmd/mimir/main.go`.
- Module wiring / service DI: `pkg/mimir/modules.go` and `pkg/mimir/mimir.go`.
- API routing: `pkg/api/api.go` (Gorilla Mux, with `RegisterRoute`/`RegisterAlertmanager`/`RegisterAPI` methods).
- Storage abstraction: `pkg/storage/bucket/client.go`.
- TSDB integration: `pkg/storage/tsdb/config.go`.
- Config descriptor (for docs gen): `cmd/mimir/config-descriptor.json`.

---

*Stack analysis: 2026-04-22*
