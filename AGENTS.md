@docs/internal/contributing/README.md

## Cursor Cloud specific instructions

- Use `/usr/local/bin/go` (Go 1.26.7). The Ubuntu `go` package is older than `go.mod`.
- `make` defaults to `BUILD_IN_CONTAINER=true` and expects the Mimir build image via Docker. Docker is not part of this environment. Run the same targets CI runs inside that image, with vendored modules:

  ```bash
  make BUILD_IN_CONTAINER=false test
  make BUILD_IN_CONTAINER=false lint
  CGO_ENABLED=0 go build -tags netgo,stringlabels -o mimir ./cmd/mimir
  ```

- On boot, an `eth0` bridge (`192.0.2.10/32`) is created because this VM's NIC is `enp0s3`. Mimir's default ring address lookup checks `eth0`/`en0` and ignores loopback, so unit tests fail without it.
- On boot, monolithic Mimir is built from the checkout and started with `docs/configurations/demo.yaml`. HTTP stays on port 9009. gRPC listens on 19095 and memberlist on 17946 so unit tests can bind the defaults 9095 and 7946. Anonymous usage reporting is disabled (`-usage-stats.enabled=false`). `GET /ready` stays unready for about 15 seconds while the ingester settles. Push samples with `POST /otlp/v1/metrics` or `POST /api/v1/push`, and query with `GET /prometheus/api/v1/query`.
- `golangci-lint` 2.12.2, `shfmt` 3.13.1, `protoc`, `shellcheck`, and the `mimir-build-image` Go tools (`faillint`, `misspell`, `protoc-gen-go`, `protoc-gen-gogoslick`, `jsonnetfmt`, and the rest of that module's `tool` list) are on `PATH`.
