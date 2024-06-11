Mimir integration tests are written in Go and based on a [custom framework](https://github.com/grafana/e2e) running Mimir and its dependencies in Docker containers and using the Go [`testing`](https://golang.org/pkg/testing/) package for assertions. Integration tests run in CI for every PR, and can be easily executed locally during development (it just requires Docker).

## How to run integration tests

When integration tests run in CI, we build the Mimir Docker image based on the PR code and then run the integration tests against it. When running tests **locally** you should build the Mimir Docker image first:

```
make ./cmd/mimir/.uptodate
```

This will locally build the `grafana/mimir:latest` image used by integration tests. Whenever the Mimir code changes (`cmd/`, `pkg/` or vendors) you should rebuild the Mimir image, while it's **not** necessary to rebuild it while developing integration tests.

Once the Docker image is built, you can run integration tests:

```
go test -v -tags=requires_docker ./integration/...
```

If you want to run a single test you can use a filter. For example, to only run `TestChunksStorageAllIndexBackends`:

```
go test -v -tags=requires_docker ./integration -run "^TestChunksStorageAllIndexBackends$"
```

When running all integration tests, the test process may time out before the tests complete. If this happens, you can increase `go test`'s default 10 minute timeout to something longer with the `-timeout` flag, for example:

```
go test -v -tags=requires_docker -timeout=20m ./integration/...
```

### Supported environment variables

- **`MIMIR_IMAGE`**<br />
  Docker image used to run Mimir in integration tests (defaults to `grafana/mimir:latest`)
- **`MIMIRTOOL_IMAGE`**<br />
  Docker image used to run `mimirtool` in integration tests (defaults to `grafana/mimirtool:latest`)
- **`MIMIR_CHECKOUT_DIR`**<br />
  The absolute path of the Mimir repository local checkout (defaults to `$GOPATH/src/github.com/grafana/mimir`)
- **`E2E_TEMP_DIR`**<br />
  The absolute path to a directory where the integration test will create an additional temporary directory to store files generated during the test.
- **`E2E_NETWORK_NAME`**<br />
  Name of the docker network to create and use for integration tests. If no variable is set, defaults to `e2e-mimir-test`.

### The `requires_docker` tag

Integration tests have `requires_docker` tag (`// +build requires_docker` line followed by empty line on top of Go files), to avoid running them unintentionally as they require Docker, e.g. by running `go test ./...` in main Mimir package.

## Isolation

Each integration test runs in isolation. For each integration test, we do create a Docker network, start Mimir and its dependencies containers, push/query series to/from Mimir and run assertions on it. Once the test is done, both the Docker network and containers are terminated and deleted.
