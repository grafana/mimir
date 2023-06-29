To upgrade the Golang version:

Upgrade the build image version:

1. Upgrade the Golang version in `mimir-build-image/Dockerfile`.
2. Open PR and Wait for mimir-build-image to be updated. Refer to the [documentation](https://github.com/grafana/mimir/blob/main/docs/internal/how-to-update-the-build-image.md) for more information.

If the minimum supported Golang version should be upgraded as well:

1. Upgrade `go` version in `go.mod`
