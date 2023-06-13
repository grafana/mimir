To upgrade the Golang version:

1. Upgrade build image version
   - Upgrade Golang version in `mimir-build-image/Dockerfile`
   - Build new image `make mimir-build-image/.uptodate`
   - Publish the new image to `grafana/mimir-build-image` (requires timed access)
2. Upgrade the reference to the latest build image called `LATEST_BUILD_IMAGE_TAG` in `Makefile`

If the minimum supported Golang version should be upgraded as well:

1. Upgrade `go` version in `go.mod`
