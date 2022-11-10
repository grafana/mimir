To upgrade the Golang version:

1. Upgrade build image version
   - Upgrade Golang version in `mimir-build-image/Dockerfile`
   - Build new image `make mimir-build-image/.uptodate`
   - Publish the new image to `grafana/mimir-build-image` (requires a maintainer)
   - Update the Docker image tag in `.github/workflows/*`
2. Upgrade integration tests version
   - Update the Golang version installed in the `integration` job in `.github/workflows/*`
3. Upgrade the reference to the latest build image called `LATEST_BUILD_IMAGE_TAG` in `Makefile`

If the minimum supported Golang version should be upgraded as well:

1. Upgrade `go` version in `go.mod`
