#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

# This script simplified building a Linux binary on non-linux systems.
# If "go build ." works for you, you can ignore this.

MIMIR_ROOT="$(git rev-parse --show-toplevel)"

# Build will be done inside mimir-trafficdump-build image, which installs libpcap-dev.

# We use "--platform=linux/amd64" so that "apt-get install" inside Dockerfile installs lib for correct arch.
docker build "$MIMIR_ROOT" --platform=linux/amd64 -f Dockerfile.build -t mimir-trafficdump-build

docker run --rm -v "$MIMIR_ROOT":/mimir mimir-trafficdump-build
