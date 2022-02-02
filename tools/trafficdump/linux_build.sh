#!/bin/bash

# This script simplified building a Linux binary on non-linux systems.
# If "go build ." works for you, you can ignore this.

MIMIR_ROOT="$(git rev-parse --show-toplevel)"

# Build will be done inside mimir-trafficdump-build image, which installs libpcap-dev.

docker build "$MIMIR_ROOT" --platform=linux/amd64 -f Dockerfile.build -t mimir-trafficdump-build

docker run --rm -v "$MIMIR_ROOT":/mimir mimir-trafficdump-build
