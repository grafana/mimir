#!/usr/bin/env bash

BUILD_IMAGE=$(make -s -f ./Makefile print-build-image)
GIT_VERSION=$(git describe --abbrev=8 --always --dirty)

docker build \
  --build-arg BUILD_IMAGE="${BUILD_IMAGE}" \
  --build-arg=EXTRA_PACKAGES="gcompat" \
  -t grafana/mimir-boringcrypto-debug:"${GIT_VERSION}" \
  -f ./build-boringcrypto-debug-image.Dockerfile .

#docker run \
#  -p "8001:8001" \
#  -p "18001:18001" \
#  docker.io/grafana/mimir-boringcrypto-debug:mimir-2.11.0-313-gb1f31379-dirty \
#  "dlv exec mimir --listen=:18001 --headless=true --api-version=2 --accept-multiclient --continue -- -target=all -server.http-listen-port=8001 -server.grpc-listen-port=9001"