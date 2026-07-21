#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

docker_compose() {
    if [ -x "$(command -v docker-compose)" ]; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

SCRIPT_DIR=$(cd "$(dirname -- "$0")" && pwd)
BUILD_IMAGE=$(make -s -f "${SCRIPT_DIR}"/../../Makefile print-build-image)

CGO_ENABLED=0 GOOS=linux go build -mod=vendor -tags=netgo,stringlabels -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" distributor-1
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml up "$@"
