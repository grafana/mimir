#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# newer compose is a subcommand of `docker`, not a hyphenated standalone command
docker_compose() {
    if [ -x "$(command -v docker-compose)" ]; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

SCRIPT_DIR=$(cd "$(dirname -- "$0")" && pwd)
BUILD_IMAGE=$(make -s -C "${SCRIPT_DIR}"/../.. print-build-image)

# Make sure docker-compose.yml is up-to-date.
cd "$SCRIPT_DIR" && make

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -tags=netgo,stringlabels -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" mimir-write-zone-a-1
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml up "$@"
