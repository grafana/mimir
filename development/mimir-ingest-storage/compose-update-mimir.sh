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
BUILD_IMAGE=$(make -s -f "${SCRIPT_DIR}"/../../Makefile print-build-image)

# Make sure docker-compose.yml is up-to-date.
cd "$SCRIPT_DIR" && make

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir
# Build docker image
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" mimir-write-zone-a-1
# Recreate Mimir containers
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml up -d --force-recreate "$@" mimir-write-zone-a-{1..3} mimir-write-zone-b-{1..3} mimir-read-{1..2} mimir-backend-{1..2}
