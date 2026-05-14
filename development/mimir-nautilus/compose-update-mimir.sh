#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Recompile the mimir binary and bounce the Mimir containers in
# place. Used by the agent iteration loop:
#
#   ./compose-update-mimir.sh
#   ./verify.sh
#
# On verify failures inspect with:
#   docker compose -f docker-compose.yml logs --tail=200 readcache-1
#   docker compose -f docker-compose.yml logs --tail=200 nautilus-rebalancer
#   docker volume inspect mimir-nautilus_nautilus-rebalancer-data

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

CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" distributor-1
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml up -d --force-recreate distributor-1 ingester-1 nautilus-rebalancer readcache-1 readcache-2 block-builder-0 block-builder-scheduler-0 query-scheduler query-frontend querier store-gateway compactor
