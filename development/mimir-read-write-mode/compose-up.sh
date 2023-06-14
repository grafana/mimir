#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ALPINE_VERSION=$(make -s -f ../../Makefile print-alpine-version)

# Make sure docker-compose.yml is up-to-date.
cd $SCRIPT_DIR && make

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o ${SCRIPT_DIR}/mimir ${SCRIPT_DIR}/../../cmd/mimir
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build --build-arg ALPINE_VERSION=${ALPINE_VERSION} mimir-write-1
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
