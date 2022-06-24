#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-s3/compose-up.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# Make sure docker-compose.yml is up-to-date.
cd $SCRIPT_DIR && make

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o ${SCRIPT_DIR}/mimir ${SCRIPT_DIR}/../../cmd/mimir
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build distributor-1
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
