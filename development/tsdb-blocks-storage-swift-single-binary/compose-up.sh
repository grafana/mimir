#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-swift-single-binary/compose-up.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

SCRIPT_DIR=$(cd `dirname $0` && pwd)

CGO_ENABLED=0 GOOS=linux go build -o ${SCRIPT_DIR}/mimir ${SCRIPT_DIR}/../../cmd/mimir && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build mimir-1 && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
