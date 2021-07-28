#!/bin/bash

set -e

SCRIPT_DIR=$(cd `dirname $0` && pwd)

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
CGO_ENABLED=0 GOOS=linux go build -mod=vendor -gcflags "all=-N -l" -o ${SCRIPT_DIR}/mimir ${SCRIPT_DIR}/../../cmd/mimir
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build distributor 
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
