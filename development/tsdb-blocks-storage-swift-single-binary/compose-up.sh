#!/bin/bash

SCRIPT_DIR=$(cd `dirname $0` && pwd)

CGO_ENABLED=0 GOOS=linux go build -o ${SCRIPT_DIR}/mimir ${SCRIPT_DIR}/../../cmd/mimir && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml build mimir-1 && \
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up $@
