#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

SCRIPT_DIR=$(cd `dirname $0` && pwd)

docker-compose -f ${SCRIPT_DIR}/docker-compose.yml down
