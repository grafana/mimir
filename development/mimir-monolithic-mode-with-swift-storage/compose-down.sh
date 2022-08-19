#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-swift-single-binary/compose-down.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

SCRIPT_DIR=$(cd `dirname $0` && pwd)

docker-compose -f ${SCRIPT_DIR}/docker-compose.yml down
