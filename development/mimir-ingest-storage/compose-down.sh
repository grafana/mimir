#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

# newer compose is a subcommand of `docker`, not a hyphenated standalone command
docker_compose() {
    if [ -x "$(command -v docker-compose)" ]; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

SCRIPT_DIR=$(cd "$(dirname -- "$0")" && pwd)

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml down
