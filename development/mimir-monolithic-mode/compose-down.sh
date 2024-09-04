#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-s3-single-binary/compose-down.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

# newer compose is a subcommand of `docker`, not a hyphenated standalone command
docker_compose() {
    if [ -x "$(command -v docker-compose)" ]; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

SCRIPT_DIR=$(cd "$(dirname -- "$0")" && pwd)

PROFILES=()
ARGS=()
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --profile)
            PROFILES+=("$1")
            shift
            PROFILES+=("$1")
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

DEFAULT_PROFILES=(
    "--profile" "prometheus"
    "--profile" "grafana-agent-static"
    "--profile" "grafana-agent-flow"
    "--profile" "otel-collector-remote-write"
    "--profile" "otel-collector-otlp-push"
    "--profile" "otel-collector-otlp-push-config-with-spanmetrics"
)
if [ ${#PROFILES[@]} -eq 0 ]; then
    PROFILES=("${DEFAULT_PROFILES[@]}")
fi

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml "${PROFILES[@]}" down "${ARGS[@]}"
