#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-s3-single-binary/compose-up.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

set -e

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

DEFAULT_PROFILES=("--profile" "prometheus" "--profile" "grafana-agent-static")
if [ ${#PROFILES[@]} -eq 0 ]; then
    PROFILES=("${DEFAULT_PROFILES[@]}")
fi

# Returns 0 (true) if the binary doesn't exist, if find fails, or if anything under the given paths is newer than the binary.
# Directories count too, so an added or deleted source file triggers a rebuild.
# All files count, not only *.go, so go:embed assets (e.g. .gohtml templates, pkg/api/static/) trigger a rebuild too.
# Only *.md files are skipped, because the build does not embed them.
needs_build() {
    local binary="$1"; shift
    [ -f "$binary" ] || return 0
    local newer
    newer=$(find "$@" ! -name '*.md' -newer "$binary" -print -quit) || return 0
    [ -n "$newer" ]
}

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
# Only recompile when sources changed (it's the slow step), but always (re)build the
# image: it's cheap and decoupling avoids a stale image if a previous image build failed
# after a successful go build.
if needs_build "${SCRIPT_DIR}/mimir" "${SCRIPT_DIR}/../../cmd/mimir" "${SCRIPT_DIR}/../../pkg" "${SCRIPT_DIR}/../../vendor" "${SCRIPT_DIR}/../../go.mod" "${SCRIPT_DIR}/compose-up.sh"; then
    CGO_ENABLED=0 GOOS=linux go build -mod=vendor -tags=netgo,stringlabels -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir
fi

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build mimir-1 && \
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml "${PROFILES[@]}" up "${ARGS[@]}"
