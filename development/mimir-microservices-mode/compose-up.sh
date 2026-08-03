#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/development/tsdb-blocks-storage-s3/compose-up.sh
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
BUILD_IMAGE=$(make -s -C "${SCRIPT_DIR}"/../.. print-build-image)
# Make sure docker-compose.yml is up-to-date.
cd "$SCRIPT_DIR" && make

# Returns 0 (true) if the binary doesn't exist or any source file under the given
# directories is newer than the binary. We consider all files (not just *.go) so
# go:embed assets (e.g. .gohtml templates, static/, JSON descriptors) trigger a
# rebuild too, but skip docs like .md which can't affect the compiled binary.
needs_build() {
    local binary="$1"; shift
    [ ! -f "$binary" ] && return 0
    find "$@" -type f ! -name '*.md' -newer "$binary" -print -quit 2>/dev/null | grep -q .
}

# -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
# GOARCH is not changed.
# Only recompile when sources changed (it's the slow step), but always (re)build the
# image: it's cheap and decoupling avoids a stale image if a previous image build failed
# after a successful go build.
if needs_build "${SCRIPT_DIR}/mimir" "${SCRIPT_DIR}/../../cmd/mimir" "${SCRIPT_DIR}/../../pkg" "${SCRIPT_DIR}/../../vendor"; then
    CGO_ENABLED=0 GOOS=linux go build -mod=vendor -tags=netgo,stringlabels -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/mimir "${SCRIPT_DIR}"/../../cmd/mimir
fi
docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" distributor-1

if [ "$(yq '.services."query-tee"' "${SCRIPT_DIR}"/docker-compose.yml)" != "null" ]; then
  # If query-tee is enabled, build its binary and image as well.
  if needs_build "${SCRIPT_DIR}/../../cmd/query-tee/query-tee" "${SCRIPT_DIR}/../../cmd/query-tee" "${SCRIPT_DIR}/../../pkg" "${SCRIPT_DIR}/../../tools/querytee" "${SCRIPT_DIR}/../../vendor"; then
    CGO_ENABLED=0 GOOS=linux go build -mod=vendor -tags=netgo,stringlabels -gcflags "all=-N -l" -o "${SCRIPT_DIR}"/../../cmd/query-tee "${SCRIPT_DIR}"/../../cmd/query-tee
  fi
  docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml build --build-arg BUILD_IMAGE="${BUILD_IMAGE}" query-tee
fi

docker_compose -f "${SCRIPT_DIR}"/docker-compose.yml up "$@"
