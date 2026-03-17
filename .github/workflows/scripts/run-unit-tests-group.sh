#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
set -o pipefail

SCRIPT_DIR=$(cd `dirname $0` && pwd)
MIMIR_DIR=$(realpath "${SCRIPT_DIR}/../../../")

# Parse args.
INDEX=""
TOTAL=""

while [[ $# -gt 0 ]]
do
  case "$1" in
    --total)
      TOTAL="$2"
      shift # skip --total
      shift # skip total value
      ;;
    --index)
      INDEX="$2"
      shift # skip --index
      shift # skip index value
      ;;
    *)  break
      ;;
  esac
done

if [[ -z "$INDEX" ]]; then
    echo "No --index provided."
    exit 1
fi

if [[ -z "$TOTAL" ]]; then
    echo "No --total provided."
    exit 1
fi

# If you change the build tags or CLI flags, update warmup-build-cache-unit-tests in the Makefile too.
BUILD_TAGS="netgo,stringlabels"
if [[ -n "$EXTRA_BUILD_TAGS" ]]; then
    BUILD_TAGS="$BUILD_TAGS,$EXTRA_BUILD_TAGS"
fi

# List all tests.
ALL_TESTS=$(go list "${MIMIR_DIR}/..." | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" 'NR % TOTAL == INDEX')

if [[ -z "$GROUP_TESTS" ]]; then
    echo "ERROR: No packages found for group $INDEX of $TOTAL. This likely indicates a compilation error or misconfiguration."
    exit 1
fi

# The tests in the MQE benchmarks package load an enormous amount of data, which causes the
# race detector to consume a large amount of memory and run incredibly slowly on CI.
# The same code is tested by other unit tests which run with the race detector enabled, so
# don't bother running the benchmark tests with the race detector enabled.
# If you add packages here, also update warmup-build-cache-unit-tests in the Makefile.
SKIP_RACE_DETECTOR_PATTERN="^github.com/grafana/mimir/pkg/streamingpromql/benchmarks$"

echo "This group will run the following tests (race detector enabled unless stated otherwise):"
echo "$GROUP_TESTS" | while read -r pkg; do
    if echo "$pkg" | grep -q -e "$SKIP_RACE_DETECTOR_PATTERN"; then
        echo "$pkg (race detector disabled)"
    else
        echo "$pkg"
    fi
done
echo

EXIT_CODE=0
FAILED_PACKAGES=""

# Run one package at a time so that a failure can be retried individually without re-running
# the entire group.
MAX_ATTEMPTS=2

for pkg in $GROUP_TESTS; do
    if echo "$pkg" | grep -q -e "$SKIP_RACE_DETECTOR_PATTERN"; then
        RACE_FLAG=""
    else
        RACE_FLAG="-race"
    fi

    for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
        if [[ $ATTEMPT -gt 1 ]]; then
            echo "Retrying failed package: $pkg"
            echo
        fi

        # shellcheck disable=SC2086 # we *want* word splitting of RACE_FLAG.
        go test -tags="${BUILD_TAGS}" -timeout 30m $RACE_FLAG "$pkg"
        PKG_EXIT_CODE=$?

        if [[ $PKG_EXIT_CODE -eq 0 ]]; then
            break
        fi
    done

    if [[ $PKG_EXIT_CODE -ne 0 ]]; then
        EXIT_CODE=1
        FAILED_PACKAGES="${FAILED_PACKAGES} ${pkg}"
    fi
done

# Store in GitHub environment variable if any packages failed.
FAILED_PACKAGES=$(echo "$FAILED_PACKAGES" | xargs)
if [[ -n "$FAILED_PACKAGES" ]]; then
    echo "FAILED_PACKAGES=${FAILED_PACKAGES}" >> "$GITHUB_ENV"
fi

exit $EXIT_CODE
