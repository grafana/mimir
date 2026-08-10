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

# List all tests, excluding integration tests which require Docker and a built Mimir image.
ALL_TESTS=$(go list "${MIMIR_DIR}/..." | grep -v "^github.com/grafana/mimir/integration" | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" 'NR % TOTAL == INDEX')

if [[ -z "$GROUP_TESTS" ]]; then
    echo "ERROR: No packages found for group $INDEX of $TOTAL. This likely indicates a compilation error or misconfiguration."
    exit 1
fi

# The tests in these MQE packages load an enormous amount of data, which causes the
# race detector to consume a large amount of memory and run incredibly slowly on CI.
# The same code is tested by other unit tests which run with the race detector enabled, so
# don't bother running these tests with the race detector enabled.
# If you add packages here, also update warmup-build-cache-unit-tests in the Makefile.
SKIP_RACE_DETECTOR_PATTERN="^github.com/grafana/mimir/pkg/streamingpromql/(benchmarks|comparisons|fuzz)$"

echo "This group will run the following tests (race detector enabled unless stated otherwise):"
echo "$GROUP_TESTS" | while read -r pkg; do
    if echo "$pkg" | grep -q --extended-regexp "$SKIP_RACE_DETECTOR_PATTERN"; then
        echo "$pkg (race detector disabled)"
    else
        echo "$pkg"
    fi
done
echo

EXIT_CODE=0
FAILED_PACKAGES=""

# Run all the packages in a single "go test" invocation: go test runs packages concurrently
# (up to -p, which defaults to GOMAXPROCS), while invoking it once per package would run them
# serially. Unit tests are mostly latency-bound (they spend their time waiting, not computing),
# so this is roughly 2x faster on a CI runner even though the runner only has a few cores.
# Failed packages are then retried individually, so a flaky package doesn't re-run the group.
MAX_ATTEMPTS=2
OUTPUT_FILE=$(mktemp)
trap 'rm -f "$OUTPUT_FILE"' EXIT

RACE_PACKAGES=$(echo "$GROUP_TESTS" | grep -v --extended-regexp "$SKIP_RACE_DETECTOR_PATTERN")
NO_RACE_PACKAGES=$(echo "$GROUP_TESTS" | grep --extended-regexp "$SKIP_RACE_DETECTOR_PATTERN")

# Runs "go test" on the given packages, setting FAILED to the space separated list of packages
# that failed. FAILED is set to "$@" if the failure can't be attributed to specific packages.
run_tests() {
    local race_flag=$1
    shift

    # shellcheck disable=SC2086 # we *want* word splitting of race_flag.
    go test -tags="${BUILD_TAGS}" -timeout 30m $race_flag "$@" 2>&1 | tee "$OUTPUT_FILE"

    if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
        FAILED=""
        return
    fi

    # "go test" reports a failed package as "FAIL<tab>package<tab>duration" (or "[build failed]").
    FAILED=$(grep --extended-regexp "^FAIL[[:space:]]+github.com/grafana/mimir/" "$OUTPUT_FILE" | awk '{print $2}' | sort -u | xargs)
    if [[ -z "$FAILED" ]]; then
        FAILED="$*"
    fi
}

for RACE_FLAG in "-race" ""; do
    if [[ "$RACE_FLAG" == "-race" ]]; then
        PACKAGES=$(echo "$RACE_PACKAGES" | xargs)
    else
        PACKAGES=$(echo "$NO_RACE_PACKAGES" | xargs)
    fi

    [[ -z "$PACKAGES" ]] && continue

    for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
        if [[ $ATTEMPT -gt 1 ]]; then
            echo
            echo "Retrying failed packages: $PACKAGES"
            echo
        fi

        # shellcheck disable=SC2086 # we *want* word splitting of PACKAGES.
        run_tests "$RACE_FLAG" $PACKAGES
        PACKAGES=$FAILED

        [[ -z "$PACKAGES" ]] && break
    done

    if [[ -n "$PACKAGES" ]]; then
        EXIT_CODE=1
        FAILED_PACKAGES="${FAILED_PACKAGES} ${PACKAGES}"
    fi
done

# Store in GitHub environment variable if any packages failed.
FAILED_PACKAGES=$(echo "$FAILED_PACKAGES" | xargs)
if [[ -n "$FAILED_PACKAGES" ]]; then
    echo "FAILED_PACKAGES=${FAILED_PACKAGES}" >> "$GITHUB_ENV"
fi

exit $EXIT_CODE
