#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
set -o pipefail

SCRIPT_DIR=$(cd `dirname $0` && pwd)
INTEGRATION_DIR=$(realpath "${SCRIPT_DIR}/../../../integration/")

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
    echo "No total provided."
    exit 1
fi

# List all tests.
ALL_TESTS=$(go test -tags=stringlabels -list 'Test.*' "${INTEGRATION_DIR}" | grep -E '^Test.*' | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" 'NR % TOTAL == INDEX')

if [[ -z "$GROUP_TESTS" ]]; then
    echo "ERROR: No tests found for group $INDEX of $TOTAL. This likely indicates a compilation error or misconfiguration."
    exit 1
fi

echo "This group will run the following tests:"
echo "$GROUP_TESTS"
echo ""

# GORACE is only applied when running with race-enabled Mimir.
# This setting tells Go runtime to exit the binary when data race is detected. This increases the chance
# that integration tests will fail on data races.
export MIMIR_ENV_VARS_JSON='{"GORACE": "halt_on_error=1"}'

# Compile the integration test binary once upfront. Running "go test" per test would repeat
# compilation/linking work on every invocation, adding significant overhead across many tests.
# If you change the build tags here, also update warmup-build-cache-integration-tests in the Makefile.
TEST_BINARY="${INTEGRATION_DIR}/integration.test"
go test -tags=stringlabels -c -o "$TEST_BINARY" "${INTEGRATION_DIR}"
if [[ $? -ne 0 ]]; then
    echo "ERROR: Failed to compile integration test binary."
    exit 1
fi

EXIT_CODE=0

# Run one test at a time so that a failure can be retried individually without re-running
# the entire group.
MAX_ATTEMPTS=2

for TEST in $GROUP_TESTS; do
    for ATTEMPT in $(seq 1 $MAX_ATTEMPTS); do
        if [[ $ATTEMPT -gt 1 ]]; then
            echo "Retrying failed test: $TEST"
            echo
        else
            echo "Running test: $TEST"
        fi

        "$TEST_BINARY" -test.timeout 2400s -test.v -test.count=1 -test.run "^${TEST}$"
        TEST_EXIT_CODE=$?

        if [[ $TEST_EXIT_CODE -eq 0 ]]; then
            break
        fi
    done

    if [[ $TEST_EXIT_CODE -ne 0 ]]; then
        EXIT_CODE=1
        echo "Test failed after $MAX_ATTEMPTS attempts: $TEST"
    fi
done

exit $EXIT_CODE
