#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

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
# If you change the build tags or CLI flags, update warmup-build-cache-integration-tests in the Makefile too.
ALL_TESTS=$(go test -tags=requires_docker,stringlabels -list 'Test.*' "${INTEGRATION_DIR}/..." | grep -E '^Test.*' | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL="$TOTAL" -v INDEX="$INDEX" 'NR % TOTAL == INDEX')

echo "This group will run the following tests:"
echo "$GROUP_TESTS"
echo ""

# GORACE is only applied when running with race-enabled Mimir.
# This setting tells Go runtime to exit the binary when data race is detected. This increases the chance
# that integration tests will fail on data races.
export MIMIR_ENV_VARS_JSON='{"GORACE": "halt_on_error=1"}'

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

        go test -tags=requires_docker,stringlabels -timeout 2400s -v -count=1 -run "^${TEST}$" "${INTEGRATION_DIR}/..." 2>&1 | tee /tmp/test-integration-output.log
        TEST_EXIT_CODE=${PIPESTATUS[0]}

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
