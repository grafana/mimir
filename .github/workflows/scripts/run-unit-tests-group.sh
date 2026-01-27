#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

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

# List all tests.
ALL_TESTS=$(go list "${MIMIR_DIR}/..." | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL=$TOTAL -v INDEX=$INDEX 'NR % TOTAL == INDEX')

# The tests in the MQE benchmarks package load an enormous amount of data, which causes the
# race detector to consume a large amount of memory and run incredibly slowly on CI.
# The same code is tested by other unit tests which run with the race detector enabled, so
# don't bother running the benchmark tests with the race detector enabled.
SKIP_RACE_DETECTOR_PATTERN="^github.com/grafana/mimir/pkg/streamingpromql/benchmarks$"

TESTS_TO_RUN_WITH_RACE_DETECTOR=$(echo "$GROUP_TESTS" | grep -v -e "$SKIP_RACE_DETECTOR_PATTERN")
TESTS_TO_RUN_WITHOUT_RACE_DETECTOR=$(echo "$GROUP_TESTS" | grep -e "$SKIP_RACE_DETECTOR_PATTERN")

echo "This group will run the following tests:"
echo "$GROUP_TESTS"
echo

if [[ -n "$TESTS_TO_RUN_WITH_RACE_DETECTOR" ]]; then
    echo "These tests will run with the race detector enabled:"
    echo "$TESTS_TO_RUN_WITH_RACE_DETECTOR"
    echo

    # shellcheck disable=SC2086 # we *want* word splitting of TESTS_TO_RUN_WITH_RACE_DETECTOR.
    go test -tags=netgo,stringlabels -timeout 30m -race ${TESTS_TO_RUN_WITH_RACE_DETECTOR} 2>&1 | tee /tmp/test-output.log
    RACE_ENABLED_EXIT_CODE=${PIPESTATUS[0]}
    echo
else
    RACE_ENABLED_EXIT_CODE=0
fi

if [[ -n "$TESTS_TO_RUN_WITHOUT_RACE_DETECTOR" ]]; then
    echo "These tests will run with the race detector disabled (if any):"
    echo "$TESTS_TO_RUN_WITHOUT_RACE_DETECTOR"
    echo

    # shellcheck disable=SC2086 # we *want* word splitting of TESTS_TO_RUN_WITHOUT_RACE_DETECTOR.
    go test -tags=netgo,stringlabels -timeout 30m ${TESTS_TO_RUN_WITHOUT_RACE_DETECTOR} 2>&1 | tee -a /tmp/test-output.log
    RACE_DISABLED_EXIT_CODE=${PIPESTATUS[0]}
    echo
else
    RACE_DISABLED_EXIT_CODE=0
fi

# Extract all failed packages
FAILED_PACKAGES=$(grep "FAIL\s*github.com/grafana/mimir/.*" /tmp/test-output.log | awk '{print $2}' | sort -u | tr '\n' ' ')

# Store in GitHub environment variable if any packages failed
if [[ -n "$FAILED_PACKAGES" ]]; then
    echo "FAILED_PACKAGES=${FAILED_PACKAGES}" >> "$GITHUB_ENV"
fi

if [[ $RACE_ENABLED_EXIT_CODE -ne 0 ]]; then
    exit $RACE_ENABLED_EXIT_CODE
fi

exit $RACE_DISABLED_EXIT_CODE
