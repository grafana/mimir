#!/bin/bash

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
ALL_TESTS=$(go test -tags=requires_docker -list 'Test.*' "${INTEGRATION_DIR}/..." | grep -E '^Test.*' | sort)

# Filter tests by the requested group.
GROUP_TESTS=$(echo "$ALL_TESTS" | awk -v TOTAL=$TOTAL -v INDEX=$INDEX 'NR % TOTAL == INDEX')

# Build the regex used to run this group's tests.
REGEX="^("
for TEST in $GROUP_TESTS; do
  REGEX="${REGEX}|${TEST}"
done
REGEX="${REGEX})$"

exec go test -tags=requires_docker -timeout 2400s -v -count=1 -run "${REGEX}" "${INTEGRATION_DIR}/..."
