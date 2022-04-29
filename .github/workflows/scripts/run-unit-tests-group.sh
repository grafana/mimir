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

echo "This group will run the following tests:"
echo "$GROUP_TESTS"
echo ""

# shellcheck disable=SC2086 # we *want* word splitting of GROUP_TESTS.
exec go test -tags=netgo -timeout 30m -race -count 1 ${GROUP_TESTS}
