#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit
set -o pipefail

GIT_REPO_ROOT=$(git rev-parse --show-toplevel || echo -n '/')
POLICIES_PATH="$GIT_REPO_ROOT/operations/helm/policies"
GENERATED_DIR="$GIT_REPO_ROOT/operations/helm/tests/intermediate"

while [[ $# -gt 0 ]]; do
  case "$1" in
  --policies-path)
    POLICIES_PATH="$2"
    shift # skip --policies-path
    shift # skip policies-path value
    ;;
  *)
    break
    ;;
  esac
done

if ! [ -d "$GENERATED_DIR" ] ; then
  echo "The generated yaml templates in $GENERATED_DIR are not present, use 'make check-helm-tests' instead of calling this script directly"
  exit 1
fi

CHART_PATH=$GIT_REPO_ROOT/operations/helm/charts/mimir-distributed
VALUES_FILES_PATH=$GIT_REPO_ROOT/operations/helm/charts/mimir-distributed/ci

for FILEPATH in $(find $VALUES_FILES_PATH -name '*.yaml'); do
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  OUTPUT_DIR="${GENERATED_DIR}/${TEST_NAME}-generated"
  echo "Testing with values file $FILEPATH"
  echo "and the generated YAML templates in $OUTPUT_DIR"
  conftest test "$OUTPUT_DIR" -p "$POLICIES_PATH" --combine --ignore '.*/mimir-distributed/charts/.*'
  echo ""
done
