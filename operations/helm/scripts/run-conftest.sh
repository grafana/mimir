#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit
set -o pipefail

CHART_PATH="operations/helm/charts/mimir-distributed"
POLICIES_PATH="operations/helm/policies"
MANIFESTS_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --chart-path)
    CHART_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  --policies-path)
    POLICIES_PATH="$2"
    shift # skip --policies-path
    shift # skip policies-path value
    ;;
  --manifests-path)
    MANIFESTS_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  *)
    break
    ;;
  esac
done

if [ -z "$MANIFESTS_PATH" ] ; then
  echo "Provide path to manifests to test in --manifests-path"
  exit 1
fi

if ! [ -d "$MANIFESTS_PATH" ] ; then
  echo "The generated yaml templates in $MANIFESTS_PATH are not present, use 'make check-helm-tests' instead of calling this script directly"
  exit 1
fi

CHART_NAME=$(basename "${CHART_PATH}")

find "$CHART_PATH/ci" -name '*.yaml' | while read -r FILEPATH ; do
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  MANIFEST_DIR="${MANIFESTS_PATH}/${TEST_NAME}-generated"
  echo "Testing with values file $TEST_NAME with manifests in ${MANIFEST_DIR}"
  conftest test "$MANIFEST_DIR/$CHART_NAME/templates" -p "$POLICIES_PATH" --combine
  echo ""
done
