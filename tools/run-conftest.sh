#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit
set -o pipefail

GIT_REPO_ROOT=$(git rev-parse --show-toplevel || echo -n '/')
CHART_PATH="$GIT_REPO_ROOT/operations/helm/charts/mimir-distributed"
POLICIES_PATH="$GIT_REPO_ROOT/operations/helm/policies"
MANIFESTS_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --chart-path)
    CHART_PATH="$GIT_REPO_ROOT/$2"
    shift # skip param name
    shift # skip param value
    ;;
  --policies-path)
    POLICIES_PATH="$GIT_REPO_ROOT/$2"
    shift # skip --policies-path
    shift # skip policies-path value
    ;;
  --manifests-path)
    MANIFESTS_PATH="$GIT_REPO_ROOT/$2"
    shift # skip param name
    shift # skip param value
    ;;
  *)
    break
    ;;
  esac
done

if ! [ -n "$MANIFESTS_PATH" -a -d "$MANIFESTS_PATH" ] ; then
  echo "The generated yaml templates in $MANIFESTS_PATH are not present, use 'make check-helm-tests' instead of calling this script directly"
  exit 1
fi


VALUES_FILES_PATH=$CHART_PATH/ci

for FILEPATH in $(find $VALUES_FILES_PATH -name '*.yaml'); do
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  MANIFEST_DIR="${MANIFESTS_PATH}/${TEST_NAME}-generated"
  echo "Testing with values file $FILEPATH"
  echo "and the generated YAML templates in $MANIFEST_DIR"
  conftest test "$MANIFEST_DIR/"$(basename "$CHART_PATH")"/templates" -p "$POLICIES_PATH" --combine
  echo ""
done
