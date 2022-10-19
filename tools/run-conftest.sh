#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit
set -o pipefail

DO_DEPENDENCY_UPDATE=0
GIT_REPO_ROOT=$(git rev-parse --show-toplevel || echo -n '/')
POLICIES_PATH=$GIT_REPO_ROOT/operations/helm/policies

while [[ $# -gt 0 ]]; do
  case "$1" in
  --policies-path)
    POLICIES_PATH="$2"
    shift # skip --policies-path
    shift # skip policies-path value
    ;;
  --temp-dir)
    TEMP_DIR="$2"
    shift # skip --temp-dir
    shift # skip temp-dir value
    ;;
  --do-dependency-update)
    DO_DEPENDENCY_UPDATE=1
    shift # skip --do-dependency-update
    ;;
  *)
    break
    ;;
  esac
done

CHART_PATH=$GIT_REPO_ROOT/operations/helm/charts/mimir-distributed
VALUES_FILES_PATH=$GIT_REPO_ROOT/operations/helm/charts/mimir-distributed/ci

if [[ -z "$TEMP_DIR" ]]; then
  TEMP_DIR=$(mktemp -d)
fi

if (( $DO_DEPENDENCY_UPDATE )); then
  helm dependency update $CHART_PATH >/dev/null
fi

for file in $(find $VALUES_FILES_PATH -name '*.yaml'); do
  echo -n "Testing with values file $file"
  output_dir=$TEMP_DIR/$(basename "$file")/
  helm template test $CHART_PATH -f $file --output-dir $output_dir --namespace citestns >/dev/null
  conftest test $output_dir -p $POLICIES_PATH --combine --ignore '.*/mimir-distributed/charts/.*'
  echo ""
done
