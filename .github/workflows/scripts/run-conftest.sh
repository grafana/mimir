#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

# This script can be executed in github actions or locally during development.
# To execute during development run `run-conftest.sh`

set -x
set -o errexit
set -o pipefail

DO_DEPENDENCY_UPDATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
  --chart-path)
    CHART_PATH="$2"
    shift # skip --chart-path
    shift # skip chart-path value
    ;;
  --values-files-path)
    VALUES_FILES_PATH="$2"
    shift # skip --values-files-path
    shift # skip values-files-path value
    ;;
  --temp-dir)
    TEMP_DIR="$2"
    shift # skip --temp-dir
    shift # skip temp-dir value
    ;;
  --policies-path)
    POLICIES_PATH="$2"
    shift # skip --policies-path
    shift # skip policies-path value
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

git_repo_root=$(git rev-parse --show-toplevel || echo -n '/')

if [[ -z "$CHART_PATH" ]]; then
  CHART_PATH=$git_repo_root/operations/helm/charts/mimir-distributed
fi

if [[ -z "$VALUES_FILES_PATH" ]]; then
  VALUES_FILES_PATH=$git_repo_root/operations/helm/charts/mimir-distributed/ci
fi

if [[ -z "$POLICIES_PATH" ]]; then
  POLICIES_PATH=$git_repo_root/operations/helm/policies
fi

if [[ -z "$TEMP_DIR" ]]; then
  TEMP_DIR=$(mktemp -d)
fi

if (( $DO_DEPENDENCY_UPDATE )); then
  helm dependency update $CHART_PATH
fi

for file in $(find $VALUES_FILES_PATH -name '*.yaml'); do
  output_dir=$TEMP_DIR/$(basename "$file")/
  helm template test $CHART_PATH -f $file --output-dir $output_dir --namespace citestns >/dev/null
  echo "Testing with values file $file"
  conftest test $output_dir -p $POLICIES_PATH --ignore '.*/mimir-distributed/charts/.*'
done
