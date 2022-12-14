#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# use a normal sed on macOS if available
SED=$(which gsed || which sed)

CHART_PATH="operations/helm/charts/mimir-distributed"
INTERMEDIATE_PATH=""
OUTPUT_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --chart-path)
    CHART_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  --output-path)
    OUTPUT_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  --intermediate-path)
    INTERMEDIATE_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  *)
    break
    ;;
  esac
done

if [ -z "$OUTPUT_PATH" ] ; then
  echo "Provide --output-path parameter for destination of generated and sanitized outputs"
  exit 1
fi

if [ -z "$INTERMEDIATE_PATH" ] ; then
  echo "Provide --manifests-path parameter for destination of raw manifests outputs"
  exit 1
fi

# Start from a clean slate
rm -rf "$INTERMEDIATE_PATH"
mkdir -p "$INTERMEDIATE_PATH"
rm -rf "$OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH"

# Find testcases
TESTS=$(find "${CHART_PATH}/ci" -name '*values.yaml')
CHART_NAME=$(basename "${CHART_PATH}")

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  INTERMEDIATE_OUTPUT_DIR="${INTERMEDIATE_PATH}/${TEST_NAME}-generated"
  OUTPUT_DIR="${OUTPUT_PATH}/${TEST_NAME}-generated"

  echo "Templating $TEST_NAME"
  set -x
  helm template "${TEST_NAME}" "${CHART_PATH}" -f "${FILEPATH}" --output-dir "${INTERMEDIATE_OUTPUT_DIR}" --namespace citestns
  set +x

  echo "Removing mutable config checksum, helm chart, application, image tag version for clarity"
  cp -r "${INTERMEDIATE_OUTPUT_DIR}" "${OUTPUT_DIR}"
  find "${OUTPUT_DIR}/${CHART_NAME}/templates" -type f -print0 | xargs -0 "${SED}" -E -i -- "/^\s+(checksum\/(alertmanager-fallback-)?config|(helm.sh\/)?chart|app.kubernetes.io\/version|image: \"grafana\/(mimir|mimir-continuous-test|enterprise-metrics)):/d"

done
