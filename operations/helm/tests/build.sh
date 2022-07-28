#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# use a normal sed on macOS if available
SED=$(which gsed || which sed)

CHART_PATH=operations/helm/charts/mimir-distributed

# Start from a clean slate
rm -rf operations/helm/tests/*-generated

# Install chart dependencies for this branch
helm dependency update "$CHART_PATH"

# Locally render the chart for every test file
TESTS=$(ls -1 ${CHART_PATH}/ci/*values.yaml)

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  OUTPUT_DIR="operations/helm/tests/${TEST_NAME}-generated"

  echo "Templating $TEST_NAME"
  helm template "${TEST_NAME}" ${CHART_PATH} -f "${FILEPATH}" --output-dir "${OUTPUT_DIR}" --namespace citestns

  echo "Removing mutable config checksum, helm chart, image tag version for clarity"
  find "${OUTPUT_DIR}/$(basename ${CHART_PATH})/templates" -type f -print0 | xargs -0 "${SED}" -E -i -- "/^\s+(checksum\/config|(helm.sh\/)?chart|image: \"grafana\/(mimir|mimir-continuous-test|enterprise-metrics)):/d"

done
