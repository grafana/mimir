#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

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

  echo "Removing mutable config checksum and helm chart version for clarity"
  find "${OUTPUT_DIR}/$(basename ${CHART_PATH})/templates" -type f -print0 | xargs -0 sed -E -i -- "/^[ ]+(checksum\/config|(helm.sh\/)?chart):/d"
done
