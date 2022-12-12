#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# use a normal sed on macOS if available
SED=$(which gsed || which sed)

CHART_PATH=operations/helm/charts/mimir-distributed

# Start from a clean slate
rm -rf operations/helm/tests/*-generated
rm -rf operations/helm/tests/intermediate
mkdir -p operations/helm/tests/intermediate

# Find testcases
TESTS=$(find ${CHART_PATH}/ci -name '*values.yaml')

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  INTERMEDIATE_OUTPUT_DIR="operations/helm/tests/intermediate/${TEST_NAME}-generated"
  OUTPUT_DIR="operations/helm/tests/${TEST_NAME}-generated"

  echo "Templating $TEST_NAME"
  set -x
  helm template "${TEST_NAME}" ${CHART_PATH} -f "${FILEPATH}" --output-dir "${INTERMEDIATE_OUTPUT_DIR}" --namespace citestns
  set +x

  echo "Removing mutable config checksum, helm chart, application, image tag version for clarity"
  cp -r "${INTERMEDIATE_OUTPUT_DIR}" "${OUTPUT_DIR}"
  find "${OUTPUT_DIR}/$(basename ${CHART_PATH})/templates" -type f -print0 | xargs -0 "${SED}" -E -i -- "/^\s+(checksum\/config|(helm.sh\/)?chart|app.kubernetes.io\/version|image: \"grafana\/(mimir|mimir-continuous-test|enterprise-metrics)):/d"

done
