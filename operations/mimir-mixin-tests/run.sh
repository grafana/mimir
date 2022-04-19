#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
MIXIN_DIR="${SCRIPT_DIR}/../mimir-mixin"
MIXIN_TEST_FILEPATH="${MIXIN_DIR}/mixin-test.libsonnet"

# Cleanup temporary copy of test file when this script exits.
trap "rm -f ${MIXIN_TEST_FILEPATH}" EXIT

TESTS=$(ls -1 "${SCRIPT_DIR}"/test-*.libsonnet)

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.libsonnet' "$FILEPATH")
  TEST_DIR="${SCRIPT_DIR}/${TEST_NAME}"

  # Begin with a clean output dir.
  rm -rf "${TEST_DIR}" && mkdir "${TEST_DIR}"

  # Temporarily copy the test file to the mixin directory. This file is deleted once
  # this script exits.
  cp "${SCRIPT_DIR}/${TEST_NAME}.libsonnet" "${MIXIN_TEST_FILEPATH}"

  mixtool generate all \
    --output-alerts "${TEST_DIR}/alerts.yaml" \
    --output-rules "${TEST_DIR}/rules.yaml" \
    --directory "${TEST_DIR}/dashboards" \
    "${MIXIN_TEST_FILEPATH}"

  # Run assertions.
  "${SCRIPT_DIR}"/"${TEST_NAME}-asserts.sh"
done
