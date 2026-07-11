#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DASHBOARDS_DIR="${SCRIPT_DIR}"/test-node-disk-device-filter/dashboards

EXPRESSIONS=$(jq -r '.. | objects | .expr? // empty' "${DASHBOARDS_DIR}"/*.json)

if ! grep -Fq 'device!~".*vda.*"' <<< "${EXPRESSIONS}"; then
  echo "Generated dashboards do not use the configured node disk device exclusion regex."
  exit 1
fi

if grep -Fq 'device!~".*sda.*"' <<< "${EXPRESSIONS}"; then
  echo "Generated dashboards still use the default node disk device exclusion regex."
  exit 1
fi
