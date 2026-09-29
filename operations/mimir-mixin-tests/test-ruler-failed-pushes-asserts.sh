#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ALERTS_FILE="${SCRIPT_DIR}/test-ruler-failed-pushes/alerts.yaml"

# Check the rendered rule, not just the source Jsonnet: a missing or mismatched
# label reason makes server-side push failures invisible to this alert.
EXPR=$(yq eval '.groups[].rules[] | select(.alert == "MimirRulerTooManyFailedPushes") | .expr' "${ALERTS_FILE}")
if [[ -z "${EXPR}" ]]; then
  echo "MimirRulerTooManyFailedPushes was not rendered" >&2
  exit 1
fi
if [[ "${EXPR}" != *'cortex_ruler_write_requests_failed_total{reason=~"(server_error|error|^$)"}'* ]]; then
  echo "Ruler push alert must include server errors and both legacy reasons, but exclude client errors" >&2
  exit 1
fi
