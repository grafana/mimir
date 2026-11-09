#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DASHBOARDS_DIR="${SCRIPT_DIR}"/test-keda-error-metrics/dashboards

# Check every rendered scaler-error query, including both the shared autoscaling
# panels and the querier-specific panel. Prefer the modern counter without adding
# the legacy counter, which would double count errors when both are exported.
for DASHBOARD in mimir-reads.json mimir-remote-ruler-reads.json mimir-writes.json; do
  echo "Checking ${DASHBOARDS_DIR}/${DASHBOARD}"
  jq -e '
    [.. | objects | .expr? // empty
      | select(test("keda_scaler_(detail_errors_total|errors)\\["))] as $queries
    | ($queries | length > 0)
      and all($queries[];
        contains("rate(keda_scaler_detail_errors_total[$__rate_interval]) or rate(keda_scaler_errors[$__rate_interval])")
        and contains("\"namespace\", \"$1\", \"exported_namespace\", \"(.+)\"")
        and contains("on(cluster, namespace, metric, scaledObject) group_left")
      )
  ' "${DASHBOARDS_DIR}/${DASHBOARD}" > /dev/null || {
    echo "KEDA scaler-error queries must prefer the modern counter, fall back to the legacy counter, and preserve the namespace/HPA join." >&2
    exit 1
  }
done
