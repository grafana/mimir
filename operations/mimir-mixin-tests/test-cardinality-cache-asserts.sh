#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
QUERIES_DASHBOARD="${SCRIPT_DIR}"/test-cardinality-cache/dashboards/mimir-queries.json
DEFAULT_QUERIES_DASHBOARD="${SCRIPT_DIR}"/../mimir-mixin-compiled/dashboards/mimir-queries.json
ROW="Query-frontend – memcached-cardinality"
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

echo "Checking ${QUERIES_DASHBOARD}"

ROW_INDEX=$(jq --arg row "${ROW}" '[.rows[].title] | index($row)' "${QUERIES_DASHBOARD}")
PREV_ROW_INDEX=$(jq '[.rows[].title] | index("Query-frontend – query splitting and results cache")' "${QUERIES_DASHBOARD}")
if [ "${ROW_INDEX}" = "null" ]; then
  assert_failed "The dashboard at ${QUERIES_DASHBOARD} does not contain the row '${ROW}'"
elif [ "${PREV_ROW_INDEX}" = "null" ] || [ "${ROW_INDEX}" -ne $((PREV_ROW_INDEX + 1)) ]; then
  assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} is not directly after the row 'Query-frontend – query splitting and results cache'"
fi

for PANEL in "Miss ratio" "Evictions / sec" "Replicas" "Pod cycles" "Items per pod" "Memory utilization per pod" "Max memory utilization"; do
  MATCHES=$(jq -r --arg row "${ROW}" --arg panel "${PANEL}" '.rows[] | select(.title == $row) | .panels[] | select(.title == $panel) | .title' "${QUERIES_DASHBOARD}")
  if [ -z "$MATCHES" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} does not contain a '${PANEL}' panel"
  fi
done

# The memcached exporter metrics must select the memcached-cardinality job only.
for METRIC in memcached_commands_total memcached_items_evicted_total memcached_current_items memcached_current_bytes memcached_limit_bytes; do
  MATCHES=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .panels[].targets[].expr' "${QUERIES_DASHBOARD}" | grep -F "${METRIC}{" || true)
  if [ -z "$MATCHES" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} contains no query on ${METRIC}"
  fi
  UNSCOPED=$(echo "$MATCHES" | grep -vF 'job=~"($namespace)/((memcached-cardinality))"' || true)
  if [ -n "$UNSCOPED" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} contains ${METRIC} queries not scoped to the memcached-cardinality job:\n$UNSCOPED"
  fi
done

echo "Checking ${DEFAULT_QUERIES_DASHBOARD}"

# The row is hidden unless cache_cardinality_enabled is set.
MATCHES=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .title' "${DEFAULT_QUERIES_DASHBOARD}")
if [ -n "$MATCHES" ]; then
  assert_failed "The dashboard at ${DEFAULT_QUERIES_DASHBOARD} unexpectedly contains the row '${ROW}'"
fi

if [ $FAILED -ne 0 ]; then
  exit 1
fi
