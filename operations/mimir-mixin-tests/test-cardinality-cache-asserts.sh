#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# No compiled dashboard enables cache_cardinality_enabled, so this test is the only place where
# the memcached-cardinality row of the Queries dashboard is rendered at all.

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

# Enabling the flag must show the row. Checked on its own so that a missing row is reported once,
# instead of as failures of every check below.
MATCHES=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .title' "${QUERIES_DASHBOARD}")
if [ -z "$MATCHES" ]; then
  assert_failed "The dashboard at ${QUERIES_DASHBOARD} does not contain the row '${ROW}'"
fi

# Every memcached cluster exports the memcached exporter metrics, so a query not scoped to the
# memcached-cardinality job would mix in the other caches. Also fails if a panel querying one of
# these metrics is missing. Each occurrence of a metric is checked on its own, as "<metric>{...}"
# or the bare metric name followed by the next character, so that a selector dropped next to a
# scoped one on the same line is caught too.
for METRIC in memcached_commands_total memcached_items_evicted_total memcached_current_items memcached_current_bytes memcached_limit_bytes; do
  OCCURRENCES=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .panels[].targets[].expr' "${QUERIES_DASHBOARD}" | grep -oE "${METRIC}(\{[^}]*\}|[^a-z_{]|$)" || true)
  if [ -z "$OCCURRENCES" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} contains no query on ${METRIC}"
  fi
  UNSCOPED=$(echo "$OCCURRENCES" | grep -vF 'job=~"($namespace)/((memcached-cardinality))"' || true)
  if [ -n "$UNSCOPED" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} contains ${METRIC} selectors not scoped to the memcached-cardinality job:\n$UNSCOPED"
  fi
done

# StatefulSet pod names repeat across clusters and namespaces. If either aggregation of the
# Pod cycles query drops these labels, unchanged pods with the same name in different selected
# clusters or namespaces have different UIDs and are reported as replaced.
POD_CYCLES_EXPR=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .panels[] | select(.title == "Pod cycles") | .targets[].expr' "${QUERIES_DASHBOARD}")
for GROUPING in "count by (cluster, namespace, pod, uid)" "count by (cluster, namespace, pod)"; do
  if ! echo "$POD_CYCLES_EXPR" | grep -qF "${GROUPING}"; then
    assert_failed "The 'Pod cycles' panel of the row '${ROW}' of the dashboard at ${QUERIES_DASHBOARD} does not aggregate with '${GROUPING}':\n$POD_CYCLES_EXPR"
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
