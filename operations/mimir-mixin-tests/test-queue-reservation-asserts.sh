#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# No compiled dashboard enables queue_reservation_enabled, so this test is the only place where
# the queue reservation panels of the Queries dashboard are rendered at all.

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
QUERIES_DASHBOARD="${SCRIPT_DIR}"/test-queue-reservation/dashboards/mimir-queries.json
DEFAULT_QUERIES_DASHBOARD="${SCRIPT_DIR}"/../mimir-mixin-compiled/dashboards/mimir-queries.json
FRONTEND_ROW="Query-frontend"
SCHEDULER_ROW="Query-scheduler"
RESERVATIONS_ROW="Query-scheduler reservations"
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

panel_titles() {
  jq -r --arg row "$2" '.rows[] | select(.title == $row) | .panels[].title' "$1"
}

row_titles() {
  jq -r '.rows[].title' "$1"
}

echo "Checking ${QUERIES_DASHBOARD}"

if ! panel_titles "${QUERIES_DASHBOARD}" "${FRONTEND_ROW}" | grep -qxF "Reservation errors"; then
  assert_failed "The row '${FRONTEND_ROW}' of the dashboard at ${QUERIES_DASHBOARD} does not contain the panel 'Reservation errors'"
fi

# The reservations row comes directly after the Query-scheduler row.
NEXT_ROW=$(row_titles "${QUERIES_DASHBOARD}" | grep -xF -A1 "${SCHEDULER_ROW}" | tail -n 1)
if [ "${NEXT_ROW}" != "${RESERVATIONS_ROW}" ]; then
  assert_failed "The row after '${SCHEDULER_ROW}' of the dashboard at ${QUERIES_DASHBOARD} is '${NEXT_ROW}', expected '${RESERVATIONS_ROW}'"
fi

PANELS=$(panel_titles "${QUERIES_DASHBOARD}" "${RESERVATIONS_ROW}" | paste -sd, -)
if [ "${PANELS}" != "Opened,Closed,Rejected,Slots" ]; then
  assert_failed "The row '${RESERVATIONS_ROW}' of the dashboard at ${QUERIES_DASHBOARD} has the panels '${PANELS}', expected 'Opened,Closed,Rejected,Slots'"
fi

# The metric is exposed as classic and native histogram, so the Slots heatmap needs a query for each.
SLOTS_TARGETS=$(jq -c --arg row "${RESERVATIONS_ROW}" '[.rows[] | select(.title == $row) | .panels[] | select(.title == "Slots") | .targets[] | {format, classic: (.expr | test("_bucket\\{")), toggle: (.expr | test("vector\\(\\$latency_metrics\\) == -?1\\)$"))}]' "${QUERIES_DASHBOARD}")
if [ "${SLOTS_TARGETS}" != '[{"format":"heatmap","classic":true,"toggle":true},{"format":"time_series","classic":false,"toggle":true}]' ]; then
  assert_failed "The 'Slots' panel of the row '${RESERVATIONS_ROW}' of the dashboard at ${QUERIES_DASHBOARD} must have a classic heatmap query and a native query behind the latency_metrics toggle, got:\n${SLOTS_TARGETS}"
fi

# The "Read path" selector must switch every reservation panel between the main and the remote ruler read path.
for METRIC in cortex_query_frontend_queue_reservations_total cortex_query_scheduler_reservation_slots cortex_query_scheduler_reservations_closed_total cortex_query_scheduler_reservations_rejected_total; do
  OCCURRENCES=$(jq -r '.rows[].panels[].targets[]?.expr' "${QUERIES_DASHBOARD}" | grep -oE "${METRIC}[a-z_]*\{[^}]*\}" || true)
  if [ -z "$OCCURRENCES" ]; then
    assert_failed "The dashboard at ${QUERIES_DASHBOARD} contains no query on ${METRIC}"
  fi
  UNSCOPED=$(echo "$OCCURRENCES" | grep -vF '$read_path_matcher' || true)
  if [ -n "$UNSCOPED" ]; then
    assert_failed "The dashboard at ${QUERIES_DASHBOARD} contains ${METRIC} selectors without \$read_path_matcher:\n$UNSCOPED"
  fi
done

echo "Checking ${DEFAULT_QUERIES_DASHBOARD}"

# The panels are hidden unless queue_reservation_enabled is set.
if panel_titles "${DEFAULT_QUERIES_DASHBOARD}" "${FRONTEND_ROW}" | grep -qxF "Reservation errors"; then
  assert_failed "The dashboard at ${DEFAULT_QUERIES_DASHBOARD} unexpectedly contains the panel 'Reservation errors'"
fi

if row_titles "${DEFAULT_QUERIES_DASHBOARD}" | grep -qxF "${RESERVATIONS_ROW}"; then
  assert_failed "The dashboard at ${DEFAULT_QUERIES_DASHBOARD} unexpectedly contains the row '${RESERVATIONS_ROW}'"
fi

if [ $FAILED -ne 0 ]; then
  exit 1
fi
