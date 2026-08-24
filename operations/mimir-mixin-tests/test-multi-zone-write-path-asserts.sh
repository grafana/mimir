#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
WRITES_DASHBOARD="${SCRIPT_DIR}"/test-multi-zone-write-path/dashboards/mimir-writes.json
RULER_DASHBOARD="${SCRIPT_DIR}"/test-multi-zone-write-path/dashboards/mimir-ruler.json
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

# The zones configured via multi_zone_write_path_zones (the test uses the default value).
ZONES="a b c"

# The matchers excluding the per-zone jobs from the aggregate series, built from the default
# multi_zone_job_name_formats and multi_zone_write_path_zones.
DISTRIBUTOR_EXCLUSION='job!~"($namespace)/((distributor-zone-a.*|distributor-zone-b.*|distributor-zone-c.*))"'
GATEWAY_EXCLUSION='job!~"($namespace)/((cortex-gw.*-zone-a|cortex-gw.*-zone-b|cortex-gw.*-zone-c))"'

echo "Checking ${WRITES_DASHBOARD}"

# Every row showing the traffic of a multi-zone write path component must contain a
# "Latency per zone" panel next to the aggregate "Latency" panel.
for ROW in "Gateway - all write requests" "Gateway - Prometheus remote write requests" "Gateway - OTLP write requests" "Gateway - Influx write requests" "Distributor"; do
  MATCHES=$(jq -r --arg row "${ROW}" '.rows[] | select(.title == $row) | .panels[] | select(.title == "Latency per zone") | .title' "${WRITES_DASHBOARD}")
  if [ -z "$MATCHES" ]; then
    assert_failed "The row '${ROW}' of the dashboard at ${WRITES_DASHBOARD} does not contain a 'Latency per zone' panel"
  fi
done

MATCHES=$(jq -r '.rows[] | select(.title == "Distributor (ingest storage)") | .panels[] | select(.title == "Kafka produced records latency per zone") | .title' "${WRITES_DASHBOARD}")
if [ -z "$MATCHES" ]; then
  assert_failed "The row 'Distributor (ingest storage)' of the dashboard at ${WRITES_DASHBOARD} does not contain a 'Kafka produced records latency per zone' panel"
fi

# The panels showing per-zone series must have them for every configured zone.
for ZONE in ${ZONES}; do
  for LEGEND in "{{status}} zone-${ZONE}" "rejected zone-${ZONE}" "99th percentile zone-${ZONE}" "success zone-${ZONE}" "100th percentile zone-${ZONE}"; do
    MATCHES=$(jq -r --arg legend "${LEGEND}" '.rows[].panels[].targets[]? | select(.legendFormat == $legend) | .legendFormat' "${WRITES_DASHBOARD}")
    if [ -z "$MATCHES" ]; then
      assert_failed "The dashboard at ${WRITES_DASHBOARD} contains no series with legend '${LEGEND}'"
    fi
  done
done

# The aggregate series of panels which also show per-zone series must exclude the per-zone jobs,
# otherwise the same traffic would be plotted twice on those panels.
for EXCLUSION in "${DISTRIBUTOR_EXCLUSION}" "${GATEWAY_EXCLUSION}"; do
  MATCHES=$(jq -r '.. | select(.expr?).expr' "${WRITES_DASHBOARD}" | grep -F "${EXCLUSION}" || true)
  if [ -z "$MATCHES" ]; then
    assert_failed "The dashboard at ${WRITES_DASHBOARD} contains no aggregate queries excluding the per-zone jobs with ${EXCLUSION}"
  fi
done

echo "Checking ${RULER_DASHBOARD}"

# The per-zone panels are specific to the Writes dashboard: the Ruler dashboard, which shares the
# Kafka produced records panels, must not show any per-zone panel and must not exclude the
# per-zone jobs from its series.
MATCHES=$(jq -r '.rows[].panels[] | select(.title | test("per zone")) | .title' "${RULER_DASHBOARD}")
if [ -n "$MATCHES" ]; then
  assert_failed "The dashboard at ${RULER_DASHBOARD} unexpectedly contains per-zone panels:\n$MATCHES"
fi

for EXCLUSION in "${DISTRIBUTOR_EXCLUSION}" "${GATEWAY_EXCLUSION}"; do
  MATCHES=$(jq -r '.. | select(.expr?).expr' "${RULER_DASHBOARD}" | grep -F "${EXCLUSION}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "The dashboard at ${RULER_DASHBOARD} unexpectedly contains queries excluding the per-zone jobs:\n$MATCHES"
  fi
done

if [ $FAILED -ne 0 ]; then
  exit 1
fi
