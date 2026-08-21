#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TEST_DIR="${SCRIPT_DIR}"/test-compartments
ALERTS_FILE="${TEST_DIR}"/alerts.yaml
RULES_FILE="${TEST_DIR}"/rules.yaml
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

assert_absent() {
  FILEPATH=$1
  NEEDLE=$2
  MSG=$3

  MATCHES=$(grep -F -- "${NEEDLE}" "${FILEPATH}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "${MSG}\nFound in $(basename "${FILEPATH}"):\n${MATCHES}"
  fi
}

assert_matches() {
  FILEPATH=$1
  PATTERN=$2
  MSG=$3

  if ! grep -qE -- "${PATTERN}" "${FILEPATH}"; then
    assert_failed "${MSG}\nNo match for '${PATTERN}' in $(basename "${FILEPATH}")"
  fi
}

echo "Checking ${ALERTS_FILE}"

# See stripDashboardVars in alerts/alerts-utils.libsonnet.
assert_absent "${ALERTS_FILE}" '$read_compartment' \
  "Alerts must not reference the \$read_compartment dashboard variable: it is not interpolated in rules, so the matcher would never match."

# The ingester ring ("ingester") is intentionally not in this list: it is shared by all compartments.
for RING_NAME in "ingester-partitions" "compactor" "store-gateway"; do
  for FILEPATH in "${ALERTS_FILE}" "${RULES_FILE}"; do
    assert_absent "${FILEPATH}" "name=\"${RING_NAME}\"" \
      "Ring \"${RING_NAME}\" is suffixed with \"-rc-<id>\" when compartments are enabled, so an equality matcher on the bare name never matches. Use a matcher that accepts the suffix."
  done
done

for ALERT in "MimirBucketIndexNotUpdated" "MimirCompactorSchedulerNotCompletingJobs" "MimirCompactorSchedulerRepeatedJobFailure" "MimirIngesterInstanceHasNoTenants"; do
  EXPR=$(ALERT="${ALERT}" yq eval '.groups[].rules[] | select(.alert == env(ALERT)) | .expr' "${ALERTS_FILE}")

  if [ -z "${EXPR}" ]; then
    assert_failed "Alert ${ALERT} was not found in $(basename "${ALERTS_FILE}"): this assertion is checking nothing."
  elif ! echo "${EXPR}" | grep -q -F -- 'read_compartment'; then
    assert_failed "Alert ${ALERT} does not group by read_compartment, so a healthy compartment can mask a failing one."
  fi
done

PARTITION_ALERT_EXPR=$(yq eval '.groups[].rules[] | select(.alert == "MimirFewerIngestersConsumingThanActivePartitions") | .expr' "${ALERTS_FILE}")
for METRIC in "cortex_partition_ring_partitions" "cortex_ingest_storage_reader_last_consumed_offset"; do
  if ! echo "${PARTITION_ALERT_EXPR}" | grep -F -- "${METRIC}" | grep -q -F -- 'read_compartment'; then
    assert_failed "MimirFewerIngestersConsumingThanActivePartitions does not derive a read_compartment label from ${METRIC}.\nPartition IDs collide across compartments, so both sides must be compared per compartment."
  fi
done

for DEPLOYMENT in single-zone multi-zone; do
  EXPR=$(yq eval ".groups[].rules[] | select(.alert == \"MimirIngesterTSDBWALCorrupted\" and .labels.deployment == \"${DEPLOYMENT}\") | .expr" "${ALERTS_FILE}")
  if echo "${EXPR}" | grep -q -F -- ', job)' ||
    ! echo "${EXPR}" | grep -q -F -- '"read_compartment"' ||
    ! echo "${EXPR}" | grep -q -F -- 'count by (cluster, namespace, read_compartment)' ||
    ! echo "${EXPR}" | grep -q -F -- 'group by (cluster, namespace, read_compartment, zone)'; then
    assert_failed "MimirIngesterTSDBWALCorrupted (${DEPLOYMENT}) does not count zones per read compartment."
  fi
done

if [ $FAILED -ne 0 ]; then
  exit 1
fi
