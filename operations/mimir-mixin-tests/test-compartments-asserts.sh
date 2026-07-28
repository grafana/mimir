#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TEST_DIR="${SCRIPT_DIR}"/test-compartments
ALERTS_FILE="${TEST_DIR}"/alerts.yaml
RULES_FILE="${TEST_DIR}"/rules.yaml
ROLLOUT_DASHBOARD="${TEST_DIR}"/dashboards/mimir-rollout-progress.json
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

# Zero matches is a valid answer, so grep's exit code is swallowed: with "set -o pipefail" it would abort.
count_occurrences() {
  { grep -o -F -- "$2" "$1" || true; } | wc -l | tr -d ' '
}

echo "Checking ${ALERTS_FILE}"

# See stripDashboardVars in alerts/alerts-utils.libsonnet.
assert_absent "${ALERTS_FILE}" '$read_compartment' \
  "Alerts must not reference the \$read_compartment dashboard variable: it is not interpolated in rules, so the matcher would never match."

# The ingester ring ("ingester") is intentionally not in this list: it is shared by all compartments.
for RING_NAME in "ingester-partitions" "compactor" "store-gateway-replicas"; do
  for FILEPATH in "${ALERTS_FILE}" "${RULES_FILE}"; do
    assert_absent "${FILEPATH}" "name=\"${RING_NAME}\"" \
      "Ring \"${RING_NAME}\" is suffixed with \"-rc-<id>\" when compartments are enabled, so an equality matcher on the bare name never matches. Use a matcher that accepts the suffix."
  done
done

# The compartment-aware regex contains the zone-only one as a substring, so count occurrences instead of
# asserting the zone-only one is absent, which would keep matching once every call site is fixed.
ZONE_ONLY_REGEX='(.*?)(?:-zone-[a-z])?'
COMPARTMENT_AWARE_REGEX='(.*?)(?:-zone-[a-z])?((?:-rc|-wc)-[0-9]+)?'
for FILEPATH in "${ALERTS_FILE}" "${RULES_FILE}" "${ROLLOUT_DASHBOARD}"; do
  TOTAL=$(count_occurrences "${FILEPATH}" "${ZONE_ONLY_REGEX}")
  COMPARTMENT_AWARE=$(count_occurrences "${FILEPATH}" "${COMPARTMENT_AWARE_REGEX}")

  if [ "${TOTAL}" != "${COMPARTMENT_AWARE}" ]; then
    assert_failed "$(basename "${FILEPATH}") groups workloads with a zone-only regex in $((TOTAL - COMPARTMENT_AWARE)) of ${TOTAL} places.\nThat leaves the \"-rc-<id>\"/\"-wc-<id>\" suffix in place, so zones of the same compartment are no longer grouped together. Use the compartment-aware workload group regex."
  fi
done

for ALERT in "MimirBucketIndexNotUpdated" "MimirCompactorSchedulerNotCompletingJobs" "MimirCompactorSchedulerRepeatedJobFailure"; do
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

if [ $FAILED -ne 0 ]; then
  exit 1
fi
