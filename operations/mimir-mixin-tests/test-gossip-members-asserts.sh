#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ALERTS_FILE="${SCRIPT_DIR}"/test-gossip-members/alerts.yaml
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

# Jobs whose pods join the memberlist cluster when the Mimir jsonnet gives them the gossip ring member label.
MEMBER_JOBS="
mimir/ingester-zone-a
mimir/query-frontend
mimir/query-scheduler
mimir/query-scheduler-zone-a
mimir/ruler-query-scheduler
mimir/ruler-query-scheduler-zone-b
mimir/ruler-query-frontend
mimir/ruler-query-frontend-zone-a
mimir/usage-tracker-zone-a
mimir/memberlist-bridge-zone-a
"

# Jobs whose pods do not join the memberlist cluster.
NON_MEMBER_JOBS="
mimir/memcached
mimir/memcached-frontend
mimir/rollout-operator
"

echo "Checking ${ALERTS_FILE}"

# The gossip member alerts compare the member count with the sum of up for the jobs that ring_members matches.
# A member job that the regex does not match lowers the expected count.
# A non-member job that the regex matches raises the expected count.
for ALERT in MimirGossipMembersTooHigh MimirGossipMembersTooLow; do
  EXPR=$(yq -r ".groups[].rules[] | select(.alert == \"${ALERT}\") | .expr" "${ALERTS_FILE}")
  REGEX=$(echo "${EXPR}" | grep -o 'up{job=~"[^"]*"' | sed -e 's/^up{job=~"//' -e 's/"$//' || true)

  if [ -z "${REGEX}" ]; then
    assert_failed "The alert ${ALERT} in ${ALERTS_FILE} has no up{job=~\"...\"} matcher"
    continue
  fi

  for JOB in ${MEMBER_JOBS}; do
    if [[ ! "${JOB}" =~ ^(${REGEX})$ ]]; then
      assert_failed "The alert ${ALERT} does not count the member job ${JOB}"
    fi
  done

  for JOB in ${NON_MEMBER_JOBS}; do
    if [[ "${JOB}" =~ ^(${REGEX})$ ]]; then
      assert_failed "The alert ${ALERT} counts the non-member job ${JOB}"
    fi
  done
done

if [ $FAILED -ne 0 ]; then
  exit 1
fi
