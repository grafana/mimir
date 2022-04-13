#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DASHBOARDS_DIR="${SCRIPT_DIR}"/test-custom-labels/dashboards
DASHBOARDS_FILES=$(ls -1 "${DASHBOARDS_DIR}"/*.json)
ALERTS_FILE="${SCRIPT_DIR}"/test-custom-labels/alerts.yaml
RULES_FILE="${SCRIPT_DIR}"/test-custom-labels/rules.yaml
FAILED=0

assert_failed() {
  MSG=$1

  echo ""
  echo -e "$MSG"
  echo ""
  FAILED=1
}

# In this test we customize some labels. We expect to not find any reference to default labels.
FORBIDDEN_LABELS="cluster instance pod"

for LABEL in ${FORBIDDEN_LABELS}; do
  QUERY_REGEX="[^\$a-z_]${LABEL}[^a-z_]"
  ALERT_VARIABLE_REGEX="\\\$labels.${LABEL}"
  RULE_PREFIX_REGEX="(^|[^a-z])${LABEL}(\$|[^a-z])"

  # Check dashboards.
  for FILEPATH in ${DASHBOARDS_FILES}; do
    echo "Checking ${FILEPATH}"

    MATCHES=$(jq '.. | select(.expr?).expr' "${FILEPATH}" | grep -E "${QUERY_REGEX}" || true)
    if [ -n "$MATCHES" ]; then
      assert_failed "The dashboard at ${FILEPATH} contains unexpected references to '${LABEL}' label in some queries:\n$MATCHES"
    fi
  done

  # Check alerts.
  echo "Checking ${ALERTS_FILE}"
  MATCHES=$(yq eval '.. | select(.expr?).expr | sub("\n", " ")' "${ALERTS_FILE}" | grep -E "${QUERY_REGEX}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "The alerts at ${ALERTS_FILE} contains unexpected references to '${LABEL}' label in some queries:\n$MATCHES"
  fi

  MATCHES=$(yq eval '.. | select(.message?).message | sub("\n", " ")' "${ALERTS_FILE}" | grep -E "${ALERT_VARIABLE_REGEX}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "The alerts at ${ALERTS_FILE} contains unexpected references to '${LABEL}' label in some messages:\n$MATCHES"
  fi

  # Check rules.
  echo "Checking ${RULES_FILE}"
  MATCHES=$(yq eval '.. | select(.expr?).expr | sub("\n", " ")' "${RULES_FILE}" | grep -E "${QUERY_REGEX}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "The rules at ${RULES_FILE} contains unexpected references to '${LABEL}' label in some queries:\n$MATCHES"
  fi

  MATCHES=$(yq eval '.. | select(.record?).record | sub("\n", " ")' "${RULES_FILE}" | grep -Eo '^[^:]+' | grep -E "${RULE_PREFIX_REGEX}" || true)
  if [ -n "$MATCHES" ]; then
    assert_failed "The rules at ${RULES_FILE} contains unexpected references to '${LABEL}' label in some rule name prefix:\n$MATCHES"
  fi
done

if [ $FAILED -ne 0 ]; then
  exit 1
fi
