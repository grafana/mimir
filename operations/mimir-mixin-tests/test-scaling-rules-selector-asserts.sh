#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
RULES_FILE="${SCRIPT_DIR}"/test-scaling-rules-selector/rules.yaml

# Matches mimir_scaling_rules_selector in test-scaling-rules-selector.libsonnet.
SELECTOR='namespace=~"mimir.*"'

# The series the scaling rules read from kube-state-metrics and cAdvisor. Each one must carry the
# configured selector, otherwise the rules aggregate every workload the Prometheus scrapes.
SELECTED_METRICS=(
  "kube_deployment_spec_replicas"
  "kube_statefulset_replicas"
  "container_cpu_usage_seconds_total"
  "container_memory_usage_bytes"
  "kube_pod_container_resource_requests"
  "kube_pod_container_resource_requests_cpu_cores"
  "kube_pod_container_resource_requests_memory_bytes"
)

FAILED=0

assert_failed() {
  local msg=$1
  echo ""
  echo -e "$msg"
  echo ""
  FAILED=1
}

echo "Checking ${RULES_FILE}"

# Strip PromQL comments, which mention some of these metric names, and prefix every line with a
# space so that a metric at the beginning of a line still has a leading character to match on.
EXPRESSIONS=$(yq eval '.groups[] | select(.name == "mimir_scaling_rules") | .rules[].expr' "${RULES_FILE}" | sed -e 's/#.*$//' -e 's/^/ /')

if [ -z "$EXPRESSIONS" ]; then
  assert_failed "No expressions found in the 'mimir_scaling_rules' group of ${RULES_FILE}."
fi

for METRIC in "${SELECTED_METRICS[@]}"; do
  # Match the metric name only where it is a series selector: not preceded by ":", which would make
  # it part of a recorded rule name, and not surrounded by characters that would make it part of a
  # longer metric name. A selector is followed by "{", so anything else is an unselected reference.
  UNSELECTED=$(echo "$EXPRESSIONS" | grep -oE "[^a-z0-9_:]${METRIC}[^a-z0-9_{]" || true)
  if [ -n "$UNSELECTED" ]; then
    assert_failed "The 'mimir_scaling_rules' group in ${RULES_FILE} references '${METRIC}' without any label matcher:\n$UNSELECTED"
  fi

  MATCHERS=$(echo "$EXPRESSIONS" | grep -oE "[^a-z0-9_:]${METRIC}\{[^}]*\}" || true)
  if [ -z "$MATCHERS" ]; then
    assert_failed "The 'mimir_scaling_rules' group in ${RULES_FILE} has no reference to '${METRIC}'. If the rules stopped using it, remove it from this test, otherwise the test silently checks nothing."
    continue
  fi

  WITHOUT_SELECTOR=$(echo "$MATCHERS" | grep -vF "${SELECTOR}" || true)
  if [ -n "$WITHOUT_SELECTOR" ]; then
    assert_failed "The 'mimir_scaling_rules' group in ${RULES_FILE} references '${METRIC}' without the configured selector '${SELECTOR}':\n$WITHOUT_SELECTOR"
  fi
done

if [[ $FAILED -ne 0 ]]; then
  exit 1
fi
