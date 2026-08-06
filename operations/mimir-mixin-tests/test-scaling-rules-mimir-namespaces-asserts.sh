#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
RULES_FILE="${SCRIPT_DIR}"/test-scaling-rules-mimir-namespaces/rules.yaml
GROUP="mimir_scaling_rules"

# The series the scaling rules read from kube-state-metrics and cAdvisor. They are exported for
# every workload the Prometheus scrapes, not only for Mimir.
UNSCOPED_METRICS=(
  "kube_deployment_spec_replicas"
  "kube_statefulset_replicas"
  "container_cpu_usage_seconds_total"
  "container_memory_usage_bytes"
  "kube_pod_container_resource_requests"
  "kube_pod_container_resource_requests_cpu_cores"
  "kube_pod_container_resource_requests_memory_bytes"
)

# Every per-deployment aggregation of these series must be followed by the join restricting it to
# the namespaces running Mimir. Rules combining two aggregations with "or" need the join on both
# sides, because "*" binds tighter than "or".
AGGREGATION='sum by (cluster, namespace, deployment)'
JOIN='* on(cluster, namespace) group_left max by(cluster, namespace) (cortex_build_info)'

FAILED=0

assert_failed() {
  local msg=$1
  echo ""
  echo -e "$msg"
  echo ""
  FAILED=1
}

# Match a metric name only where it is a series selector: not preceded by ":", which would make it
# part of a recorded rule name, and not surrounded by characters that would make it part of a
# longer metric name.
selector_regex() {
  echo "(^|[^a-z0-9_:])$1([^a-z0-9_]|$)"
}

echo "Checking ${RULES_FILE}"

RULES_COUNT=$(yq eval "[.groups[] | select(.name == \"${GROUP}\") | .rules[]] | length" "${RULES_FILE}")
if [ "$RULES_COUNT" -eq 0 ]; then
  assert_failed "No rules found in the '${GROUP}' group of ${RULES_FILE}."
fi

ALL_EXPRESSIONS=""

for ((i = 0; i < RULES_COUNT; i++)); do
  RECORD=$(yq eval "[.groups[] | select(.name == \"${GROUP}\") | .rules[]][$i].record" "${RULES_FILE}")
  # Strip PromQL comments, which mention some of these metric names.
  EXPR=$(yq eval "[.groups[] | select(.name == \"${GROUP}\") | .rules[]][$i].expr" "${RULES_FILE}" | sed -e 's/#.*$//')
  ALL_EXPRESSIONS+="${EXPR}"$'\n'

  READS_UNSCOPED=0
  for METRIC in "${UNSCOPED_METRICS[@]}"; do
    if echo "$EXPR" | grep -qE "$(selector_regex "$METRIC")"; then
      READS_UNSCOPED=1
    fi
  done
  if [ "$READS_UNSCOPED" -eq 0 ]; then
    continue
  fi

  AGGREGATIONS=$(echo "$EXPR" | grep -cF "$AGGREGATION" || true)
  JOINS=$(echo "$EXPR" | grep -cF "$JOIN" || true)
  if [ "$JOINS" -eq 0 ] || [ "$JOINS" -ne "$AGGREGATIONS" ]; then
    assert_failed "The rule recording '${RECORD}' in the '${GROUP}' group of ${RULES_FILE} has ${AGGREGATIONS} '${AGGREGATION}' aggregation(s) of kube-state-metrics or cAdvisor series but ${JOINS} '${JOIN}' join(s), so it records the workloads of namespaces not running Mimir:\n${EXPR}"
  fi
done

for METRIC in "${UNSCOPED_METRICS[@]}"; do
  if ! echo "$ALL_EXPRESSIONS" | grep -qE "$(selector_regex "$METRIC")"; then
    assert_failed "The '${GROUP}' group in ${RULES_FILE} has no reference to '${METRIC}'. If the rules stopped using it, remove it from this test, otherwise the test silently checks nothing."
  fi
done

if [[ $FAILED -ne 0 ]]; then
  exit 1
fi
