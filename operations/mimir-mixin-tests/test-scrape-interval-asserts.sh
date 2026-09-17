#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ALERTS_FILE="${SCRIPT_DIR}"/test-scrape-interval/alerts.yaml
RULES_FILE="${SCRIPT_DIR}"/test-scrape-interval/rules.yaml

SCRAPE_INTERVAL_SECONDS=300  # matches scrape_interval: '5m' in test-scrape-interval.libsonnet
MIN_RATE_INTERVAL_SECONDS=$((4 * SCRAPE_INTERVAL_SECONDS))
FAILED=0

assert_failed() {
  local msg=$1
  echo ""
  echo -e "$msg"
  echo ""
  FAILED=1
}

parse_duration_to_seconds() {
  local duration=$1
  if [[ $duration =~ ^([0-9]+)h$ ]]; then
    echo $(( ${BASH_REMATCH[1]} * 3600 ))
  elif [[ $duration =~ ^([0-9]+)m$ ]]; then
    echo $(( ${BASH_REMATCH[1]} * 60 ))
  elif [[ $duration =~ ^([0-9]+)s$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "ERROR: cannot parse duration '${duration}'" >&2
    exit 1
  fi
}

check_expressions() {
  local file=$1
  local yq_filter=$2

  while IFS= read -r line; do
    # Each line is a single expression (newlines replaced with spaces by yq).
    # Extract all [duration] and [duration:step] brackets.
    # For subquery notation [range:step], we only check the range (first part before the colon).
    local remaining="$line"
    while [[ $remaining =~ \[([0-9]+[smh])[^]]*\] ]]; do
      duration="${BASH_REMATCH[1]}"
      seconds=$(parse_duration_to_seconds "$duration")

      if [[ $seconds -lt $MIN_RATE_INTERVAL_SECONDS ]]; then
        assert_failed "Expression contains [${duration}...] which is less than 4x the scrape interval (${MIN_RATE_INTERVAL_SECONDS}s):\n  ${line}"
      fi

      # Advance past the matched bracket to continue scanning the rest of the line.
      local matched="${BASH_REMATCH[0]}"
      remaining="${remaining#*"${matched}"}"
    done
  done < <(yq eval "$yq_filter" "$file")
}

echo "Checking ${ALERTS_FILE}"
# Exclude alerts from vendor mixins (e.g. rollout-operator) which we don't control.
check_expressions "${ALERTS_FILE}" \
  '.groups[] | select(.name | test("^rollout_operator") | not) | .rules[].expr | sub("\n", " ")'

echo "Checking ${RULES_FILE}"
# Exclude recording rules whose names encode a fixed rate interval (e.g. :rate5m, :rate1m),
# since their interval is intentional and baked into the name consumed by dashboards.
check_expressions "${RULES_FILE}" \
  '.groups[].rules[] | select(has("expr")) | select((has("record") | not) or (.record | test(":[a-z]+[0-9]+[smhd]$") | not)) | .expr | sub("\n", " ")'

if [[ $FAILED -ne 0 ]]; then
  exit 1
fi
