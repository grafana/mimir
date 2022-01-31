#!/usr/bin/env bash

set -eu -o pipefail

SCRIPT_DIR=$(realpath "$(dirname "${0}")")

# List all alerts.
if ! ALERTS=$(yq eval '.groups.[].rules.[].alert' "${SCRIPT_DIR}/../../mimir-mixin-compiled/alerts.yaml" 2> /dev/stdout); then
  echo "Unable to list alerts. Got output:"
  echo "$ALERTS"
  exit 1
elif [ -z "$ALERTS" ]; then
  echo "No alerts found. Something went wrong with the listing."
  exit 1
fi

# Check if each alert is referenced in the playbooks.
STATUS=0

for ALERT in $ALERTS; do
  if ! grep -q "${ALERT}$" "${SCRIPT_DIR}/../docs/playbooks.md"; then
    echo "Missing playbook for: $ALERT"
    STATUS=1
  fi
done

exit $STATUS
