#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

SED_BIN=${SED_BIN:-sed}
SCRIPT_DIR=$(realpath "$(dirname "${0}")")
STATUS=0

# List all alerts.
if ! ALERTS=$(yq eval '.groups.[].rules.[].alert' "${SCRIPT_DIR}/../operations/mimir-mixin-compiled/alerts.yaml" 2> /dev/stdout); then
  echo "Unable to list alerts. Got output:"
  echo "$ALERTS"
  exit 1
elif [ -z "$ALERTS" ]; then
  echo "No alerts found. Something went wrong with the listing."
  exit 1
fi

# Check if each alert is referenced in the runbooks.
for ALERT in $ALERTS; do
  if ! grep -q "# ${ALERT}$" "${SCRIPT_DIR}/../docs/sources/mimir/operators-guide/mimir-runbooks/_index.md"; then
    echo "Missing runbook for alert: $ALERT"
    STATUS=1
  fi
done

# List all global error IDs.
if ! ERROR_IDS=$(${SED_BIN} --quiet -E 's/^.*ID\s+=\s+"([^"]+)"$/\1/p' "${SCRIPT_DIR}/../pkg/util/globalerror/errors.go"); then
  echo "Unable to list error IDs. Got output:"
  echo "$ERROR_IDS"
  exit 1
elif [ -z "$ERROR_IDS" ]; then
  echo "No error IDs found. Something went wrong with the listing."
  exit 1
fi

for ID in $ERROR_IDS; do
  # Prepend the expected prefix.
  ID="err-mimir-${ID}"

  if ! grep -q "# ${ID}$" "${SCRIPT_DIR}/../docs/sources/mimir/operators-guide/mimir-runbooks/_index.md"; then
    echo "Missing runbook for error: $ID"
    STATUS=1
  fi
done

exit $STATUS
