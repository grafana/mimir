#! /usr/bin/env bash

set -euo pipefail

RESULTS_FILE="$1"

STANDARD_RESULTS=$(grep --invert-match "streaming" "$RESULTS_FILE" | sed 's#/standard##g')
STREAMING_RESULTS=$(grep --invert-match "standard" "$RESULTS_FILE" | sed 's#/streaming##g')

benchstat <(echo "$STANDARD_RESULTS") <(echo "$STREAMING_RESULTS")
