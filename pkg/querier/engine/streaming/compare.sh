#! /usr/bin/env bash

set -euo pipefail

RESULTS_FILE="$1"

STANDARD_RESULTS_FILE=$(mktemp /tmp/standard.XXXX)
STREAMING_RESULTS_FILE=$(mktemp /tmp/streaming.XXXX)

grep --invert-match "streaming-" "$RESULTS_FILE" | sed -E 's#/standard-[0-9]+##g' > "$STANDARD_RESULTS_FILE"
grep --invert-match "standard-" "$RESULTS_FILE" | sed -E 's#/streaming-[0-9]+##g' > "$STREAMING_RESULTS_FILE"

benchstat "$STANDARD_RESULTS_FILE" "$STREAMING_RESULTS_FILE"
