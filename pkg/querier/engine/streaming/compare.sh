#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# This script compares benchmark results for the two engines.

set -euo pipefail

RESULTS_FILE="$1" # Should be the path to a file produced by a command like `go test -run=XXX -bench="BenchmarkQuery" -count=6 -benchmem -timeout=1h .`

STANDARD_RESULTS_FILE=$(mktemp /tmp/standard.XXXX)
STREAMING_RESULTS_FILE=$(mktemp /tmp/streaming.XXXX)

grep --invert-match "streaming-" "$RESULTS_FILE" | sed -E 's#/standard-[0-9]+##g' > "$STANDARD_RESULTS_FILE"
grep --invert-match "standard-" "$RESULTS_FILE" | sed -E 's#/streaming-[0-9]+##g' > "$STREAMING_RESULTS_FILE"

benchstat "$STANDARD_RESULTS_FILE" "$STREAMING_RESULTS_FILE" | sed "s#$STANDARD_RESULTS_FILE#     standard     #g" | sed "s#$STREAMING_RESULTS_FILE#     streaming     #g"

rm "$STANDARD_RESULTS_FILE" "$STREAMING_RESULTS_FILE"
