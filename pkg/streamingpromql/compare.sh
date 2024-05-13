#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# This script compares benchmark results for the two engines.

set -euo pipefail

RESULTS_FILE="$1" # Should be the path to a file produced by a command like `go run ./tools/benchmark-query-engine -count=6 | tee output.txt`

PROMETHEUS_RESULTS_FILE=$(mktemp /tmp/prometheus.XXXX)
STREAMING_RESULTS_FILE=$(mktemp /tmp/streaming.XXXX)

grep --invert-match "streaming-" "$RESULTS_FILE" | sed -E 's#/Prometheus-[0-9]+##g' > "$PROMETHEUS_RESULTS_FILE"
grep --invert-match "Prometheus-" "$RESULTS_FILE" | sed -E 's#/streaming-[0-9]+##g' > "$STREAMING_RESULTS_FILE"

benchstat "$PROMETHEUS_RESULTS_FILE" "$STREAMING_RESULTS_FILE" | sed "s#$PROMETHEUS_RESULTS_FILE#     Prometheus     #g" | sed "s#$STREAMING_RESULTS_FILE#     streaming     #g"

rm "$PROMETHEUS_RESULTS_FILE" "$STREAMING_RESULTS_FILE"
