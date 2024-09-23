#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# This script compares benchmark results for the two engines.

set -euo pipefail

RESULTS_FILE="$1" # Should be the path to a file produced by a command like `go run . -count=6 | tee output.txt`

benchstat -col="/engine@(Prometheus Mimir)" "$RESULTS_FILE"
