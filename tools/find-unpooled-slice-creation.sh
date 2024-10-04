#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

MATCHES=$(
  cd "$PROJECT_DIR";
  grep \
    -R \
    -n \
    --exclude '*_test.go' \
    --exclude '*pool.go' \
    -e 'make(\[\]types\.SeriesMetadata,' \
    -e 'make(\[\]promql\.FPoint,' \
    -e 'make(\[\]promql\.HPoint,' \
    -e 'make(\[\]float64,' \
    -e 'make(\[\]bool,' \
    -e 'make(\[\]\*histogram\.FloatHistogram,' \
    -e 'make(promql\.Vector,' \
    'pkg/streamingpromql' || true
)

if [ -n "$MATCHES" ]; then
  echo "Found one or more instances of creating a slice directly that should be taken from a pool:"
  echo "$MATCHES"
  exit 1
fi
