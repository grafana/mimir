#! /usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Please provide a target name." >/dev/stderr
  exit 1
fi

TARGET_NAME="$1"

# We use queriers below, rather than query-frontends, to ensure we avoid any caching of queries.
case $TARGET_NAME in
standard)
  TARGET_ADDRESS="localhost:8204" # querier-standard
  ;;
streaming)
  TARGET_ADDRESS="localhost:8304" # querier-streaming
  ;;
*)
  echo "Unknown target '$TARGET_NAME'." >/dev/stderr
  exit 1
  ;;
esac

VUS=1 # Don't run queries in parallel.
DURATION=10s
SCRIPT="up.js"
OUTPUT="result-$SCRIPT-$TARGET_NAME-$(date -u +%Y-%m-%dT%H-%M-%SZ).json"

k6 run --vus=$VUS --duration=$DURATION --summary-export="$OUTPUT" --env TARGET_ADDRESS="$TARGET_ADDRESS" "$SCRIPT"
