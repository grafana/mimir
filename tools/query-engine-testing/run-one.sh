#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

if [ "$#" -ne 3 ]; then
  echo "Please provide a target name and path, and output path." >/dev/stderr
  exit 1
fi

TARGET_NAME="$1"
TARGET_PATH="$2"
OUTPUT_PATH="$3"

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
DURATION=10s # TODO: run for longer once test setup is stable

k6 run --vus=$VUS --duration=$DURATION --summary-export="$OUTPUT_PATH" --env TARGET_ADDRESS="$TARGET_ADDRESS" --env TARGET_PATH="$TARGET_PATH" "$SCRIPT_DIR/test.js"
