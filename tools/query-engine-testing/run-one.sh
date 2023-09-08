#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

if [ "$#" -ne 4 ]; then
  echo "Please provide a target name and path, test duration and output path." >/dev/stderr
  exit 1
fi

TARGET_NAME="$1"
TARGET_PATH="$2"
DURATION="$3"
OUTPUT_PATH="$4"
TARGET_ADDRESS="$("$SCRIPT_DIR/target-to-url.sh" "$TARGET_NAME")"

VUS=1 # Don't run queries in parallel.

k6 run --vus=$VUS --duration=$DURATION --summary-export="$OUTPUT_PATH" --env TARGET_ADDRESS="$TARGET_ADDRESS" --env TARGET_PATH="$TARGET_PATH" "$SCRIPT_DIR/test.js"
