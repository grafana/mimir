#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
TARGET_PATH="prometheus/api/v1/query_range?query=up{}&start=2023-09-07T05:00:00Z&end=2023-09-07T05:30:00Z&step=15"

for target in standard streaming; do
  "$SCRIPT_DIR/run-one.sh" "$target" "$TARGET_PATH" "result-$target-up-$(date -u +%Y-%m-%dT%H-%M-%SZ).json"
done

