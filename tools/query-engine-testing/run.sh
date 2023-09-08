#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
TARGET_PATH="prometheus/api/v1/query_range?query=up&start=2023-09-08T01:00:00Z&end=2023-09-08T01:30:00Z&step=15"

echo "Checking both engines produce the same result..."

STANDARD_RESULT=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "standard")/$TARGET_PATH")
STREAMING_RESULT=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "streaming")/$TARGET_PATH")

diff --color --unified --label "standard" --label "streaming" <(echo "$STANDARD_RESULT" | jq --sort-keys .)  <(echo "$STREAMING_RESULT" | jq --sort-keys .)

echo "No differences, starting tests..."

for target in standard streaming; do
  "$SCRIPT_DIR/run-one.sh" "$target" "$TARGET_PATH" "k6-result-$target-up-$(date -u +%Y-%m-%dT%H-%M-%SZ).json"
done

echo "Done."
