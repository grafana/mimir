#! /usr/bin/env bash

set -euo pipefail

# Convert leading whitespace to two spaces
sed -E -i '' 's/^[[:space:]]+/  /g' "$1"
