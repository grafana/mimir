#! /usr/bin/env bash

set -euo pipefail

# Ensure all lines except load, eval and clear commands and comments are indented
sed -E -i '' '/^(load|eval|clear|.*#)/! s/^[[:space:]]*/  /g' "$@"

# Convert leading whitespace to two spaces
sed -E -i '' 's/^[[:space:]]+/  /g' "$@"

# Strip trailing whitespace
sed -E -i '' 's/[[:space:]]+$//g' "$@"
