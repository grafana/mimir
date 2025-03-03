#! /usr/bin/env bash

set -euo pipefail

if [[ "$(uname)" == "Darwin" ]]; then
  EDIT_IN_PLACE=(-i '')
else
  EDIT_IN_PLACE=(-i'')
fi

# Ensure all lines except load, eval and clear commands and comments are indented
sed -E "${EDIT_IN_PLACE[@]}" '/^(load|eval|clear|.*#)/! s/^[[:space:]]*/  /g' "$@"

# Convert leading whitespace to two spaces
sed -E "${EDIT_IN_PLACE[@]}" 's/^[[:space:]]+/  /g' "$@"

# Strip trailing whitespace
sed -E "${EDIT_IN_PLACE[@]}" 's/[[:space:]]+$//g' "$@"
