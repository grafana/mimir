#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

if [[ "$(uname)" == "Darwin" ]]; then
  EDIT_IN_PLACE=(-i '')
else
  EDIT_IN_PLACE=(-i'')
fi

# Ensure all lines except load, eval and clear commands and comments are indented
sed -E "${EDIT_IN_PLACE[@]}" '/^(load|eval|clear|.*#)/! s/^[[:space:]]*/  /g' "$@"

# Ensure load, eval and clear commands are not indented
sed -E "${EDIT_IN_PLACE[@]}" 's/^[[:space:]]+(load|eval|clear)/\1/g' "$@"

# Convert leading whitespace to two spaces
sed -E "${EDIT_IN_PLACE[@]}" 's/^[[:space:]]+/  /g' "$@"

# Strip trailing whitespace
sed -E "${EDIT_IN_PLACE[@]}" 's/[[:space:]]+$//g' "$@"

# Strip multiple consecutive blank lines
# https://unix.stackexchange.com/a/216550/22142 explains the incantation below
sed -E "${EDIT_IN_PLACE[@]}" '$!N;/^\n$/!P;D' "$@"
