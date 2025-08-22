#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

# Check for git merge conflict markers in source files, excluding vendor directory.

conflict_files=$(find . \
  -path "./vendor" -prune \
  -o -path "./.pkg" -prune \
  -o -type f \
    \( \
      -name "*.go" \
      -o -name "*.yml" \
      -o -name "*.yaml" \
      -o -name "*.json" \
      -o -name "*.md" \
      -o -name "*.txt" \
      -o -name "*.sh" \
      -o -name "*.jsonnet" \
      -o -name "*.libsonnet" \
    \) \
  -print \
  | xargs grep -l "^<<<<<<<\|^=======\|^>>>>>>>" 2>/dev/null || true \
)

if [ -n "$conflict_files" ]; then
    echo "Git merge conflict markers found in the codebase:"
    echo "$conflict_files" | xargs grep -Hn "^<<<<<<<\|^=======\|^>>>>>>>"
    exit 1
fi