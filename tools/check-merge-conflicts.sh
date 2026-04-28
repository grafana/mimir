#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

# Check for git merge conflict markers in source files, using git ls-files to respect .gitignore.

conflict_files=$(git ls-files --exclude-standard --cached -- \
  '*.go' \
  '*.yml' \
  '*.yaml' \
  '*.json' \
  '*.md' \
  '*.txt' \
  '*.sh' \
  '*.jsonnet' \
  '*.libsonnet' \
  | grep -v '^vendor/' \
  | xargs grep -l "^<<<<<<<\|^=======\|^>>>>>>>" 2>/dev/null || true \
)

if [ -n "$conflict_files" ]; then
    echo "Git merge conflict markers found in the codebase:"
    echo "$conflict_files" | xargs grep -Hn "^<<<<<<<\|^=======\|^>>>>>>>"
    exit 1
fi