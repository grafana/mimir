#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

# Check for git merge conflict markers in source files, excluding directories from .gitignore.

# Build find exclusions from .gitignore
find_exclusions=""
if [ -f .gitignore ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    
    # Remove leading/trailing whitespace
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    
    # Skip if still empty after whitespace removal
    [ -z "$line" ] && continue
    
    # Handle directory patterns (ending with /)
    if [[ "$line" =~ /$ ]]; then
      # Remove trailing slash and add as path exclusion
      dir_pattern=$(echo "$line" | sed 's|/$||')
      find_exclusions="$find_exclusions -path \"./$dir_pattern\" -prune -o"
    elif [[ ! "$line" =~ \* ]] && [[ ! "$line" =~ ^/ ]]; then
      # For simple directory names without wildcards or leading slash
      find_exclusions="$find_exclusions -path \"./$line\" -prune -o"
    fi
  done < .gitignore
fi

# Always exclude vendor directory for backward compatibility
find_exclusions="-path \"./vendor\" -prune $find_exclusions"

# Build and execute find command
eval "conflict_files=\$(find . $find_exclusions -type f \\
    \\( \\
      -name \"*.go\" \\
      -o -name \"*.yml\" \\
      -o -name \"*.yaml\" \\
      -o -name \"*.json\" \\
      -o -name \"*.md\" \\
      -o -name \"*.txt\" \\
      -o -name \"*.sh\" \\
      -o -name \"*.jsonnet\" \\
      -o -name \"*.libsonnet\" \\
    \\) \\
  -print \\
  | xargs grep -l \"^<<<<<<<\\|^=======\\|^>>>>>>>\" 2>/dev/null || true \\
)"

if [ -n "$conflict_files" ]; then
    echo "Git merge conflict markers found in the codebase:"
    echo "$conflict_files" | xargs grep -Hn "^<<<<<<<\|^=======\|^>>>>>>>"
    exit 1
fi