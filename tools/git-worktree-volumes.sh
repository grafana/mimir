#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Generate Docker volume mounts for git worktrees.
# In git worktrees, .git is a file pointing to split metadata directories.
# This script detects worktrees and outputs the required volume mounts.

set -o errexit
set -o nounset
set -o pipefail

# Check if we're in a git worktree (.git is a file, not a directory)
if [[ ! -f .git ]]; then
    # Not in a worktree, no additional volumes needed
    exit 0
fi

# Extract the worktree git directory path from .git file
worktree_gitdir=$(cat .git | sed 's/^gitdir: //')

# Check if the worktree git directory exists
if [[ ! -d "$worktree_gitdir" ]]; then
    exit 0
fi

# Find the common git directory by reading commondir
if [[ -f "$worktree_gitdir/commondir" ]]; then
    common_gitdir=$(cd "$worktree_gitdir" && cd "$(cat commondir)" && pwd)
    
    # Output volume mounts for both directories (without mount options, those are added by make)
    echo "-v $worktree_gitdir:$worktree_gitdir"
    if [[ -d "$common_gitdir" ]]; then
        echo "-v $common_gitdir:$common_gitdir"
    fi
fi
