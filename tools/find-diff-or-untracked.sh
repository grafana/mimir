#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

function find_diff_or_untracked {
    git diff --exit-code -- "$@"

    local untracked_files=$(git ls-files --others --exclude-standard "$@")
    if [ -n "${untracked_files}" ] ; then
      echo "$0: FAIL: untracked files found:"
      echo "${untracked_files}"
      return 1
    fi
}

if [ $# -eq 0 ] ; then
  echo "Too few arguments to $0. Provide paths to check"
  exit 1
fi

find_diff_or_untracked "$@"
