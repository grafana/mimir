#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

function join_to_regex {
    for i in "$@" ; do
        echo -n "$i|"
    done
}

function find_diff_or_untracked {
    git diff --exit-code -- "$@"

    local regex=$(join_to_regex "$@")
    regex=${regex::-1}
    local untracked_files=$(git ls-files --others --exclude-standard)
    set +e
	echo "${untracked_files}" | grep -E "^(${regex})"
    local rc=$?
    set -e
    if [ "${rc}" -eq 0 ] ; then  # flip the grep result
      echo "$0: Untracked files found"
      return 1
    fi
}

if [ $# -eq 0 ] ; then
  echo "Too few argument to $0. Provide a messages and paths"
  exit 1
fi

find_diff_or_untracked "$@"
