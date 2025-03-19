#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

function apply_expected_diffs {
    for file in "$@" ; do
        expdiff_file="$file.expdiff"
        if [ ! -f "${expdiff_file}" ] ; then
            # expected diff does not exist
            continue
        fi

        if git diff -s --exit-code $file > /dev/null
        then
            # file has not changed, not applying expected diff
            continue
        fi

        echo "$file: applying expected diff: $expdiff_file"
        git apply -R "$expdiff_file"
    done
}

apply_expected_diffs "$@"
