#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -eu -o pipefail

function apply_expected_diffs {
    for file in "$@" ; do
        if [ -f "${file}" ] ; then
            git apply -R "${file}"
        fi
    done
}

apply_expected_diffs "$@"
