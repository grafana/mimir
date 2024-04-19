#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit

CHART_PATH="$1"

cd "$CHART_PATH"

DEPENDENCIES=$( helm dependency list )

if echo "$DEPENDENCIES" |tail +2|grep -v "^$"|grep -c -E -v "ok[[:space:]]*$" ; then
    helm dependency update && touch Chart.lock
else
    echo "Helm dependencies are up to date"
fi
