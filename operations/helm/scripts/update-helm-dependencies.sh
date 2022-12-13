#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o errexit

CHART_PATH="$1"

cd "$CHART_PATH"

DEPENDENCIES=$( helm dependency list )

NOT_OK_DEPENDENCY=$( echo "$DEPENDENCIES" |tail +2|grep -v "^$"|grep -E -v "ok[[:space:]]*$"|wc -l )

if [ "$NOT_OK_DEPENDENCY" != "0" ] ; then
    helm dependency update && touch Chart.lock
else
    echo "Helm dependencies are up to date"
fi
