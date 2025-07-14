#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

LAST_PR=$(hub pr list --state all -L 1 --format '%I')
LAST_ISSUE=$(hub issue --format '%I' -L 1)

if [ "$LAST_PR" == "" ] && [ "$LAST_ISSUE" == "" ]; then
  echo "Unable to get last PR and/or Issue number. Is your current working directory a public GitHub repository?"
  exit 1
elif [ "$LAST_PR" -gt "$LAST_ISSUE" ]; then
  echo $((LAST_PR + 1))
else
  echo $((LAST_ISSUE + 1))
fi
