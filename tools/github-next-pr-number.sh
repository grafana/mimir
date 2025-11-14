#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Use "// empty" to return empty string if no PRs/issues exist.
LAST_PR=$(gh pr list --repo grafana/mimir --state all --limit 1 --json number --jq '.[0].number // empty')
LAST_ISSUE=$(gh issue list --repo grafana/mimir --limit 1 --json number --jq '.[0].number // empty')

if [ "$LAST_PR" == "" ] && [ "$LAST_ISSUE" == "" ]; then
  echo "Unable to get last PR and/or Issue number."
  exit 1
elif [ "$LAST_PR" -gt "$LAST_ISSUE" ]; then
  echo $((LAST_PR + 1))
else
  echo $((LAST_ISSUE + 1))
fi
