#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

RULES="$1"
LIMIT="$2"

BIG_GROUPS=$(yq -o json eval "$RULES" | jq "[.groups[] | select(.rules | length > $LIMIT) | [.name, (.rules | length)]]" )
CNT=$(echo "$BIG_GROUPS" | jq "length")

if [[ ${CNT} -gt 0 ]]; then
  echo Found rule groups with more than $LIMIT rules:
  echo
  echo "$BIG_GROUPS" | jq -c -r '.[] | .[0] + ": " + (.[1] | tostring)'
  exit 1
fi
