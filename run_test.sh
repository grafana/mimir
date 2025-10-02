#!/usr/bin/env bash

set -e

test="TestGrafanaAlertmanagerWithNestedRoutes"

go test -v -tags=requires_docker ./integration -run "^${test}\$" | tee logs.txt

dir=$(cat logs.txt | grep -Eo "Shared dir: (.*)" | awk '{ print $3 }')

cp logs.txt "${dir}/"
echo "Logs copied to ${dir}/logs.txt"

if ! command -v pbcopy >/dev/null; then
  echo "pbcopy not found, skipping clipboard copy"
  exit 0
fi

echo "${dir}" | pbcopy
echo "Log path copied to clipboard"
