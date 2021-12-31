#!/usr/bin/env bash
set -euo pipefail

# Start from a clean setup.
rm -rf jsonnet-tests && mkdir jsonnet-tests
cd jsonnet-tests

# Initialise the Tanka.
tk init --k8s=1.18

# Install Mimir jsonnet from this branch.
jb install ../operations/mimir

# Copy tests to dedicated environments and build them.
export PAGER=cat
TESTS=$(ls -1 ../operations/mimir-tests/test*.jsonnet)

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.jsonnet' "$FILEPATH")

  echo "Importing $TEST_NAME"
  mkdir -p "environments/${TEST_NAME}"
  cp "$FILEPATH" "environments/${TEST_NAME}/main.jsonnet"

  # Copy spec.json from environments/default which is created by tk init.
  # We just need the default spec.json to get tk compile the environment.
  cp environments/default/spec.json "environments/${TEST_NAME}/spec.json"

  echo "Compiling $TEST_NAME"
  tk show --dangerous-allow-redirect "environments/${TEST_NAME}" > ../operations/mimir-tests/${TEST_NAME}-generated.yaml
done
