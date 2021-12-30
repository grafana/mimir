#!/usr/bin/env bash

# Start from a clean setup.
rm -rf jsonnet-tests && mkdir jsonnet-tests
cd jsonnet-tests

# Initialise the Tanka.
tk init --k8s=1.18

# Install Mimir jsonnet (so that we get dependencies) and then override it with this branch code.
jb install github.com/grafana/mimir/operations/mimir@main
rm -fr ./vendor/mimir
cp -r ../operations/mimir ./vendor/mimir

# Copy tests to dedicated environments and build them.
export PAGER=cat
TESTS=$(ls -1 ../operations/mimir-tests/test*.jsonnet)

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.jsonnet' "$FILEPATH")
  TEST_NAMES="${TEST_NAMES}${TEST_NAME}\n"

  echo "Importing $TEST_NAME"
  mkdir -p "environments/${TEST_NAME}"
  cp "$FILEPATH" "environments/${TEST_NAME}/main.jsonnet"

  echo "Compiling $TEST_NAME"
  cp environments/default/spec.json "environments/${TEST_NAME}/spec.json"
  tk show --dangerous-allow-redirect "environments/${TEST_NAME}" > ../operations/mimir-tests/${TEST_NAME}-generated.yaml
done
