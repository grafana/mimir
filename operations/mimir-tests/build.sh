#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# Start from a clean setup.
rm -rf jsonnet-tests && mkdir jsonnet-tests
cd jsonnet-tests

# Initialise the Tanka.
tk init --k8s=1.21

# Install Mimir jsonnet from this branch.
jb install ../operations/mimir

# Create a test environment.
mkdir -p "environments/test"

# Import all test files so we can have a test inheriting from another one.
cp ../operations/mimir-tests/test*.jsonnet environments/test/

# Run tests.
export PAGER=cat
TESTS=$(ls -1 ../operations/mimir-tests/test*.jsonnet)

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.jsonnet' "$FILEPATH")

  echo "Importing $TEST_NAME"

  # Copy the desired test in main.jsonnet so that it will be compiled by default.
  cp "$FILEPATH" "environments/test/main.jsonnet"

  # Copy spec.json from environments/default which is created by tk init.
  # We just need the default spec.json to get tk compile the environment.
  cp environments/default/spec.json "environments/test/spec.json"

  echo "Compiling $TEST_NAME"
  tk show --dangerous-allow-redirect "environments/test" > ../operations/mimir-tests/${TEST_NAME}-generated.yaml
done
