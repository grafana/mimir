#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# Start from a clean setup.
rm -rf jsonnet-tests && mkdir jsonnet-tests
cd jsonnet-tests

# Initialise the Tanka.
tk init --k8s=false
# Preserve the legacy import aliases used by the release manifests.
printf '%s\n' '{"version":1,"dependencies":[],"legacyImports":true}' > jsonnetfile.json
jb install github.com/jsonnet-libs/k8s-libsonnet/1.29@bf9a62cfd32a58c071b8410bfcdec058475dd25e
printf "%s\n" "import 'github.com/jsonnet-libs/k8s-libsonnet/1.29/main.libsonnet'" > lib/k.libsonnet

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
  tk show --dangerous-allow-redirect "environments/test" > ../operations/mimir-tests/"${TEST_NAME}"-generated.yaml
done
