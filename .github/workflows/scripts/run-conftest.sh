#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -x
set -o errexit
set -o nounset
set -o pipefail

helm dependency update $CHART_PATH

for file in $(find $VALUES_FILES_PATH -name '*.yaml'); do
  output_dir=$TEMP_DIR/$(basename "$file")
  helm template test $CHART_PATH -f $file --output-dir $output_dir --namespace citestns >/dev/null
  echo "Testing with values file $file"
  conftest test -p $POLICIES_PATH $output_dir
done
