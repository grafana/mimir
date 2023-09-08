#! /usr/bin/env bash

set -euo pipefail

# Run this script with something like: ./create-data-generation-config.sh 1000 10 > data-generation-config.yaml

if [ "$#" -ne 2 ]; then
  echo "Please provide the number of series and number of groups." >/dev/stderr
  exit 1
fi

NUM_SERIES="$1"
NUM_GROUPS="$2"

cat <<EOF
interval: 15s
# With a 15s interval, 1 hour's data is 240 points.
time_series:
EOF

for (( series_index = 0; series_index < NUM_SERIES; series_index++ )); do
  group_index=$((series_index % NUM_GROUPS))

    cat <<EOF
  - series: test_metric{index="$series_index", group="$group_index"}
    values: $group_index+${series_index}x240
EOF
done
