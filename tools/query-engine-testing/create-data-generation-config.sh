#! /usr/bin/env bash

set -euo pipefail

# Run this script with something like: ./create-data-generation-config.sh 20000 10

if [ "$#" -ne 2 ]; then
  echo "Please provide the number of series and number of groups." >/dev/stderr
  exit 1
fi

NUM_SERIES="$1"
NUM_GROUPS="$2"

function main() {
  # The data generation tool in prometheus-toolbox doesn't scale well beyond ~10k series, so break up the config into
  # multiple chunks, each with 10k series each.
  local chunk_size=10000
  local chunk_index=0

  for (( first_index = 0; first_index < NUM_SERIES; first_index+=chunk_size )); do
    local count=$chunk_size
    chunk_index=$(( chunk_index + 1 ))

    if (( first_index+count > NUM_SERIES )); then
      count=$(( NUM_SERIES % chunk_size ))
    fi

    generate "$first_index" "$count" > "data-generation-config-$chunk_index.yaml"
  done
}

function generate() {
    local first_index="$1"
    local count="$2"

    cat <<EOF
interval: 15s
# With a 15s interval, 12 hours' data is 2880 points.
time_series:
EOF

    for (( series_index = first_index; series_index < first_index + count; series_index++ )); do
      group_index=$((series_index % NUM_GROUPS))

        cat <<EOF
  - series: test_metric{index="$series_index", group="$group_index"}
    values: $group_index+${series_index}x2880
EOF
    done
}

main


