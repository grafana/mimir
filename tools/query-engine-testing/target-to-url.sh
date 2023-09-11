#! /usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Please provide a target name." >/dev/stderr
  exit 1
fi

TARGET_NAME="$1"

# We use queriers below, rather than query-frontends, to ensure we avoid any caching of queries.
case $TARGET_NAME in
standard)
  echo "localhost:8204" # querier-standard
  ;;
streaming)
  echo "localhost:8304" # querier-streaming
  ;;
thanos)
  echo "localhost:8404" # querier-thanos
  ;;
*)
  echo "Unknown target '$TARGET_NAME'." >/dev/stderr
  exit 1
  ;;
esac
