#! /usr/bin/env bash

set -euo pipefail

TENANT="anonymous"

function main() {
  if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument: the path to a block directory." >/dev/stderr
    exit 1
  fi

  local BLOCK_DIR="$1"

  if [ ! -d "$BLOCK_DIR" ]; then
    echo "'$BLOCK_DIR' is not a directory." >/dev/stderr
    exit 1
  fi

  BLOCK_ULID="$(basename "$BLOCK_DIR")"

  echo "Block ULID is $BLOCK_ULID."
  echo "Uploading no-compact marker..."

  cat <<EOF | aws_with_creds s3 cp - "s3://mimir-tsdb/$TENANT/markers/$BLOCK_ULID-no-compact.json"
  {
    "id": "$BLOCK_ULID",
    "version": 1,
    "no_compact_time": $(date -u +%s),
    "reason": "manual",
    "details": "block uploaded for debugging purposes"
  }
EOF

  echo "Uploading block contents..."
  aws_with_creds s3 cp --recursive "$BLOCK_DIR" "s3://mimir-tsdb/$TENANT/$BLOCK_ULID"

  echo "Done."
}

function aws_with_creds() {
  AWS_ACCESS_KEY_ID=mimir AWS_SECRET_ACCESS_KEY=supersecret aws --endpoint-url http://localhost:9000 "$@"
}

main "$@"
