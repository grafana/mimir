#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

SCRIPT_DIR="$(realpath "$(dirname "${0}")")"
TENANT="anonymous"
AWS_ACCESS_KEY_ID=mimir
AWS_SECRET_ACCESS_KEY=supersecret
S3_ENDPOINT=localhost:9000
S3_BUCKET_NAME="mimir-tsdb"

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
  if ! go run "$SCRIPT_DIR/../../../tools/ulidtime" "$BLOCK_ULID" >/dev/null 2>&1; then
    echo "'$BLOCK_ULID' is not a valid ULID." >/dev/stderr
    exit 1
  fi

  echo "Block ULID is $BLOCK_ULID."
  echo "Uploading no-compact marker..."
  markblocks \
    -tenant="$TENANT" \
    -mark=no-compact \
    -details="block uploaded for debugging purposes" \
    -skip-existence-check=true \
    "$BLOCK_ULID"

  echo "Uploading block contents..."
  aws_with_creds s3 cp --recursive "$BLOCK_DIR" "s3://$S3_BUCKET_NAME/$TENANT/$BLOCK_ULID"

  echo "Done."
}

function markblocks() {
  go run "$SCRIPT_DIR/../../../tools/markblocks" \
    -backend="s3" \
    -s3.access-key-id="$AWS_ACCESS_KEY_ID" \
    -s3.secret-access-key="$AWS_SECRET_ACCESS_KEY" \
    -s3.endpoint="$S3_ENDPOINT" \
    -s3.insecure=true \
    -s3.bucket-name="$S3_BUCKET_NAME" \
    "$@"
}

function aws_with_creds() {
  AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY aws --endpoint-url "http://$S3_ENDPOINT" "$@"
}

main "$@"
