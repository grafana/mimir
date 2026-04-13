#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Generates synthetic kube_node_info TSDB blocks and uploads them to the local
# MinIO instance used by the mimir-microservices-mode dev environment.
#
# Usage:
#   ./generate-and-upload.sh [--num-series 3000] [--days-back 15]
#
# Prerequisites:
#   - MinIO must be running (docker compose up minio)
#   - aws CLI must be installed (brew install awscli)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

TENANT="anonymous"
AWS_ACCESS_KEY_ID=mimir
AWS_SECRET_ACCESS_KEY=supersecret
S3_ENDPOINT="http://localhost:9000"
S3_BUCKET="mimir-tsdb"

NUM_SERIES=3000
DAYS_BACK=15
BLOCK_DURATION_HOURS=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --num-series) NUM_SERIES="$2"; shift 2 ;;
    --days-back) DAYS_BACK="$2"; shift 2 ;;
    --block-duration-hours) BLOCK_DURATION_HOURS="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

BLOCKS_DIR=$(mktemp -d)
trap 'rm -rf "$BLOCKS_DIR"' EXIT

echo "=== Generating TSDB blocks ==="
echo "  Series:         $NUM_SERIES"
echo "  Days back:      $DAYS_BACK"
echo "  Block duration: ${BLOCK_DURATION_HOURS}h"
echo "  Output dir:     $BLOCKS_DIR"
echo ""

go run "$SCRIPT_DIR" \
  -output-dir="$BLOCKS_DIR" \
  -num-series="$NUM_SERIES" \
  -days-back="$DAYS_BACK" \
  -block-duration-hours="$BLOCK_DURATION_HOURS"

echo ""
echo "=== Uploading blocks to MinIO ==="

# Ensure bucket exists.
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://$S3_BUCKET" 2>/dev/null || true

BLOCK_COUNT=0
for block_dir in "$BLOCKS_DIR"/*/; do
  [ -d "$block_dir" ] || continue
  BLOCK_ULID=$(basename "$block_dir")

  echo "Uploading block $BLOCK_ULID..."
  AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    aws --endpoint-url "$S3_ENDPOINT" s3 cp --recursive --quiet \
    "$block_dir" "s3://$S3_BUCKET/$TENANT/$BLOCK_ULID/"

  BLOCK_COUNT=$((BLOCK_COUNT + 1))
done

echo ""
echo "=== Done ==="
echo "Uploaded $BLOCK_COUNT blocks to s3://$S3_BUCKET/$TENANT/"
echo ""
echo "Wait ~1-2 minutes for store-gateways to sync, then run the OOM-triggering query:"
echo ""
echo '  curl -s "http://localhost:8007/prometheus/api/v1/query" \'
echo '    --data-urlencode "query=max by (asserts_env, asserts_site, cluster, container_runtime_version, internal_ip, kernel_version, kubelet_version, node, os_image, pod_cidr, provider_id, system_uuid) (kube_node_info)" \'
echo "    --data-urlencode \"time=$(date -u +%s)\" \'
echo '    --data-urlencode "step=60" | head -c 500'
echo ""
echo "For the full fan-out (range query over 15 days):"
echo ""
echo '  curl -s "http://localhost:8007/prometheus/api/v1/query_range" \'
echo '    --data-urlencode "query=max by (asserts_env, asserts_site, cluster, container_runtime_version, internal_ip, kernel_version, kubelet_version, node, os_image, pod_cidr, provider_id, system_uuid) (kube_node_info)" \'
echo "    --data-urlencode \"start=$(date -u -v-15d +%s 2>/dev/null || date -u -d '15 days ago' +%s)\" \'
echo "    --data-urlencode \"end=$(date -u +%s)\" \'
echo '    --data-urlencode "step=60" | head -c 500'
