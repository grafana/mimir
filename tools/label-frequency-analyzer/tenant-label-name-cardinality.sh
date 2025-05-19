#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Begin of configuration.
K8S_CONTEXT="dev-us-central-0"
K8S_NAMESPACE="mimir-dev-14"
# Comma-separated list of tenant IDs to query
TENANT_IDS="9960"
# End of configuration.

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
OUTPUT_DIR="tenant-name-cardinality-dump"

# Randomize port and start from higher ports, so we don't collide with random apps on your laptop.
LOCAL_PORT=$(( ((RANDOM % 49152) + 16384) ))

mkdir -p "$OUTPUT_DIR"

# Print a message in green.
print_success() {
  echo -e "\033[0;32m${1}\033[0m"
}

# Print a message in red.
print_failure() {
  echo -e "\033[0;31m${1}\033[0m"
}

# Start port-forward to cortex-gw-internal
echo "Starting port-forward to cortex-gw-internal..."
kubectl port-forward --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" svc/cortex-gw-internal ${LOCAL_PORT}:80 > /dev/null &
KUBECTL_PID=$!

# Wait for port-forward to be ready
sleep 5

# Function to query cardinality for a single tenant
query_tenant() {
  local tenant_id=$1
  local output_file="$OUTPUT_DIR/tenant-${tenant_id}.json"

  echo "Querying tenant $tenant_id..."

  curl -s \
    -H "X-Scope-OrgID: $tenant_id" \
    -H "Sharding-Control: 128" \
    --data-urlencode 'selector={__name__=~".+"}' \
    "http://localhost:${LOCAL_PORT}/prometheus/api/v1/cardinality/active_series" > "$output_file"

  if [ $? -eq 0 ]; then
    print_success "Successfully queried tenant $tenant_id"
  else
    print_failure "Failed to query tenant $tenant_id"
  fi
}

# Convert comma-separated list to array
IFS=',' read -ra TENANTS <<< "$TENANT_IDS"

# Query each tenant
for tenant in "${TENANTS[@]}"; do
  query_tenant "$tenant"
done

# Cleanup port-forward
kill $KUBECTL_PID > /dev/null
wait $KUBECTL_PID > /dev/null 2> /dev/null

echo ""
echo "Results saved in $OUTPUT_DIR/" 