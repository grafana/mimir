#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Begin of configuration.
K8S_CONTEXT="dev-us-central-0"
K8S_NAMESPACE="cortex-dev-01"
MIMIR_TENANT_ID="9960"
# End of configuration.

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
OUTPUT_DIR="chunks-dump"
NEXT_PORT=9010

# File used to keep track of the list of ingesters failed to be queried.
# Reset it each time this script is called.
FAILURES_TRACKING_FILE="${SCRIPT_DIR}/${OUTPUT_DIR}/.failures"
echo -n "" > "${FAILURES_TRACKING_FILE}"

mkdir -p "$OUTPUT_DIR"

# Print a message in green.
print_success() {
  echo -e "\033[0;32m${1}\033[0m"
}

# Print a message in red.
print_failure() {
  echo -e "\033[0;31m${1}\033[0m"
}

# Utility function to query a single ingester.
#
# Parameters:
# - $1: The pod ID
# - $2: The local port to use
query_ingester() {
  POD=$1
  LOCAL_PORT=$2

  echo "Querying $POD"

  # Open port-forward
  kubectl port-forward --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" "$POD" ${LOCAL_PORT}:9095 > /dev/null &
  KUBECTL_PID=$!

  # Wait some time
  sleep 5

  # HACK
  # If you get an error resolving the reference to "github.com/grafana/mimir/pkg/mimirpb/mimir.proto" in
  # pkg/ingester/client/ingester.proto, you need to manually modify the import statement to be just
  # "pkg/mimirpb/mimir.proto".
  cat "$SCRIPT_DIR/download-chunks-from-ingesters-query.json" | grpcurl \
    -d @ \
    -H "X-Scope-OrgID: $MIMIR_TENANT_ID" \
    -proto pkg/ingester/client/ingester.proto \
    -import-path "$SCRIPT_DIR/../.." \
    -import-path "$SCRIPT_DIR/../../vendor" \
    -plaintext \
    localhost:${LOCAL_PORT} "cortex.Ingester/QueryStream" > "$OUTPUT_DIR/$POD"
  STATUS_CODE=$?

  kill $KUBECTL_PID > /dev/null
  wait $KUBECTL_PID > /dev/null 2> /dev/null

  if [ $STATUS_CODE -eq 0 ]; then
    print_success "Successfully queried $POD"
  else
    print_failure "Failed to query $POD"

    # Keep track of the failure.
    echo "$POD" >> "${FAILURES_TRACKING_FILE}"
  fi
}

# Get list of pods.
PODS=$(kubectl --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" get pods --no-headers | grep ingester | grep 28 | awk '{print $1}')

# Concurrently query ingesters.
for POD in $PODS; do
  query_ingester "${POD}" "${NEXT_PORT}" &

  NEXT_PORT=$((NEXT_PORT+1))

  # Throttle to reduce the likelihood of networking issues and K8S rate limiting.
  sleep 0.25
done

# Wait for all background jobs to finish
wait

# Print final report.
echo ""
echo ""

if [ ! -s "${FAILURES_TRACKING_FILE}" ]; then
  print_success "Successfully queried all ingesters"
  exit 0
else
  # Count the number of failed ingesters.
  FAILURES_COUNT=$(wc -l "${FAILURES_TRACKING_FILE}" | awk '{print $1}')

  print_failure "Failed to query $FAILURES_COUNT ingesters:"

  # Print the list of failed ingesters.
  sort < "${FAILURES_TRACKING_FILE}" | sed 's/^/- /g'

  exit 1
fi
