#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Begin of configuration.
K8S_CONTEXT=""
K8S_NAMESPACE=""
MIMIR_TENANT_ID=""
# End of configuration.

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
OUTPUT_DIR="chunks-dump"

mkdir -p "$OUTPUT_DIR"

# Get list of pods.
PODS=$(kubectl --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" get pods --no-headers | grep ingester | awk '{print $1}')

for POD in $PODS; do
  echo "Querying $POD"

  # Open port-forward
  kubectl port-forward --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" "$POD" 9095:9095 &
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
    localhost:9095 "cortex.Ingester/QueryStream" > "$OUTPUT_DIR/$POD"

  kill $KUBECTL_PID
  wait $KUBECTL_PID
done
