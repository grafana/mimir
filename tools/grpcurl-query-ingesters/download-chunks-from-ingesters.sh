#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# Begin of configuration.
K8S_CONTEXT=""
K8S_NAMESPACE=""
MIMIR_TENANT_ID=""
# End of configuration.

mkdir -p chunks-dump/

# Get list of pods.
PODS=$(kubectl --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" get pods --no-headers | grep ingester | awk '{print $1}')

for POD in $PODS; do
  echo "Querying $POD"

  # Open port-forward
  kubectl port-forward --context "$K8S_CONTEXT" -n "$K8S_NAMESPACE" "$POD" 9095:9095 &
  KUBECTL_PID=$!

  # Wait some time
  sleep 5

  cat query.json | grpcurl \
    -d @ \
    -H "X-Scope-OrgID: $MIMIR_TENANT_ID" \
  	-proto pkg/ingester/client/ingester.proto \
  	-import-path . \
  	-import-path ./vendor \
  	-plaintext \
  	localhost:9095 "cortex.Ingester/QueryStream" > "chunks-dump/$POD"

  kill $KUBECTL_PID
  wait $KUBECTL_PID
done
