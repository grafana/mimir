#!/bin/bash

set -eo pipefail

# --- Configuration ---
HELM_CHART_PATH="operations/helm/charts/mimir-distributed"
MIMIR_RELEASE_NAME="mimir-benchmark"
CONTINUOUS_TEST_JOB_NAME="mimir-continuous-test-job"

# --- Helper Functions ---
log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*"
}

usage() {
  echo "Usage: $0 --architecture <amd64|arm64> --kubeconfig <path> --mimir-image-tag <tag> --mimir-continuous-test-image <image_uri> --target-active-series <num> --target-sps <num> --workload-duration <duration>"
  exit 1
}

# --- Parameter Parsing ---
ARCHITECTURE=""
KUBECONFIG_PATH=""
MIMIR_IMAGE_TAG=""
MIMIR_CONTINUOUS_TEST_IMAGE=""
TARGET_ACTIVE_SERIES=""
TARGET_SPS=""
WORKLOAD_DURATION=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --architecture) ARCHITECTURE="$2"; shift ;;
    --kubeconfig) KUBECONFIG_PATH="$2"; shift ;;
    --mimir-image-tag) MIMIR_IMAGE_TAG="$2"; shift ;;
    --mimir-continuous-test-image) MIMIR_CONTINUOUS_TEST_IMAGE="$2"; shift ;;
    --target-active-series) TARGET_ACTIVE_SERIES="$2"; shift ;;
    --target-sps) TARGET_SPS="$2"; shift ;;
    --workload-duration) WORKLOAD_DURATION="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

if [ -z "$ARCHITECTURE" ] || [ -z "$KUBECONFIG_PATH" ] || [ -z "$MIMIR_IMAGE_TAG" ] || \
   [ -z "$MIMIR_CONTINUOUS_TEST_IMAGE" ] || [ -z "$TARGET_ACTIVE_SERIES" ] || \
   [ -z "$TARGET_SPS" ] || [ -z "$WORKLOAD_DURATION" ]; then
  log "ERROR: Missing one or more required parameters."
  usage
fi

export KUBECONFIG="$KUBECONFIG_PATH"
NAMESPACE="${MIMIR_RELEASE_NAME}-${ARCHITECTURE}"
HELM_RELEASE_NAME_NS="${MIMIR_RELEASE_NAME}-${ARCHITECTURE}" # Helm release name, can be same as namespace or different

log "Parameters:"
log "  Architecture: ${ARCHITECTURE}"
log "  Kubeconfig: ${KUBECONFIG_PATH}"
log "  Mimir Image Tag: ${MIMIR_IMAGE_TAG}"
log "  Mimir Continuous Test Image: ${MIMIR_CONTINUOUS_TEST_IMAGE}"
log "  Target Active Series: ${TARGET_ACTIVE_SERIES}"
log "  Target SPS: ${TARGET_SPS}"
log "  Workload Duration: ${WORKLOAD_DURATION}"
log "  Namespace: ${NAMESPACE}"
log "  Helm Release Name: ${HELM_RELEASE_NAME_NS}"

# --- 1. Deploy Mimir using Helm ---
log "Creating namespace ${NAMESPACE} if it doesn't exist..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

NODE_SELECTOR_KEY="kubernetes.io/arch"
NODE_SELECTOR_VALUE="${ARCHITECTURE}"

# Define Mimir components that typically need node selectors and image overrides
# This list should align with the components in your Helm chart
MIMIR_COMPONENTS=(
  "alertmanager"
  "compactor"
  "distributor"
  "ingester"
  "querier"
  "queryFrontend" # Helm charts often use camelCase for value keys
  "storeGateway"  # Helm charts often use camelCase for value keys
  "ruler"
  # "overridesExporter" # If used
  # "queryScheduler" # If used
)
# Minio is often a subchart or has a different values structure
MINIO_COMPONENT="minio"

helm_set_args=()

# Global image settings (assuming chart supports this)
# helm_set_args+=("--set" "global.imageRegistry=grafana") # Assuming grafana/ is the prefix
helm_set_args+=("--set" "global.imageTag=${MIMIR_IMAGE_TAG}")
helm_set_args+=("--set" "global.nodeSelector.${NODE_SELECTOR_KEY}=${NODE_SELECTOR_VALUE}")


# Per-component node selectors and image overrides
# This part is highly dependent on the chart's values.yaml structure.
# We assume a common pattern: componentName.nodeSelector and componentName.image.tag/repository
# For Mimir components, we assume they use a common image like grafana/mimir, grafana/mimir-distributor etc.
# For simplicity in this script, we'll set a global tag and assume the chart uses `grafana/mimir` for main components
# or that component-specific image repos are already correctly set in the chart's default values.
# If images are like `grafana/mimir-ingester`, `grafana/mimir-distributor`, this needs more specific overrides.
# For now, we are mostly relying on global.imageTag and per-component nodeSelector.

# Common Mimir image (if chart uses a single image for multiple components like distributor, ingester etc.)
# helm_set_args+=("--set" "mimir.image.repository=grafana/mimir") # Adjust if needed
# helm_set_args+=("--set" "mimir.image.tag=${MIMIR_IMAGE_TAG}")   # Covered by global.imageTag

for component in "${MIMIR_COMPONENTS[@]}"; do
  # Node selectors
  helm_set_args+=("--set" "${component}.nodeSelector.${NODE_SELECTOR_KEY}=${NODE_SELECTOR_VALUE}")
  # Image overrides - assuming global.imageTag is sufficient for the tag.
  # If specific repositories like grafana/mimir-<component> are needed, add:
  # helm_set_args+=("--set" "${component}.image.repository=grafana/mimir-${component}") # Example
  # helm_set_args+=("--set" "${component}.image.tag=${MIMIR_IMAGE_TAG}") # Covered by global
done

# Replica counts
helm_set_args+=("--set" "ingester.replicas=3")
helm_set_args+=("--set" "distributor.replicas=3")
helm_set_args+=("--set" "queryFrontend.replicas=2")
helm_set_args+=("--set" "querier.replicas=3")
helm_set_args+=("--set" "storeGateway.replicas=3")
helm_set_args+=("--set" "compactor.replicas=1")
# helm_set_args+=("--set" "ruler.replicas=1") # Default usually 1 or 2
# helm_set_args+=("--set" "alertmanager.replicas=1") # Default usually 1 or 2

# MinIO configuration
helm_set_args+=("--set" "minio.enabled=true")
helm_set_args+=("--set" "minio.mode=standalone") # Or distributed, adjust as needed
helm_set_args+=("--set" "minio.replicas=1") # For standalone
helm_set_args+=("--set" "minio.nodeSelector.${NODE_SELECTOR_KEY}=${NODE_SELECTOR_VALUE}")
# helm_set_args+=("--set" "minio.image.tag=${MINIO_IMAGE_TAG}") # If minio needs a specific tag

# Example of setting resources (adjust as needed for benchmark)
# helm_set_args+=("--set" "ingester.resources.requests.cpu=1")
# helm_set_args+=("--set" "ingester.resources.requests.memory=4Gi")

log "Deploying Mimir with Helm..."
echo "Helm arguments: ${helm_set_args[@]}"

helm upgrade --install "${HELM_RELEASE_NAME_NS}" \
  "${HELM_CHART_PATH}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  "${helm_set_args[@]}" \
  --wait --timeout 15m

log "Mimir Helm deployment initiated."

# --- 2. Wait for Mimir pods to be ready ---
log "Waiting for all Mimir pods to be ready..."
# This selector might need adjustment based on the labels set by the Helm chart.
# Common labels are app.kubernetes.io/name, app.kubernetes.io/instance
POD_SELECTOR="app.kubernetes.io/instance=${HELM_RELEASE_NAME_NS}"

# Wait for critical components first
MIMIR_CRITICAL_COMPONENTS=("ingester" "distributor" "querier" "query-frontend" "store-gateway" "compactor" "minio" "alertmanager" "ruler")
for component in "${MIMIR_CRITICAL_COMPONENTS[@]}"; do
    log "Waiting for ${component} pods to be ready..."
    # Adjust label selector if component label is different e.g. app.kubernetes.io/component
    kubectl wait --for=condition=Ready pod \
      -l "${POD_SELECTOR},app.kubernetes.io/component=${component}" \
      -n "${NAMESPACE}" --timeout=10m || {
        log "ERROR: Timeout waiting for ${component} pods to be ready."
        kubectl get pods -n "${NAMESPACE}" -o wide
        kubectl describe pods -n "${NAMESPACE}" -l "${POD_SELECTOR},app.kubernetes.io/component=${component}"
        exit 1
      }
done
log "All Mimir core component pods are ready."

# --- 3. Deploy mimir-continuous-test Job ---
log "Deploying mimir-continuous-test job..."

# Determine Mimir service address (usually within the same namespace)
# This depends on how the chart exposes Mimir, typically distributor or query-frontend
MIMIR_ADDRESS="http://${HELM_RELEASE_NAME_NS}-mimir-distributor.${NAMESPACE}.svc.cluster.local/api/v1/push"
MIMIR_QUERY_ADDRESS="http://${HELM_RELEASE_NAME_NS}-mimir-query-frontend.${NAMESPACE}.svc.cluster.local/prometheus"

# Create Kubernetes Job manifest
# Adjust arguments for mimir-continuous-test as per its CLI
# Using basic arguments here, refer to mimir-continuous-test --help for more
cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${CONTINUOUS_TEST_JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  template:
    metadata:
      labels:
        app: mimir-continuous-test
        architecture: ${ARCHITECTURE}
    spec:
      nodeSelector:
        "${NODE_SELECTOR_KEY}": "${NODE_SELECTOR_VALUE}"
      containers:
      - name: mimir-continuous-test
        image: ${MIMIR_CONTINUOUS_TEST_IMAGE}
        imagePullPolicy: IfNotPresent
        args:
        - "-target.address=${MIMIR_ADDRESS}"
        - "-target.query-address=${MIMIR_QUERY_ADDRESS}"
        - "-target.id=benchmark_user" # Tenant ID
        - "-test.series=${TARGET_ACTIVE_SERIES}"
        - "-test.ingestion-rate=${TARGET_SPS}" # Samples per second
        - "-test.duration=${WORKLOAD_DURATION}"
        - "-test.ingestion-tenants=1" # Number of tenants for ingestion
        # Add other flags as needed, e.g., for query mix, specific metrics
        # - "-target.tls-enabled=false" # If Mimir is not using TLS internally
        # - "-test.query-types=exemplars,label_names,label_values,metric_metadata,rules,series,series_metadata"
        # - "-test.heavy-queries-ratio=0.1"
        # - "-test.query-interval=5s"
        # - "-test.query-parallelism=10"
        resources: # Define resources for the benchmark client if needed
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
      restartPolicy: Never
  backoffLimit: 0 # Do not retry the job on failure
EOF

log "mimir-continuous-test job manifest applied."

# --- 4. Wait for mimir-continuous-test Job to complete ---
log "Waiting for mimir-continuous-test job (${CONTINUOUS_TEST_JOB_NAME}) to complete..."
kubectl wait --for=condition=Complete job/${CONTINUOUS_TEST_JOB_NAME} -n "${NAMESPACE}" --timeout="${WORKLOAD_DURATION}h" || {
  log "ERROR: Timeout or failure waiting for mimir-continuous-test job to complete."
  kubectl get job "${CONTINUOUS_TEST_JOB_NAME}" -n "${NAMESPACE}" -o yaml
  log "Dumping logs from mimir-continuous-test pod(s):"
  PODS=$(kubectl get pods -n "${NAMESPACE}" -l job-name=${CONTINUOUS_TEST_JOB_NAME} -o jsonpath='{.items[*].metadata.name}')
  for pod in $PODS; do
    log "Logs for pod ${pod}:"
    kubectl logs "${pod}" -n "${NAMESPACE}" --tail=1000 || true
  done
  exit 1
}

log "mimir-continuous-test job completed successfully."

# --- 5. Print completion message ---
log "Workload finished for ${ARCHITECTURE}."
log "To inspect results or clean up, use namespace: ${NAMESPACE} and Helm release: ${HELM_RELEASE_NAME_NS}"
log "Example cleanup: helm delete ${HELM_RELEASE_NAME_NS} -n ${NAMESPACE}"
log "                 kubectl delete namespace ${NAMESPACE}"

exit 0
