#!/bin/bash

set -eo pipefail

# --- Script paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_BENCHMARK_SETUP_SCRIPT="${SCRIPT_DIR}/run_benchmark_setup.sh"
DEPLOY_PROMETHEUS_SCRIPT="${SCRIPT_DIR}/deploy_prometheus_for_benchmark.sh"
QUERY_METRICS_SCRIPT="${SCRIPT_DIR}/query_benchmark_metrics.sh"

# --- Helper Functions ---
log() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%S%z')]: Orchestrator: $*"
}

usage() {
  echo "Usage: $0 --architecture <amd64|arm64> --kubeconfig <path> \\"
  echo "          --mimir-image-tag <tag> --mimir-continuous-test-image <image_uri> \\"
  echo "          --target-active-series <num> --target-sps <num> \\"
  echo "          --workload-duration <duration> --output-dir <base_path>"
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
BASE_OUTPUT_DIR=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --architecture) ARCHITECTURE="$2"; shift ;;
    --kubeconfig) KUBECONFIG_PATH="$2"; shift ;;
    --mimir-image-tag) MIMIR_IMAGE_TAG="$2"; shift ;;
    --mimir-continuous-test-image) MIMIR_CONTINUOUS_TEST_IMAGE="$2"; shift ;;
    --target-active-series) TARGET_ACTIVE_SERIES="$2"; shift ;;
    --target-sps) TARGET_SPS="$2"; shift ;;
    --workload-duration) WORKLOAD_DURATION="$2"; shift ;;
    --output-dir) BASE_OUTPUT_DIR="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

if [ -z "$ARCHITECTURE" ] || [ -z "$KUBECONFIG_PATH" ] || [ -z "$MIMIR_IMAGE_TAG" ] || \
   [ -z "$MIMIR_CONTINUOUS_TEST_IMAGE" ] || [ -z "$TARGET_ACTIVE_SERIES" ] || \
   [ -z "$TARGET_SPS" ] || [ -z "$WORKLOAD_DURATION" ] || [ -z "$BASE_OUTPUT_DIR" ]; then
  log "ERROR: Missing one or more required parameters."
  usage
fi

export KUBECONFIG="$KUBECONFIG_PATH"
MIMIR_BENCHMARK_NAMESPACE="mimir-benchmark-${ARCHITECTURE}"
ARCH_OUTPUT_DIR="${BASE_OUTPUT_DIR}/${ARCHITECTURE}"
CONTINUOUS_TEST_JOB_NAME="mimir-continuous-test-job" # Must match what's in run_benchmark_setup.sh

# --- Cleanup Function ---
cleanup() {
  log "Initiating cleanup for namespace: ${MIMIR_BENCHMARK_NAMESPACE}..."
  if kubectl get namespace "${MIMIR_BENCHMARK_NAMESPACE}" > /dev/null 2>&1; then
    log "Deleting namespace ${MIMIR_BENCHMARK_NAMESPACE}..."
    kubectl delete namespace "${MIMIR_BENCHMARK_NAMESPACE}" --wait=true || \
      log "WARN: Failed to delete namespace ${MIMIR_BENCHMARK_NAMESPACE}. Manual cleanup might be required."
  else
    log "Namespace ${MIMIR_BENCHMARK_NAMESPACE} does not exist or already deleted."
  fi
  log "Cleanup finished."
}

# Trap ERR and EXIT signals to ensure cleanup is called
trap 'cleanup_exit_code=$?; log "Benchmark run finished with exit code: ${cleanup_exit_code}. Triggering cleanup..."; cleanup; exit ${cleanup_exit_code};' EXIT
trap 'log "Received ERR signal. Triggering cleanup..."; cleanup; exit 1;' ERR
trap 'log "Received INT signal. Triggering cleanup..."; cleanup; exit 1;' INT
trap 'log "Received TERM signal. Triggering cleanup..."; cleanup; exit 1;' TERM


log "Starting benchmark orchestration for architecture: ${ARCHITECTURE}"
log "Mimir Namespace: ${MIMIR_BENCHMARK_NAMESPACE}"
log "Output Directory: ${ARCH_OUTPUT_DIR}"

# 1. Create output directory
log "Creating output directory: ${ARCH_OUTPUT_DIR}"
mkdir -p "${ARCH_OUTPUT_DIR}"

# 2. Deploy Prometheus
log "Step 1: Deploying Prometheus..."
# The deploy_prometheus_for_benchmark.sh script outputs "Prometheus Service: <name>:<port>"
prometheus_service_info=$(bash "${DEPLOY_PROMETHEUS_SCRIPT}" \
  --kubeconfig "${KUBECONFIG_PATH}" \
  --namespace "${MIMIR_BENCHMARK_NAMESPACE}" \
  --architecture "${ARCHITECTURE}")

# Extract service name and port
PROMETHEUS_FULL_SERVICE=$(echo "${prometheus_service_info}" | grep "Prometheus Service:" | awk -F 'Service: ' '{print $2}')
PROMETHEUS_SERVICE_NAME=$(echo "${PROMETHEUS_FULL_SERVICE}" | awk -F ':' '{print $1}')
PROMETHEUS_SERVICE_PORT=$(echo "${PROMETHEUS_FULL_SERVICE}" | awk -F ':' '{print $2}')

if [ -z "${PROMETHEUS_SERVICE_NAME}" ] || [ -z "${PROMETHEUS_SERVICE_PORT}" ]; then
  log "ERROR: Failed to get Prometheus service details. Output was: ${prometheus_service_info}"
  exit 1
fi
log "Prometheus deployed. Service: ${PROMETHEUS_SERVICE_NAME}, Port: ${PROMETHEUS_SERVICE_PORT}"

# 3. Run Mimir Setup and Workload
log "Step 2: Deploying Mimir and starting workload generation via run_benchmark_setup.sh..."
# run_benchmark_setup.sh will block until the mimir-continuous-test job completes.
bash "${RUN_BENCHMARK_SETUP_SCRIPT}" \
  --architecture "${ARCHITECTURE}" \
  --kubeconfig "${KUBECONFIG_PATH}" \
  --mimir-image-tag "${MIMIR_IMAGE_TAG}" \
  --mimir-continuous-test-image "${MIMIR_CONTINUOUS_TEST_IMAGE}" \
  --target-active-series "${TARGET_ACTIVE_SERIES}" \
  --target-sps "${TARGET_SPS}" \
  --workload-duration "${WORKLOAD_DURATION}"
  # This script already creates the Mimir namespace if it doesn't exist.

log "Workload generation via run_benchmark_setup.sh has completed."

# 4. Capture Benchmark Start and End Times from the Kubernetes Job
log "Step 3: Capturing actual workload start and end times from Kubernetes Job..."
BENCHMARK_JOB_START_TIME_RFC3339=$(kubectl get job "${CONTINUOUS_TEST_JOB_NAME}" -n "${MIMIR_BENCHMARK_NAMESPACE}" -o jsonpath='{.status.startTime}')
BENCHMARK_JOB_COMPLETION_TIME_RFC3339=$(kubectl get job "${CONTINUOUS_TEST_JOB_NAME}" -n "${MIMIR_BENCHMARK_NAMESPACE}" -o jsonpath='{.status.completionTime}')

if [ -z "${BENCHMARK_JOB_START_TIME_RFC3339}" ] || [ -z "${BENCHMARK_JOB_COMPLETION_TIME_RFC3339}" ]; then
  log "ERROR: Failed to retrieve start/completion time for job ${CONTINUOUS_TEST_JOB_NAME} in namespace ${MIMIR_BENCHMARK_NAMESPACE}."
  log "Attempting to get job details for debugging:"
  kubectl get job "${CONTINUOUS_TEST_JOB_NAME}" -n "${MIMIR_BENCHMARK_NAMESPACE}" -o yaml || true
  exit 1
fi
log "Actual Workload Start Time (from K8s Job): ${BENCHMARK_JOB_START_TIME_RFC3339}"
log "Actual Workload Completion Time (from K8s Job): ${BENCHMARK_JOB_COMPLETION_TIME_RFC3339}"

# Save these times to a file in the output directory for record keeping
echo "BENCHMARK_JOB_START_TIME_RFC3339=${BENCHMARK_JOB_START_TIME_RFC3339}" > "${ARCH_OUTPUT_DIR}/benchmark_times.txt"
echo "BENCHMARK_JOB_COMPLETION_TIME_RFC3339=${BENCHMARK_JOB_COMPLETION_TIME_RFC3339}" >> "${ARCH_OUTPUT_DIR}/benchmark_times.txt"

# 5. Query Benchmark Metrics
log "Step 4: Querying benchmark metrics..."
bash "${QUERY_METRICS_SCRIPT}" \
  --kubeconfig "${KUBECONFIG_PATH}" \
  --namespace "${MIMIR_BENCHMARK_NAMESPACE}" \
  --prometheus-service "${PROMETHEUS_SERVICE_NAME}" \
  --prometheus-port "${PROMETHEUS_SERVICE_PORT}" \
  --benchmark-start-time "${BENCHMARK_JOB_START_TIME_RFC3339}" \
  --benchmark-end-time "${BENCHMARK_JOB_COMPLETION_TIME_RFC3339}" \
  --output-dir "${ARCH_OUTPUT_DIR}/metrics_data" # Save metrics in a sub-folder

log "Metrics querying complete. Results are in ${ARCH_OUTPUT_DIR}/metrics_data"

log "Benchmark run for architecture ${ARCHITECTURE} completed successfully."
# Cleanup will be handled by the EXIT trap automatically.
exit 0
