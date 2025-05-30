#!/bin/bash

set -eo pipefail

# --- Helper Functions ---
log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*"
}

usage() {
  echo "Usage: $0 --kubeconfig <path> --namespace <prometheus_namespace> \\"
  echo "          --prometheus-service <svc_name> --prometheus-port <port> \\"
  echo "          --benchmark-start-time <timestamp_or_rfc3339> \\"
  echo "          --benchmark-end-time <timestamp_or_rfc3339> \\"
  echo "          --output-dir <dir_path>"
  exit 1
}

# --- Parameter Parsing ---
KUBECONFIG_PATH=""
NAMESPACE=""
PROMETHEUS_SERVICE=""
PROMETHEUS_PORT=""
BENCHMARK_START_TIME=""
BENCHMARK_END_TIME=""
OUTPUT_DIR=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --kubeconfig) KUBECONFIG_PATH="$2"; shift ;;
    --namespace) NAMESPACE="$2"; shift ;;
    --prometheus-service) PROMETHEUS_SERVICE="$2"; shift ;;
    --prometheus-port) PROMETHEUS_PORT="$2"; shift ;;
    --benchmark-start-time) BENCHMARK_START_TIME="$2"; shift ;;
    --benchmark-end-time) BENCHMARK_END_TIME="$2"; shift ;;
    --output-dir) OUTPUT_DIR="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

if [ -z "$KUBECONFIG_PATH" ] || [ -z "$NAMESPACE" ] || [ -z "$PROMETHEUS_SERVICE" ] || \
   [ -z "$PROMETHEUS_PORT" ] || [ -z "$BENCHMARK_START_TIME" ] || \
   [ -z "$BENCHMARK_END_TIME" ] || [ -z "$OUTPUT_DIR" ]; then
  log "ERROR: Missing one or more required parameters."
  usage
fi

export KUBECONFIG="$KUBECONFIG_PATH"

log "Parameters:"
log "  Kubeconfig: ${KUBECONFIG_PATH}"
log "  Prometheus Namespace: ${NAMESPACE}"
log "  Prometheus Service: ${PROMETHEUS_SERVICE}"
log "  Prometheus Port: ${PROMETHEUS_PORT}"
log "  Benchmark Start Time: ${BENCHMARK_START_TIME}"
log "  Benchmark End Time: ${BENCHMARK_END_TIME}"
log "  Output Directory: ${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}"

# Convert RFC3339 to Unix timestamp if necessary (Prometheus API prefers Unix timestamps or RFC3339)
# For simplicity, assuming Prometheus API handles RFC3339 directly. If not, conversion needed:
# START_TS=$(date -d "${BENCHMARK_START_TIME}" "+%s" 2>/dev/null || echo "${BENCHMARK_START_TIME}")
# END_TS=$(date -d "${BENCHMARK_END_TIME}" "+%s" 2>/dev/null || echo "${BENCHMARK_END_TIME}")
# Using them directly as Prometheus API v1 usually supports RFC3339 format.
START_TS="${BENCHMARK_START_TIME}"
END_TS="${BENCHMARK_END_TIME}"

# Define PromQL queries
# Note: These queries assume labels 'namespace', 'pod', 'component' are available from Prometheus scrape config.
# The 'namespace' for Mimir components should be the one where Mimir is deployed, which is passed to deploy_prometheus.sh
# For these queries, we assume Prometheus is scraping the correct Mimir namespace.
# The {namespace="${NAMESPACE}"} selector in queries might be redundant if Prometheus *only* scrapes that Mimir namespace.
# However, it's good for explicitness if Prometheus could see other namespaces.
# For container metrics, they are typically cluster-wide but we filter by Mimir's namespace.

declare -A QUERIES

# CPU Usage (rate per core)
QUERIES["mimir_component_cpu_usage_rate_avg_per_pod"]="sum(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"}[5m])) by (pod, component, namespace) / sum(machine_cpu_cores) by (pod, component, namespace) * 100"
QUERIES["mimir_component_cpu_usage_rate_sum_by_component"]="sum(rate(container_cpu_usage_seconds_total{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"}[5m])) by (component, namespace)"

# Memory Usage (working set)
QUERIES["mimir_component_memory_working_set_bytes_avg_per_pod"]="avg by (pod, component, namespace) (container_memory_working_set_bytes{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"})"
QUERIES["mimir_component_memory_working_set_bytes_sum_by_component"]="sum by (component, namespace) (container_memory_working_set_bytes{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"})"

# CPU Throttling
QUERIES["mimir_component_cpu_cfs_throttled_periods_rate_avg_per_pod"]="avg by (pod, component, namespace) (rate(container_cpu_cfs_throttled_periods_total{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"}[5m]))"
QUERIES["mimir_component_cpu_cfs_throttled_periods_rate_sum_by_component"]="sum by (component, namespace) (rate(container_cpu_cfs_throttled_periods_total{namespace=\"${NAMESPACE}\", container!=\"\", pod=~\"mimir-benchmark-.*\"}[5m]))"

# Network I/O
QUERIES["mimir_component_network_receive_bytes_rate_sum_by_component"]="sum(rate(container_network_receive_bytes_total{namespace=\"${NAMESPACE}\", pod=~\"mimir-benchmark-.*\"}[5m])) by (component, namespace)"
QUERIES["mimir_component_network_transmit_bytes_rate_sum_by_component"]="sum(rate(container_network_transmit_bytes_total{namespace=\"${NAMESPACE}\", pod=~\"mimir-benchmark-.*\"}[5m])) by (component, namespace)"

# Disk I/O (if applicable, depends on metrics available)
QUERIES["mimir_component_disk_read_bytes_rate_sum_by_component"]="sum(rate(container_fs_reads_bytes_total{namespace=\"${NAMESPACE}\", pod=~\"mimir-benchmark-.*\"}[5m])) by (component, namespace)" # Example, check metric names
QUERIES["mimir_component_disk_write_bytes_rate_sum_by_component"]="sum(rate(container_fs_writes_bytes_total{namespace=\"${NAMESPACE}\", pod=~\"mimir-benchmark-.*\"}[5m])) by (component, namespace)" # Example, check metric names


# Mimir-specific metrics
QUERIES["mimir_ingester_memory_series"]="sum(mimir_ingester_memory_series{namespace=\"${NAMESPACE}\"}) by (pod, component, namespace)"
QUERIES["mimir_ingester_tsdb_head_active_series"]="sum(mimir_tsdb_head_active_series{namespace=\"${NAMESPACE}\", component=\"ingester\"}) by (pod, component, namespace)"
QUERIES["mimir_distributor_replicated_samples_total_rate"]="sum(rate(mimir_distributor_replicated_samples_total{namespace=\"${NAMESPACE}\"}[5m])) by (pod, component, namespace)"
QUERIES["mimir_ingester_ingested_samples_total_rate"]="sum(rate(mimir_ingester_ingested_samples_total{namespace=\"${NAMESPACE}\"}[5m])) by (pod, component, namespace)"

# Query latencies (example for querier, adjust based on actual metrics)
# Assuming histogram buckets are available, e.g., mimir_request_duration_seconds_bucket
QUERIES["mimir_querier_query_latency_p99"]="histogram_quantile(0.99, sum(rate(mimir_request_duration_seconds_bucket{namespace=\"${NAMESPACE}\", component=\"querier\"}[5m])) by (le, pod, component, namespace))"
QUERIES["mimir_query_frontend_query_latency_p99"]="histogram_quantile(0.99, sum(rate(mimir_request_duration_seconds_bucket{namespace=\"${NAMESPACE}\", component=\"query-frontend\"}[5m])) by (le, pod, component, namespace))"

# Saturation Metrics
QUERIES["mimir_ingester_append_failures_total_rate"]="sum(rate(mimir_ingester_append_failures_total{namespace=\"${NAMESPACE}\"}[5m])) by (pod, component, namespace)"
QUERIES["mimir_distributor_replication_factor"]="avg(mimir_distributor_replication_factor{namespace=\"${NAMESPACE}\"}) by (pod, component, namespace)" # Gauge, no rate

# MinIO metrics (if it's scraped by this Prometheus and labeled with namespace)
QUERIES["minio_disk_storage_free_bytes"]="sum(minio_disk_storage_free_bytes{namespace=\"${NAMESPACE}\"}) by (pod, component, namespace)"
QUERIES["minio_http_requests_total_rate"]="sum(rate(minio_http_requests_total{namespace=\"${NAMESPACE}\"}[5m])) by (pod, component, namespace, method, type)"


# --- Port Forwarding and Querying ---
LOCAL_PROM_PORT=9091 # Choose a local port for port-forwarding
log "Setting up kubectl port-forward to ${PROMETHEUS_SERVICE}:${PROMETHEUS_PORT} on local port ${LOCAL_PROM_PORT}..."

kubectl port-forward "svc/${PROMETHEUS_SERVICE}" -n "${NAMESPACE}" "${LOCAL_PROM_PORT}:${PROMETHEUS_PORT}" &
PORT_FORWARD_PID=$!
log "Port-forward process ID: ${PORT_FORWARD_PID}"

# Allow time for port-forwarding to establish
sleep 5

# Check if port-forward is active
if ! kill -0 $PORT_FORWARD_PID 2>/dev/null; then
  log "ERROR: kubectl port-forward failed to start."
  exit 1
fi

# Cleanup function to kill port-forwarding
cleanup() {
  log "Cleaning up port-forwarding (PID: ${PORT_FORWARD_PID})..."
  kill "${PORT_FORWARD_PID}" || true
  wait "${PORT_FORWARD_PID}" 2>/dev/null || true # Wait for it to actually exit
  log "Cleanup complete."
}
trap cleanup EXIT # Register cleanup function to be called on script exit

PROMETHEUS_API_URL="http://localhost:${LOCAL_PROM_PORT}/api/v1/query_range"
STEP_SECONDS="15" # Corresponds to scrape_interval or a multiple of it

for query_name in "${!QUERIES[@]}"; do
  query="${QUERIES[$query_name]}"
  output_file="${OUTPUT_DIR}/${query_name}.json"

  log "Executing query: ${query_name}"
  log "  Query: ${query}"
  log "  Output: ${output_file}"

  # Using --data-urlencode for query parameters for robustness
  response_code=$(curl -s -o "${output_file}" -w "%{http_code}" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${START_TS}" \
    --data-urlencode "end=${END_TS}" \
    --data-urlencode "step=${STEP_SECONDS}s" \
    "${PROMETHEUS_API_URL}")

  if [ "$response_code" -ge 200 ] && [ "$response_code" -lt 300 ]; then
    log "Query ${query_name} successful (HTTP ${response_code}). Output saved to ${output_file}."
    # Optional: Basic validation of JSON output
    if ! jq -e . "${output_file}" >/dev/null 2>&1; then
        log "WARNING: Output for ${query_name} is not valid JSON. HTTP code was ${response_code}."
        cat "${output_file}" # Print potentially erroneous output
    fi
  else
    log "ERROR: Query ${query_name} failed (HTTP ${response_code})."
    log "  Response body (if any) in ${output_file}:"
    cat "${output_file}" # Print error response from Prometheus
    # Consider exiting or continuing based on desired behavior for partial failures
  fi
  log "---"
done

log "All queries processed. Output files are in ${OUTPUT_DIR}."
# Cleanup is handled by trap EXIT

exit 0
