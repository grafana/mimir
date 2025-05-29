#!/bin/bash

set -eo pipefail

# --- Configuration ---
PROMETHEUS_RELEASE_NAME="prometheus-benchmark"
PROMETHEUS_IMAGE="prom/prometheus:v2.47.2" # Specify a recent, stable version
PROMETHEUS_CONFIG_MAP_NAME="${PROMETHEUS_RELEASE_NAME}-config"
PROMETHEUS_DEPLOYMENT_NAME="${PROMETHEUS_RELEASE_NAME}-deployment"
PROMETHEUS_SERVICE_NAME="${PROMETHEUS_RELEASE_NAME}-service"
PROMETHEUS_SERVICE_PORT="80" # Internal port for Prometheus service, targetPort will be 9090
PROMETHEUS_DATA_DIR="/prometheus"
SCRAPE_INTERVAL="15s"

# --- Helper Functions ---
log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*"
}

usage() {
  echo "Usage: $0 --kubeconfig <path> --namespace <mimir_namespace> --architecture <amd64|arm64>"
  exit 1
}

# --- Parameter Parsing ---
KUBECONFIG_PATH=""
NAMESPACE="" # This is the Mimir namespace to scrape
ARCHITECTURE=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --kubeconfig) KUBECONFIG_PATH="$2"; shift ;;
    --namespace) NAMESPACE="$2"; shift ;;
    --architecture) ARCHITECTURE="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

if [ -z "$KUBECONFIG_PATH" ] || [ -z "$NAMESPACE" ] || [ -z "$ARCHITECTURE" ]; then
  log "ERROR: Missing one or more required parameters."
  usage
fi

export KUBECONFIG="$KUBECONFIG_PATH"

log "Parameters:"
log "  Kubeconfig: ${KUBECONFIG_PATH}"
log "  Mimir Namespace (to scrape): ${NAMESPACE}"
log "  Prometheus Architecture: ${ARCHITECTURE}"

NODE_SELECTOR_KEY="kubernetes.io/arch"

# --- Create Prometheus ConfigMap ---
log "Creating Prometheus ConfigMap: ${PROMETHEUS_CONFIG_MAP_NAME} in namespace ${NAMESPACE}..."
cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${PROMETHEUS_CONFIG_MAP_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: prometheus-benchmark
data:
  prometheus.yml: |
    global:
      scrape_interval: ${SCRAPE_INTERVAL}
      evaluation_interval: 1m
    scrape_configs:
      - job_name: 'mimir-pods'
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names:
                - ${NAMESPACE}
        relabel_configs:
          # Scrape only pods in the target Mimir namespace that have prometheus.io/scrape=true
          - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
            action: keep
            regex: true
          # Mimir specific: only scrape pods that are part of the Mimir instance
          # This assumes Mimir pods are labeled with app.kubernetes.io/name=mimir or similar
          # and app.kubernetes.io/instance=<helm_release_name_for_mimir>
          # Adjust if Mimir helm chart uses different labels for selection
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
            action: keep
            regex: (mimir|minio) # Keep mimir and minio pods; minio often part of mimir stack
          # Allow specifying port via annotation
          - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
            action: replace
            target_label: __address__
            regex: (\d+)
            replacement: \${1}:\$1
          # Or use a default port if annotation is not present (e.g. http-metrics, grpc-metrics)
          # This might need refinement based on actual Mimir pod metric port names
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: (http-metrics|grpc-metrics|metrics) # Common metrics port names
          # Relabel namespace and pod name
          - source_labels: [__meta_kubernetes_namespace]
            target_label: namespace
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
            target_label: component

      - job_name: 'kubernetes-nodes-cadvisor'
        kubernetes_sd_configs:
          - role: node
        scheme: https
        tls_config:
          ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
          insecure_skip_verify: false # Set to true if strictly necessary and understand risks
        bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
        relabel_configs:
          - action: labelmap
            regex: __meta_kubernetes_node_label_(.+)
          - target_label: __address__
            replacement: kubernetes.default.svc:443 # kube-apiserver address
          - source_labels: [__meta_kubernetes_node_name]
            regex: (.+)
            target_label: __metrics_path__
            replacement: /api/v1/nodes/\${1}/proxy/metrics/cadvisor
EOF

# --- Create Prometheus Deployment ---
log "Creating Prometheus Deployment: ${PROMETHEUS_DEPLOYMENT_NAME} in namespace ${NAMESPACE}..."
cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${PROMETHEUS_DEPLOYMENT_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: prometheus-benchmark
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus-benchmark
      deployment: ${PROMETHEUS_DEPLOYMENT_NAME}
  template:
    metadata:
      labels:
        app: prometheus-benchmark
        deployment: ${PROMETHEUS_DEPLOYMENT_NAME}
    spec:
      nodeSelector:
        "${NODE_SELECTOR_KEY}": "${ARCHITECTURE}"
      containers:
        - name: prometheus
          image: ${PROMETHEUS_IMAGE}
          args:
            - "--config.file=/etc/prometheus/prometheus.yml"
            - "--storage.tsdb.path=${PROMETHEUS_DATA_DIR}"
            - "--web.console.libraries=/usr/share/prometheus/console_libraries"
            - "--web.console.templates=/usr/share/prometheus/consoles"
            - "--web.enable-lifecycle" # Allows /-/reload via HTTP POST
          ports:
            - name: http
              containerPort: 9090
          volumeMounts:
            - name: config-volume
              mountPath: /etc/prometheus
            - name: data-volume
              mountPath: ${PROMETHEUS_DATA_DIR}
          resources: # Adjust as needed for Prometheus itself
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
      volumes:
        - name: config-volume
          configMap:
            name: ${PROMETHEUS_CONFIG_MAP_NAME}
        - name: data-volume # Consider PersistentVolume for longer retention if needed
          emptyDir: {}
      serviceAccountName: default # Ensure this SA has rights for k8s_sd_configs (nodes role)
      # If RBAC is strict, you might need a dedicated ServiceAccount with ClusterRoleBinding
      # for nodes/proxy and pods/metrics scraping. For now, assuming default SA might work in some envs.
      # For nodes/cadvisor, the 'system:kubelet-api-admin' ClusterRole or similar is often needed.
      # This typically means Prometheus SA needs a ClusterRoleBinding to a role that can access node proxy.
EOF

# --- Create Prometheus Service ---
log "Creating Prometheus Service: ${PROMETHEUS_SERVICE_NAME} in namespace ${NAMESPACE}..."
cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: Service
metadata:
  name: ${PROMETHEUS_SERVICE_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: prometheus-benchmark
spec:
  selector:
    app: prometheus-benchmark
    deployment: ${PROMETHEUS_DEPLOYMENT_NAME}
  ports:
    - name: http
      port: ${PROMETHEUS_SERVICE_PORT}
      targetPort: 9090
  type: ClusterIP # Or NodePort/LoadBalancer if external access is needed directly
EOF

# --- Wait for Prometheus pod to be ready ---
log "Waiting for Prometheus pod to be ready..."
kubectl wait --for=condition=Ready pod \
  -l "app=prometheus-benchmark,deployment=${PROMETHEUS_DEPLOYMENT_NAME}" \
  -n "${NAMESPACE}" --timeout=5m || {
    log "ERROR: Timeout waiting for Prometheus pod to be ready."
    kubectl get pods -n "${NAMESPACE}" -l "app=prometheus-benchmark,deployment=${PROMETHEUS_DEPLOYMENT_NAME}" -o wide
    kubectl describe pods -n "${NAMESPACE}" -l "app=prometheus-benchmark,deployment=${PROMETHEUS_DEPLOYMENT_NAME}"
    exit 1
  }

log "Prometheus deployment completed and pod is ready."
log "Prometheus Service: ${PROMETHEUS_SERVICE_NAME}:${PROMETHEUS_SERVICE_PORT}"

exit 0
