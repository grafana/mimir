#!/usr/bin/env bash

# Config
URL="http://localhost:8080/api/prom"
TENANT_ID="9960"
SHARDS="16"

# Typical rate.
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" \
  --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "6H 24H 7d 30d" --shards "${SHARDS}"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "10d" --shards "${SHARDS}"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "histogram_quantile(0.99, sum by(le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "${SHARDS}"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "histogram_quantile(0.99, sum by(verb, resource, le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "${SHARDS}"

# Rate with a very large period (picked from slow queries in prod).
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "avg_over_time(kube_persistentvolume_capacity_bytes[1440m:1m] offset 42330m)" --time-ranges "1H 6H 24H" --shards "${SHARDS}"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "sum(rate(node_cpu_seconds_total[1440m:1m] offset 40865m)) by (kubernetes_node, cluster, mode)" --time-ranges "1H 6H 24H" --shards "${SHARDS}"

# Histograms.
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query "histogram_quantile(0.99, sum by(le) (rate(tempo_spanmetrics_latency_bucket[5m])))" --time-ranges "1H 6H 24H 2d" --shards "${SHARDS}"

# Analytics.
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query 'count({__name__=~".+"})' --time-ranges "5M" --step "5m" --shards "${SHARDS}"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
  --query 'topk(10, count by(__name__) ({__name__=~".+"}))' --time-ranges "5M" --step "5m" --shards "${SHARDS}"

