#!/usr/bin/env bash

# Config
URL="http://localhost:8080/api/prom"
TENANT_ID="9960"
SHARDS="16"

# Typical rate.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "6H 24H 7d 30d" --shards "${SHARDS}"
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "10d" --shards "16"
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "histogram_quantile(0.99, sum by(le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "16"
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "histogram_quantile(0.99, sum by(verb, resource, le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "16"

# Histograms.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "histogram_quantile(0.99, sum by(le) (rate(tempo_spanmetrics_latency_bucket[5m])))" --time-ranges "1H 6H 24H 2d" --shards "${SHARDS}"

# Analytics.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query 'count({__name__=~".+"})' --time-ranges "5M" --step "5m" --shards "${SHARDS}"
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query 'topk(10, count by(__name__) ({__name__=~".+"}))' --time-ranges "5M" --step "5m" --shards "${SHARDS}"
