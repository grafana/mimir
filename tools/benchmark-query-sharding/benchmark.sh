#!/usr/bin/env bash

# Config
URL="http://localhost:8080/api/prom"
TENANT_ID="9960"
SHARDS="16"

./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "6H 24H 7d 30d" --shards "${SHARDS}"
./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --query "histogram_quantile(0.99, sum by(le) (rate(tempo_spanmetrics_latency_bucket[5m])))" --time-ranges "1H 6H 24H 2d" --shards "${SHARDS}"
