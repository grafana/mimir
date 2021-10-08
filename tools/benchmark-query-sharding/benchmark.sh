#!/usr/bin/env bash

# Config
URL="http://localhost:8080/api/prom"
TENANT_ID="10428"
SHARDS="16"

# Typical rate.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" \
#  --query 'sum by(job) (rate(container_cpu_user_seconds_total{id=~".+",name=~".*"}[1m]))' --time-ranges "24H" --shards "${SHARDS}"
#  --query 'sum by(job) (rate(container_cpu_user_seconds_total{id=~".+",name=~".*"}[1m]))' --time-ranges "2H 4H 6H" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" \
#  --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "6H 24H 7d 30d" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "sum by(job) (rate(container_cpu_user_seconds_total[1m]))" --time-ranges "10d" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "histogram_quantile(0.99, sum by(le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "histogram_quantile(0.99, sum by(verb, resource, le) (rate(apiserver_request_duration_seconds_bucket[1m])))" --time-ranges "6H 24H 7d" --shards "${SHARDS}"

# Rate with a very large period (picked from slow queries in prod).
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "avg_over_time(kube_persistentvolume_capacity_bytes[1440m:1m] offset 42330m)" --time-ranges "1H 6H 24H" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "sum(rate(node_cpu_seconds_total[1440m:1m] offset 40865m)) by (kubernetes_node, cluster, mode)" --time-ranges "1H 6H 24H" --shards "${SHARDS}"

# Histograms.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query "histogram_quantile(0.99, sum by(le) (rate(tempo_spanmetrics_latency_bucket[5m])))" --time-ranges "1H 6H 24H 2d" --shards "${SHARDS}"

# Analytics.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query 'count({__name__=~".+"})' --time-ranges "5M" --step "5m" --shards "${SHARDS}"

#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header \
#  --query 'topk(10, count by(__name__) ({__name__=~".+"}))' --time-ranges "5M" --step "5m" --shards "${SHARDS}"

#
# Real slow queries in ops.
#

# 8x speed up with sharding.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header --time-ranges "5M" --step "5m" --shards "${SHARDS}" \
#  --query 'sum(rate(node_cpu_seconds_total[1440m:1m] offset 40865m)) by (kubernetes_node, cluster, mode)'

# 6x speed up with sharding.
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header --time-ranges "1d" --shards "${SHARDS}" \
#  --query 'count(kube_pod_container_info) by (cluster, namespace)'

# Way slower with sharding (see: https://raintank-corp.slack.com/archives/C029912SXT8/p1633707737059500)
#./run.sh --url "${URL}" --tenant-id "${TENANT_ID}" --tsv-no-header --time-ranges "2d" --shards "${SHARDS}" \
#  --query '100 - ( sum (rate(cortex_request_duration_seconds_bucket{namespace="gr-prod-03-mirrored", cluster="prod-us-central-0", le="25.0", route=~"graphite_.*",route!~"graphite_metrics|graphite_config_.*", job=~"gr-.*/cortex-gw"}[5m])) / sum(rate(cortex_request_duration_seconds_count{namespace="gr-prod-03-mirrored", cluster="prod-us-central-0", route=~"graphite_.*",route!~"graphite_metrics|graphite_config_.*", job=~"gr-.*/cortex-gw"}[5m]) ) * 100)'
