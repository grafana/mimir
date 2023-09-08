#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
TARGET_PATH="prometheus/api/v1/query_range?query=up&start=2023-09-08T01:00:00Z&end=2023-09-08T01:30:00Z&step=15"

echo "Checking both engines produce the same result..."

STANDARD_RESULT=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "standard")/$TARGET_PATH")
STREAMING_RESULT=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "streaming")/$TARGET_PATH")

diff --color --unified --label "standard" --label "streaming" <(echo "$STANDARD_RESULT" | jq --sort-keys .)  <(echo "$STREAMING_RESULT" | jq --sort-keys .)

echo "No differences, starting tests..."

SUMMARY_LINES=()

for target in standard streaming; do
  echo "Running test for $target engine..."
  start_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  k6_output_file="k6-result-$target-up-$(date -u +%Y-%m-%dT%H-%M-%SZ).json"
  "$SCRIPT_DIR/run-one.sh" "$target" "$TARGET_PATH" "$k6_output_file"
  end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  throughput=$(jq -r '.metrics.http_reqs.rate' "$k6_output_file")
  avg_latency=$(jq -r '.metrics.http_req_duration.avg' "$k6_output_file")
  min_latency=$(jq -r '.metrics.http_req_duration.min' "$k6_output_file")
  med_latency=$(jq -r '.metrics.http_req_duration.med' "$k6_output_file")
  p90_latency=$(jq -r '.metrics.http_req_duration["p(90)"]' "$k6_output_file")
  p95_latency=$(jq -r '.metrics.http_req_duration["p(95)"]' "$k6_output_file")
  max_latency=$(jq -r '.metrics.http_req_duration.max' "$k6_output_file")

  SUMMARY_LINES+=("Summary of results for $target engine:")
  SUMMARY_LINES+=(" - Throughput: $throughput req/s")
  SUMMARY_LINES+=(" - Latency: avg: $avg_latency ms, min: $min_latency ms, p50: $med_latency ms, p90: $p90_latency ms, p95: $p95_latency ms, max: $max_latency ms")
  SUMMARY_LINES+=(" - Detailed latency data available in $k6_output_file")
  SUMMARY_LINES+=(" - CPU utilisation: http://localhost:3000/explore?orgId=1&left=%7B%22datasource%22:%22mimir%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22mimir%22%7D,%22editorMode%22:%22code%22,%22expr%22:%22rate%28process_cpu_seconds_total%7Bcontainer%3D%5C%22querier-$target%5C%22%7D%5B%24__rate_interval%5D%29%22,%22legendFormat%22:%22%7B%7Bcontainer%7D%7D%20CPU%20utilisation%22,%22range%22:true,%22instant%22:true%7D%5D,%22range%22:%7B%22from%22:%22$start_time%22,%22to%22:%22$end_time%22%7D%7D")
  SUMMARY_LINES+=(" - Memory utilisation: http://localhost:3000/explore?orgId=1&left=%7B%22datasource%22:%22mimir%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22mimir%22%7D,%22editorMode%22:%22code%22,%22expr%22:%22go_memstats_heap_inuse_bytes%7Bcontainer%3D%5C%22querier-$target%5C%22%7D%20%2F%20%281024%2A1024%29%22,%22legendFormat%22:%22%7B%7Bcontainer%7D%7D%20memory%20utilisation%20%28MB%29%22,%22range%22:true,%22instant%22:false%7D%5D,%22range%22:%7B%22from%22:%22$start_time%22,%22to%22:%22$end_time%22%7D%7D")
  SUMMARY_LINES+=("")
done

echo "Done."
echo
for line in "${SUMMARY_LINES[@]}"; do
  echo "$line"
done
