#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

QUERY="test_metric{index=~\"1\\\\\\d\\\\\\d\\\\\\d\"}" # Select all series with index 1000-1999 (ie. 1000 series)
START="2023-09-13T00:03:00Z"
END="2023-09-13T01:03:00Z"
ENCODED_QUERY=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$QUERY'''))")
TARGET_PATH="prometheus/api/v1/query_range?query=$ENCODED_QUERY&start=$START&end=$END&step=15"
DURATION="5m"

MONITORING_QUERIER_URL="http://localhost:8004"

function main() {
  echo "Running load test for $TARGET_PATH continuously over $DURATION."
  echo "Checking both engines produce the same result..."
  checkQueriesProduceSameResult
  echo "No differences, starting tests..."
  runTests
}

function checkQueriesProduceSameResult() {
  standard_result=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "standard")/$TARGET_PATH")
  streaming_result=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "streaming")/$TARGET_PATH")
  thanos_result=$(curl --fail-with-body --show-error --silent "$("$SCRIPT_DIR/target-to-url.sh" "thanos")/$TARGET_PATH")

  diff --color --unified --label "standard" --label "streaming" <(echo "$standard_result" | jq --sort-keys .) <(echo "$streaming_result" | jq --sort-keys .)
  diff --color --unified --label "standard" --label "thanos" <(echo "$standard_result" | jq --sort-keys .) <(echo "$thanos_result" | jq --sort-keys .)
}

function runTests() {
  local summary_lines=()

  for target in standard streaming thanos; do
    echo "Running test for $target engine..."
    local start_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local k6_output_file="k6-result-$target-$(date -u +%Y-%m-%dT%H-%M-%SZ).json"
    "$SCRIPT_DIR/run-one.sh" "$target" "$TARGET_PATH" "$DURATION" "$k6_output_file"
    local end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    throughput=$(jq -r '.metrics.http_reqs.rate' "$k6_output_file")
    avg_latency=$(jq -r '.metrics.http_req_duration.avg' "$k6_output_file")
    min_latency=$(jq -r '.metrics.http_req_duration.min' "$k6_output_file")
    med_latency=$(jq -r '.metrics.http_req_duration.med' "$k6_output_file")
    p90_latency=$(jq -r '.metrics.http_req_duration["p(90)"]' "$k6_output_file")
    p95_latency=$(jq -r '.metrics.http_req_duration["p(95)"]' "$k6_output_file")
    max_latency=$(jq -r '.metrics.http_req_duration.max' "$k6_output_file")

    # FIXME: these queries aren't perfect - they may miss the first few seconds of the test and include time after the end of the test.
    avg_memory=$(metamonitoringQuery "avg_over_time(go_memstats_heap_inuse_bytes{container=\"querier-$target\"}[$DURATION]) / (1024*1024)" "$end_time")
    peak_memory=$(metamonitoringQuery "max_over_time(go_memstats_heap_inuse_bytes{container=\"querier-$target\"}[$DURATION]) / (1024*1024)" "$end_time")
    avg_cpu=$(metamonitoringQuery "avg_over_time(rate(process_cpu_seconds_total{container=\"querier-$target\"}[15s])[$DURATION:15s]) * 100" "$end_time")
    peak_cpu=$(metamonitoringQuery "max_over_time(rate(process_cpu_seconds_total{container=\"querier-$target\"}[15s])[$DURATION:15s]) * 100" "$end_time")

    summary_lines+=("Summary of results for $target engine:")
    summary_lines+=(" - Throughput: $throughput req/s")
    summary_lines+=(" - Latency: avg: $avg_latency ms, min: $min_latency ms, p50: $med_latency ms, p90: $p90_latency ms, p95: $p95_latency ms, max: $max_latency ms")
    summary_lines+=(" - CPU utilisation: avg: $avg_cpu%, max: $peak_cpu%")
    summary_lines+=(" - Memory utilisation: avg: $avg_memory MB, max: $peak_memory MB")
    summary_lines+=(" - Detailed latency data available in $k6_output_file")
    summary_lines+=(" - CPU utilisation over time: http://localhost:3000/explore?orgId=1&left=%7B%22datasource%22:%22mimir%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22mimir%22%7D,%22editorMode%22:%22code%22,%22expr%22:%22rate%28process_cpu_seconds_total%7Bcontainer%3D%5C%22querier-$target%5C%22%7D%5B%24__rate_interval%5D%29%20%2A%20100%22,%22legendFormat%22:%22%7B%7Bcontainer%7D%7D%20CPU%20utilisation%22,%22range%22:true,%22instant%22:true%7D%5D,%22range%22:%7B%22from%22:%22$start_time%22,%22to%22:%22$end_time%22%7D%7D")
    summary_lines+=(" - Memory utilisation over time: http://localhost:3000/explore?orgId=1&left=%7B%22datasource%22:%22mimir%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22mimir%22%7D,%22editorMode%22:%22code%22,%22expr%22:%22go_memstats_heap_inuse_bytes%7Bcontainer%3D%5C%22querier-$target%5C%22%7D%20%2F%20%281024%2A1024%29%22,%22legendFormat%22:%22%7B%7Bcontainer%7D%7D%20memory%20utilisation%20%28MB%29%22,%22range%22:true,%22instant%22:false%7D%5D,%22range%22:%7B%22from%22:%22$start_time%22,%22to%22:%22$end_time%22%7D%7D")
    summary_lines+=("")
  done

  echo "Done."
  echo
  for line in "${summary_lines[@]}"; do
    echo "$line"
  done
}

function metamonitoringQuery() {
  local query="$1"
  local time="$2"

  curl --fail-with-body --show-error --silent -X POST "$MONITORING_QUERIER_URL/prometheus/api/v1/query" -d "query=$query" -d "time=$time" | jq -r '.data.result[0].value[1]'
}

main
