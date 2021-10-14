#!/usr/bin/env bash

# Parse CLI flags.
URL=""
TENANT_ID=""
TSV_HEADER="true"
TEST_QUERY=""
TEST_STEP=""
TEST_SHARDS="4 8 16 32"
TEST_TIME_RANGES="1H 6H 12H 24H"

while [[ $# -gt 0 ]]
do
    case "$1" in
    --url)
        URL=$2
        shift
        shift
        ;;
    --tenant-id)
        TENANT_ID=$2
        shift
        shift
        ;;
    --query)
        TEST_QUERY=$2
        shift
        shift
        ;;
    --step)
        TEST_STEP=$2
        shift
        shift
        ;;
    --shards)
        TEST_SHARDS=$2
        shift
        shift
        ;;
    --time-ranges)
        TEST_TIME_RANGES=$2
        shift
        shift
        ;;
    --tsv-no-header)
        TSV_HEADER="false"
        shift
        ;;
    *)  break
        ;;
    esac
done

if [[ -z "$URL" ]]; then
    usage "No --url provided."
    exit 1
fi

if [[ -z "$TENANT_ID" ]]; then
    echo "No --tenant-id provided."
    exit 1
fi

if [[ -z "$TEST_QUERY" ]]; then
    echo "No --query provided."
    exit 1
fi

DATE_BIN=$(which date)
# This script uses BSD date, the default one on Mac OS.
# Fall back to BSD date if coreutils is used.
# If you're on linux, you probably have GNU date by default,
# and -v param below won't work for you. You can use '-d' instead, 
# but your time ranges will have to be 3HOUR instead of 3H.
if [[ $DATE_BIN == *"coreutils"* ]]; then
    DATE_BIN=/bin/date
fi

# Regex used to get the query response time from the HTTP headers.
RESPONSE_TIME_REGEX="response_time;dur=([0-9\\.]+)"

write_tsv_header() {
  echo -e "Query\tTimerange\tStep\tShards\tResponse time (ms)"
}

# Params:
# $1 - Query
# $2 - Timerange
# $3 - Step
# $4 - Num shards
# $5 - Response time (ms)
write_tsv_line() {
  echo -e "$1\t$2\t$3\t$4\t$5"
}

# Params:
# $1 - Query
# $2 - Timerange
# $3 - Step (if empty it's auto computed based on timerange)
# $4 - Sharding enabled
# $5 - Shards size
benchmark_query() {
  QUERY="$1"
  TIME_RANGE="$2"
  STEP="$3"
  SHARDING_ENABLED="$4"
  SHARD_SIZE="$5"
  START_TIME="$($DATE_BIN -v -${TIME_RANGE} +%s)"
  END_TIME="$($DATE_BIN +%s)"
  HEADERS_FILE=".benchmark-response-headers"

  # Compute the step based on the query time range,
  # in order to have 1000 points in output.
  if [ -z "$STEP" ]; then
    STEP="$(((END_TIME-$START_TIME)/1000))s"
    if [ "$STEP" == "0s" ]; then
      STEP="1s"
    fi
  fi

  # Cleanup any headers file left from a previous run.
  rm -f "${HEADERS_FILE}"

  # Prepare the sharding control header.
  if [ "${SHARDING_ENABLED}" == "yes" ]; then
    SHARDING_CONTROL_HEADER="Sharding-Control: ${SHARD_SIZE}"
  else
    SHARDING_CONTROL_HEADER="Sharding-Control: 0"
  fi

  # Run the query.
  curl \
    --get \
    --silent \
    --fail \
    --data-urlencode "start=${START_TIME}" \
    --data-urlencode "end=${END_TIME}" \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "step=${STEP}" \
    -H "X-Scope-OrgID: ${TENANT_ID}" \
    -H "Cache-Control: no-store" \
    -H "${SHARDING_CONTROL_HEADER}" \
    --dump-header "${HEADERS_FILE}" \
    "${URL}/api/v1/query_range" > /dev/null
  STATUS=$?

  # Parse the response time reported by the server.
  RESPONSE_HEADERS="$(cat ${HEADERS_FILE})"
  if [ $STATUS -ne 0 ]; then
    RESPONSE_TIME_MS="Failed"
  elif [[ "$RESPONSE_HEADERS" =~ ${RESPONSE_TIME_REGEX} ]]; then
    RESPONSE_TIME_MS="${BASH_REMATCH[1]}"
  else
    RESPONSE_TIME_MS="N/A"
  fi

  # Write the TSV entry.
  write_tsv_line "$QUERY" "$TIME_RANGE" "$STEP" "${SHARD_SIZE:-No sharding}" "$RESPONSE_TIME_MS"
}

benchmark_query_with_multiple_runs() {
  for i in {1..3}; do
   benchmark_query "$1" "$2" "$3" "$4" "$5"
  done
}

# Run the benchmark.
if [ "$TSV_HEADER" == "true" ]; then
  write_tsv_header
fi

for TIME_RANGE in $TEST_TIME_RANGES; do
  # Sharding enabled.
  for SHARD_SIZE in $TEST_SHARDS; do
    benchmark_query_with_multiple_runs "$TEST_QUERY" "$TIME_RANGE" "$TEST_STEP" "yes" "${SHARD_SIZE}"
  done

  # Sharding disabled.
  benchmark_query_with_multiple_runs "$TEST_QUERY" "$TIME_RANGE" "$TEST_STEP" "no" ""
done
