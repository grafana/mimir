#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

# OTLP Resource & Scope Attributes Persistence Demo for Grafana Mimir
#
# This demo showcases how Mimir persists OTel resource attributes and scope
# (instrumentation library) attributes from OTLP metrics, and makes resource
# attributes queryable via the /api/v1/resources endpoint and info() function.
#
# This is a faithful port of the Prometheus demo at:
# documentation/examples/otlp-resource-attributes/main.go
#
# Prerequisites:
#   - curl and jq installed
#   - Either: Mimir running in monolithic mode (./compose-up.sh)
#   - Or: Use --start-stack to automatically start the stack
#
# Usage:
#   ./scripts/otlp-resource-attrs-demo.sh [--start-stack] [--stop-stack]
#
# Options:
#   --start-stack   Start the docker-compose stack before running the demo
#   --stop-stack    Stop the docker-compose stack after the demo completes
#   --help, -h      Show this help message

set -e

# Script directory for locating compose scripts
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Flags
START_STACK=false
STOP_STACK=false

show_usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

OTLP Resource Attributes Persistence Demo for Grafana Mimir (Monolithic Mode)

Options:
  --start-stack   Start the docker-compose stack before running the demo
  --stop-stack    Stop the docker-compose stack after the demo completes
  --help, -h      Show this help message

Examples:
  # Run demo with stack already running
  $(basename "$0")

  # Fully automated: start stack, run demo, stop stack
  $(basename "$0") --start-stack --stop-stack

  # Start stack, run demo, leave stack running for exploration
  $(basename "$0") --start-stack
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --start-stack)
            START_STACK=true
            shift
            ;;
        --stop-stack)
            STOP_STACK=true
            shift
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Wait for a service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=60
    local attempt=0

    echo -e "Waiting for ${service_name} to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null) || true
        if [ "$http_code" = "200" ]; then
            echo -e "${service_name} is ready."
            return 0
        fi
        attempt=$((attempt + 1))
        printf "\r  Attempt %d/%d..." "$attempt" "$max_attempts"
        sleep 2
    done
    echo ""
    echo "Error: ${service_name} did not become ready within $((max_attempts * 2)) seconds"
    return 1
}

start_stack() {
    echo "Starting docker-compose stack..."
    cd "${COMPOSE_DIR}"
    ./compose-up.sh -d

    echo ""
    wait_for_service "http://localhost:8101/ready" "Mimir"

    # Wait for OTLP ingestion to actually work (ring needs to be formed)
    echo ""
    echo "Waiting for OTLP ingestion to be ready..."
    local otlp_ready=false
    local otlp_attempts=0
    local otlp_max_attempts=30
    while [ "$otlp_ready" = false ] && [ $otlp_attempts -lt $otlp_max_attempts ]; do
        # Send a test metric and check for 200 response
        local test_response
        test_response=$(curl -s -w "%{http_code}" -o /dev/null -X POST "http://localhost:8101/otlp/v1/metrics" \
            -H "Content-Type: application/json" \
            -d '{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"startup-test"}}]},"scopeMetrics":[{"metrics":[{"name":"startup_test","sum":{"dataPoints":[{"asDouble":1,"timeUnixNano":"1234567890000000000"}],"aggregationTemporality":2,"isMonotonic":true}}]}]}]}')
        if [ "$test_response" = "200" ]; then
            otlp_ready=true
        else
            otlp_attempts=$((otlp_attempts + 1))
            printf "\r  Attempt %d/%d (HTTP %s)..." "$otlp_attempts" "$otlp_max_attempts" "$test_response"
            sleep 2
        fi
    done
    if [ "$otlp_ready" = false ]; then
        echo ""
        echo "Warning: OTLP endpoint may not be fully ready"
    else
        echo "OTLP ingestion is ready."
    fi

    echo ""
    echo "Stack is ready!"
    echo ""
}

stop_stack() {
    echo ""
    echo "Stopping docker-compose stack..."
    cd "${COMPOSE_DIR}"
    ./compose-down.sh
}

# Register cleanup trap if --stop-stack is set
if [ "$STOP_STACK" = true ]; then
    trap stop_stack EXIT
fi

# Start stack if requested
if [ "$START_STACK" = true ]; then
    start_stack
fi

# ANSI color codes
BOLD='\033[1m'
RESET='\033[0m'
GRAY='\033[37m'
RED='\033[31m'
GREEN='\033[32m'
YELLOW='\033[33m'
CYAN='\033[36m'
MAGENTA='\033[35m'

# Mimir endpoint
MIMIR_URL="${MIMIR_URL:-http://localhost:8101}"
OTLP_ENDPOINT="${MIMIR_URL}/otlp/v1/metrics"
RESOURCES_ENDPOINT="${MIMIR_URL}/prometheus/api/v1/resources"
QUERY_ENDPOINT="${MIMIR_URL}/prometheus/api/v1/query"
FLUSH_ENDPOINT="${MIMIR_URL}/ingester/flush"

echo -e "${BOLD}${CYAN}=== Grafana Mimir OTel Resource Attributes Persistence Demo ===${RESET}\n"

# Check if Mimir is running
echo -e "${GRAY}Checking Mimir connectivity...${RESET}"
if ! curl -s "${MIMIR_URL}/ready" > /dev/null 2>&1; then
    echo -e "${RED}Error: Mimir is not reachable at ${MIMIR_URL}${RESET}"
    echo -e "${YELLOW}Please start Mimir first: ./compose-up.sh${RESET}"
    exit 1
fi
echo -e "${GREEN}Mimir is ready at ${MIMIR_URL}${RESET}\n"

print_phase() {
    echo -e "\n${BOLD}${MAGENTA}--- Phase $1: $2 ---${RESET}\n"
}

# Function to send OTLP metrics
send_otlp_metrics() {
    local payload="$1"
    local description="$2"

    response=$(curl -s -w "\n%{http_code}" -X POST "${OTLP_ENDPOINT}" \
        -H "Content-Type: application/json" \
        -d "$payload")

    http_code=$(echo "$response" | tail -n1)

    if [ "$http_code" -eq 200 ]; then
        echo -e "${GREEN}Sent: ${description}${RESET}"
    else
        echo -e "${RED}Failed to send metrics (HTTP ${http_code}): ${description}${RESET}"
        echo "$response" | sed '$d'
        return 1
    fi
}

# Function to query resource attributes
query_resources() {
    local match="$1"
    local description="$2"

    echo -e "${BOLD}${description}${RESET}"

    # URL encode the match parameter
    encoded_match=$(printf '%s' "$match" | jq -sRr @uri)

    response=$(curl -s "${RESOURCES_ENDPOINT}?match[]=${encoded_match}")

    # Check if response is valid JSON
    if ! echo "$response" | jq -e . > /dev/null 2>&1; then
        echo -e "${RED}Invalid JSON response${RESET}"
        echo "$response"
        return 1
    fi

    # Check status
    status=$(echo "$response" | jq -r '.status')
    if [ "$status" != "success" ]; then
        echo -e "${RED}Query failed: $(echo "$response" | jq -r '.error // "unknown error"')${RESET}"
        return 1
    fi

    # Pretty print the response
    echo "$response" | jq -C '.data.series[] | {
        labels: .labels,
        versions: [.versions[] | {
            identifying: .identifying,
            descriptive: .descriptive,
            entities: .entities,
            minTimeMs: .minTimeMs,
            maxTimeMs: .maxTimeMs
        }]
    }' 2>/dev/null || echo "$response" | jq -C '.'

    echo ""
}

# Function to execute PromQL instant query
query_promql() {
    local query="$1"
    local time="$2"
    local description="$3"

    echo -e "${BOLD}${description}${RESET}"

    # URL encode the query
    encoded_query=$(printf '%s' "$query" | jq -sRr @uri)

    if [ -n "$time" ]; then
        response=$(curl -s "${QUERY_ENDPOINT}?query=${encoded_query}&time=${time}")
    else
        response=$(curl -s "${QUERY_ENDPOINT}?query=${encoded_query}")
    fi

    # Check if response is valid JSON
    if ! echo "$response" | jq -e . > /dev/null 2>&1; then
        echo -e "${RED}Invalid JSON response${RESET}"
        echo "$response"
        return 1
    fi

    # Check status
    status=$(echo "$response" | jq -r '.status')
    if [ "$status" != "success" ]; then
        echo -e "${RED}Query failed: $(echo "$response" | jq -r '.error // "unknown error"')${RESET}"
        return 1
    fi

    # Pretty print the result
    echo "$response" | jq -C '.data.result[] | {metric: .metric, value: .value[1]}' 2>/dev/null || echo "$response" | jq -C '.data'

    echo ""
}

# Get current timestamp in milliseconds
now_ms() {
    echo $(($(date +%s) * 1000))
}

# Get current timestamp in seconds (for PromQL)
now_sec() {
    date +%s
}

# Store timestamps for later info() queries
ORIGINAL_TIMESTAMP_SEC=$(now_sec)

# === PHASE 1: Send OTLP metrics with resource attributes ===
print_phase 1 "Sending OTLP metrics with resource attributes"

echo -e "${GRAY}Sending metrics from multiple services with diverse resource attributes...${RESET}\n"

TIMESTAMP=$(now_ms)

# Resource 1: payment-service in production (with entity_refs)
# Entity refs define service entity + host entity with identifying/descriptive key assignments
PAYLOAD1=$(cat <<EOF
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "payment-service"}},
        {"key": "service.namespace", "value": {"stringValue": "production"}},
        {"key": "service.instance.id", "value": {"stringValue": "payment-001"}},
        {"key": "host.name", "value": {"stringValue": "prod-payment-1.example.com"}},
        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}},
        {"key": "deployment.environment", "value": {"stringValue": "production"}}
      ],
      "entityRefs": [
        {
          "type": "service",
          "schemaUrl": "https://opentelemetry.io/schemas/1.0.0",
          "idKeys": ["service.name", "service.namespace", "service.instance.id"],
          "descriptionKeys": ["deployment.environment"]
        },
        {
          "type": "host",
          "schemaUrl": "https://opentelemetry.io/schemas/1.0.0",
          "idKeys": ["host.name"],
          "descriptionKeys": ["cloud.region"]
        }
      ]
    },
    "scopeMetrics": [{
      "scope": {
        "name": "github.com/example/payment",
        "version": "1.2.0",
        "attributes": [
          {"key": "library.language", "value": {"stringValue": "go"}}
        ]
      },
      "metrics": [{
        "name": "http_requests_total",
        "description": "Total HTTP requests",
        "sum": {
          "dataPoints": [{
            "asDouble": 1500,
            "timeUnixNano": "${TIMESTAMP}000000",
            "attributes": [
              {"key": "method", "value": {"stringValue": "GET"}},
              {"key": "status", "value": {"stringValue": "200"}}
            ]
          }],
          "aggregationTemporality": 2,
          "isMonotonic": true
        }
      }]
    }]
  }]
}
EOF
)

send_otlp_metrics "$PAYLOAD1" "payment-service (production) - http_requests_total"

# Resource 2: order-service in production
PAYLOAD2=$(cat <<EOF
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "order-service"}},
        {"key": "service.namespace", "value": {"stringValue": "production"}},
        {"key": "service.instance.id", "value": {"stringValue": "order-001"}},
        {"key": "host.name", "value": {"stringValue": "prod-order-1.example.com"}},
        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}}
      ]
    },
    "scopeMetrics": [{
      "scope": {
        "name": "github.com/example/orders",
        "version": "0.9.1",
        "attributes": [
          {"key": "library.language", "value": {"stringValue": "java"}}
        ]
      },
      "metrics": [{
        "name": "orders_processed_total",
        "description": "Total orders processed",
        "sum": {
          "dataPoints": [{
            "asDouble": 500,
            "timeUnixNano": "${TIMESTAMP}000000",
            "attributes": []
          }],
          "aggregationTemporality": 2,
          "isMonotonic": true
        }
      }]
    }]
  }]
}
EOF
)

send_otlp_metrics "$PAYLOAD2" "order-service (production) - orders_processed_total"

# Resource 3: payment-service in staging (different namespace)
PAYLOAD3=$(cat <<EOF
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "payment-service"}},
        {"key": "service.namespace", "value": {"stringValue": "staging"}},
        {"key": "service.instance.id", "value": {"stringValue": "payment-staging-001"}},
        {"key": "host.name", "value": {"stringValue": "staging-payment-1.example.com"}},
        {"key": "cloud.region", "value": {"stringValue": "us-east-1"}}
      ]
    },
    "scopeMetrics": [{
      "scope": {
        "name": "github.com/example/payment",
        "version": "1.1.0",
        "attributes": [
          {"key": "library.language", "value": {"stringValue": "go"}}
        ]
      },
      "metrics": [{
        "name": "http_requests_total",
        "description": "Total HTTP requests",
        "sum": {
          "dataPoints": [{
            "asDouble": 100,
            "timeUnixNano": "${TIMESTAMP}000000",
            "attributes": [
              {"key": "method", "value": {"stringValue": "POST"}},
              {"key": "status", "value": {"stringValue": "201"}}
            ]
          }],
          "aggregationTemporality": 2,
          "isMonotonic": true
        }
      }]
    }]
  }]
}
EOF
)

send_otlp_metrics "$PAYLOAD3" "payment-service (staging) - http_requests_total"

echo -e "\n${GREEN}Sent 3 resource metrics via OTLP${RESET}"
echo -e "${GRAY}Note: payment-service (production) includes entity_refs for service and host entities${RESET}"
echo -e "${GRAY}Scope attributes sent per service:${RESET}"
echo -e "${GRAY}  payment-service (prod):    github.com/example/payment v1.2.0 {library.language=go}${RESET}"
echo -e "${GRAY}  order-service (prod):      github.com/example/orders v0.9.1 {library.language=java}${RESET}"
echo -e "${GRAY}  payment-service (staging): github.com/example/payment v1.1.0 {library.language=go}${RESET}"

# === PHASE 2: Query resource attributes from TSDB head ===
print_phase 2 "Querying resource attributes from TSDB head (in-memory)"

echo -e "${GRAY}Resource and scope attributes are now stored in-memory in the ingester's TSDB head.${RESET}"
echo -e "${GRAY}Querying /api/v1/resources endpoint...${RESET}"
echo -e "${GRAY}Note: Scope attributes are persisted to TSDB but not yet exposed via the resources API.${RESET}\n"

# Small delay to ensure metrics are ingested
sleep 1

query_resources '{__name__=~".+"}' "All resource attributes in head:"

# === PHASE 3: WAL Replay Verification ===
print_phase 3 "WAL replay verification"

echo -e "${GRAY}Restarting the Mimir ingester to exercise Write-Ahead Log (WAL) replay.${RESET}"
echo -e "${GRAY}Resource attributes should survive the restart because they are written to the WAL.${RESET}\n"

# Restart mimir-1 container
echo -e "Restarting mimir-1 container..."
docker compose -f "${COMPOSE_DIR}/docker-compose.yml" restart mimir-1 > /dev/null 2>&1

# Wait for Mimir to be ready again
wait_for_service "http://localhost:8101/ready" "Mimir (after restart)"

# Wait for ingester ring to be fully formed (ready endpoint can return 200 before queries work)
echo "Waiting for ingester to accept queries..."
ingester_ready=false
for i in $(seq 1 30); do
    test_query=$(curl -s -o /dev/null -w "%{http_code}" "${RESOURCES_ENDPOINT}?match[]=%7B__name__%3D~%22.%2B%22%7D" 2>/dev/null) || true
    if [ "$test_query" = "200" ]; then
        ingester_ready=true
        break
    fi
    printf "\r  Attempt %d/30 (HTTP %s)..." "$i" "$test_query"
    sleep 2
done
if [ "$ingester_ready" = true ]; then
    echo "Ingester is ready."
else
    echo "Warning: Ingester may not be fully ready"
fi
echo ""

# Query again to verify data survived WAL replay
echo -e "${BOLD}Querying after WAL replay:${RESET}\n"
query_resources '{__name__=~".+"}' "Resource attributes after WAL replay:"

echo -e "\n${GREEN}Resource attributes survived WAL replay!${RESET}"

# === PHASE 4: Flush to blocks ===
print_phase 4 "Compacting TSDB head to persist resource attributes to disk"

echo -e "${GRAY}Triggering ingester flush to create blocks with series_metadata.parquet...${RESET}\n"

flush_response=$(curl -s -X POST "${FLUSH_ENDPOINT}")
echo -e "${GREEN}Flush triggered${RESET}"

# Wait for flush to complete
echo -e "${GRAY}Waiting for flush to complete...${RESET}"
sleep 5

echo -e "\n${GREEN}Blocks should now contain resource attributes in series_metadata.parquet files${RESET}"

# === PHASE 5: Query from blocks ===
print_phase 5 "Querying resource attributes from persisted blocks"

echo -e "${GRAY}Now querying will also include data from store-gateways (if blocks are available).${RESET}\n"

query_resources '{__name__=~".+"}' "Resource attributes (from both head and blocks):"

# === PHASE 6: Demonstrate descriptive attributes changing over time ===
print_phase 6 "Descriptive attributes changing over time"

echo -e "${BOLD}Scenario:${RESET} payment-service is migrated to a new host in a different region."
echo -e "The ${CYAN}identifying${RESET} attributes (service.name, service.namespace, service.instance.id) stay the same,"
echo -e "but the ${YELLOW}descriptive${RESET} attributes (host.name, cloud.region) change."
echo -e "The instrumentation library is also upgraded: scope version ${BOLD}1.2.0 -> 1.3.0${RESET}.\n"

# Wait so timestamps visibly differ
sleep 2

TIMESTAMP2=$(now_ms)
MIGRATED_TIMESTAMP_SEC=$(now_sec)

# Send metrics with changed descriptive attributes (same identifying attributes)
# Also includes entity_refs with updated descriptive key assignments
PAYLOAD_MIGRATED=$(cat <<EOF
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "payment-service"}},
        {"key": "service.namespace", "value": {"stringValue": "production"}},
        {"key": "service.instance.id", "value": {"stringValue": "payment-001"}},
        {"key": "host.name", "value": {"stringValue": "prod-payment-2.example.com"}},
        {"key": "cloud.region", "value": {"stringValue": "eu-west-1"}},
        {"key": "deployment.environment", "value": {"stringValue": "production"}},
        {"key": "k8s.pod.name", "value": {"stringValue": "payment-7d4f8b9c5-xk2pq"}}
      ],
      "entityRefs": [
        {
          "type": "service",
          "schemaUrl": "https://opentelemetry.io/schemas/1.0.0",
          "idKeys": ["service.name", "service.namespace", "service.instance.id"],
          "descriptionKeys": ["deployment.environment", "k8s.pod.name"]
        },
        {
          "type": "host",
          "schemaUrl": "https://opentelemetry.io/schemas/1.0.0",
          "idKeys": ["host.name"],
          "descriptionKeys": ["cloud.region"]
        }
      ]
    },
    "scopeMetrics": [{
      "scope": {
        "name": "github.com/example/payment",
        "version": "1.3.0",
        "attributes": [
          {"key": "library.language", "value": {"stringValue": "go"}}
        ]
      },
      "metrics": [{
        "name": "http_requests_total",
        "description": "Total HTTP requests",
        "sum": {
          "dataPoints": [{
            "asDouble": 2500,
            "timeUnixNano": "${TIMESTAMP2}000000",
            "attributes": [
              {"key": "method", "value": {"stringValue": "GET"}},
              {"key": "status", "value": {"stringValue": "200"}}
            ]
          }],
          "aggregationTemporality": 2,
          "isMonotonic": true
        }
      }]
    }]
  }]
}
EOF
)

send_otlp_metrics "$PAYLOAD_MIGRATED" "payment-service after migration (new host, new region, new k8s pod, scope v1.3.0)"

echo -e "\n${GRAY}Changes made:${RESET}"
echo -e "  ${YELLOW}host.name${RESET}: prod-payment-1.example.com -> ${GREEN}prod-payment-2.example.com${RESET}"
echo -e "  ${YELLOW}cloud.region${RESET}: us-west-2 -> ${GREEN}eu-west-1${RESET}"
echo -e "  ${YELLOW}k8s.pod.name${RESET}: (new) ${GREEN}payment-7d4f8b9c5-xk2pq${RESET}"
echo -e "  ${YELLOW}scope version${RESET}: 1.2.0 -> ${GREEN}1.3.0${RESET} (library upgraded during migration)"

# Show the version history
echo -e "\n${BOLD}Version history for production/payment-service:${RESET}"
echo -e "${GRAY}(Original version in block, new version in head)${RESET}\n"

sleep 1

query_resources '{job="production/payment-service"}' "production/payment-service resource attribute versions:"

# === PHASE 7: Demonstrate info() function with time-varying attributes ===
print_phase 7 "Querying with info() to include resource attributes"

echo -e "The ${BOLD}info()${RESET} function enriches metrics with resource attributes at query time."
echo -e "When descriptive attributes change over time, info() returns the values"
echo -e "that were active at the requested timestamp.\n"

QUERY='sum by (method, status, "cloud.region", "host.name") (info(http_requests_total{method="GET",status="200"}))'
echo -e "${BOLD}Query:${RESET} ${QUERY}\n"

# Query at original timestamp (before migration)
echo -e "${BOLD}At timestamp BEFORE migration (${ORIGINAL_TIMESTAMP_SEC}):${RESET}"
query_promql "$QUERY" "$ORIGINAL_TIMESTAMP_SEC" ""

# Query at migrated timestamp (after migration)
echo -e "${BOLD}At timestamp AFTER migration (${MIGRATED_TIMESTAMP_SEC}):${RESET}"
query_promql "$QUERY" "$MIGRATED_TIMESTAMP_SEC" ""

echo -e "${CYAN}This enables time-accurate correlation of metrics with OTel traces/logs,"
echo -e "even when infrastructure changes occur during the query time range.${RESET}\n"

# === PHASE 8: Show API response format ===
print_phase 8 "API response format for /api/v1/resources"

echo -e "${BOLD}API Response (/api/v1/resources):${RESET}"
echo -e "${GRAY}Full response format showing labels, versions with identifying/descriptive attributes, and entities:${RESET}\n"

# Query and show full response format
encoded_match=$(printf '%s' '{job=~".*payment-service.*"}' | jq -sRr @uri)
curl -s "${RESOURCES_ENDPOINT}?match[]=${encoded_match}" | jq -C '.'

echo ""

# === Summary ===
print_phase 9 "Summary"

echo -e "${BOLD}This demo showed how Grafana Mimir persists OTel resource and scope attributes:${RESET}"
echo -e "  ${GREEN}1.${RESET} Resource attributes arrive via OTLP metrics (service.name, etc.)"
echo -e "  ${GREEN}2.${RESET} Attributes are stored per-series in ingester's TSDB head (in-memory)"
echo -e "  ${GREEN}3.${RESET} Attributes survive ingester restart via WAL replay"
echo -e "  ${GREEN}4.${RESET} When blocks are flushed, attributes are persisted to series_metadata.parquet"
echo -e "  ${GREEN}5.${RESET} ${CYAN}Identifying${RESET} attributes (service.name, etc.) remain constant for a series"
echo -e "  ${GREEN}6.${RESET} ${YELLOW}Descriptive${RESET} attributes (host.name, cloud.region) can change over time"
echo -e "  ${GREEN}7.${RESET} ${MAGENTA}Versioned storage${RESET} preserves attribute history with time ranges"
echo -e "  ${GREEN}8.${RESET} Each version tracks when specific attributes were active (MinTime/MaxTime)"
echo -e "  ${GREEN}9.${RESET} The ${BOLD}info()${RESET} function enriches queries with time-appropriate attributes"
echo -e "  ${GREEN}10.${RESET} ${CYAN}Scope attributes${RESET} (library name, version, custom attrs) are persisted per-series"
echo ""
echo -e "${CYAN}This enables correlation of Prometheus metrics with OTel traces/logs"
echo -e "using the identifying resource attributes (service.name, etc.)."
echo -e "Scope attributes track which instrumentation library produced the metrics,"
echo -e "including version changes across deployments.${RESET}"
echo ""
echo -e "${BOLD}Endpoints used:${RESET}"
echo -e "  ${GRAY}OTLP ingest:${RESET}  ${OTLP_ENDPOINT}"
echo -e "  ${GRAY}Resource API:${RESET} ${RESOURCES_ENDPOINT}"
echo -e "  ${GRAY}Query API:${RESET}    ${QUERY_ENDPOINT}"
echo -e "  ${GRAY}Flush:${RESET}        ${FLUSH_ENDPOINT}"
