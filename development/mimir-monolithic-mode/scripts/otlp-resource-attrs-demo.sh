#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

# OTLP Resource Attributes Persistence Demo for Grafana Mimir
#
# This demo showcases how Mimir persists OTel resource attributes from OTLP metrics
# and makes them queryable via the /api/v1/resources endpoint.
#
# Prerequisites:
#   - Mimir running in monolithic mode (./compose-up.sh)
#   - curl and jq installed
#
# Usage:
#   ./scripts/otlp-resource-attrs-demo.sh

set -e

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
    echo -e "\n${BOLD}${MAGENTA}=== Phase $1: $2 ===${RESET}\n"
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
        echo "$response" | head -n -1
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

# Get current timestamp in milliseconds
now_ms() {
    echo $(($(date +%s) * 1000))
}

# === PHASE 1: Send OTLP metrics with resource attributes ===
print_phase 1 "Sending OTLP metrics with resource attributes"

echo -e "${GRAY}Sending metrics from multiple services with diverse resource attributes...${RESET}\n"

TIMESTAMP=$(now_ms)

# Resource 1: payment-service in production
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
      ]
    },
    "scopeMetrics": [{
      "scope": {"name": "demo"},
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
      "scope": {"name": "demo"},
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

# Resource 3: payment-service in staging
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
      "scope": {"name": "demo"},
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

# === PHASE 2: Query resource attributes from TSDB head ===
print_phase 2 "Querying resource attributes from TSDB head (in-memory)"

echo -e "${GRAY}Resource attributes are now stored in-memory in the ingester's TSDB head.${RESET}"
echo -e "${GRAY}Querying /api/v1/resources endpoint...${RESET}\n"

# Small delay to ensure metrics are ingested
sleep 1

query_resources '{__name__=~".+"}' "All resource attributes in head:"

# === PHASE 3: Flush to blocks ===
print_phase 3 "Flushing TSDB head to persist resource attributes to blocks"

echo -e "${GRAY}Triggering ingester flush to create blocks with series_metadata.parquet...${RESET}\n"

flush_response=$(curl -s -X POST "${FLUSH_ENDPOINT}")
echo -e "${GREEN}Flush triggered${RESET}"

# Wait for flush to complete
echo -e "${GRAY}Waiting for flush to complete...${RESET}"
sleep 5

echo -e "\n${GREEN}Blocks should now contain resource attributes in series_metadata.parquet files${RESET}"

# === PHASE 4: Query from blocks ===
print_phase 4 "Querying resource attributes from persisted blocks"

echo -e "${GRAY}Now querying will also include data from store-gateways (if blocks are available).${RESET}\n"

query_resources '{__name__=~".+"}' "Resource attributes (from both head and blocks):"

# === PHASE 5: Simulate service migration ===
print_phase 5 "Descriptive attributes changing over time (service migration)"

echo -e "${BOLD}Scenario:${RESET} payment-service is migrated to a new host in a different region."
echo -e "The ${CYAN}identifying${RESET} attributes (service.name, service.namespace, service.instance.id) stay the same,"
echo -e "but the ${YELLOW}descriptive${RESET} attributes (host.name, cloud.region) change.\n"

# Wait so timestamps visibly differ
sleep 2

TIMESTAMP2=$(now_ms)

# Send metrics with changed descriptive attributes (same identifying attributes)
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
      ]
    },
    "scopeMetrics": [{
      "scope": {"name": "demo"},
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

send_otlp_metrics "$PAYLOAD_MIGRATED" "payment-service after migration (new host, new region, new k8s pod)"

echo -e "\n${GRAY}Changes made:${RESET}"
echo -e "  ${YELLOW}host.name${RESET}: prod-payment-1.example.com -> ${GREEN}prod-payment-2.example.com${RESET}"
echo -e "  ${YELLOW}cloud.region${RESET}: us-west-2 -> ${GREEN}eu-west-1${RESET}"
echo -e "  ${YELLOW}k8s.pod.name${RESET}: (new) ${GREEN}payment-7d4f8b9c5-xk2pq${RESET}"

# === PHASE 6: Show versioned resource attributes ===
print_phase 6 "Querying versioned resource attributes"

echo -e "${GRAY}Now querying will show version history with different MinTime/MaxTime ranges.${RESET}"
echo -e "${GRAY}Each version captures when specific attribute values were active.${RESET}\n"

sleep 1

query_resources '{service_name="payment-service",service_namespace="production"}' "production/payment-service resource attribute versions:"

# === Summary ===
print_phase 7 "Summary"

echo -e "${BOLD}This demo showed how Grafana Mimir persists OTel resource attributes:${RESET}"
echo -e "  ${GREEN}1.${RESET} Resource attributes arrive via OTLP metrics (service.name, etc.)"
echo -e "  ${GREEN}2.${RESET} Attributes are stored per-series in ingester's TSDB head (in-memory)"
echo -e "  ${GREEN}3.${RESET} When blocks are flushed, attributes are persisted to series_metadata.parquet"
echo -e "  ${GREEN}4.${RESET} ${CYAN}Identifying${RESET} attributes (service.name, etc.) remain constant for a series"
echo -e "  ${GREEN}5.${RESET} ${YELLOW}Descriptive${RESET} attributes (host.name, cloud.region) can change over time"
echo -e "  ${GREEN}6.${RESET} ${MAGENTA}Versioned storage${RESET} preserves attribute history with time ranges"
echo -e "  ${GREEN}7.${RESET} Query both ingesters and store-gateways via /api/v1/resources"
echo ""
echo -e "${CYAN}This enables correlation of Prometheus metrics with OTel traces/logs"
echo -e "using the identifying resource attributes (service.name, etc.)."
echo -e "The version history allows tracking infrastructure changes over time.${RESET}"
echo ""
echo -e "${BOLD}Endpoints used:${RESET}"
echo -e "  ${GRAY}OTLP ingest:${RESET} ${OTLP_ENDPOINT}"
echo -e "  ${GRAY}Resource API:${RESET} ${RESOURCES_ENDPOINT}"
echo -e "  ${GRAY}Flush:${RESET}        ${FLUSH_ENDPOINT}"
