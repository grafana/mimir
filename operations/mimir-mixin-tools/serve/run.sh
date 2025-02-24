#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

SCRIPT_DIR=$(cd `dirname $0` && pwd)
# Ensure we run recent Grafana.
GRAFANA_VERSION=11.1.3
DOCKER_CONTAINER_NAME="mixin-serve-grafana"
DOCKER_OPTS=""

function usage() {
    echo "Usage: $0 [--docker-network <name>]"
    echo ""
    echo "Options:"
    echo "  --docker-network <name>   Joins the Docker network <name>"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --docker-network)
    DOCKER_OPTS="${DOCKER_OPTS} --network=$2"
    shift
    shift
    ;;
  *)  break
    ;;
  esac
done

if [[ $# -gt 0 ]]; then
  usage
  exit 1
fi

# Check if the config file exists.
if [ ! -e "${SCRIPT_DIR}/.config" ]; then
  echo "This tool expects a local config file stored at ${SCRIPT_DIR}/.config and containing the following content:"
  echo ""
  echo "DATASOURCE_URL=\"<grafana-cloud-url ending with /prometheus>\""
  echo "DATASOURCE_USERNAME=\"<grafana-cloud-instance-id>\""
  echo "DATASOURCE_PASSWORD=\"<grafana-cloud-api-key>\""
  echo "LOKI_DATASOURCE_URL=\"<grafana-cloud-url ending with (optional)>\""
  echo "LOKI_DATASOURCE_USERNAME=\"<grafana-cloud-instance-id (optional)>\""
  echo "LOKI_DATASOURCE_PASSWORD=\"<grafana-cloud-api-key (optional)>\""
  echo "GRAFANA_PUBLISHED_PORT=\"<grafana-port-on-the-host>\""
  echo ""
  exit 1
fi

# Load config.
source "${SCRIPT_DIR}/.config"

function cleanup() {
  echo "Cleaning up Docker setup"
  docker rm --force "${DOCKER_CONTAINER_NAME}" || true
  echo "Cleaned up Docker setup"
}

# Start from a clean setup and also trigger a cleanup on exit.
cleanup
trap cleanup EXIT

docker pull grafana/grafana:${GRAFANA_VERSION}

# Run Grafana.
echo "Starting Grafana container with name ${DOCKER_CONTAINER_NAME} listening on host port ${GRAFANA_PUBLISHED_PORT}"
docker run \
  $DOCKER_OPTS \
  --rm \
  --name "$DOCKER_CONTAINER_NAME" \
  --env "GF_AUTH_ANONYMOUS_ENABLED=true" \
  --env "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin" \
  --env "GF_USERS_DEFAULT_THEME=light" \
  --env "GF_LOG_LEVEL=warn" \
  --env "DATASOURCE_URL=${DATASOURCE_URL}" \
  --env "DATASOURCE_USERNAME=${DATASOURCE_USERNAME}" \
  --env "DATASOURCE_PASSWORD=${DATASOURCE_PASSWORD}" \
  --env "LOKI_DATASOURCE_URL=${LOKI_DATASOURCE_URL}" \
  --env "LOKI_DATASOURCE_USERNAME=${LOKI_DATASOURCE_USERNAME}" \
  --env "LOKI_DATASOURCE_PASSWORD=${LOKI_DATASOURCE_PASSWORD}" \
  -v "${SCRIPT_DIR}/../../mimir-mixin-compiled/dashboards:/var/lib/grafana/dashboards" \
  -v "${SCRIPT_DIR}/provisioning-dashboards.yaml:/etc/grafana/provisioning/dashboards/provisioning-dashboards.yaml" \
  -v "${SCRIPT_DIR}/provisioning-datasources.yaml:/etc/grafana/provisioning/datasources/provisioning-datasources.yaml" \
  --expose 3000 \
  --publish "${GRAFANA_PUBLISHED_PORT}:3000" \
  grafana/grafana:${GRAFANA_VERSION}
