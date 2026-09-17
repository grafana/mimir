#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

SCRIPT_DIR=$(cd `dirname $0` && pwd)
DOCKET_NETWORK="mixin-serve"
DOCKER_APP_IMAGE="mixin-screenshots-taker"
DOCKER_APP_NAME="mixin-screenshots-taker"
GRAFANA_PID=""
TEMP_MIXIN_OUT=""

# Check if the config file exists.
if [ ! -e "${SCRIPT_DIR}/.config" ]; then
  echo "This tool expects a local config file stored at ${SCRIPT_DIR}/.config and containing the following content:"
  echo ""
  echo "CLUSTER=\"<cluster-to-query>\""
  echo "MIMIR_NAMESPACE=\"<namespace-where-mimir-is-running>\""
  echo "ALERTMANAGER_NAMESPACE=\"<namespace-where-alertmanager-is-running>\""
  echo "MIMIR_USER=\"<mimir-tenant-id>\""
  echo "DASHBOARDS_HISTOGRAM_MODE=\"native\" # Optional. Defaults to 'native'. Set to 'inherit' to provision the dashboards as already built."
  echo ""
  exit 1
fi

# Load config.
source "${SCRIPT_DIR}/.config"

# Default to native histogram mode if not set in .config.
DASHBOARDS_HISTOGRAM_MODE="${DASHBOARDS_HISTOGRAM_MODE:-native}"

function cleanup() {
  echo "Cleaning up Docker setup"
  if [[ ! -z "${GRAFANA_PID}" ]]; then
    kill "${GRAFANA_PID}" 2>/dev/null || echo "Grafana process already stopped"
  fi
  docker rm --force "${DOCKER_APP_NAME}" 2>/dev/null || echo "Container ${DOCKER_APP_NAME} not found or already removed"
  docker network rm "${DOCKET_NETWORK}" 2>/dev/null || echo "Network ${DOCKET_NETWORK} not found or has active endpoints"
  if [[ -n "${TEMP_MIXIN_OUT}" ]]; then
    rm -rf "${TEMP_MIXIN_OUT}"
    echo "Removed temporary mixin output ${TEMP_MIXIN_OUT}"
  fi
  echo "Cleaned up Docker setup"
}

# Start from a clean setup and also trigger a cleanup on exit.
cleanup
trap cleanup EXIT

DASHBOARDS_DIR="${SCRIPT_DIR}/../../mimir-mixin-compiled/dashboards"

# If DASHBOARDS_HISTOGRAM_MODE is set to a real mode (not "inherit"), recompile the mixin with the desired mode.
if [ "${DASHBOARDS_HISTOGRAM_MODE}" != "inherit" ]; then
  echo "Compiling mixin with dashboards_default_latency_mode='${DASHBOARDS_HISTOGRAM_MODE}'"

  TEMP_MIXIN_OUT=$(mktemp -d)
  REPO_ROOT="${SCRIPT_DIR}/../../.."

  make -C "${REPO_ROOT}" build-mixin \
      BUILD_IN_CONTAINER=false \
      MIXIN_OUT_PATH="${TEMP_MIXIN_OUT}" \
      MIXIN_OUT_PATH_SUFFIXES='""' \
      DASHBOARDS_HISTOGRAM_MODE="${DASHBOARDS_HISTOGRAM_MODE}"

  DASHBOARDS_DIR="${TEMP_MIXIN_OUT}/dashboards"
  echo "Mixin compiled to ${TEMP_MIXIN_OUT}"
fi

# Build the Docker image.
echo "Building Docker image ${DOCKER_APP_IMAGE}"
docker build -t "${DOCKER_APP_IMAGE}" "${SCRIPT_DIR}"

# Create Docker network (ignore if already exists).
echo "Creating Docker network ${DOCKET_NETWORK}"
docker network create "$DOCKET_NETWORK" 2>/dev/null || echo "Network $DOCKET_NETWORK already exists, continuing..."

# Before starting Grafana, let's make sure the Docker image pulling time
# is not taken in account when we'll wait below.
echo "Pulling latest Grafana image"
docker pull grafana/grafana:latest

# Start Grafana in background.
"${SCRIPT_DIR}/../serve/run.sh" --docker-network "${DOCKET_NETWORK}" --dashboards-dir "${DASHBOARDS_DIR}" &
GRAFANA_PID="$!"

# Give Grafana some time to startup. It's an hack, but an easy one.
sleep 10

# Start application to take screenshots.
echo "Start screenshot taker container with name ${DOCKER_APP_NAME}"
docker run \
  --rm \
  --name "$DOCKER_APP_NAME" \
  --network "$DOCKET_NETWORK" \
  --env "CLUSTER=${CLUSTER}" \
  --env "MIMIR_NAMESPACE=${MIMIR_NAMESPACE}" \
  --env "ALERTMANAGER_NAMESPACE=${ALERTMANAGER_NAMESPACE}" \
  --env "MIMIR_USER=${MIMIR_USER}" \
  -v "${DASHBOARDS_DIR}:/input" \
  -v "${SCRIPT_DIR}/../../../docs/sources/mimir/manage/monitor-grafana-mimir/dashboards:/output" \
  "${DOCKER_APP_IMAGE}"
