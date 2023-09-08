#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

TOOLBOX_REPO="github.com/charleskorn/prometheus-toolbox"
TOOLBOX_VERSION="4a0171338197be37d3e527eb3bdaa493458dce36"

TOOLBOX_DIR="$SCRIPT_DIR/.tools/$TOOLBOX_REPO/$TOOLBOX_VERSION"
TOOLBOX_BINARY="$TOOLBOX_DIR/prometheus-toolbox"

if [ ! -d "$TOOLBOX_DIR" ]; then
  mkdir -p "$TOOLBOX_DIR"

  {
    cd "$TOOLBOX_DIR"
    git clone --no-checkout -c advice.detachedHead=false "https://$TOOLBOX_REPO.git" .
    git checkout "$TOOLBOX_VERSION"
    go build -o "$TOOLBOX_BINARY" .
  }
fi

ROOT_DATA_DIR="$(cd "$SCRIPT_DIR/../../development/mimir-microservices-mode" && pwd)"
YELLOW="$(tput setaf 3)"
RESET="$(tput sgr0)"

if [ -d "$ROOT_DATA_DIR/.data-ingester-1/anonymous" ]; then
  echo "${YELLOW}WARNING: data is already present in ingester-1!${RESET}"
fi

if [ -d "$ROOT_DATA_DIR/.data-ingester-2/anonymous" ]; then
  echo "${YELLOW}WARNING: data is already present in ingester-2!${RESET}"
fi

if [ -d "$ROOT_DATA_DIR/.data-minio/mimir-tsdb/anonymous" ]; then
  echo "${YELLOW}WARNING: data is already present in long-term storage!${RESET}"
fi

# Port 8000 is distributor-1
"$TOOLBOX_BINARY" --prometheus.url="http://localhost:8000" --prometheus.url.suffix="/api/v1/push" --config.file="$SCRIPT_DIR/data-generation-config.yaml"
