#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# vim: ai:ts=8:sw=8:noet
# This script will bootstrap a new kubernetes cluster using kind and install the
# mimir-distributed chart

set -eufo pipefail

command -v helm >/dev/null 2>&1 || { echo "helm is not installed"; exit 1; }
command -v kind >/dev/null 2>&1 || { echo "kind is not installed"; exit 1; }

function usage {
  cat <<EOF
Bootstrap kind cluster to test mimir-distributed helm chart

Usage:
  $0 [options]

Options:
  -ne opensource only feature
  -e  enterprise feature

Examples:
  $0 -ne
EOF
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

enterprise=false
non_enterprise=false

while [[ $# -gt 0 ]] ; do
case "$1" in
  -e)
    enterprise=true
    ;;
  -ne)
    non_enterprise=true
    ;;
  -h) usage && exit 0 ;;
esac
shift
done

chart_path="../."

# Run this once to get the dependencies
echo "# Fetching helm dependencies"
helm dependency update "${chart_path}"

echo "# Deleting pre-existing mimir-demo cluster if any"
kind delete cluster --name="mimir-demo"
echo "# Creating new kind cluster"
kind create cluster --name mimir-demo

if [ "${non_enterprise}" = "true" ] ; then
  helm upgrade --install mimir-demo \
    "${chart_path}" \
    --values "${chart_path}/values.yaml" \
    --kube-context kind-mimir-demo
fi

if [ "${enterprise}" = "true" ] ; then
  if ! test -f "license.jwt"; then
      echo
      echo "ERROR: Please place a license.jwt file in this directory"
      exit 1
  fi

  helm upgrade --install mimir-demo \
    "${chart_path}" \
    --values "${chart_path}/values.yaml" \
    --set 'enterprise.enabled=true' \
    --set 'graphite.enabled=true' \
    --set-file 'license.contents=./license.jwt' \
    --kube-context kind-mimir-demo
fi