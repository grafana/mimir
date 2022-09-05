#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# vim: ai:ts=8:sw=8:noet
# This script will bootstrap a new kubernetes cluster using kind and install the
# mimir-distributed chart

command -v helm >/dev/null 2>&1 || { echo "helm is not installed"; exit 1; }
command -v kind >/dev/null 2>&1 || { echo "kind is not installed"; exit 1; }

chart_path="../."

# Run this once to get the dependencies
echo "# Fetching helm dependencies"
helm dependency update "${chart_path}"

echo "# Deleting pre-existing mimir-demo cluster if any"
kind delete cluster --name="mimir-demo"
echo "# Creating new kind cluster"
kind create cluster --name mimir-demo

# OSS Mimir
helm upgrade --install mimir-demo \
  "${chart_path}" \
  --values "${chart_path}/values.yaml" \
  --kube-context kind-mimir-demo

# Enterprise and graphite enabled
# if ! test -f "license.jwt"; then
#     echo "Please place a license.jwt file in this directory"
#     exit 1
# fi
#
# helm upgrade --install mimir-demo \
#   "${chart_path}" \
#   --values "${chart_path}/values.yaml" \
#   --set 'enterprise.enabled=true' \
#   --set 'graphite.enabled=true' \
#   --set-file 'license.contents=./license.jwt' \
#   --kube-context kind-mimir-demo
