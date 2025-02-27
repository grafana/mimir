#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# This script flushes all cache items in all Memcached pods in a Kubernetes StatefulSet, waiting between each pod.

set -euo pipefail

if [ $# -ne 4 ]; then
  echo "This script takes exactly four arguments: $0 <Kubernetes context> <Kubernetes namespace> <StatefulSet name> <delay between replicas, in seconds>" >/dev/stderr
  exit 1
fi

K8S_CONTEXT="$1"
K8S_NAMESPACE="$2"
STATEFULSET_NAME="$3"
DELAY="$4"

function flush_pod() {
  POD_NAME=$1
  local LOCAL_PORT=11211

  kubectl --context "$K8S_CONTEXT" --namespace "$K8S_NAMESPACE" port-forward "$POD_NAME" $LOCAL_PORT:11211 >/dev/null &
  KUBECTL_PID=$!

  wait_for_port_available "$LOCAL_PORT"

  (
    echo "flush_all"
    echo "quit"
  ) | nc 127.0.0.1 $LOCAL_PORT | grep 'OK'

  kill $KUBECTL_PID
  wait $KUBECTL_PID || true
}

function wait_for_port_available() {
  PORT="$1"

  echo -n "Waiting for port-forward to be up..."

  for i in {1..10}; do
    echo -n "."
    if nc -vz 127.0.0.1 "$PORT" >/dev/null 2>&1; then
      echo
      break
    elif [ "$i" -eq 10 ]; then
      echo
      echo "Port-forward did not start in time" >/dev/stderr
      exit 1
    else
      sleep 0.5
    fi
  done
}

function main() {
  echo "> Getting replica count for $STATEFULSET_NAME StatefulSet in $K8S_CONTEXT/$K8S_NAMESPACE..."

  POD_COUNT=$(kubectl --context "$K8S_CONTEXT" --namespace "$K8S_NAMESPACE" get sts "$STATEFULSET_NAME" -o jsonpath='{.spec.replicas}')

  echo "Will flush $POD_COUNT pods."

  for ((index = 0; index < "$POD_COUNT"; index++)); do
    POD_NAME="$STATEFULSET_NAME-$index"
    echo "> Flushing $POD_NAME..."
    flush_pod "$POD_NAME"

    if [ "$index" -ne $((POD_COUNT - 1)) ]; then
      echo "> Done, sleeping ${DELAY}s until next pod..."
      sleep "$DELAY"
    fi
  done

  echo "> Complete!"
}

main
