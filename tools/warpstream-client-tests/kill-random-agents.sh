#!/usr/bin/env bash

set -euo pipefail

: "${CONTEXT:?CONTEXT env variable is required}"
: "${NAMESPACE:?NAMESPACE env variable is required}"

COUNT=1

usage() {
    echo "Usage: $0 [-n COUNT]" >&2
    echo "  -n COUNT  Number of write agents to kill per zone (default: 1)" >&2
}

while getopts ":n:h" opt; do
    case "$opt" in
        n) COUNT="$OPTARG" ;;
        h) usage; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage; exit 1 ;;
        :) echo "Option -$OPTARG requires an argument" >&2; usage; exit 1 ;;
    esac
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
    echo "COUNT must be a positive integer, got: $COUNT" >&2
    exit 1
fi

pick_random_pods() {
    local deployment="$1"
    local count="$2"
    kubectl --context "$CONTEXT" -n "$NAMESPACE" get pods \
        -l "name=${deployment}" \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[*].metadata.name}' \
        | tr ' ' '\n' \
        | shuf -n "$count"
}

PIDS=()
PODS=()

for zone in a b; do
    deployment="warpstream-agent-write-zone-${zone}"
    pods=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && pods+=("$line")
    done < <(pick_random_pods "$deployment" "$COUNT")
    if [[ ${#pods[@]} -eq 0 ]]; then
        echo "No running pods found for $deployment" >&2
        exit 1
    fi
    if [[ ${#pods[@]} -lt $COUNT ]]; then
        echo "Requested $COUNT pods for $deployment but only ${#pods[@]} are running" >&2
        exit 1
    fi
    PODS+=("${pods[@]}")
done

echo "Killing ${PODS[*]} simultaneously..."

for pod in "${PODS[@]}"; do
    kubectl --context "$CONTEXT" -n "$NAMESPACE" delete pod "$pod" --grace-period=0 --force --wait=false &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait "$pid"
done
