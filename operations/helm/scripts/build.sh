#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# use a normal sed on macOS if available
SED=$(which gsed || which sed)

# DEFAULT_KUBE_VERSION is used if the input values do not contain "kubeVersionOverride"
DEFAULT_KUBE_VERSION="1.20"

CHART_PATH="operations/helm/charts/mimir-distributed"
INTERMEDIATE_PATH=""
OUTPUT_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --chart-path)
    CHART_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  --output-path)
    OUTPUT_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  --intermediate-path)
    INTERMEDIATE_PATH="$2"
    shift # skip param name
    shift # skip param value
    ;;
  *)
    break
    ;;
  esac
done

if [ -z "$OUTPUT_PATH" ] ; then
  echo "Provide --output-path parameter for destination of generated and sanitized outputs"
  exit 1
fi

if [ -z "$INTERMEDIATE_PATH" ] ; then
  echo "Provide --manifests-path parameter for destination of raw manifests outputs"
  exit 1
fi

# Start from a clean slate
rm -rf "$INTERMEDIATE_PATH"
mkdir -p "$INTERMEDIATE_PATH"
rm -rf "$OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH"

# Find testcases
TESTS=$(find "${CHART_PATH}/ci" -name '*values.yaml')
CHART_NAME=$(basename "${CHART_PATH}")

# Array to store child process IDs
pids=()

function generate_manifests() {
  TEST_NAME=$1
  INTERMEDIATE_OUTPUT_DIR=$2
  OUTPUT_DIR=$3

  # Get the pid of the subshell calling this function in a portable way.
  # Do not use BASHPID because some shells don't support it.
  # shellcheck disable=SC2016
  PID=$(exec sh -c 'echo "$PPID"')

  echo ""
  echo "Templating $TEST_NAME"
  ARGS=("${TEST_NAME}" "${CHART_PATH}" "-f" "${FILEPATH}" "--output-dir" "${INTERMEDIATE_OUTPUT_DIR}" "--namespace" "citestns" "--set-string" "regoTestGenerateValues=true")

  echo "Checking for kubeVersionOverride inside tests' values.yaml ..."
  if ! grep "^kubeVersionOverride:" "${FILEPATH}" ; then
    echo "Warning: injecting Kubernetes version override: kubeVersionOverride=${DEFAULT_KUBE_VERSION}"
    ARGS+=("--set-string" "kubeVersionOverride=${DEFAULT_KUBE_VERSION}")
  fi

  echo "Rendering helm test $TEST_NAME in PID $PID: 'helm template ${ARGS[*]}'"
  helm template "${ARGS[@]}" 1>/dev/null
  cp -r "${INTERMEDIATE_OUTPUT_DIR}" "${OUTPUT_DIR}"
  rm "${OUTPUT_DIR}/${CHART_NAME}/templates/values-for-rego-tests.yaml"
  find "${OUTPUT_DIR}/${CHART_NAME}/templates" -type f -print0 | xargs -0 "${SED}" -E -i -- "/^\s+(checksum\/(alertmanager-fallback-)?config|(helm.sh\/)?chart|app.kubernetes.io\/version|image: \"grafana\/(mimir|mimir-continuous-test|enterprise-metrics)):/d"
}

for FILEPATH in $TESTS; do
  # Extract the filename (without extension).
  TEST_NAME=$(basename -s '.yaml' "$FILEPATH")
  INTERMEDIATE_OUTPUT_DIR="${INTERMEDIATE_PATH}/${TEST_NAME}-generated"
  OUTPUT_DIR="${OUTPUT_PATH}/${TEST_NAME}-generated"

  # Prefix every stdout and stderr line from the test name. This makes the interleaved output of different tests easier to follow
  generate_manifests "$TEST_NAME" "$INTERMEDIATE_OUTPUT_DIR" "$OUTPUT_DIR" > >(sed "s/^/$TEST_NAME (stdout): /") 2> >(sed "s/^/$TEST_NAME (stderr): /" >&2) &
  pid=$!
  pids+=("$pid")
done

# Wait for all child processes to finish. We turn off `set -e` because we want to check the exit code of each child process
# and print a message if it's non-zero. set -e will otherwise cause the script to exit immediately if any child process has exited with non-zero exit code.
set +e
for p in "${pids[@]}"; do
    wait "$p"
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        wait # wait for the rest of the renderings to finish so that we don't have incomplete results
        echo "helm template PID $p exited with non-zero exit code $exit_code. Aborting."
        exit $exit_code
    fi
done
set -e
