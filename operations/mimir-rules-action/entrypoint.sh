#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# shellcheck shell=dash
#
# Interact with the Mimir Ruler API using mimirtool.

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: " + "$@" >&2
}

# For commands that interact with the server, we need to verify that the
# MIMIR_TENANT_ID and MIMIR_ADDRESS are set.
verifyTenantAndAddress() {
  if [ -z "${MIMIR_TENANT_ID}" ]; then
    err "MIMIR_TENANT_ID has not been set."
    exit 1
  fi

  if [ -z "${MIMIR_ADDRESS}" ]; then
    err "MIMIR_ADDRESS has not been set."
    exit 1
  fi
}

LINT_CMD=lint
CHECK_CMD=check
PREPARE_CMD=prepare
SYNC_CMD=sync
DIFF_CMD="diff"
PRINT_CMD=print

if [ -z "${RULES_DIR}" ]; then
  echo "RULES_DIR not set, using './' as a default."
  RULES_DIR="./"
fi

if [ -z "${ACTION}" ]; then
  err "ACTION has not been set."
  exit 1
fi

case "${ACTION}" in
  "$SYNC_CMD")
    verifyTenantAndAddress
    OUTPUT=$(/bin/mimirtool rules sync --rule-dirs="${RULES_DIR}" ${NAMESPACES:+ --namespaces=${NAMESPACES}} "$@")
    STATUS=$?
    ;;
  "$DIFF_CMD")
    verifyTenantAndAddress
    OUTPUT=$(/bin/mimirtool rules diff --rule-dirs="${RULES_DIR}" ${NAMESPACES:+ --namespaces=${NAMESPACES}} --disable-color "$@")
    STATUS=$?
    ;;
  "$LINT_CMD")
    OUTPUT=$(/bin/mimirtool rules lint --rule-dirs="${RULES_DIR}" "$@")
    STATUS=$?
    ;;
  "$PREPARE_CMD")
    OUTPUT=$(/bin/mimirtool rules prepare -i --rule-dirs="${RULES_DIR}" --label-excluded-rule-groups="${LABEL_EXCLUDED_RULE_GROUPS}" "$@")
    STATUS=$?
    ;;
  "$CHECK_CMD")
    OUTPUT=$(/bin/mimirtool rules check --rule-dirs="${RULES_DIR}" "$@")
    STATUS=$?
    ;;
  "$PRINT_CMD")
      OUTPUT=$(/bin/mimirtool rules print --disable-color "$@")
      STATUS=$?
      ;;
  *)
    err "Unexpected action '${ACTION}'"
    exit 1
    ;;
esac

SINGLE_LINE_OUTPUT=$(echo "${OUTPUT}" | awk 'BEGIN { RS="" } { gsub(/%/, "%25"); gsub(/\r/, "%0D"); gsub(/\n/, "%0A") } { print }')
echo ::set-output name=detailed::"${SINGLE_LINE_OUTPUT}"
SUMMARY=$(echo "${OUTPUT}" | grep Summary)
echo ::set-output name=summary::"${SUMMARY}"

exit $STATUS
