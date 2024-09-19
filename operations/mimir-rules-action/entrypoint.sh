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

# Only one of the namespace selection flags can be provided to diff and sync actions.
# If more than one selection is made, exit with an error.
verifyAndConstructNamespaceSelection() {
  NAMESPACE_SELECTIONS_COUNT=0

  if [ -n "${NAMESPACES}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="${NAMESPACES:+ --namespaces=${NAMESPACES}}"
  fi
  if [ -n "${NAMESPACES_REGEX}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="${NAMESPACES_REGEX:+ --namespaces-regex=${NAMESPACES_REGEX}}"
  fi
  if [ -n "${IGNORED_NAMESPACES}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="${IGNORED_NAMESPACES:+ --ignored-namespaces=${IGNORED_NAMESPACES}}"
  fi
  if [ -n "${IGNORED_NAMESPACES_REGEX}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="${IGNORED_NAMESPACES_REGEX:+ --ignored-namespaces-regex=${IGNORED_NAMESPACES_REGEX}}"
  fi

  if [ "${NAMESPACE_SELECTIONS_COUNT}" -gt 1 ]; then
    err "Only one of the namespace selection flags can be specified."
    exit 1
  fi
}

case "${ACTION}" in
  "$SYNC_CMD")
    verifyTenantAndAddress
    verifyAndConstructNamespaceSelection
    # Don't quote $NAMESPACES_SELECTION since we don't want an empty string when it's not set
    # shellcheck disable=SC2086
    OUTPUT=$(/bin/mimirtool rules sync --rule-dirs="${RULES_DIR}" $NAMESPACES_SELECTION "$@")
    STATUS=$?
    ;;
  "$DIFF_CMD")
    verifyTenantAndAddress
    verifyAndConstructNamespaceSelection
    # Don't quote $NAMESPACES_SELECTION since we don't want an empty string when it's not set
    # shellcheck disable=SC2086
    OUTPUT=$(/bin/mimirtool rules diff --rule-dirs="${RULES_DIR}" $NAMESPACES_SELECTION --disable-color "$@")
    STATUS=$?
    ;;
  "$LINT_CMD")
    OUTPUT=$(/bin/mimirtool rules lint --rule-dirs="${RULES_DIR}" "$@")
    STATUS=$?
    ;;
  "$PREPARE_CMD")
    OUTPUT=$(/bin/mimirtool rules prepare -i --rule-dirs="${RULES_DIR}" ${LABEL:+ --label=${LABEL}} --label-excluded-rule-groups="${LABEL_EXCLUDED_RULE_GROUPS}" "$@")
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

SINGLE_LINE_OUTPUT=$(echo "${OUTPUT}" | awk 'BEGIN { RS="%0A" } { gsub(/%/, "%25"); gsub(/\r/, "%0D"); gsub(/\n/, "%0A") } { print }')
echo "detailed=${SINGLE_LINE_OUTPUT}" >> "${GITHUB_OUTPUT}"
SUMMARY=$(echo "${OUTPUT}" | grep Summary)
# shellcheck disable=SC2129
echo 'summary<<ENDOFSUMMARY' >> "${GITHUB_OUTPUT}"
echo "${SUMMARY}" >> "${GITHUB_OUTPUT}"
echo 'ENDOFSUMMARY' >> "${GITHUB_OUTPUT}"

exit $STATUS
