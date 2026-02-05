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
    NAMESPACES_SELECTION="--namespaces="
    NAMESPACES_VALUE="${NAMESPACES}"
  fi
  if [ -n "${NAMESPACES_REGEX}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="--namespaces-regex="
    NAMESPACES_VALUE="${NAMESPACES_REGEX}"
  fi
  if [ -n "${IGNORED_NAMESPACES}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="--ignored-namespaces="
    NAMESPACES_VALUE="${IGNORED_NAMESPACES}"
  fi
  if [ -n "${IGNORED_NAMESPACES_REGEX}" ]; then
    NAMESPACE_SELECTIONS_COUNT=$((NAMESPACE_SELECTIONS_COUNT + 1))
    NAMESPACES_SELECTION="--ignored-namespaces-regex="
    NAMESPACES_VALUE="${IGNORED_NAMESPACES_REGEX}"
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
    if [ -n "$NAMESPACES_SELECTION" ]; then
      OUTPUT=$(/bin/mimirtool rules sync --rule-dirs="${RULES_DIR}" "$NAMESPACES_SELECTION""$NAMESPACES_VALUE" "$@")
    else
      OUTPUT=$(/bin/mimirtool rules sync --rule-dirs="${RULES_DIR}" "$@")
    fi
    STATUS=$?
    ;;
  "$DIFF_CMD")
    verifyTenantAndAddress
    verifyAndConstructNamespaceSelection
    if [ -n "$NAMESPACES_SELECTION" ]; then
      OUTPUT=$(/bin/mimirtool rules diff --rule-dirs="${RULES_DIR}" "$NAMESPACES_SELECTION""$NAMESPACES_VALUE" --disable-color "$@")
    else
      OUTPUT=$(/bin/mimirtool rules diff --rule-dirs="${RULES_DIR}" --disable-color "$@")
    fi
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

{ echo "detailed<<EOF"; echo "$OUTPUT"; echo "EOF"; } >> "$GITHUB_OUTPUT"
SUMMARY=$(echo "${OUTPUT}" | grep Summary)
# shellcheck disable=SC2129
echo 'summary<<ENDOFSUMMARY' >> "${GITHUB_OUTPUT}"
echo "${SUMMARY}" >> "${GITHUB_OUTPUT}"
echo 'ENDOFSUMMARY' >> "${GITHUB_OUTPUT}"

exit $STATUS
