#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_tools
find_last_release
find_prev_release

# Build dist binaries.
echo "Building binaries (this may take a while)..."
cd "${CURR_DIR}"/../../ && make dist && cd -

# Generate release notes draft.
echo "Generating the draft release notes..."
RELEASE_NOTES_FILE="./tmp-release-notes.md"
trap 'rm -f "${RELEASE_NOTES_FILE}"' EXIT
"${CURR_DIR}"/generate-release-notes.sh > "${RELEASE_NOTES_FILE}"

# Create the draft release.
echo "Creating the draft release (uploading assets may take a while)..."
gh release create \
  --draft \
  --prerelease \
  --title "${LAST_RELEASE_VERSION}" \
  --notes-file "${RELEASE_NOTES_FILE}" \
  "${LAST_RELEASE_TAG}" "${CURR_DIR}"/../../dist/*

# Print instructions to move on.
echo ""
echo "The draft release has been created. To continue:"
echo "1. Copy the release notes from the documentation and fix the links."
echo "2. Review the draft release."
echo "3. Publish it."
