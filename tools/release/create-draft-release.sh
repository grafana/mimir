#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_setup
find_last_release

# Build binaries and packages.
echo "Building binaries (this may take a while)..."
cd "${CURR_DIR}"/../../ && make dist packages && cd -

# Generate release notes draft.
echo "Generating the draft release notes..."
RELEASE_NOTES_FILE="./tmp-release-notes.md"
trap 'rm -f "${RELEASE_NOTES_FILE}"' EXIT
"${CURR_DIR}"/create-draft-release-notes.sh > "${RELEASE_NOTES_FILE}"

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
echo "1. Copy the release notes from the documentation and fix the links. Skip if publishing a patch release."
echo "2. Review the draft release."
echo "3. If this is a stable release, remove the tick from 'This is a pre-release'."
echo "4. Publish it."
