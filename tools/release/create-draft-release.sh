#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_setup
find_last_release

# Generate release notes draft.
echo "Generating the draft release notes..."
RELEASE_NOTES_FILE="./tmp-release-notes.md"
trap 'rm -f "${RELEASE_NOTES_FILE}"' EXIT
"${CURR_DIR}"/create-draft-release-notes.sh > "${RELEASE_NOTES_FILE}"

# Create the draft release.
echo "Creating the draft release..."
gh release create \
  --draft \
  --prerelease \
  --title "${LAST_RELEASE_VERSION}" \
  --notes-file "${RELEASE_NOTES_FILE}" \
  "${LAST_RELEASE_TAG}"

# Print instructions to move on.
echo ""
echo "The draft release has been created: https://github.com/grafana/mimir/releases/tag/${LAST_RELEASE_TAG}"
echo ""
echo "A workflow to build and attach the release artifacts should have been triggered: https://github.com/grafana/mimir/actions/workflows/release-make-dist.yml?query=branch%3A${LAST_RELEASE_TAG}"
echo ""
echo "Once the workflow completes:"
echo "1. Copy the release notes from the documentation and fix the links. Skip if publishing a patch release."
echo "2. Review the draft release and its attached artifacts."
echo "3. If this is a stable release, remove the tick from 'This is a pre-release'."
echo "4. Publish it."
