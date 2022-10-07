#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

CHANGELOG_PATH="${CURR_DIR}/../../CHANGELOG.md"

#
# Contributors
#

NUM_PRS=$(git log --pretty=format:"%s" "${LAST_RELEASE_TAG}...${PREV_RELEASE_TAG}" | grep -Eo '#[0-9]+' | wc -l | grep -Eo '[0-9]+')
NUM_AUTHORS=$(git log --pretty=format:"%an" "${LAST_RELEASE_TAG}...${PREV_RELEASE_TAG}" | sort | uniq -i | wc -l | grep -Eo '[0-9]+')
printf "This release contains %s PRs from %s authors. Thank you!\n\n" "${NUM_PRS}" "${NUM_AUTHORS}"

#
# Release notes
#

# Title
printf "# Grafana Mimir version %s release notes\n\n" "${LAST_RELEASE_VERSION}"

# Add a place holder for the release notes.
printf "**TODO: add release notes here**\n\n"

#
# CHANGELOG
#

# Find the line at which the CHANGELOG for this version begins.
CHANGELOG_SECTION_TITLE="## ${LAST_RELEASE_VERSION}"
CHANGELOG_BEGIN_LINE=$(awk "/^${CHANGELOG_SECTION_TITLE}$/{ print NR; exit }" "${CHANGELOG_PATH}")
if [ -z "${CHANGELOG_BEGIN_LINE}" ]; then
  echo "Unable to find the section title '${CHANGELOG_SECTION_TITLE}' in the ${CHANGELOG_PATH}"
  exit 1
fi

# Find the line at which the CHANGELOG for this version ends.
CHANGELOG_END_LINE=$(tail -n +$((CHANGELOG_BEGIN_LINE + 1)) "${CHANGELOG_PATH}" | awk "/^## /{ print NR - 1; exit }")
if [ -z "${CHANGELOG_END_LINE}" ]; then
  echo "Unable to find the end of the section '${CHANGELOG_SECTION_TITLE}' in the ${CHANGELOG_PATH}"
  exit 1
fi

# Append the CHANGELOG section to the release notes.
printf "# Changelog\n\n"
tail -n +"${CHANGELOG_BEGIN_LINE}" "${CHANGELOG_PATH}" | head -$((CHANGELOG_END_LINE + 1))
