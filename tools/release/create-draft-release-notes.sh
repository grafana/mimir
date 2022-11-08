#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_setup
find_last_release
find_prev_release

CHANGELOG_PATH="${CURR_DIR}/../../CHANGELOG.md"

#
# Contributors
#

# We use the 2-dots notation to diff because in this context we only want to get the new commits in the last release.
NUM_PRS=$(git log --pretty=format:"%s" "${PREV_RELEASE_TAG}..${LAST_RELEASE_TAG}" | grep -cE '#[0-9]+')
NUM_AUTHORS=$(git log --pretty=format:"%an" "${PREV_RELEASE_TAG}..${LAST_RELEASE_TAG}" | sort | uniq -i | wc -l | grep -Eo '[0-9]+')
NEW_AUTHORS=$(diff <(git log --pretty=format:"%an" "${PREV_RELEASE_TAG}" | sort | uniq -i) <(git log --pretty=format:"%an" "${LAST_RELEASE_TAG}" | sort | uniq -i) | grep -E '^>' | cut -c 3- | gsed -z 's/\n/, /g;s/, $//')

if [ -z "${NEW_AUTHORS}" ]; then
  printf "This release contains %s PRs from %s authors. Thank you!\n\n" "${NUM_PRS}" "${NUM_AUTHORS}"
else
  printf "This release contains %s PRs from %s authors, including new contributors %s. Thank you!\n\n" "${NUM_PRS}" "${NUM_AUTHORS}" "${NEW_AUTHORS}"
fi

#
# Release notes
#

# We don't publish release notes for patch versions.
PREV_RELEASE_MINOR_VERSION=$(echo -n "${PREV_RELEASE_VERSION}" | grep -Eo '^[0-9]+\.[0-9]+')
LAST_RELEASE_MINOR_VERSION=$(echo -n "${LAST_RELEASE_VERSION}" | grep -Eo '^[0-9]+\.[0-9]+')

if [ "${PREV_RELEASE_MINOR_VERSION}" != "${LAST_RELEASE_MINOR_VERSION}" ]; then
  # Title
  printf "# Grafana Mimir version %s release notes\n\n" "${LAST_RELEASE_VERSION}"

  # Add a place holder for the release notes.
  printf "**TODO: add release notes here**\n\n"
fi

#
# CHANGELOG
#

# Find the line at which the CHANGELOG for this version begins.
CHANGELOG_SECTION_TITLE="## ${LAST_RELEASE_VERSION}"
CHANGELOG_BEGIN_LINE=$(awk "/^${CHANGELOG_SECTION_TITLE}$/{ print NR; exit }" "${CHANGELOG_PATH}")
if [ -z "${CHANGELOG_BEGIN_LINE}" ]; then
  echo "Unable to find the section title '${CHANGELOG_SECTION_TITLE}' in the ${CHANGELOG_PATH}" > /dev/stderr
  exit 1
fi

# Find the line at which the CHANGELOG for this version ends.
CHANGELOG_END_LINE=$(tail -n +$((CHANGELOG_BEGIN_LINE + 1)) "${CHANGELOG_PATH}" | awk "/^## /{ print NR - 1; exit }")
if [ -z "${CHANGELOG_END_LINE}" ]; then
  echo "Unable to find the end of the section '${CHANGELOG_SECTION_TITLE}' in the ${CHANGELOG_PATH}" > /dev/stderr
  exit 1
fi

# Append the CHANGELOG section to the release notes.
printf "# Changelog\n\n"
tail -n +"${CHANGELOG_BEGIN_LINE}" "${CHANGELOG_PATH}" | head -$((CHANGELOG_END_LINE + 1))
printf "\n"

# Link to changes.
printf "**All changes in this release**: https://github.com/grafana/mimir/compare/%s...%s\n" "${PREV_RELEASE_TAG}" "${LAST_RELEASE_TAG}"
