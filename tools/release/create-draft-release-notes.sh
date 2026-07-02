#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

find_last_release
find_prev_release

REPO_ROOT="${CURR_DIR}/../.."
CHANGELOG_PATH="${REPO_ROOT}/CHANGELOG.md"

#
# Contributors
#

# We use the 2-dots notation to diff because in this context we only want to get the new commits in the last release.
NUM_PRS=$(git log --pretty=format:"%s" "${PREV_RELEASE_TAG}..${LAST_RELEASE_TAG}" | grep -cE '#[0-9]+')
NUM_AUTHORS=$(git log --pretty=format:"%an" "${PREV_RELEASE_TAG}..${LAST_RELEASE_TAG}" | sort | uniq -i | wc -l | grep -Eo '[0-9]+')
NEW_AUTHORS=$(diff <(git log --pretty=format:"%an" "${PREV_RELEASE_TAG}" | sort | uniq -i) <(git log --pretty=format:"%an" "${LAST_RELEASE_TAG}" | sort | uniq -i) | grep -E '^>' | cut -c 3- | $SED -z 's/\n/, /g;s/, $//')

if [ -z "${NEW_AUTHORS}" ]; then
  printf "This release contains %s PRs from %s authors. Thank you!\n\n" "${NUM_PRS}" "${NUM_AUTHORS}"
else
  printf "This release contains %s PRs from %s authors, including new contributors %s. Thank you!\n\n" "${NUM_PRS}" "${NUM_AUTHORS}" "${NEW_AUTHORS}"
fi

#
# Release notes
#

# We only publish release notes for major and minor releases (including their release
# candidates), which are the ones whose patch version is 0. Patch releases only get a CHANGELOG.
RELEASE_PATCH=$(echo -n "${LAST_RELEASE_VERSION}" | $SED -E 's/^[0-9]+\.[0-9]+\.([0-9]+).*/\1/')
RELEASE_MINOR=$(echo -n "${LAST_RELEASE_VERSION}" | grep -Eo '^[0-9]+\.[0-9]+')

if [ "${RELEASE_PATCH}" = "0" ]; then
  RELEASE_NOTES_DOC="${REPO_ROOT}/docs/sources/mimir/release-notes/v${RELEASE_MINOR}.md"

  if [ ! -f "${RELEASE_NOTES_DOC}" ]; then
    echo "Expected release notes document '${RELEASE_NOTES_DOC}' for release ${LAST_RELEASE_VERSION}, but it was not found." > /dev/stderr
    echo "The release notes document must be committed to the release branch before tagging the release." > /dev/stderr
    exit 1
  fi

  FIRST_HEADING_LINE=$(grep -n -m1 '^# ' "${RELEASE_NOTES_DOC}" | cut -d: -f1)
  tail -n +"${FIRST_HEADING_LINE}" "${RELEASE_NOTES_DOC}" | # Remove everything before the first heading (the YAML front matter)
    $SED -E '/^<!--.*-->$/d' | # Remove HTML comments (e.g. vale directives)
    $SED -E '/^\{\{[<%].*[%>]\}\}$/d' # Remove Hugo shortcodes, keeping inner content
  printf "\n"
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
