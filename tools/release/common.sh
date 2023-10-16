#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# This is a library shared between scripts for managing the release process.
#

set -e

# Use GNU sed on MacOS falling back to `sed` everywhere else
SED=$(which gsed || which sed)

check_required_setup() {
  # Ensure "gh" tool is installed.
  if ! command -v gh &> /dev/null; then
    echo "The 'gh' command cannot be found. Please install it: https://cli.github.com" > /dev/stderr
    exit 1
  fi

  # Ensure the repository has only 1 remote named "origin".
  if [ "$(git remote)" != "origin" ]; then
    echo "The release automation scripts require the git clone to only have 1 remote named 'origin', but found:"
    git remote

    exit 1
  fi
}

# Last release is the version we want to release now
find_last_release() {
  LAST_RELEASE_TAG=$(git describe --abbrev=0 --match 'mimir-[0-9]*')

  if [ -z "${LAST_RELEASE_TAG}" ]; then
    echo "Unable to find the last release git tag" > /dev/stderr
    exit 1
  fi

  # Find the last release version. Since all tags start with "mimir-"
  # we can simply find the version:
  LAST_RELEASE_VERSION=$(echo "${LAST_RELEASE_TAG}" | cut -c 7-)

  export LAST_RELEASE_TAG
  export LAST_RELEASE_VERSION
}

# Previous release is one version before last release.
# If last release is an rc we will find any previous release.
# If last release is a stable (non-rc) release, previous release also must be a non-rc release.
find_prev_release() {
  find_last_release

  ALL_RELEASE_TAGS=$(git tag --list 'mimir-[0-9]*')

  # To sort rc version correctly we use sed to append non-rc version with a temporary suffix
  SORTED_RELEASE_TAGS=$(echo "$ALL_RELEASE_TAGS" | $SED '/rc/b; s/\(.*\)/\1-xx.x/' | sort --version-sort | gsed 's/-xx.x//')

  if [[ $LAST_RELEASE_VERSION =~ "-rc" ]]; then
    PREV_RELEASE_TAG=$(echo "$SORTED_RELEASE_TAGS" | grep $(echo $LAST_RELEASE_VERSION) -B1 | head -1)
  else
    PREV_RELEASE_TAG=$(echo "$SORTED_RELEASE_TAGS" | grep -v -- '-rc' | grep $(echo $LAST_RELEASE_VERSION) -B1 | head -1)
  fi

  if [ -z "${PREV_RELEASE_TAG}" ]; then
    echo "Unable to find the previous release git tag" > /dev/stderr
    exit 1
  fi

  # Find the prev release version. Since all tags start with "mimir-"
  # we can simply find the version:
  PREV_RELEASE_VERSION=$(echo "${PREV_RELEASE_TAG}" | cut -c 7-)

  export PREV_RELEASE_TAG
  export PREV_RELEASE_VERSION
}
