#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# This is a library shared between scripts for managing the release process.
#

set -e

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

find_last_release() {
  LAST_RELEASE_TAG=$(git tag --list --sort=taggerdate 'mimir-[0-9]*' | tail -1)

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

# Find the previous release. If the last release is a stable release then it only takes in account stable releases
# (the previous release of a stable release must be another stable release).
find_prev_release() {
  find_last_release

  if [[ $LAST_RELEASE_VERSION =~ "-rc" ]]; then
    PREV_RELEASE_TAG=$(git tag --list 'mimir-[0-9]*' | tail -2 | head -1)
  else
    PREV_RELEASE_TAG=$(git tag --list 'mimir-[0-9]*' | grep -v -- '-rc' | tail -2 | head -1)
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
