#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# This is a library shared between scripts for managing the release process.
#

set -e

check_required_tools() {
  # Ensure "gh" tool is installed.
  if ! command -v gh &> /dev/null; then
      echo "The 'gh' command cannot be found. Please install it: https://cli.github.com"
      exit 1
  fi
}

find_last_release() {
  LAST_RELEASE_TAG=$(git tag --list 'mimir-[0-9]*' | sort -V | tail -1)

  if [ -z "${LAST_RELEASE_TAG}" ]; then
    echo "Unable to find the last release git tag"
    exit 1
  fi

  # Find the last release version. Since all tags start with "mimir-"
  # we can simply find the version:
  LAST_RELEASE_VERSION=$(echo "${LAST_RELEASE_TAG}" | cut -c 7-)

  export LAST_RELEASE_TAG
  export LAST_RELEASE_VERSION
}

find_prev_release() {
  PREV_RELEASE_TAG=$(git tag --list 'mimir-[0-9]*' | sort -V | tail -2 | head -1)

  if [ -z "${PREV_RELEASE_TAG}" ]; then
    echo "Unable to find the previous release git tag"
    exit 1
  fi

  export PREV_RELEASE_TAG
}
