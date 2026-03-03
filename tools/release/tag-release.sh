#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_setup

# Ensure the current branch is a release one.
BRANCH=$(git branch --show-current)
if [[ ! $BRANCH =~ ^release-([0-9]+\.[0-9]+)$ ]]; then
  echo "The current branch '${BRANCH}' is not a release branch." > /dev/stderr
  exit 1
fi

# Get the actual branch version from the previous regex.
BRANCH_VERSION=${BASH_REMATCH[1]}

# Load the version and ensure it matches.
ACTUAL_VERSION=$(cat VERSION)
if [[ ! $ACTUAL_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$ ]]; then
  echo "The VERSION file contains an invalid version '${ACTUAL_VERSION}'. Expected format: MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-rc.N" > /dev/stderr
  exit 1
fi
if [[ ! $ACTUAL_VERSION =~ ^$BRANCH_VERSION ]]; then
  echo "The current branch '${BRANCH}' doesn't match the content of the VERSION file '${ACTUAL_VERSION}'" > /dev/stderr
  exit 1
fi

# Ask confirmation.
read -p "You're about to tag the version '${ACTUAL_VERSION}'. Do you want to continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting ..." > /dev/stderr
    exit 1
fi

git tag -s "mimir-${ACTUAL_VERSION}" -m "v${ACTUAL_VERSION}"
git push origin "mimir-${ACTUAL_VERSION}"

echo ""
echo "Version '${ACTUAL_VERSION}' successfully tagged."
