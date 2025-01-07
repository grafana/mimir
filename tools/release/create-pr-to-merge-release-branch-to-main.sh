#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Check input parameters.
if [ $# -gt 1 ]; then
  echo "Usage: $0 [release-branch]"
  echo ""
  echo "  release-branch  The release branch name. If omitted, the latest"
  echo "                  release branch will be auto-detected."
  echo ""
  exit 1
elif [ $# -eq 1 ]; then
  LAST_RELEASE_BRANCH="$1"
fi

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

check_required_setup

# Ensure there are not uncommitted changes.
if [ -n "$(git status --porcelain=v1 2> /dev/null)" ]; then
  echo "Please make sure there are no uncommitted changes."
  exit 1
fi

# Auto-detect the last release branch, if it wasn't provided.
if [ -z "${LAST_RELEASE_BRANCH}" ]; then
  LAST_RELEASE_BRANCH=$(git branch --list 'release-*' | sort -V | grep -Eo 'release-[^ ]+' | tail -1)
  if [ -z "${LAST_RELEASE_BRANCH}" ]; then
    echo "Unable to find the last release branch. You can provide it directly to the script."
    exit 1
  fi
fi

BRANCH_NAME="merge-${LAST_RELEASE_BRANCH}-to-main"

# Ask confirmation.
read -p "You're about to open a PR to merge '${LAST_RELEASE_BRANCH}' to main. Do you want to continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting ..."
    exit 1
fi

# Prepare the branch.
git checkout main
git pull
git checkout -b "${BRANCH_NAME}"

# The merge command could fail if there are conflicts but we don't want to abort the script in that case.
echo ""
git merge -m "Merge ${LAST_RELEASE_BRANCH} branch to main" "origin/${LAST_RELEASE_BRANCH}" || true
echo ""

# Wait until the status is clean.
while true; do
  # Print some instructions on how to proceed.
  echo "The '${LAST_RELEASE_BRANCH}' branch has been merged to main. Please do the following:"
  echo "1. Ensure VERSION is not changed (keep the same version as in main)."
  echo "2. Fix any conflict."
  echo "3. Clean up CHANGELOG.md: incoming entries from release branch should be under proper release version. There may be duplicate entries already."
  echo "4. Run 'git commit'."

  read -p "Press any key to continue... " -n 1 -r
  echo ""

  # Ensure the status is clean.
  if [ -z "$(git status --porcelain=v1 2> /dev/null)" ]; then
    break
  fi
done

# Open the PR.
git push -u origin "${BRANCH_NAME}"
gh pr create --title "Merge ${LAST_RELEASE_BRANCH} to main" --body "In this PR I'm merging the ${LAST_RELEASE_BRANCH} branch to main branch. Please merge this PR using a **merge commit** or by using \`tools/release/merge-approved-pr-branch-to-main.sh\` script."
