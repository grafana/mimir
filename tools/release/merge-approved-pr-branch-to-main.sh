#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

BRANCH_NAME="$1"

# Check input parameters.
if [[ -z "$BRANCH_NAME" ]]; then
  cat <<END
Usage: $0 [branch]

  branch    Branch that should be merged to main using merge commit.
            This is typically the branch created by create-pr-to-merge-release-branch-to-main.sh script.
            Merging of this branch needs to be approved by PR first, otherwise this script will fail.

END
  exit 1
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

# Get latest changes from origin, this includes both main and our merged branch.
git fetch origin

FULL_BRANCH_NAME="remotes/origin/$BRANCH_NAME"
FULL_MAIN_NAME="remotes/origin/main"

# Verify that merged branch exists.
if ! git branch -a | grep -q "$FULL_BRANCH_NAME"; then
  echo "Branch $BRANCH_NAME not found in this Git repository"
  exit 1
fi

# Prepare the main branch for merging.
git checkout main

# Make sure that main is up-to-date wrt. origin/main. We use reset to avoid any weird
# changes that one can have on the main branch (eg. from previous run of this script).
# (git pull could fail in that case.)
git reset --hard "$FULL_MAIN_NAME"

# We use $FULL_BRANCH_NAME here to make sure we merge the correct version of the branch
# as found on server (updated via git fetch origin), and not possibly outdated local copy.
git merge -n -m "Merge branch $BRANCH_NAME to main." "$FULL_BRANCH_NAME" || (echo; echo "Merge failed. This can happen if there are unresolved merge conflicts."; echo "Please update $BRANCH_NAME to fix conflicts and run this script again."; exit 1)

echo
echo "Merge successful, pushing to origin..."
echo

# Pushing to Github only succeeds if PR for $BRANCH_NAME was approved.
# Successful push will also automatically close the PR.
if ! git push origin HEAD:main; then
  echo
  echo "Push to Github failed. This can happen if PR was not approved yet"
  echo "or if there was another commit pushed to main in the meantime."
  echo "In that case, running this script again may succeed."
  exit 1
fi
