#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"
CHANGELOG_PATH="$1"

check_required_setup

# Config
NOTIFICATION_LABEL="release/notified-changelog-cut"

# List all open PRs that modify CHANGELOG.md.
OPEN_PR_IDS=$(gh pr list --repo grafana/mimir --limit 1000 --state open --json number,title,files --jq '.[] | select(.files[].path == "'$CHANGELOG_PATH'") | .number')

for PR_ID in ${OPEN_PR_IDS}; do
  # Get PR details
  PR_TITLE=$(gh pr view --comments "${PR_ID}" --json title --jq '.title')
  PR_LABELS=$(gh pr view --comments "${PR_ID}" --json labels --jq '.labels[].name')

  # Log the PR we're currently processing.
  echo "- ${PR_ID}: ${PR_TITLE}"

  # Check if the notification has already been posted.
  # We suppress the linter warning about looking for the extract substring instead of doing a regex
  # match, but that's exactly what we want.
  # shellcheck disable=SC2076
  if [[ ${PR_LABELS} =~ "${NOTIFICATION_LABEL}" ]]; then
    echo "  Notification already posted, nothing to do"
    continue
  fi

  # The backtick here is markdown and we don't want to get it evaluated by the shell.
  # shellcheck disable=SC2016
  PR_COMMENT_LINK=$(gh pr comment "${PR_ID}" --body 'The CHANGELOG has just been cut to prepare for the next release. Please rebase `main` and eventually move the CHANGELOG entry added / updated in this PR to the top of the '$CHANGELOG_PATH' document. Thanks!')
  gh pr edit "${PR_ID}" --add-label "${NOTIFICATION_LABEL}" >/dev/null

  echo "  Notification posted: ${PR_COMMENT_LINK}"
done
