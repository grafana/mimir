#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Creates a draft GitHub release for the release tag currently checked out.
#
# It prints a JSON object with the release metadata needed to publish it:
#
#   {"tag": "...", "title": "...", "prerelease": <bool>, "latest": <bool>}
#
# Usage:
#   create-draft-release.sh [--dry-run] [--artifacts-dir DIR]
#
#   --dry-run            Don't create the draft release, just print the release details.
#   --artifacts-dir DIR  Directory containing the artifacts to attach (default: ./dist).

set -e

# Load common lib.
CURR_DIR="$(dirname "$0")"
. "${CURR_DIR}/common.sh"

DRY_RUN=false
ARTIFACTS_DIR="${CURR_DIR}/../../dist"

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --artifacts-dir)
      ARTIFACTS_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" > /dev/stderr
      exit 1
      ;;
  esac
done

check_required_setup
find_last_release
find_prev_release

# Release candidates (version ending in -rc.N) are published as pre-releases and are never
# marked as "latest".
if [[ "${LAST_RELEASE_VERSION}" == *-rc.* ]]; then
  PRERELEASE=true
  LATEST=false
else
  PRERELEASE=false
  # Only mark as "latest" when this is the newest stable release, so that patching an older
  # minor doesn't steal the "latest" badge from a newer one.
  NEWEST_STABLE_TAG=$(git tag --list 'mimir-[0-9]*' | grep -E '^mimir-[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n 1)
  if [ "${NEWEST_STABLE_TAG}" = "${LAST_RELEASE_TAG}" ]; then
    LATEST=true
  else
    LATEST=false
  fi
fi

RELEASE_TITLE="Mimir ${LAST_RELEASE_VERSION}"

# Generate the release notes.
RELEASE_NOTES_FILE="$(mktemp)"
trap 'rm -f "${RELEASE_NOTES_FILE}"' EXIT
"${CURR_DIR}"/create-draft-release-notes.sh > "${RELEASE_NOTES_FILE}"

{
  echo "Artifacts:"
  for artifact in "${ARTIFACTS_DIR}"/*; do
    [ -f "${artifact}" ] || continue
    printf '%s\t%s\tsha256:%s\n' "$(basename "${artifact}")" "$(du -h "${artifact}" | cut -f1)" "$(sha256sum "${artifact}" | cut -d' ' -f1)"
  done
} > /dev/stderr

if [ "${DRY_RUN}" = "true" ]; then
  {
    echo ""
    echo "Release notes:"
    echo "----------------------------------------"
    cat "${RELEASE_NOTES_FILE}"
    echo "----------------------------------------"

    echo "Dry run: not creating the draft release ${LAST_RELEASE_TAG}."
  } > /dev/stderr
else
  # Create the release as a draft, so the artifacts can be attached before the caller publishes
  # it (and it becomes immutable). If a draft already exists (e.g. this is a re-run after a
  # transient failure), update it instead so that re-runs are idempotent.
  if gh release view "${LAST_RELEASE_TAG}" > /dev/null 2>&1; then
    echo "Updating existing release ${LAST_RELEASE_TAG}..." > /dev/stderr
    gh release edit "${LAST_RELEASE_TAG}" \
      --draft \
      --prerelease="${PRERELEASE}" \
      --title "${RELEASE_TITLE}" \
      --notes-file "${RELEASE_NOTES_FILE}" > /dev/stderr
  else
    echo "Creating the draft release ${LAST_RELEASE_TAG}..." > /dev/stderr
    gh release create "${LAST_RELEASE_TAG}" \
      --draft \
      --prerelease="${PRERELEASE}" \
      --title "${RELEASE_TITLE}" \
      --notes-file "${RELEASE_NOTES_FILE}" > /dev/stderr
  fi

  echo "Attaching the artifacts..." > /dev/stderr
  gh release upload "${LAST_RELEASE_TAG}" "${ARTIFACTS_DIR}"/* --clobber > /dev/stderr

  echo "Draft release created: https://github.com/grafana/mimir/releases/tag/${LAST_RELEASE_TAG}" > /dev/stderr
fi

jq -nc \
  --arg tag "${LAST_RELEASE_TAG}" \
  --arg title "${RELEASE_TITLE}" \
  --argjson prerelease "${PRERELEASE}" \
  --argjson latest "${LATEST}" \
  '{tag: $tag, title: $title, prerelease: $prerelease, latest: $latest}'
