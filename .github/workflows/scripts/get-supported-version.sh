#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -o errexit
set -o nounset
set -o pipefail

IMAGES_NAME="$1"
tags=$(skopeo --override-os linux inspect docker://$IMAGES_NAME | jq -r '.RepoTags' | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+" | sort -ur)
supported_versions=$(echo "$tags" | awk -F '.' '{print $1 "." $2}' | sort -ur | head -n 2)

previous_major=$(echo "$tags" | awk -F '.' '{print $1}' | head -n 1)
previous_major=$((previous_major - 1))

previous_major_tags=$(echo "$tags" | grep -Eo "${previous_major}\.[0-9]+\.[0-9]+")
if [ -z "$previous_major_tags" ]; then
    echo "No previous minor version found"
else
    previous_minor=$(echo "$previous_major_tags" | awk -F '.' '{print $1 "." $2}' | sort -ur | head -n 1)
    echo "Previous minor version is $previous_minor" 
fi
# Construct the pattern for the last minor version of the previous major version
# pattern="^$previous_major\.[0-9]+$"
# | awk -F '.' '{print $1 "." $2}'
# Extract the last minor version of the previous major version
# last_minor_of_previous_major=$(echo "$latest_versions" | grep -E "$pattern" | sort -u | tail -n 1)

# previous_major=$(echo "$current_major-1" | bc)


# latest_versions=$(printf "%s\n%s" "$latest_versions" "$previous_minor")
# Add prefix "release-" to each element
# latest_versions=$(echo "$latest_versions" | sed 's/^/release-/')

echo "$previous_minor"
