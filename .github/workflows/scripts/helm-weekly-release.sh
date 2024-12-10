#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -exo pipefail

# Uses docker hub image tags to figure out what is the latest image tag
find_latest_image_tag() {
  local docker_hub_repo=$1
  echo $(crane ls "${docker_hub_repo}" | grep -P 'r\d+-[a-z0-9]+' | sort -Vur | head -1)
}

# takes r197-abcdef and returns r197
extract_r_version() {
  local version=$1
  echo $(grep -P -o 'r\d+' <<< $version)
}

# This generates a new file where the yaml node is updated.
# The problem is that yq strips new lines when you update the file.
# So we use a workaround from https://github.com/mikefarah/yq/issues/515 which:
# generates the new file, diffs it with the original, removes all non-whitespace changes, and applies that to the original file.
update_yaml_node() {
  local filename=$1
  local yaml_node=$2
  local new_value=$3
  patch "$filename" <<< $(diff -U0 -w -b --ignore-blank-lines $filename <(yq eval "$yaml_node = \"$new_value\"" $filename))
}

get_yaml_node() {
  local filename=$1
  local yaml_node=$2
  echo $(yq $yaml_node $filename)
}

# Increments the part of the semver string
# $1: version itself
# $2: number of part: 0 – major, 1 – minor, 2 – patch
increment_semver() {
  local delimiter=.
  local array=($(echo "$1" | tr $delimiter '\n'))
  array[$2]=$((array[$2]+1))
  echo $(local IFS=$delimiter ; echo "${array[*]}")
}

# Sets the patch segment of a semver to 0
# $1: version itself
set_semver_patch_to_zero() {
  local delimiter=.
  local array=($(echo "$1" | tr $delimiter '\n'))
  array[2]="0"
  echo $(local IFS=$delimiter ; echo "${array[*]}")
}

calculate_next_chart_version() {
  local current_chart_version=$1
  local latest_image_tag=$2

  local current_chart_semver=$(echo $current_chart_version | grep -P -o '^(\d+.){2}\d+')
  local new_chart_weekly=$(extract_r_version $latest_image_tag | grep -P -o '\d+')
  local new_chart_semver=$current_chart_semver
  if [[ $current_chart_version != *weekly* ]]; then
    # If previous version was not a weekly, then it was a stable release.
    # _This_ weekly release should have a semver that's one above the stable release.
    new_chart_semver=$(increment_semver $current_chart_semver 1)
    # Also reset the patch release number to 0.
    new_chart_semver=$(set_semver_patch_to_zero $new_chart_semver)
  fi
  echo "$new_chart_semver-weekly.$new_chart_weekly"
}

validate_version_update() {
  local new_chart_version=$1
  local current_chart_version=$2
  local latest_gem_tag=$3
  local latest_mimir_tag=$4

  if [[ "$new_chart_version" == "$current_chart_version" ]]; then
    echo "New chart version ($new_chart_version) is the same as current version ($current_chart_version); not submitting weekly PR"
    exit 1
  fi

  local gem_weekly_version=$(extract_r_version $latest_gem_tag)
  local mimir_weekly_version=$(extract_r_version $latest_mimir_tag)
  if [[ "$gem_weekly_version" != "$mimir_weekly_version" ]]; then
    echo "GEM weekly version ($gem_weekly_version) does not match Mimir weekly version ($mimir_weekly_version); not submitting PR"
    exit 1
  fi
}

values_file=operations/helm/charts/mimir-distributed/values.yaml
chart_file=operations/helm/charts/mimir-distributed/Chart.yaml

latest_mimir_tag=$(find_latest_image_tag grafana/mimir)
latest_gem_tag=$(find_latest_image_tag grafana/enterprise-metrics)
current_chart_version=$(get_yaml_node $chart_file .version)
new_chart_version=$(calculate_next_chart_version $current_chart_version $latest_mimir_tag)

validate_version_update $new_chart_version $current_chart_version $latest_gem_tag $latest_mimir_tag

update_yaml_node $values_file .image.tag $latest_mimir_tag
update_yaml_node $values_file .enterprise.image.tag $latest_gem_tag
update_yaml_node $chart_file .appVersion $(extract_r_version $latest_mimir_tag)
update_yaml_node $chart_file .version $new_chart_version

make TTY='' doc

echo "::set-output name=new_chart_version::$new_chart_version"
