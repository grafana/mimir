#!/usr/bin/env bash

set -exo pipefail

grep --version
which grep
# Uses docker hub image tags to figure out what is the latest image tag
find_latest_image_tag() {
  docker_hub_repo=$1
  echo $(crane ls "${docker_hub_repo}" | grep 'r\d\+-[a-z0-9]\+' | sort -Vur | head -1)
}

# This generates a new file where the yaml node is updated.
# The problem is that yq strips new lines when you update the file.
# So we use a workaround from https://github.com/mikefarah/yq/issues/515 which:
# generates the new file, diffs it with the original, removes all non-whitespace changes, and applies that to the original file.
update_yaml_node() {
  filename=$1
  yaml_node=$2
  new_value=$3
  patch "$filename" <<< $(diff -U0 -w -b --ignore-blank-lines $filename <(yq eval "$yaml_node = \"$new_value\"" $filename))
}

get_yaml_node() {
  filename=$1
  yaml_node=$2
  echo $(yq $yaml_node $filename)
}

### Increments the part of the string
## $1: version itself
## $2: number of part: 0 – major, 1 – minor, 2 – patch
increment_semver() {
  local delimiter=.
  local array=($(echo "$1" | tr $delimiter '\n'))
  array[$2]=$((array[$2]+1))
  echo $(local IFS=$delimiter ; echo "${array[*]}")
}

latest_mimir_tag=$(find_latest_image_tag grafana/mimir)
latest_gem_tag=$(find_latest_image_tag grafana/enterprise-metrics)

values_file=operations/helm/charts/mimir-distributed/values.yaml
chart_file=operations/helm/charts/mimir-distributed/Chart.yaml

calculate_next_chart_version() {
  current_chart_version=$(get_yaml_node $chart_file .version)
  current_chart_semver=$(echo $current_chart_version | grep -o '^\(\d\+.\)\{2\}\d\+')
  new_chart_weekly=$(echo $latest_mimir_tag | grep 'r\d\+' -o | grep -o '\d\+')
  new_chart_semver=$current_chart_semver
  if [[ $current_chart_version != *weekly* ]]; then
    # If previous version was not a weekly, then it was a stable release.
    # _This_ weekly release should have a semver that's one above the
    new_chart_semver=$(increment_semver $current_chart_semver 1)
  fi
  echo "$new_chart_semver-weekly.$new_chart_weekly"
}

new_chart_version=$(calculate_next_chart_version)

update_yaml_node $values_file .image.tag $latest_mimir_tag
update_yaml_node $values_file .smoke_test.image.tag $latest_mimir_tag
update_yaml_node $values_file .enterprise.image.tag $latest_mimir_tag
update_yaml_node $chart_file .appVersion $latest_mimir_tag
update_yaml_node $chart_file .version $new_chart_version

make build-helm-tests doc

echo "::set-output name=new_chart_version::$new_chart_version"
