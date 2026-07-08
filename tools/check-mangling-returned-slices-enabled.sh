#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# This script checks that every test package including and beneath
# pkg/streamingpromql enables types.EnableManglingReturnedSlices in an init
# function. Mangling slices returned to the pool helps detect use-after-return
# bugs, so we want it enabled whenever the engine's tests run.
#
# A single directory can contain test files split across an internal test
# package ("xxx") and an external test package ("xxx_test"). Both are compiled
# into the same test binary, so the init function can live in either package.
# This script therefore checks all *_test.go files in a directory together and
# passes the directory if any of them enables mangling in an init function.

set -euo pipefail

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT="pkg/streamingpromql"

cd "$PROJECT_DIR"

# Returns success if any of the given test files sets EnableManglingReturnedSlices
# to true inside an init function.
has_init_enabling_mangling() {
  awk '
    # Enter an init function. Brace tracking below detects when we leave it,
    # allowing multiple init functions per file.
    /^func[ \t]+init\(\)[ \t]*{/ { in_init = 1 }

    in_init {
      if (index($0, "EnableManglingReturnedSlices") && $0 ~ /=[ \t]*true/) {
        found = 1
      }

      opens = gsub(/{/, "{")
      closes = gsub(/}/, "}")
      depth += opens - closes

      if (depth <= 0) {
        in_init = 0
      }
    }

    END { exit(found ? 0 : 1) }
  ' "$@"
}

FAILURES=()

# Find every directory including and beneath the root that contains test files.
while IFS= read -r dir; do
  # shellcheck disable=SC2046
  # Word splitting is intended here: pass each test file as a separate argument.
  if ! has_init_enabling_mangling $(find "$dir" -maxdepth 1 -name '*_test.go'); then
    FAILURES+=("$dir")
  fi
done < <(find "$ROOT" -name '*_test.go' -exec dirname {} \; | sort -u)

if [ ${#FAILURES[@]} -ne 0 ]; then
  echo "The following test packages do not set types.EnableManglingReturnedSlices to true in an init function:"
  echo
  for dir in "${FAILURES[@]}"; do
    echo "  $dir"
  done
  echo
  echo "Add an init function to one of the package's test files (either the 'xxx' or 'xxx_test' package):"
  echo
  echo "  func init() {"
  echo "  	types.EnableManglingReturnedSlices = true"
  echo "  }"
  echo
  echo "This helps detect use-after-return bugs by mangling slices returned to the pool."
  exit 1
fi

echo "All test packages including and beneath $ROOT enable types.EnableManglingReturnedSlices in an init function."
