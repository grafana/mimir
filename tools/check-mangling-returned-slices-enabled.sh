#! /usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# This script checks that every test package including and beneath
# pkg/streamingpromql enables types.EnableManglingReturnedSlices in an init
# function. Mangling slices returned to the pool helps detect use-after-return
# bugs, so we want it enabled whenever the engine's tests run.
#
# Rather than trying to parse Go, we require each test directory to contain an
# init_test.go file whose contents exactly match one of two canonical
# templates. The package name is the only part allowed to vary, so we read it
# from the file and regenerate the expected content to compare against:
#
#   - most packages import the types package and set
#     types.EnableManglingReturnedSlices; and
#   - the types package itself refers to EnableManglingReturnedSlices directly,
#     as it cannot import itself.

set -euo pipefail

SCRIPT_DIR=$(realpath "$(dirname "${0}")")
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT="pkg/streamingpromql"

cd "$PROJECT_DIR"

main() {
  FAILURES=()

  # Find every directory including and beneath the root that contains test files.
  while IFS= read -r dir; do
    file="$dir/init_test.go"

    if [ ! -f "$file" ]; then
      FAILURES+=("$dir (missing init_test.go)")
      continue
    fi

    # Read the declared package name. Go enforces that it matches the directory's
    # package, so we only need it to regenerate the expected content.
    pkg=$(awk '$1 == "package" { print $2; exit }' "$file")

    if diff <(expected_with_import "$pkg") "$file" >/dev/null 2>&1; then
      continue
    fi

    if diff <(expected_without_import "$pkg") "$file" >/dev/null 2>&1; then
      continue
    fi

    FAILURES+=("$dir (init_test.go does not match the required content)")
  done < <(find "$ROOT" -name '*_test.go' -exec dirname {} \; | sort -u)

  if [ ${#FAILURES[@]} -ne 0 ]; then
    echo "The following test packages do not enable types.EnableManglingReturnedSlices in an init function:"
    echo
    for failure in "${FAILURES[@]}"; do
      echo "  $failure"
    done
    echo
    echo "Each test directory must contain an init_test.go with exactly this content (using the package's own name):"
    echo
    echo "  // SPDX-License-Identifier: AGPL-3.0-only"
    echo
    echo "  package <package>"
    echo
    echo "  import ("
    echo "  	\"github.com/grafana/mimir/pkg/streamingpromql/types\""
    echo "  )"
    echo
    echo "  func init() {"
    echo "  	types.EnableManglingReturnedSlices = true"
    echo "  }"
    echo
    echo "This helps detect use-after-return bugs by mangling slices returned to the pool."
    exit 1
  fi

  echo "All test packages including and beneath $ROOT enable types.EnableManglingReturnedSlices in an init function."
}

# Prints the init_test.go contents expected for a package that imports the
# types package. $1 is the package name.
expected_with_import() {
  cat <<EOF
// SPDX-License-Identifier: AGPL-3.0-only

package $1

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	types.EnableManglingReturnedSlices = true
}
EOF
}

# Prints the init_test.go contents expected for the types package itself, which
# refers to EnableManglingReturnedSlices directly. $1 is the package name.
expected_without_import() {
  cat <<EOF
// SPDX-License-Identifier: AGPL-3.0-only

package $1

func init() {
	EnableManglingReturnedSlices = true
}
EOF
}

main
