#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -o errexit
set -o nounset
set -o pipefail

OUTPUT="$1"
shift

mkdir "$OUTPUT"

# Build all supplied targets (will be prefixed with push-multiarch- prefix)
# shellcheck disable=SC2068
for target in $@
do
  DIRNAME="$(dirname "$target")"
  NAME="$(basename "$DIRNAME")"
  make \
    BUILD_IN_CONTAINER=false \
    PUSH_MULTIARCH_TARGET="type=oci,dest=$OUTPUT/$NAME.oci" \
    PUSH_MULTIARCH_TARGET_CONTINUOUS_TEST="type=oci,dest=$OUTPUT/$NAME\-continuous\-test.oci" \
    push-multiarch-$target
done
