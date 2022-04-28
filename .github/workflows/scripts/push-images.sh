#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -o errexit
set -o nounset
set -o pipefail

IMAGES_DIR="$1"
IMAGE_PREFIX="$2"
TAG="$3"

# If image is the latest stable git tag, also push :latest image.
# Do not tag with latest any release candidate (tag ends with "-rc.*").
EXTRA_TAG=""
if [[ "$(git tag | grep -E '^mimir-[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n 1)" == "mimir-${TAG}" ]]; then
  EXTRA_TAG="latest"
fi

# Push images from OCI archives to docker registry.
for image in "$IMAGES_DIR"/*
do
  NAME=$(basename ${image%%.oci})
  # --all uploads all platform images from OCI
  echo
  echo "Uploading ${IMAGE_PREFIX}${NAME}:${TAG}"
  echo
  skopeo copy --all --retry-times 3 oci-archive:${image} "docker://${IMAGE_PREFIX}${NAME}:${TAG}"

  if [[ -n "${EXTRA_TAG}" ]]; then
    echo "Tagging with ${EXTRA_TAG}"
    skopeo copy --all --retry-times 3 "docker://${IMAGE_PREFIX}${NAME}:${TAG}" "docker://${IMAGE_PREFIX}${NAME}:${EXTRA_TAG}"
  fi
done
