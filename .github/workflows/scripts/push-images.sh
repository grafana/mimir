#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

IMAGES_DIR="$1"
IMAGE_PREFIX="$2"
TAG="$3"

# Push images from OCI archives to docker registry.
for image in "$IMAGES_DIR"/*
do
  NAME=$(basename ${image%%.oci})
  # --all uploads all platform images from OCI
  echo
  echo "Uploading ${IMAGE_PREFIX}${NAME}:${TAG}"
  echo
  skopeo copy --all --retry-times 3 oci-archive:${image} "docker://${IMAGE_PREFIX}${NAME}:${TAG}"
done
