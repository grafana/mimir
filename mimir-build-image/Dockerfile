# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

# TEMPORARY: minimal stub used only to validate the CI publish path to GAR.
# The full Dockerfile is in git history at the previous commit (`git show HEAD~:mimir-build-image/Dockerfile`)
# and must be restored before this branch is merged.

ARG goproxyValue
ARG revision

FROM alpine:3.20

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
