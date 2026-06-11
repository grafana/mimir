# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:3592aa8171c77482f62bbc4164e6a2d141c6122554ace66e5cc910cadb961ff0

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       copyblocks${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /copyblocks
EXPOSE     8080
ENTRYPOINT ["/copyblocks"]

ARG revision
LABEL org.opencontainers.image.title="copyblocks" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/tools/copyblocks" \
      org.opencontainers.image.revision="${revision}"
