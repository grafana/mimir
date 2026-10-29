# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:15f8637509792e05b97d83451b0b6ccfa4abd548f997e406ac1e72c87fe1fa78

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
