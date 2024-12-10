# SPDX-License-Identifier: AGPL-3.0-only

FROM       alpine:3.21.0
ARG        EXTRA_PACKAGES
RUN        apk add --no-cache ca-certificates tzdata $EXTRA_PACKAGES
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
