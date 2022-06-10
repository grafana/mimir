# SPDX-License-Identifier: AGPL-3.0-only

FROM       alpine:3.16.0
RUN        apk add --no-cache ca-certificates
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       mimir-continuous-test${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /mimir-continuous-test
ENTRYPOINT ["/mimir-continuous-test"]

ARG revision
LABEL org.opencontainers.image.title="mimir-continuous-test" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir-continuous-test" \
      org.opencontainers.image.revision="${revision}"
