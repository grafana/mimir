# SPDX-License-Identifier: AGPL-3.0-only

FROM       alpine:3.16.2
RUN        apk add --no-cache ca-certificates
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       store-gateway-load-test${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /store-gateway-load-test
ENTRYPOINT ["/store-gateway-load-test"]

ARG revision
LABEL org.opencontainers.image.title="store-gateway-load-test" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/store-gateway-load-test" \
      org.opencontainers.image.revision="${revision}"
