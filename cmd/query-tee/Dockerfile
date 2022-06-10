# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/cmd/query-tee/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM       alpine:3.16.0
RUN        apk add --no-cache ca-certificates
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       query-tee${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /query-tee
ENTRYPOINT ["/query-tee"]

ARG revision
LABEL org.opencontainers.image.title="query-tee" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/query-tee" \
      org.opencontainers.image.revision="${revision}"
