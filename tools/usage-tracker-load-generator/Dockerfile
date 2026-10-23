# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:9e0130bec991f471279b70f1630ada11e46dad439ef2949544f96001225329fb

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       usage-tracker-load-generator${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /usage-tracker-load-generator
EXPOSE     8080
ENTRYPOINT ["/usage-tracker-load-generator"]

ARG revision
LABEL org.opencontainers.image.title="usage-tracker-load-generator" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/tools/usage-tracker-load-generator" \
      org.opencontainers.image.revision="${revision}"
