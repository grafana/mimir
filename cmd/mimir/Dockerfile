# SPDX-License-Identifier: AGPL-3.0-only
# We use different base images for mimir and mimir race images since race-detector needs glibc packages.
# See difference between distroless static and base-nossl at https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md.

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:15f8637509792e05b97d83451b0b6ccfa4abd548f997e406ac1e72c87fe1fa78

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying mimir binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       mimir${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/mimir
EXPOSE     8080
ENTRYPOINT [ "/bin/mimir" ]

ARG revision
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir" \
      org.opencontainers.image.revision="${revision}"
