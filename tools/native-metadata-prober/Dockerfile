# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:448810357cc8e67b7208988abdba0f5ec3d3aebee0a5b13eb624979792cffc0f

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       native-metadata-prober${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /native-metadata-prober
EXPOSE     8080
ENTRYPOINT ["/native-metadata-prober"]

ARG revision
LABEL org.opencontainers.image.title="native-metadata-prober" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/tools/native-metadata-prober" \
      org.opencontainers.image.revision="${revision}"
