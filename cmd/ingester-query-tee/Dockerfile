# SPDX-License-Identifier: AGPL-3.0-only

ARG BASEIMG=gcr.io/distroless/static-debian13@sha256:3592aa8171c77482f62bbc4164e6a2d141c6122554ace66e5cc910cadb961ff0
FROM ${BASEIMG}
ARG TARGETOS
ARG TARGETARCH
ARG BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
ARG USE_BINARY_SUFFIX
COPY ingester-query-tee${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /ingester-query-tee
ENTRYPOINT ["/ingester-query-tee"]

ARG revision
LABEL org.opencontainers.image.title="ingester-query-tee" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/ingester-query-tee" \
      org.opencontainers.image.revision="${revision}"
