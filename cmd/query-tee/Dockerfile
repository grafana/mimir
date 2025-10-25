# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/cmd/query-tee/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

ARG        BASEIMG=gcr.io/distroless/static-debian12@sha256:87bce11be0af225e4ca761c40babb06d6d559f5767fbf7dc3c47f0f1a466b92c
# TODO(rwwiv): Remove once CA bundle is updated upstream.
FROM debian:testing AS updated-ca-bundle
RUN apt update && apt install -y ca-certificates && update-ca-certificates && apt clean

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       query-tee${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /query-tee
# Copy the updated CA bundle from the updated-ca-bundle stage
COPY --from=updated-ca-bundle /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/query-tee"]

ARG revision
LABEL org.opencontainers.image.title="query-tee" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/query-tee" \
      org.opencontainers.image.revision="${revision}"
