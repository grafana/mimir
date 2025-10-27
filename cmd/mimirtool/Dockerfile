# SPDX-License-Identifier: AGPL-3.0-only

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
COPY       mimirtool${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/mimirtool
# Copy the updated CA bundle from the updated-ca-bundle stage
COPY --from=updated-ca-bundle /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT [ "/bin/mimirtool" ]

ARG revision
LABEL org.opencontainers.image.title="mimirtool" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimirtool" \
      org.opencontainers.image.revision="${revision}"
