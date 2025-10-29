# SPDX-License-Identifier: AGPL-3.0-only
# We use different base images for mimir and mimir race images since race-detector needs glibc packages.
# See difference between distroless static and base-nossl at https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md.

ARG        BASEIMG=gcr.io/distroless/static-debian12@sha256:87bce11be0af225e4ca761c40babb06d6d559f5767fbf7dc3c47f0f1a466b92c
# TODO(rwwiv): Remove once CA bundle is updated upstream.
FROM debian:testing AS updated-ca-bundle
RUN apt update && apt install -y ca-certificates && update-ca-certificates && apt clean

FROM       ${BASEIMG}
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${BINARY_SUFFIX} when copying mimir binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       mimir${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/mimir
# Copy the updated CA bundle from the updated-ca-bundle stage
COPY --from=updated-ca-bundle /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE     8080
ENTRYPOINT [ "/bin/mimir" ]

ARG revision
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir" \
      org.opencontainers.image.revision="${revision}"
