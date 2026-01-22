# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian12@sha256:cd64bec9cec257044ce3a8dd3620cf83b387920100332f2b041f19c4d2febf93
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
COPY       write-tee${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /write-tee
# Copy the updated CA bundle from the updated-ca-bundle stage
COPY --from=updated-ca-bundle /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/write-tee"]

ARG revision
LABEL org.opencontainers.image.title="write-tee" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/write-tee" \
      org.opencontainers.image.revision="${revision}"
