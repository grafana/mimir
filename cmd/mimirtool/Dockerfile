# SPDX-License-Identifier: Apache-2.0
ARG ALPINE_VERSION=please-use-make-to-build-images
FROM       alpine:${ALPINE_VERSION}
RUN        apk add --no-cache ca-certificates tzdata
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       mimirtool${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/mimirtool
ENTRYPOINT [ "/bin/mimirtool" ]
