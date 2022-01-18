# SPDX-License-Identifier: Apache-2.0

FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying mimir binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       cortextool${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/cortextool
ENTRYPOINT [ "/bin/cortextool" ]
