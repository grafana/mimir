# SPDX-License-Identifier: AGPL-3.0-only

FROM       alpine:3.16.0
RUN        apk add --no-cache ca-certificates
# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG        TARGETOS
ARG        TARGETARCH
ARG        BINARY_SUFFIX="_${TARGETOS}_${TARGETARCH}"
# Set to non-empty value to use ${TARGET_SUFFIX} when copying mimir binary, leave unset to use no suffix.
ARG        USE_BINARY_SUFFIX
COPY       memberlist-ha${USE_BINARY_SUFFIX:+${BINARY_SUFFIX}} /bin/memberlist-ha
EXPOSE     8080
ENTRYPOINT [ "/bin/memberlist-ha" ]

ARG revision
