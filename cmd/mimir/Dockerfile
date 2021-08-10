# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/cmd/cortex/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       mimir /bin/mimir
EXPOSE     80
ENTRYPOINT [ "/bin/mimir" ]

ARG revision
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir" \
      org.opencontainers.image.revision="${revision}"
