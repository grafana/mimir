# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/cmd/thanosconvert/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       metaconvert /
ENTRYPOINT ["/metaconvert"]

ARG revision
LABEL org.opencontainers.image.title="metaconvert" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/metaconvert" \
      org.opencontainers.image.revision="${revision}"
