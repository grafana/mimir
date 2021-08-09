# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/cmd/blocksconvert/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       blocksconvert /
ENTRYPOINT ["/blocksconvert"]

ARG revision
LABEL org.opencontainers.image.title="blocksconvert" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/tools/blocksconvert" \
      org.opencontainers.image.revision="${revision}"
