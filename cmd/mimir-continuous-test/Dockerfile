# SPDX-License-Identifier: AGPL-3.0-only

FROM       alpine:3.15.0
RUN        apk add --no-cache ca-certificates
COPY       mimir-continuous-test /
ENTRYPOINT ["/mimir-continuous-test"]

ARG revision
LABEL org.opencontainers.image.title="mimir-continuous-test" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir-continuous-test" \
      org.opencontainers.image.revision="${revision}"
