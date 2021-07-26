FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       test-exporter /
ENTRYPOINT ["/test-exporter"]

ARG revision
LABEL org.opencontainers.image.title="test-exporter" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/test-exporter" \
      org.opencontainers.image.revision="${revision}"
