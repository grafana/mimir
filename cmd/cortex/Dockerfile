FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       migrations /migrations/
COPY       cortex /bin/cortex
EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

ARG revision
LABEL org.opencontainers.image.title="cortex" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/master/cmd/cortex" \
      org.opencontainers.image.revision="${revision}"
