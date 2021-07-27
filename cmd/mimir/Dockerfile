FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       migrations /migrations/
COPY       mimir /bin/mimir
EXPOSE     80
ENTRYPOINT [ "/bin/mimir" ]

ARG revision
LABEL org.opencontainers.image.title="mimir" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/cmd/mimir" \
      org.opencontainers.image.revision="${revision}"
