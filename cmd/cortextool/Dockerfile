FROM alpine:3.9.4
RUN apk --update add --no-cache ca-certificates
COPY /bin/cortex-tool /bin/cortex-tool
ENTRYPOINT [ "/bin/cortex-tool" ]
