FROM       alpine:3.14
RUN        apk add --update --no-cache ca-certificates
COPY       cortextool /usr/bin/cortextool
EXPOSE     80
ENTRYPOINT [ "/usr/bin/cortextool" ]
