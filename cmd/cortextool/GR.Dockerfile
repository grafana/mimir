FROM       alpine:3.9
RUN        apk add --update --no-cache ca-certificates
COPY       cortextool /usr/bin/cortextool
EXPOSE     80
ENTRYPOINT [ "/usr/bin/cortextool" ]
