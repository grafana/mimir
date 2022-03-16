FROM alpine:3.15.0

RUN     mkdir /mimr
WORKDIR /mimir
ADD     ./mimir ./
