FROM alpine:3.13

RUN     mkdir /mimr
WORKDIR /mimir
ADD     ./mimir ./
