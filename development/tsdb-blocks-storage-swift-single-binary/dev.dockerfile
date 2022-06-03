FROM alpine:3.16.0

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
