FROM alpine:3.18.0

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
