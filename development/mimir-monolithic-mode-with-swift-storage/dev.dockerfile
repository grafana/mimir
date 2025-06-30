FROM alpine:3.22.0

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
