FROM alpine:3.15.0

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
