FROM alpine:3.18.3

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
