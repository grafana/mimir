FROM alpine:3.20.6

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
