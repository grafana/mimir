FROM alpine:3.20.2

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
