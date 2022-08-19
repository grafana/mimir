FROM alpine:3.16.2

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
