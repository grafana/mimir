FROM alpine:3.17.3

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
