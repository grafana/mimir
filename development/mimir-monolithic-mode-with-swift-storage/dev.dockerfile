FROM alpine:3.22.2

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
