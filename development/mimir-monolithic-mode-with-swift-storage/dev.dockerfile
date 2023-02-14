FROM alpine:3.17.2

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
