FROM alpine:3.13

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
