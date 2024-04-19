FROM alpine:3.19.1

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
