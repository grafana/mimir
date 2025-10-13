FROM alpine:3.21.5

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
