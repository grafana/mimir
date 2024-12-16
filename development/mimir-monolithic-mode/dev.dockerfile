FROM alpine:3.21.0

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
