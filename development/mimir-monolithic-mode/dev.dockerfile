FROM alpine:3.20.3

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
