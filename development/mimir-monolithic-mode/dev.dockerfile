FROM alpine:3.18.4

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
