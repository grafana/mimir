FROM alpine:3.16

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
