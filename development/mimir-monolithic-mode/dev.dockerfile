FROM alpine:3.18.2

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
