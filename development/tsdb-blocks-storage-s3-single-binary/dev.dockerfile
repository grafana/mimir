FROM alpine:3.15.4

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
