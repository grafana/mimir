FROM alpine:3.21.3

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
