FROM alpine:3.22.1

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
