FROM alpine:3.19.0

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
