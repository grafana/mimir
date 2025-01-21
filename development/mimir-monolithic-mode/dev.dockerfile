FROM alpine:3.21.2

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
