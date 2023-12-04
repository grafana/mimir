FROM alpine:3.18.5

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
