FROM alpine:3.17.1

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
