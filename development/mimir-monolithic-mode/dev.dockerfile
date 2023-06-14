# check the alpine version in makefile, and use that version as --build-arg
ARG ALPINE_VERSION=please-use-compose-up-to-build-images
FROM       alpine:${ALPINE_VERSION}

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
