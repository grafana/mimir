# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/packaging/fpm/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM alpine:3.16.2

RUN apk add --no-cache \
        bash \
        ruby \
        ruby-dev \
        ruby-etc \
        gcc \
        git \
        libc-dev \
        libffi-dev \
        make \
        rpm \
        tar \
        && gem install --no-document fpm

COPY package.sh /
ENTRYPOINT ["/package.sh"]

ARG revision
LABEL org.opencontainers.image.title="fpm" \
        # TODO: should this label point to the fpm source code?
        org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/packaging/fpm" \
        org.opencontainers.image.revision="${revision}"
