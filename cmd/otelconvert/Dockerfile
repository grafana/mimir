FROM gcr.io/distroless/static:debug

# Expose TARGETOS and TARGETARCH variables. These are supported by Docker when using BuildKit, but must be "enabled" using ARG.
ARG TARGETOS
ARG TARGETARCH
ARG BINARY_VARIANT=""
ARG BINARY_SUFFIX="-${TARGETOS}-${TARGETARCH}${BINARY_VARIANT}"

ARG revision
ARG version

SHELL [ "/busybox/sh", "-c" ]

RUN ln -s /busybox/sh /bin/sh

COPY ./cmd/otelconvert/otelconvert${BINARY_SUFFIX} /usr/bin/otelconvert

LABEL org.opencontainers.image.title="otelconvert" \
    org.opencontainers.image.vendor="Grafana Labs" \
    org.opencontainers.image.revision="${revision}" \
    org.opencontainers.image.version="${version}"

ENTRYPOINT [ "tail", "-f", "/dev/null" ]