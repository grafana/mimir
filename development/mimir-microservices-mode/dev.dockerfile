ARG BUILD_IMAGE # Use ./compose-up.sh to build this image.
FROM $BUILD_IMAGE
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.25.2

FROM alpine:3.23.0@sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
