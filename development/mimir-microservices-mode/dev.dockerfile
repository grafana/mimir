ARG BUILD_IMAGE # Use ./compose-up.sh to build this image.
FROM $BUILD_IMAGE
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.21.0

FROM alpine:3.19.1

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
