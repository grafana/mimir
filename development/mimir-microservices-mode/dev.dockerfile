ARG ALPINE_VERSION=please-use-compose-up-to-build-images
FROM golang:1.20.5
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.20.2

# check the alpine version in makefile, and use that version as --build-arg
FROM    alpine:$ALPINE_VERSION

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
