
ARG GO_VERSION=please-use-compose-up-to-build-images
FROM golang:${GO_VERSION}
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.20.2

FROM alpine:3.18.2

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
