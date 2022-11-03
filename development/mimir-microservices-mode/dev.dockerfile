FROM golang:1.19.3
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.9.1

FROM alpine:3.16.2

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
