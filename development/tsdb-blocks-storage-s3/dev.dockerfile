FROM golang:1.17
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.7.3

FROM alpine:3.13

RUN     mkdir /mimir
WORKDIR /mimir
ADD     ./mimir ./
COPY --from=0 /go/bin/dlv ./
