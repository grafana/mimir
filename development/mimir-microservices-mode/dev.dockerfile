
ARG BUILD_IMAGE=use-compose-up-to-build-image
FROM ${BUILD_IMAGE}
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.20.2

FROM alpine:3.18.2

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir ./
COPY --from=0 /go/bin/dlv ./
