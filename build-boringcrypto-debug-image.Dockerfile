ARG BUILD_IMAGE
FROM $BUILD_IMAGE
ENV CGO_ENABLED=1
RUN go install github.com/go-delve/delve/cmd/dlv@v1.22.0

FROM alpine:3.19.0

ARG        EXTRA_PACKAGES
RUN        apk add --no-cache ca-certificates tzdata $EXTRA_PACKAGES

RUN     mkdir /mimir
WORKDIR /mimir
COPY     ./mimir-boringcrypto-debug ./mimir
COPY     ./mimir-boringcrypto-debug /usr/local/bin/mimir
COPY --from=0 /go/bin/dlv  /usr/local/bin/dlv

#CMD "./dlv exec mimir --listen=:18001 --headless=true --api-version=2 --accept-multiclient --continue -- -target=all -server.http-listen-port=8001 -server.grpc-listen-port=9001"