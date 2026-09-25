# Debug image for the development environment: runs grpc-tee under Delve.
ARG BUILD_IMAGE # Use development/mimir-microservices-mode/compose-up.sh to build this image.
FROM $BUILD_IMAGE
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.27.1

FROM alpine:3.24.2@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6

COPY ./grpc-tee /bin/grpc-tee
COPY --from=0 /go/bin/dlv /bin/dlv
