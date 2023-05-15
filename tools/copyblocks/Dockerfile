FROM --platform=$BUILDPLATFORM golang:1.20.4-bullseye AS build

ARG TARGETOS
ARG TARGETARCH

COPY . /src/copyblocks
WORKDIR /src/copyblocks
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} make

FROM alpine:3.17
RUN apk add --no-cache ca-certificates

COPY --from=build /src/copyblocks/copyblocks /bin/copyblocks
ENTRYPOINT [ "/bin/copyblocks" ]

# Create copyblocks user to run as non-root.
RUN addgroup -g 10000 -S copyblocks && \
    adduser  -u 10000 -S copyblocks -G copyblocks
USER copyblocks:copyblocks

ARG revision
LABEL org.opencontainers.image.title="copyblocks" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tools/copyblocks" \
      org.opencontainers.image.revision="${revision}"

