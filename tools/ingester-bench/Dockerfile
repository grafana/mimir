# SPDX-License-Identifier: AGPL-3.0-only
# The bench and a Mimir binary built from the same tree, the ingesters' unless the configuration gives them images:
#
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tools/ingester-bench/mimir ./cmd/mimir
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tools/ingester-bench/ingester-bench ./tools/ingester-bench
#   docker build --platform linux/amd64 -t <image> tools/ingester-bench

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:3592aa8171c77482f62bbc4164e6a2d141c6122554ace66e5cc910cadb961ff0
FROM       ${BASEIMG}
COPY       mimir /bin/mimir
COPY       ingester-bench /bin/ingester-bench
ENTRYPOINT [ "/bin/ingester-bench" ]
