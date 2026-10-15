# SPDX-License-Identifier: AGPL-3.0-only
# The bench and the Mimir binary it runs, built beforehand from the same tree:
#
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tools/ingester-bench/mimir ./cmd/mimir
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tools/ingester-bench/ingester-bench ./tools/ingester-bench
#   docker build --platform linux/amd64 -t <image> tools/ingester-bench

# The Mimir release to compare with, as -binary-a=/bin/mimir-release.
ARG        RELEASE=grafana/mimir:r416-1df78b2a@sha256:7e42cd97b8dba8abf5ae3a2d808672bdb6da4134802f8efec6a667aefb4b1add
ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:3592aa8171c77482f62bbc4164e6a2d141c6122554ace66e5cc910cadb961ff0
FROM       ${RELEASE} AS release
FROM       ${BASEIMG}
COPY       --from=release /bin/mimir /bin/mimir-release
COPY       mimir /bin/mimir
COPY       ingester-bench /bin/ingester-bench
ENTRYPOINT [ "/bin/ingester-bench" ]
