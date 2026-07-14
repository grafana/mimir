# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM registry.k8s.io/kustomize/kustomize:v5.4.3 AS kustomize
FROM alpine/helm:4.2.3@sha256:b97ba4f9b27fe7af16ee3d37e6815783c9d4a51289b6240a9024ec471611ae9b AS helm
FROM quay.io/skopeo/stable:v1.22.2-immutable@sha256:4a16d57b37617a04b3d643079a477a2848efe892dffcdf0ce56df4262b65f810 AS skopeo
FROM golang:1.26.4-trixie@sha256:0dcba0d95dbfb072e9917a106b9e07d7cc298097dc83e9307056ef1889de654d
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
# Override toolchain directive in go.mod, to ensure the image's Go version is used.
# Be aware that the official Go Dockerfiles already do this, but let's be explicit.
# https://github.com/docker-library/golang/issues/472
ENV GOTOOLCHAIN=local

ENV NODE_MAJOR_VERSION=26
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
RUN <<EOF cat > /etc/apt/sources.list.d/nodesource.sources
Types: deb
URIs: https://deb.nodesource.com/node_${NODE_MAJOR_VERSION}.x/
Suites: nodistro
Components: main
Signed-By: /etc/apt/keyrings/nodesource.gpg
EOF

RUN apt-get update && \
    apt-get install -y \
      curl \
      python3-requests \
      python3-yaml \
      file \
      jq \
      zip \
      unzip \
      protobuf-compiler \
      libprotobuf-dev \
      shellcheck \
      libpcap-dev \
      nodejs \
      # Dependencies required to run Skopeo.
      libgpgme11t64 libbtrfs0t64 libassuan9 libsubid5 \
    && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set --ignore-scripts to avoid arbitrary code execution in build hook scripts. (Prettier 2.3.2 doesn't happen to have any.)
RUN npm install -g --ignore-scripts prettier@3.8.4

# renovate: datasource=github-release-attachments depName=mvdan/sh
ENV SHFMT_VERSION=v3.13.1
# renovate: datasource=github-release-attachments depName=mvdan/sh digestVersion=v3.13.1
ENV SHFMT_AMD64_DIGEST=fb096c5d1ac6beabbdbaa2874d025badb03ee07929f0c9ff67563ce8c75398b1
# renovate: datasource=github-release-attachments depName=mvdan/sh digestVersion=v3.13.1
ENV SHFMT_ARM64_DIGEST=32d92acaa5cd8abb29fc49dac123dc412442d5713967819d8af2c29f1b3857c7
RUN set -o pipefail && \
    GOARCH=$(go env GOARCH) && \
    DIGEST=$(case "$GOARCH" in amd64) echo $SHFMT_AMD64_DIGEST;; arm64) echo $SHFMT_ARM64_DIGEST;; *) echo "unknown architecture $GOARCH" >/dev/stderr && exit 1;; esac) && \
    curl -fsSLo /usr/bin/shfmt "https://github.com/mvdan/sh/releases/download/${SHFMT_VERSION}/shfmt_${SHFMT_VERSION}_linux_${GOARCH}" && \
    echo "$DIGEST  /usr/bin/shfmt" | sha256sum --check && \
    chmod a+x /usr/bin/shfmt

# renovate: datasource=github-release-attachments depName=grafana/tanka
ENV TANKA_VERSION=v0.37.5
# renovate: datasource=github-release-attachments depName=grafana/tanka digestVersion=v0.37.5
ENV TANKA_AMD64_DIGEST=b16b71d7a7bb16c16da67843a9e77efb5222dcb67922d3df385a291b044cb0dd
# renovate: datasource=github-release-attachments depName=grafana/tanka digestVersion=v0.37.5
ENV TANKA_ARM64_DIGEST=38ea58e6105d5cc65528ce54b9780e95fe5cf4589ccd7d52fa1989a893560d58
RUN set -o pipefail && \
    GOARCH=$(go env GOARCH) && \
    DIGEST=$(case "$GOARCH" in amd64) echo $TANKA_AMD64_DIGEST;; arm64) echo $TANKA_ARM64_DIGEST;; *) echo "unknown architecture $GOARCH" >/dev/stderr && exit 1;; esac) && \
    curl -fsSLo /usr/bin/tk "https://github.com/grafana/tanka/releases/download/${TANKA_VERSION}/tk-linux-${GOARCH}" && \
    echo "$DIGEST  /usr/bin/tk" | sha256sum --check && \
    chmod a+x /usr/bin/tk

# renovate: datasource=github-release-attachments depName=golangci/golangci-lint
ENV GOLANGCI_LINT_VERSION=v2.12.2
# renovate: datasource=github-release-attachments depName=golangci/golangci-lint digestVersion=v2.12.2
ENV GOLANGCI_LINT_AMD64_DIGEST=8df580d2670fed8fa984aac0507099af8df275e665215f5c7a2ae3943893a553
# renovate: datasource=github-release-attachments depName=golangci/golangci-lint digestVersion=v2.12.2
ENV GOLANGCI_LINT_ARM64_DIGEST=44cd40a8c76c86755375adfeea52cfd3533cb43d7bd647771e0ae065e166df3a
RUN set -o pipefail && \
    GOARCH=$(go env GOARCH) && \
    DIGEST=$(case "$GOARCH" in amd64) echo $GOLANGCI_LINT_AMD64_DIGEST;; arm64) echo $GOLANGCI_LINT_ARM64_DIGEST;; *) echo "unknown architecture $GOARCH" >/dev/stderr && exit 1;; esac) && \
    VERSION_SLUG=golangci-lint-${GOLANGCI_LINT_VERSION#v}-linux-${GOARCH} && \
    TARBALL=$VERSION_SLUG.tar.gz && \
    cd /tmp && \
    curl -fsSLo "/tmp/$TARBALL" "https://github.com/golangci/golangci-lint/releases/download/${GOLANGCI_LINT_VERSION}/$TARBALL" && \
    echo "$DIGEST  /tmp/$TARBALL" | sha256sum --check && \
    tar -xzvf "/tmp/$TARBALL" -C /usr/bin --strip-components=1 "$VERSION_SLUG/golangci-lint" && \
    rm /tmp/$TARBALL

COPY go.mod go.sum /tools/
RUN \
  cd /tools && \
	go install tool && \
	rm -rf /go/pkg /go/src /root/.cache /tools/go.mod /tools/go.sum

COPY --from=helm /usr/bin/helm /usr/bin/helm
COPY --from=kustomize /app/kustomize /usr/bin/kustomize

COPY --from=skopeo /etc/containers /etc/containers
COPY --from=skopeo /usr/bin/skopeo /usr/bin/skopeo
COPY --from=skopeo /usr/share/containers /usr/share/containers
COPY --from=skopeo /var/lib/containers /var/lib/containers

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
