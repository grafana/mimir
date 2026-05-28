# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM registry.k8s.io/kustomize/kustomize:v5.4.3 AS kustomize
FROM alpine/helm:4.1.4@sha256:4b0bdd2cf18ff6bca12aba0b2c5671384dab5035c19c57f0c58b854a0baf65be AS helm
FROM golang:1.26.3-trixie@sha256:d08bf3ed2bd263088ca8e23fefaf10f1b71769f6932f0a4017ba28d2a5baf001
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
      # Dependencies required to build Skopeo below.
      libgpgme-dev libassuan-dev libbtrfs-dev libdevmapper-dev pkg-config \
    && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set --ignore-scripts to avoid arbitrary code execution in build hook scripts. (Prettier 2.3.2 doesn't happen to have any.)
RUN npm install -g --ignore-scripts prettier@2.3.2

# renovate: datasource=github-release-attachments depName=mvdan/sh
ENV SHFMT_VERSION=3.13.1
RUN GOARCH=$(go env GOARCH) && \
    URL=https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_${GOARCH}; \
    curl -fsSLo shfmt "${URL}" && \
	chmod +x shfmt && \
	mv shfmt /usr/bin

# renovate: datasource=github-release-attachments depName=grafana/tanka
ENV TANKA_VERSION=0.37.0
RUN GOARCH=$(go env GOARCH) && \
    curl -fSL -o "/usr/bin/tk" "https://github.com/grafana/tanka/releases/download/v${TANKA_VERSION}/tk-linux-${GOARCH}" && \
    chmod a+x /usr/bin/tk

# renovate: datasource=github-release-attachments depName=golangci/golangci-lint
ENV GOLANGCI_LINT_VERSION=2.12.2
RUN set -o pipefail && \
    VERSION_SLUG=golangci-lint-${GOLANGCI_LINT_VERSION}-linux-$(go env GOARCH) && \
    TARBALL=$VERSION_SLUG.tar.gz && \
    cd /tmp && \
    curl -sfL "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/$TARBALL" -o "/tmp/$TARBALL" && \
    curl -sfL "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/golangci-lint-${GOLANGCI_LINT_VERSION}-checksums.txt" | grep -E "\s+$TARBALL$" | sha256sum --check - && \
    tar -xzvf "/tmp/$TARBALL" -C /usr/bin --strip-components=1 "$VERSION_SLUG/golangci-lint" && \
    rm /tmp/$TARBALL

# renovate: datasource=github-release-attachments depName=containers/skopeo
ENV SKOPEO_VERSION=v1.22.2
RUN git clone --depth 1 --branch ${SKOPEO_VERSION} https://github.com/containers/skopeo /go/src/github.com/containers/skopeo && \
    DISABLE_DOCS=1 make -C /go/src/github.com/containers/skopeo install && \
    rm -rf /go/pkg /go/src /root/.cache

COPY go.mod go.sum /tools/
RUN \
  cd /tools && \
	go install tool && \
	rm -rf /go/pkg /go/src /root/.cache /tools/go.mod /tools/go.sum

COPY --from=helm /usr/bin/helm /usr/bin/helm
COPY --from=kustomize /app/kustomize /usr/bin/kustomize
ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
