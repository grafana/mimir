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

ENV SKOPEO_DEPS="libgpgme-dev libassuan-dev libbtrfs-dev libdevmapper-dev pkg-config"
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
      $SKOPEO_DEPS \
    && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN npm install -g prettier@2.3.2

# renovate: depName=github.com/mvdan/sh
ENV SHFMT_VERSION=3.13.1
RUN GOARCH=$(go env GOARCH) && \
    URL=https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_${GOARCH}; \
    curl -fsSLo shfmt "${URL}" && \
	chmod +x shfmt && \
	mv shfmt /usr/bin

# renovate: depName=github.com/grafana/tanka
ENV TANKA_VERSION=0.37.0
RUN GOARCH=$(go env GOARCH) && \
    curl -fSL -o "/usr/bin/tk" "https://github.com/grafana/tanka/releases/download/v${TANKA_VERSION}/tk-linux-${GOARCH}" && \
    chmod a+x /usr/bin/tk

# renovate: depName=github.com/golangci/golangci-lint
ENV GOLANGCI_LINT_VERSION=2.12.2
RUN GOARCH=$(go env GOARCH) && \
    VERSION_SLUG=golangci-lint-${GOLANGCI_LINT_VERSION}-linux-$(go env GOARCH) && \
    TARBALL=$VERSION_SLUG.tar.gz && \
    cd /tmp && \
    curl -sfL "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/$TARBALL" -o "/tmp/$TARBALL" && \
    curl -sfL "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/golangci-lint-${GOLANGCI_LINT_VERSION}-checksums.txt" | grep -E "\s+$TARBALL$" | sha256sum --check - && \
    tar -xzvf "/tmp/$TARBALL" -C /usr/bin --strip-components=1 "$VERSION_SLUG/golangci-lint" && \
    rm /tmp/$TARBALL

# renovate: depName=github.com/containers/skopeo
ENV SKOPEO_VERSION=v1.22.2
RUN git clone --depth 1 --branch ${SKOPEO_VERSION} https://github.com/containers/skopeo /go/src/github.com/containers/skopeo && \
    DISABLE_DOCS=1 make -C /go/src/github.com/containers/skopeo install && \
    rm -rf /go/pkg /go/src /root/.cache

RUN GO111MODULE=on \
	go install github.com/client9/misspell/cmd/misspell@v0.3.4 && \
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11 && \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2 && \
	go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.2 && \
	go install github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 && \
	go install github.com/fatih/faillint@v1.15.0 && \
	go install github.com/campoy/embedmd/v2@v2.0.0 && \
	go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@v0.6.0 && \
	go install github.com/monitoring-mixins/mixtool/cmd/mixtool@b97ae11 && \
	go install github.com/mikefarah/yq/v4@v4.53.2 && \
	go install github.com/google/go-jsonnet/cmd/jsonnet@v0.22.0 && \
	go install github.com/google/go-jsonnet/cmd/jsonnetfmt@v0.22.0 && \
	go install github.com/norwoodj/helm-docs/cmd/helm-docs@v1.14.2 && \
	go install github.com/open-policy-agent/conftest@v0.68.2 && \
	go install github.com/uber-go/gopatch@v0.4.0 && \
	go install github.com/bufbuild/buf/cmd/buf@v1.69.0 && \
	rm -rf /go/pkg /go/src /root/.cache

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
