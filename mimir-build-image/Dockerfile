# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM registry.k8s.io/kustomize/kustomize:v5.4.3 as kustomize
FROM alpine/helm:3.14.4 as helm
FROM golang:1.23.2-bookworm
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
ENV SKOPEO_DEPS="libgpgme-dev libassuan-dev libbtrfs-dev libdevmapper-dev pkg-config"
# Override toolchain directive in go.mod, to ensure the image's Go version is used.
# Be aware that the official Go Dockerfiles already do this, but let's be explicit.
# https://github.com/docker-library/golang/issues/472
ENV GOTOOLCHAIN=local
RUN apt-get update && apt-get install -y curl python3-requests python3-yaml file jq zip unzip protobuf-compiler libprotobuf-dev shellcheck libpcap-dev $SKOPEO_DEPS && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN go install golang.org/x/tools/cmd/goimports@3fce476f0a782aeb5034d592c189e63be4ba6c9e
RUN curl -sL https://deb.nodesource.com/setup_20.x | bash -
RUN apt-get install -y nodejs && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN npm install -g prettier@2.3.2

ENV SHFMT_VERSION=3.2.4
RUN GOARCH=$(go env GOARCH) && \
	if [ "$GOARCH" = "amd64" ]; then \
    	DIGEST=3f5a47f8fec27fae3e06d611559a2063f5d27e4b9501171dde9959b8c60a3538; \
    elif [ "$GOARCH" = "arm64" ]; then \
    	DIGEST=6474d9cc08a1c9fe2ef4be7a004951998e3067d46cf55a011ddd5ff7bfab3de6; \
    fi && \
    URL=https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_${GOARCH}; \
    curl -fsSLo shfmt "${URL}" && \
	echo "$DIGEST shfmt" | sha256sum -c && \
	chmod +x shfmt && \
	mv shfmt /usr/bin

ENV TANKA_VERSION=0.26.0
RUN GOARCH=$(go env GOARCH) && \
    curl -fSL -o "/usr/bin/tk" "https://github.com/grafana/tanka/releases/download/v${TANKA_VERSION}/tk-linux-${GOARCH}" && \
    chmod a+x /usr/bin/tk

RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b /usr/bin v1.60.2

ENV SKOPEO_VERSION=v1.15.1
RUN git clone --depth 1 --branch ${SKOPEO_VERSION} https://github.com/containers/skopeo /go/src/github.com/containers/skopeo && \
    DISABLE_DOCS=1 make -C /go/src/github.com/containers/skopeo install && \
    rm -rf /go/pkg /go/src /root/.cache

RUN GO111MODULE=on \
	go install github.com/client9/misspell/cmd/misspell@v0.3.4 && \
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0 && \
	go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0 && \
	go install github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 && \
	go install github.com/fatih/faillint@v1.12.0 && \
	go install github.com/campoy/embedmd@v1.0.0 && \
	go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@v0.4.0 && \
	go install github.com/monitoring-mixins/mixtool/cmd/mixtool@b97ae11 && \
	go install github.com/mikefarah/yq/v4@v4.13.4 && \
	go install github.com/google/go-jsonnet/cmd/jsonnet@v0.19.1 && \
	go install github.com/google/go-jsonnet/cmd/jsonnetfmt@v0.19.1 && \
	go install github.com/norwoodj/helm-docs/cmd/helm-docs@v1.8.1 && \
	go install github.com/open-policy-agent/conftest@v0.42.1 && \
	go install github.com/uber-go/gopatch@v0.4.0 && \
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
