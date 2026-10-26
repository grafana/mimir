# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM golang:1.27.3-trixie@sha256:604106102d33b1084e37ca08adf98ac0813624ae8595bd0d7f3965ecebded09c AS go-base
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
# Override toolchain directive in go.mod, to ensure the image's Go version is used.
# Be aware that the official Go Dockerfiles already do this, but let's be explicit.
# https://github.com/docker-library/golang/issues/472
ENV GOTOOLCHAIN=local

FROM go-base AS rebuilt-tools
RUN apt-get update && apt-get install -y --no-install-recommends libgpgme-dev libassuan-dev libbtrfs-dev pkg-config && rm -rf /var/lib/apt/lists/*
ENV GOBIN=/out

# renovate: datasource=github-releases depName=mvdan/sh
ENV SHFMT_VERSION=v3.13.1
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install mvdan.cc/sh/v3/cmd/shfmt@${SHFMT_VERSION}

# renovate: datasource=github-releases depName=grafana/tanka
ENV TANKA_VERSION=v0.37.5
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install -ldflags="-X github.com/grafana/tanka/pkg/tanka.CurrentVersion=${TANKA_VERSION}" github.com/grafana/tanka/cmd/tk@${TANKA_VERSION}

# renovate: datasource=github-releases depName=golangci/golangci-lint
ENV GOLANGCI_LINT_VERSION=v2.14.0
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install -ldflags="-X main.version=${GOLANGCI_LINT_VERSION#v}" github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}

# renovate: datasource=github-releases depName=helm/helm
ENV HELM_VERSION=v4.2.3
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install -ldflags="-X helm.sh/helm/v4/internal/version.version=${HELM_VERSION} -X helm.sh/helm/v4/internal/version.metadata=" helm.sh/helm/v4/cmd/helm@${HELM_VERSION}

# renovate: datasource=go depName=sigs.k8s.io/kustomize/kustomize/v5
ENV KUSTOMIZE_VERSION=v5.4.3
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install -ldflags="-X sigs.k8s.io/kustomize/api/provenance.version=kustomize/${KUSTOMIZE_VERSION}" sigs.k8s.io/kustomize/kustomize/v5@${KUSTOMIZE_VERSION}

# renovate: datasource=github-releases depName=containers/skopeo
ENV SKOPEO_VERSION=v1.22.2
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go install github.com/containers/skopeo/cmd/skopeo@${SKOPEO_VERSION}

FROM quay.io/skopeo/stable:v1.22.2-immutable@sha256:4a16d57b37617a04b3d643079a477a2848efe892dffcdf0ce56df4262b65f810 AS skopeo
FROM go-base

RUN apt-get update && \
    apt-get install -y \
      curl \
      libbrotli1 \
      python3-requests \
      python3-yaml \
      file \
      jq \
      zip \
      unzip \
      xz-utils \
      zstd \
      protobuf-compiler \
      libprotobuf-dev \
      shellcheck \
      libpcap-dev \
      # Dependencies required to run Skopeo.
      libgpgme11t64 libbtrfs0t64 libassuan9 libsubid5 \
    && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Update both archive checksums when changing NODE_VERSION.
# renovate: datasource=node-version depName=node
ENV NODE_VERSION=26.12.1
ENV NODE_AMD64_DIGEST=6170b2d474e1c626f43cff3adc118df9fe3b573258a4fc3d850c8dbc6b079bbd
ENV NODE_ARM64_DIGEST=42aa04922591ee5d4dbc7740d452621acf28508239daa512f5ee8c47ee224fdb
RUN set -eu; \
    case "$(go env GOARCH)" in \
      amd64) arch=x64; digest=$NODE_AMD64_DIGEST ;; \
      arm64) arch=arm64; digest=$NODE_ARM64_DIGEST ;; \
      *) exit 1 ;; \
    esac; \
    curl -fsSLo /tmp/node.tar.xz "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${arch}.tar.xz"; \
    echo "$digest  /tmp/node.tar.xz" | sha256sum --check; \
    tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1; \
    rm /tmp/node.tar.xz

RUN npm install -g --ignore-scripts prettier@3.8.4

COPY go.mod go.sum /tools/
RUN \
  cd /tools && \
	go install tool && \
	rm -rf /go/pkg /go/src /root/.cache /tools/go.mod /tools/go.sum

COPY --from=rebuilt-tools /out/ /usr/bin/

COPY --from=skopeo /etc/containers /etc/containers
COPY --from=skopeo /usr/share/containers /usr/share/containers
COPY --from=skopeo /var/lib/containers /var/lib/containers

ENV NODE_PATH=/usr/local/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
