# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM golang:1.27.3-trixie@sha256:fbb7ae40b62ff8afe31662df570d39b4f5052ac32b90dae8e60d7657a20a74f4 AS go-base
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
FROM go-base AS markdown-formatter

RUN apt-get update && \
    apt-get install -y \
      curl \
      python3-requests \
      python3-yaml \
      file \
      jq \
      zip \
      unzip \
      zstd \
      protobuf-compiler \
      libprotobuf-dev \
      shellcheck \
      libpcap-dev \
      # Dependencies required to run Skopeo.
      libgpgme11t64 libbtrfs0t64 libassuan9 libsubid5 \
      # Pull the trixie-security fix for CVE-2026-111497.
      # Can be dropped once the golang base image ships libbrotli1 >= 1.1.0-2+deb13u1.
      libbrotli1 \
    && \
    # Verify that libbrotli1 >= 1.1.0-2+deb13u1
    dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libbrotli1)" ge 1.1.0-2+deb13u1 && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Run only Bun's native formatter in Wasm, without installing Node, npm, or Bun.
# renovate: datasource=github-releases depName=dprint/dprint
ENV DPRINT_VERSION=0.64.3
# renovate: datasource=github-release-attachments depName=dprint/dprint digestVersion=0.64.3
ENV DPRINT_AMD64_DIGEST=f71cb7e22d6776439eba168d5cb296092d6413fea7148fe4d70684a2702a4fee
# renovate: datasource=github-release-attachments depName=dprint/dprint digestVersion=0.64.3
ENV DPRINT_ARM64_DIGEST=c820bfc67b0cd6f5f0fa4d00ecdcb056944c86030859f5e50151c88d9164b30d
RUN set -eu; \
    case "$(go env GOARCH)" in \
      amd64) arch=x86_64; digest=$DPRINT_AMD64_DIGEST ;; \
      arm64) arch=aarch64; digest=$DPRINT_ARM64_DIGEST ;; \
      *) exit 1 ;; \
    esac; \
    curl -fsSLo /tmp/dprint.zip "https://github.com/dprint/dprint/releases/download/${DPRINT_VERSION}/dprint-${arch}-unknown-linux-gnu.zip"; \
    echo "$digest  /tmp/dprint.zip" | sha256sum --check; \
    unzip -p /tmp/dprint.zip dprint > /usr/local/bin/dprint; \
    chmod 755 /usr/local/bin/dprint; \
    rm /tmp/dprint.zip

# renovate: datasource=github-releases depName=dprint/dprint-plugin-bun
ENV DPRINT_BUN_VERSION=0.1.0
# renovate: datasource=github-release-attachments depName=dprint/dprint-plugin-bun digestVersion=0.1.0
ENV DPRINT_BUN_DIGEST=c3a97b6036ab2ee6aae81a9ae7a39cd399d86502ebac20ed3dd77044707cc572
RUN mkdir -p /usr/local/lib/dprint && \
    curl -fsSLo /usr/local/lib/dprint/bun.wasm "https://github.com/dprint/dprint-plugin-bun/releases/download/${DPRINT_BUN_VERSION}/plugin.wasm" && \
    echo "$DPRINT_BUN_DIGEST  /usr/local/lib/dprint/bun.wasm" | sha256sum --check

COPY --chmod=755 markdownfmt.py /usr/local/bin/markdownfmt
ENV MARKDOWN_FORMATTER=/usr/local/bin/markdownfmt
COPY test_markdownfmt.py /tmp/test_markdownfmt.py
RUN python3 /tmp/test_markdownfmt.py && \
    rm /tmp/test_markdownfmt.py && \
    ! command -v node && ! command -v npm && ! command -v bun

FROM markdown-formatter
COPY go.mod go.sum /tools/
RUN \
  cd /tools && \
	go install tool && \
	rm -rf /go/pkg /go/src /root/.cache /tools/go.mod /tools/go.sum

COPY --from=rebuilt-tools /out/ /usr/bin/

COPY --from=skopeo /etc/containers /etc/containers
COPY --from=skopeo /usr/share/containers /usr/share/containers
COPY --from=skopeo /var/lib/containers /var/lib/containers

COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
