# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/build-image/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM golang:1.16.6-buster
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
RUN apt-get update && apt-get install -y curl python-requests python-yaml file jq unzip protobuf-compiler libprotobuf-dev shellcheck && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN go get -u golang.org/x/tools/cmd/goimports@3fce476f0a782aeb5034d592c189e63be4ba6c9e
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install website builder dependencies. Whenever you change these version, please also change website/package.json
# and vice versa.
RUN npm install -g postcss-cli@5.0.1 autoprefixer@9.8.5 prettier@2.3.2

ENV HUGO_VERSION=v0.72.0
RUN git clone https://github.com/gohugoio/hugo.git --branch ${HUGO_VERSION} --depth 1 && \
	cd hugo && go install --tags extended && cd ../ && \
	rm -rf hugo/ && rm -rf /go/pkg /go/src /root/.cache

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

RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b /usr/bin v1.27.0

RUN GO111MODULE=on \
	go get github.com/client9/misspell/cmd/misspell@v0.3.4 && \
	go get github.com/golang/protobuf/protoc-gen-go@v1.3.1 && \
	go get github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0 && \
	go get github.com/gogo/protobuf/gogoproto@v1.3.0 && \
	go get github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 && \
	go get github.com/fatih/faillint@v1.5.0 && \
	go get github.com/campoy/embedmd@v1.0.0 && \
	go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@v0.4.0 && \
	go get github.com/monitoring-mixins/mixtool/cmd/mixtool@bca3066 && \
	go get github.com/mikefarah/yq/v4@v4.13.4 && \
	go get github.com/google/go-jsonnet/cmd/jsonnetfmt@v0.17.0 && \
	rm -rf /go/pkg /go/src /root/.cache

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="mimir-build-image" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/mimir-build-image" \
      org.opencontainers.image.revision="${revision}"
