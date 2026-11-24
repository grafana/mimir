# SPDX-License-Identifier: AGPL-3.0-only

FROM --platform=$BUILDPLATFORM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS rust-builder
ARG TARGETARCH
# Cross toolchains are only needed when the build host's architecture differs from the target's.
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-linux-gnu; package=x86-64-linux-gnu; libc=amd64 ;; \
      arm64) triple=aarch64-linux-gnu; package=aarch64-linux-gnu; libc=arm64 ;; \
      *) echo "unsupported architecture $TARGETARCH"; exit 1 ;; \
    esac; \
    if [ "$(uname -m)" != "${triple%%-*}" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "gcc-$package" "g++-$package" "libc6-dev-$libc-cross"; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "${triple%%-*}-unknown-linux-gnu"; \
    echo "$triple" > /triple
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
ARG ALLOCATOR=jemalloc
RUN set -eu; \
    triple=$(cat /triple); target="${triple%%-*}-unknown-linux-gnu"; \
    export "CARGO_TARGET_$(echo "$target" | tr a-z- A-Z_)_LINKER=$triple-gcc" CC="$triple-gcc" CXX="$triple-g++" AR="$triple-ar"; \
    cargo build --locked --release --target "$target" --no-default-features --features "$ALLOCATOR" --bin mimir-rust-kafka-ingester; \
    mkdir -p /out && cp "target/$target/release/mimir-rust-kafka-ingester" /out/

FROM --platform=$BUILDPLATFORM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS certs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
COPY --from=certs /etc/ssl/certs /etc/ssl/certs
COPY --from=rust-builder /out/mimir-rust-kafka-ingester /usr/local/bin/mimir-rust-kafka-ingester
COPY LICENSE.go-math-rand /usr/share/licenses/mimir/LICENSE.go-math-rand
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mimir-rust-kafka-ingester"]
