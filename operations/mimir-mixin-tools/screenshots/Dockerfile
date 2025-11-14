# SPDX-License-Identifier: AGPL-3.0-only
FROM grafana/mimir-build-image AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o mimir-mixin-screenshots main.go

FROM debian:bullseye-slim

# Install Chromium and dependencies (works across architectures)
RUN apt-get update && apt-get install -y \
    chromium \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon-x11-0 \
    libxcomposite-dev \
    libxdamage1 \
    libxrandr2 \
    libgbm-dev \
    libasound2 \
    pngquant \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/mimir-mixin-screenshots /usr/local/bin/mimir-mixin-screenshots

ENTRYPOINT ["/usr/local/bin/mimir-mixin-screenshots"]