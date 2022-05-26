# SPDX-License-Identifier: AGPL-3.0-only
FROM node:17-bullseye

# Install chromium along with its dependencies.
RUN apt-get update && apt-get install -y \
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
    chromium

# Tell puppeter to skip chromium installation
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Install sources and dependencies.
RUN mkdir /app
COPY package*.json app.js /app/
RUN cd /app && npm ci

WORKDIR /app
