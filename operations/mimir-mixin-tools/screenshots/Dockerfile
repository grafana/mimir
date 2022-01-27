FROM node:17-bullseye

# Install Chrome dependencies.
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
    libasound2

# Install sources and dependencies.
RUN mkdir /app
COPY package*.json app.js /app
RUN cd /app && npm ci

WORKDIR /app
