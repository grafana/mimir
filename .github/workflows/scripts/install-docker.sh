#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -x
VER="29.2.1"
curl -L -o /tmp/docker-$VER.tgz https://download.docker.com/linux/static/stable/x86_64/docker-$VER.tgz
tar -xz -C /tmp -f /tmp/docker-$VER.tgz
mv /tmp/docker/* /usr/bin
