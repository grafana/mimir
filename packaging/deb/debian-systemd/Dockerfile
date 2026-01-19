# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/packaging/deb/debian-systemd/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

FROM debian:13@sha256:5cf544fad978371b3df255b61e209b373583cb88b733475c86e49faa15ac2104
ENV container docker
ENV LC_ALL C
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
        && apt-get install -y systemd \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN rm -f /lib/systemd/system/multi-user.target.wants/* \
        /etc/systemd/system/*.wants/* \
        /lib/systemd/system/local-fs.target.wants/* \
        /lib/systemd/system/sockets.target.wants/*udev* \
        /lib/systemd/system/sockets.target.wants/*initctl* \
        /lib/systemd/system/sysinit.target.wants/systemd-tmpfiles-setup* \
        /lib/systemd/system/systemd-update-utmp*

VOLUME [ "/sys/fs/cgroup" ]
CMD ["/lib/systemd/systemd"]
