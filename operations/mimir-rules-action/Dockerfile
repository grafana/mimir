# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.2.4 AS mimirtool

FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
