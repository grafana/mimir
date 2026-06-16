# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.1.1 AS mimirtool

FROM alpine:3.24.0@sha256:a2d49ea686c2adfe3c992e47dc3b5e7fa6e6b5055609400dc2acaeb241c829f4

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
