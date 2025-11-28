# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.1 AS mimirtool

FROM alpine:3.22.2

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
