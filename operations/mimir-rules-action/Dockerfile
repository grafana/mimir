# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.4@sha256:bde2a9fef8cbde7bb9b8952bd9cfc38963043317dcc610ad761af3550988cd91 AS mimirtool

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
