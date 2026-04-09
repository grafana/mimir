# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.5@sha256:41191341dd57c4e9a45e8987fca7c02e27b2e74971013446431d97101bb60266 AS mimirtool

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
