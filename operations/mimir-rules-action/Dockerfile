# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.6@sha256:b1c991f1a6501e6f8154a4ffde571f2195683d409c070aa27e316849a7c70b0d AS mimirtool

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
