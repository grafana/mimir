# SPDX-License-Identifier: AGPL-3.0-only

<<<<<<< HEAD
FROM grafana/mimirtool:3.0.3-rc.0 AS mimirtool

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool
=======
FROM grafana/mimirtool:3.0.3-rc.0
>>>>>>> origin/release-3.0

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
