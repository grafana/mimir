# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.5@sha256:41191341dd57c4e9a45e8987fca7c02e27b2e74971013446431d97101bb60266 AS mimirtool

FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
