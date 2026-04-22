# SPDX-License-Identifier: AGPL-3.0-only

FROM grafana/mimirtool:3.0.6@sha256:b1c991f1a6501e6f8154a4ffde571f2195683d409c070aa27e316849a7c70b0d AS mimirtool

FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

COPY --from=mimirtool /bin/mimirtool /bin/mimirtool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
