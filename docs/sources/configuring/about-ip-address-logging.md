---
title: "About IP address logging of a reverse proxy"
description: ""
weight: 10
---

# About IP address logging of a reverse proxy

If a reverse proxy is used in front of Mimir, it may be difficult to troubleshoot errors. The following settings can be used to log the IP address passed along by the reverse proxy in headers such as `X-Forwarded-For`.

- `-server.log-source-ips-enabled`

  Set this to true to add IP address logging when a `Forwarded`, `X-Real-IP` or `X-Forwarded-For` header is used. A field called `sourceIPs` will be added to error logs when data is pushed into Grafana Mimir.

- `-server.log-source-ips-header`

  Header field storing the source IP addresses. It is only used if `-server.log-source-ips-enabled` is true, and if `-server.log-source-ips-regex` is set. If not set, the default `Forwarded`, `X-Real-IP` or `X-Forwarded-For` headers are searched.

- `-server.log-source-ips-regex`

  Regular expression for matching the source IPs. It should contain at least one capturing group the first of which will be returned. Only used if `-server.log-source-ips-enabled` is true and if `-server.log-source-ips-header` is set.
