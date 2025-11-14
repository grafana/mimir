---
aliases:
  - ../../operators-guide/tools/tenant-injector/
description: Use the tenant injector to query data for a tenant during development and troubleshooting.
menuTitle: Tenant injector
title: Grafana Mimir tenant injector
weight: 20
---

# Grafana Mimir tenant injector

The tenant injector is a standalone HTTP proxy that injects the `X-Scope-OrgID` header with the value that you specify in the `-tenant-id` flag into incoming HTTP requests. It then forwards the modified requests to the URL you specify in the `-remote-address` flag.

## Download the tenant injector

Download the tenant injector as a separate binary from Grafana Mimir. Refer to [Grafana Mimir releases](https://github.com/grafana/mimir/releases) on GitHub.

## Use the tenant injector

You can use the tenant injector to query data for a tenant during development or troubleshooting.

```
Usage of tenant-injector:
  -local-address string
    	Local address to listen on (host:port or :port). (default ":8080")
  -remote-address string
    	URL of target to forward requests to to (eg. http://domain.com:80).
  -tenant-id string
    	Tenant ID to inject to proxied requests.
```
