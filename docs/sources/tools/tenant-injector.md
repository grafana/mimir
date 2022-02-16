---
title: "Tenant injector"
description: ""
weight: 100
---

# Tenant injector

Tenant injector is a standalone HTTP proxy that injects the `X-Scope-OrgID` header with value specified by `-tenant-id` flag to incoming HTTP requests, and forwards such modified requests to the URL specified by `-remote-address`.

This can be used to query data for a specific tenant during development or troubleshooting.

```
Usage of tenant-injector:
  -local-address string
    	Local address to listen on (host:port or :port). (default ":8080")
  -remote-address string
    	URL of target to forward requests to to (eg. http://domain.com:80).
  -tenant-id string
    	Tenant ID to inject to proxied requests.
```
