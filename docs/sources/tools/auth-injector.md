---
title: "Auth Injector"
description: ""
weight: 100
---

# Auth injector

Auth injector is an HTTP proxy that injects `X-Scope-OrgId` header with value specified by `-tenant-id` option
to incoming HTTP requests, and forwards such modified requests to specified `-remote-address`.

This can be used to query data for a specific tenant during development or troubleshooting.

```
Usage of auth-injector:
  -local-address string
    	Local address to listen on (host:port or :port). (default ":8080")
  -remote-address string
    	URL of target to forward requests to to (eg. http://domain.com:80).
  -tenant-id string
    	Tenant ID to inject to proxied requests.
```
