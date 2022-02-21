---
title: "Tenant injector"
description: ""
weight: 100
---

# Tenant injector

The tenant injector is a standalone HTTP proxy that injects the `X-Scope-OrgID` header with a value that you specify in the `-tenant-id` flag to incoming HTTP requests, and forwards the modified requests to the URL you specify in `-remote-address`.

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
