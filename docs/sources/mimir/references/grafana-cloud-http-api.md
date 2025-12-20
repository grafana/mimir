---
title: Query Grafana Cloud Mimir HTTP API
description: Authenticate and query Grafana Cloud Mimir HTTP API.
weight: 125
---

# Query Grafana Cloud Mimir HTTP API

Use this page to authenticate and run HTTP API requests against Grafana Cloud Mimir. It highlights differences from the on‑prem Mimir HTTP API reference.

## Before you begin

- Grafana Cloud account
- Tenant ID
- API token with the required permissions  
  <!-- TODO: SME-QUESTION-1: Confirm required scopes and token type -->

## Base URLs

Grafana Cloud Mimir uses cloud‑hosted endpoints that differ from on‑prem Mimir.

<!-- TODO: SME-QUESTION-2: Provide canonical base URL patterns and region variants -->

## Authentication

Use a Basic authorization header. Format credentials as `<TENANT_ID>:<TOKEN>`, Base64‑encode them, and pass the header with each request.

Generate the Base64 value on Linux:

```sh
echo "<TENANT_ID>:<TOKEN>" | base64 -w0
```

Example:

```sh
curl -sS \
  -H "Authorization: Basic <BASE64_VALUE>" \
  "https://<GC_MIMIR_BASE_URL>/<prometheus-http-prefix>/api/v1/rules"
```

<!-- TODO: SME-QUESTION-5: Confirm whether X-Scope-OrgID is required in GC -->

## Supported endpoints

Grafana Cloud supports a subset of the on‑prem Mimir HTTP API and uses different paths for some services.

<!-- TODO: SME-QUESTION-1: Provide supported/unsupported endpoint list or guidance -->

- Queries (instant, range)
- Rules (read)
- Alertmanager (read)  
  <!-- TODO: SME-QUESTION-7: Confirm GC support -->

## Examples

List rules:

```sh
curl -sS \
  -H "Authorization: Basic <BASE64_VALUE>" \
  "https://<GC_MIMIR_BASE_URL>/<prometheus-http-prefix>/api/v1/rules"
```

Run an instant query:

```sh
curl -sS \
  -H "Authorization: Basic <BASE64_VALUE>" \
  "https://<GC_MIMIR_BASE_URL>/<prometheus-http-prefix>/api/v1/query" \
  --data-urlencode 'query=up'
```

## See also

- [HTTP API](./) (on‑prem reference)
- [Authentication and authorization](../../manage/secure/authentication-and-authorization/)

