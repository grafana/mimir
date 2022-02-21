---
title: "About authentication and authorization"
description: ""
weight: 10
---

# About authentication and authorization

Grafana Mimir is a multi-tenant system where each tenant has their own isolated series and alerts.
Tenants can only query metrics and alerts written with their specific tenant ID.
All Grafana Mimir components take the tenant ID from an HTTP header with the name `X-Scope-OrgID` on each request.
Components trust the value of this header completely.

In order to protect Grafana Mimir from accidental or malicious calls you must add an additional layer of protection like an authenticating reverse proxy.
The reverse proxy authenticates requests and injects the appropriate tenant ID into the `X-Scope-OrgID` header.

## Configuring Prometheus remote write

For a full reference of the Prometheus remote write configuration, refer to [remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

## With an authenticating reverse proxy

To use bearer authentication with a token stored in a file, the remote write configuration block would include:

```yaml
authorization:
  type: Bearer
  credentials_file: <PATH TO BEARER TOKEN FILE>
```

To use basic authentication with a username and password stored in a file, the remote write configuration block would include:

```yaml
basic_auth:
  username: <AUTHENTICATION PROXY USERNAME>
  password_file: <PATH TO AUTHENTICATION PROXY PASSWORD FILE>
```

## Without an authenticating reverse proxy

To configure the `X-Scope-OrgID` header directly, the remote write configuration block would include:

```yaml
headers:
  "X-Scope-OrgID": <TENANT ID>
```

## Extracting tenant ID from Prometheus labels

In trusted environments where you wish to split series on Prometheus labels, you can run [cortex-tenant](https://github.com/blind-oracle/cortex-tenant) between a Prometheus server and Grafana Mimir.

> **Note:** cortex-tenant is a third-party community project and it's not maintained by the Grafana team.

With specific configuration, cortex-tenant uses the values of specified labels as the `X-Scope-OrgID` header when proxying the timeseries to Grafana Mimir.
To configure cortex-tenant, refer to its [configuration](https://github.com/blind-oracle/cortex-tenant#configuration).

## Disabling multi-tenancy

To disable the multi-tenant functionality, you can pass the argument `-auth.multitenancy-enabled=false` to every Grafana Mimir component.
Each component internally sets the tenant ID to the string `anonymous` for every request.

If you want to set an alternative tenant ID, use the `-auth.no-auth-tenant` flag.
Not all tenant IDs are valid. To understand the restrictions on tenant IDs, refer to [About tenant IDs]({{<relref "./about-tenant-ids.md" >}}).
