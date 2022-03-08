---
title: "Authentication and authorization"
description: ""
weight: 10
---

# Authentication and authorization

Grafana Mimir is a multi-tenant system where tenants can query metrics and alerts that include their tenant ID. The query takes the tenant ID from the `X-Scope-OrgID` parameter that exists in the HTTP header of each request.

To protect Grafana Mimir from accidental or malicious calls, you must add a layer of protection such as a reverse proxy that authenticates requests and injects the appropriate tenant ID into the `X-Scope-OrgID` header.

## Configuring Prometheus remote write

For more information about Prometheus remote write configuration, refer to [remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

## With an authenticating reverse proxy

To use bearer authentication with a token stored in a file, the remote write configuration block includes the following parameters:

```yaml
authorization:
  type: Bearer
  credentials_file: <PATH TO BEARER TOKEN FILE>
```

To use basic authentication with a username and password stored in a file, the remote write configuration block includes the following parameters:

```yaml
basic_auth:
  username: <AUTHENTICATION PROXY USERNAME>
  password_file: <PATH TO AUTHENTICATION PROXY PASSWORD FILE>
```

## Without an authenticating reverse proxy

To configure the `X-Scope-OrgID` header directly, the remote write configuration block includes the following parameters:

```yaml
headers:
  "X-Scope-OrgID": <TENANT ID>
```

## Extracting tenant ID from Prometheus labels

In trusted environments where you want to split series on Prometheus labels, you can run [cortex-tenant](https://github.com/blind-oracle/cortex-tenant) between a Prometheus server and Grafana Mimir.

> **Note:** cortex-tenant is a third-party community project that is not maintained by Grafana Labs.

When proxying the timeseries to Grafana Mimir, you can configure cortex-tenant to use specified labels as the `X-Scope-OrgID` header.

To configure cortex-tenant, refer to [configuration](https://github.com/blind-oracle/cortex-tenant#configuration).

## Disabling multi-tenancy

To disable multi-tenant functionality, pass the following argument to every Grafana Mimir component:

`-auth.multitenancy-enabled=false`

After you disable multi-tenancy, Grafana Mimir components internally set the tenant ID to the string `anonymous` for every request.

To set an alternative tenant ID, use the `-auth.no-auth-tenant` flag.

> **Note**: Not all tenant IDs are valid. For more inforamtion about tenant ID restrictions, refer to [About tenant IDs]({{< relref "../about-tenant-ids.md" >}}).
