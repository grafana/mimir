---
title: "About authentication and authorization"
description: ""
weight: 100
---

All Grafana Mimir components take the tenant ID from a header `X-Scope-OrgID`
on each request. A tenant (also called "user" or "org") is the owner of
a set of series written to and queried from Grafana Mimir. All Grafana Mimir components
trust this value completely: if you need to protect your Grafana Mimir installation
from accidental or malicious calls then you must add an additional layer
of protection.

Typically this means you run Grafana Mimir behind a reverse proxy, and you must
ensure that all callers, both machines sending data over the `remote_write`
interface and humans sending queries from GUIs, supply credentials
which identify them and confirm they are authorised.

When configuring the `remote_write` API in Prometheus there is no way to
add extra headers. The user and password fields of http Basic auth, or
Bearer token, can be used to convey the tenant ID and/or credentials.
See the **Grafana Mimir-Tenant** section below for one way to solve this.

To disable the multi-tenant functionality, you can pass the argument
`-auth.enabled=false` to every Grafana Mimir component, which will set the OrgID
to the string `anonymous` for every request (configurable with `-auth.no-auth-tenant` option).

Note that the tenant ID that is used to write the series to the datastore
should be the same as the one you use to query the data. If they don't match
you won't see any data. As of now, you can't see series from other tenants.

For more information regarding the tenant ID limits, refer to: [Tenant ID limitations](./limitations.md#tenant-id-naming)

### Cortex-tenant

One way to add `X-Scope-OrgID` to Prometheus requests is to use a [cortex-tenant](https://github.com/blind-oracle/cortex-tenant)
proxy which is able to extract the tenant ID from Prometheus labels.

It can be placed between Prometheus and Grafana Mimir and will search for a predefined
label and use its value as `X-Scope-OrgID` header when proxying the timeseries to Grafana Mimir.

This can help to run Grafana Mimir in a trusted environment where you want to separate your metrics
into distinct namespaces by some criteria (e.g. teams, applications, etc).

Be advised that **cortex-tenant** is a third-party community project and it's not maintained by Grafana Mimir team.
