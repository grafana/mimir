---
aliases:
  - ../configuring/about-tenant-ids/
description: Learn about tenant ID restrictions.
menuTitle: Tenant IDs
title: About Grafana Mimir tenant IDs
weight: 10
---

# About Grafana Mimir tenant IDs

Within a Grafana Mimir cluster, the tenant ID is the unique identifier of a tenant.
For information about how Grafana Mimir components use tenant IDs, refer to [Authentication and authorization]({{< relref "../manage/secure/authentication-and-authorization" >}}).

## Restrictions

Tenant IDs must be less-than or equal-to 150 bytes or characters in length and can only include the following supported characters:

- Alphanumeric characters
  - `0-9`
  - `a-z`
  - `A-Z`
- Special characters
  - Exclamation point (`!`)
  - Hyphen (`-`)
  - Underscore (`_`)
  - Single period (`.`)
  - Asterisk (`*`)
  - Single quote (`'`)
  - Open parenthesis (`(`)
  - Close parenthesis (`)`)

{{< admonition type="note" >}}
For security reasons, `.` and `..` aren't valid tenant IDs.
{{< /admonition >}}

{{< admonition type="note" >}}
`__mimir_cluster` isn't a valid tenant ID because Mimir uses the name internally.
{{< /admonition >}}

All other characters, including slashes and whitespace, aren't supported.
