---
title: "About tenant IDs"
description: ""
weight: 10
---

# About tenant IDs

The tenant ID is the unique identifier of a tenant within a Grafana Mimir cluster.
For information about how Grafana Mimir components use tenant IDs, refer to [About authentication and authorization]({{<relref "./about-authentication-and-authorization.md" >}}).

## Restrictions

Tenant IDs must be less than 150 bytes/characters in length and must comprise only supported characters:

- Alphanumeric characters
  - `0-9`
  - `a-z`
  - `A-Z`
- Special characters
  - Exclamation point (`!`)
  - Hyphen (`-`)
  - Underscore (`_`)
  - Single Period (`.`)
  - Asterisk (`*`)
  - Single quote (`'`)
  - Open parenthesis (`(`)
  - Close parenthesis (`)`)

> **Note:** For security reasons, `.` and `..` aren't valid tenant IDs.

All other characters, including slashes and whitespace, aren't supported.
