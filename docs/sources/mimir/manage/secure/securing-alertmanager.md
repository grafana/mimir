---
aliases:
  - ../../operators-guide/securing/securing-alertmanager/
  - ../../operators-guide/secure/securing-alertmanager/
description: Learn how to secure the Alertmanager.
menuTitle: Securing Alertmanager
title: Securing Grafana Mimir Alertmanager
weight: 40
---

# Securing Grafana Mimir Alertmanager

By default, the Alertmanager exposes API endpoints that enable a user to configure the Alertmanager.
The Alertmanager configuration includes receivers that create network connections to send the alerting notifications.
For example, the `webhook` receiver enables a user to configure an arbitrary URL to which the Alertmanager sends a customizable request for every alerting notification.
If the Alertmanager network security is not hardened, Grafana Mimir users might configure the Alertmanager to issue requests to any network address both in the local network and the Internet.

We recommend hardening the network on which the Alertmanager runs.
Although hardening the network is out of the scope for Grafana Mimir, Grafana Mimir provides a basic built-in firewall that blocks connections created by Alertmanager receivers:

- To block specific network addresses in Alertmanager receivers, set `-alertmanager.receivers-firewall-block-cidr-networks` to a comma-separated list of network CIDRs to block.
- To block private and local addresses in Alertmanager receivers, set `-alertmanager.receivers-firewall-block-private-addresses=true`.

You can override the Alertmanager built-in firewall settings on a per-tenant basis in the overrides section of the [runtime configuration](../../../configure/about-runtime-configuration/).

## Reject file-loading settings in tenant configurations

Tenant Alertmanager configurations must not tell Alertmanager to read secrets or other values from the Alertmanager host filesystem.
If a tenant could set file-loading fields such as `password_file` or `api_url_file`, the Alertmanager process might open paths on the local disk.

When a tenant uploads a configuration through the Alertmanager configuration API, Grafana Mimir rejects settings that load files:

- Any non-empty YAML field whose name ends with `_file`, such as `password_file`, `bearer_token_file`, `credentials_file`, `client_secret_file`, or `api_url_file`
- HTTP header values loaded from files through `http_headers.<header-name>.files`

If you upload such a configuration, the API returns HTTP `400` with an error that includes `settings that load files are not allowed` or `setting http_headers.*.files is not allowed`.

Provide secrets and URLs as inline values in the tenant configuration instead of file paths.
For the request format and an example body, refer to [Set Alertmanager configuration](../../../references/http-api/#set-alertmanager-configuration).

{{< admonition type="note" >}}
You can disable the Alertmanager configuration API by setting `-alertmanager.enable-api=false`.
{{< /admonition >}}
