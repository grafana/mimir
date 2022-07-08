---
aliases:
  - /docs/mimir/latest/operators-guide/configuring/about-dns-service-discovery/
description:
  DNS service discovery finds addresses of backend services to which Grafana
  Mimir connects.
menuTitle: About DNS service discovery
title: About Grafana Mimir DNS service discovery
weight: 20
---

# About Grafana Mimir DNS service discovery

Some clients in Grafana Mimir support service discovery via DNS to locate the addresses of backend servers to connect to. The following clients support service discovery via DNS:

- [Block storage's memcached cache]({{< relref "reference-configuration-parameters/index.md#blocks_storage" >}})
- [All caching memcached servers]({{< relref "reference-configuration-parameters/index.md#memcached" >}})
- [Memberlist KV store]({{< relref "reference-configuration-parameters/index.md#memberlist" >}})

## Supported discovery modes

DNS service discovery supports different discovery modes.
You select a discovery mode by adding one of the following supported prefixes to the address:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`.
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+_memcached._tcp.memcached.namespace.svc.cluster.local`.
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+_memcached._tcp.memcached.namespace.svc.cluster.local`.

If no prefix is provided, the provided IP or hostname is used without pre-resolving it.
