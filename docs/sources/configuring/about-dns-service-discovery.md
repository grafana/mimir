---
title: "About DNS service discovery"
description: ""
weight: 10
---

# About DNS service discovery

Some clients in Grafana Mimir support service discovery via DNS to find the addresses of backend servers to connect to. These clients support service discovery via DNS:

- [Block storage's memcached cache]({{< relref "reference-configuration-parameters/#blocks_storage" >}})
- [All caching memcached servers]({{< relref "reference-configuration-parameters/#memcached" >}})
- [Memberlist KV store]({{< relref "reference-configuration-parameters#memberlist" >}})

## Supported discovery modes

The DNS service discovery supports different discovery modes. A discovery mode is selected adding a specific prefix to the address. Supported prefixes are:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`.
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+_memcached._tcp.memcached.namespace.svc.cluster.local`.
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+_memcached._tcp.memcached.namespace.svc.cluster.local`.

If no prefix is provided, the provided IP or hostname will be used without pre-resolving it.
