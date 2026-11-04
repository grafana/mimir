---
aliases:
  - ../configuring/about-dns-service-discovery/
description: DNS service discovery finds addresses of backend services to which Grafana Mimir connects.
menuTitle: DNS service discovery
title: About Grafana Mimir DNS service discovery
weight: 260
---

# About Grafana Mimir DNS service discovery

Some clients in Grafana Mimir support service discovery via DNS to locate the addresses of backend servers to connect to.
When using DNS service discovery, search domains are not supported. As a result, all domain names must be [fully qualified](https://en.wikipedia.org/wiki/Fully_qualified_domain_name).
The following clients support service discovery via DNS:

- [Memcached server addresses](../configuration-parameters/#memcached)
  - `-blocks-storage.bucket-store.chunks-cache.memcached.addresses`
  - `-blocks-storage.bucket-store.index-cache.memcached.addresses`
  - `-blocks-storage.bucket-store.metadata-cache.memcached.addresses`
  - `-query-frontend.results-cache.memcached.addresses`
- [Memberlist KV store](../configuration-parameters/#memberlist)
  - `-memberlist.join`
- [Alertmanager URL configured in the ruler](../configuration-parameters/#ruler)
  - `-ruler.alertmanager-url`

## Memcached DNS requirements

Starting with Grafana Mimir 3.0, Memcached service discovery uses TCP for all DNS queries instead of trying UDP first.
This applies to the `dns+`, `dnssrv+`, and `dnssrvnoa+` discovery modes.
Ensure that Mimir can reach the DNS servers configured in `/etc/resolv.conf` over TCP on port `53`, including through any firewall, network policy, or service mesh in the DNS request path.
A successful DNS lookup over UDP doesn't confirm that DNS over TCP is reachable.

In Grafana Mimir 2.16 and 2.17, this TCP-only resolver is available by setting the experimental `-<prefix>.memcached.addresses-provider` flag to `miekgdns2`.
Grafana Mimir 3.0 makes it the default and removes this flag.

## Supported discovery modes

DNS service discovery supports different discovery modes.
You select a discovery mode by adding one of the following supported prefixes to the address:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`.
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+_memcached._tcp.memcached.namespace.svc.cluster.local`.
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+_memcached._tcp.memcached.namespace.svc.cluster.local`.

The Alertmanager URL must also contain the scheme. For example: `dns+http://alermanager.local:8080`.

If no prefix is provided, the provided IP or hostname is used without pre-resolving it.
