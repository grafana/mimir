---
title: "Prometheus Frontend"
linkTitle: "Prometheus Frontend"
weight: 3
slug: prometheus-frontend
---

You can use the Cortex query frontend with any Prometheus-API compatible
service, including Prometheus and Thanos. Use this config file to get
the benefits of query parallelisation and caching.

<!-- prettier-ignore-start -->
[embedmd]:# (./prometheus-frontend.yml)
```yml
# You can use the Cortex query frontend with any Prometheus-API compatible
# service, including Prometheus and Thanos.  Use this config file to get
# the benefits of query parallelisation and caching.

# Disable the requirement that every request to Cortex has a
# X-Scope-OrgID header. `fake` will be substituted in instead.
auth_enabled: false

# We only want to run the query-frontend module.
target: query-frontend

# We don't want the usual /api/prom prefix.
http_prefix:

server:
  http_listen_port: 9091

query_range:
  split_queries_by_interval: 24h
  align_queries_with_step: true
  cache_results: true

  results_cache:
    backend: "memcached"

    memcached:
      # You can either configure a headless service in Kubernetes and Mimir will discover the individual
      # instances using a SRV DNS query (host) or list comma separated memcached addresses.
      addresses: "dnssrvnoa+memcached.mimir.svc.cluster.local:11211"

frontend:
  log_queries_longer_than: 1s
  compress_responses: true

  # The Prometheus URL to which the query-frontend should connect to.
  downstream_url: http://prometheus.mydomain.com
```
<!-- prettier-ignore-end -->
