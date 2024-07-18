---
title: "Configure Redis cache"
menuTitle: "Redis cache"
description: "Learn how to configure Grafana Mimir to use external Redis as cache"
---

# Configure Redis cache

Besides support for Memcached, Mimir also supports Redis for the chunks-cache, index-cache, results-cache and metadata-cache. To use Redis, deploy Redis instances, disable the built-in Memcached configuration flag in values.yaml of `mimir-distributed` Helm chart, and then configure Mimir to use Redis.

To disable Memcached, remove any `chunks-cache`, `index-cache`, `metadata-cache` and `results-cache` configuration from your Helm `values.yaml` file. Alternatively, explicitly disable each of the Memcached instances by setting `enabled` to `false`:

```yaml
chunks-cache:
  enabled: false
index-cache:
  enabled: false
metadata-cache:
  enabled: false
results-cache:
  enabled: false
```

Next, configure Mimir to connect to Redis using `structuredConfig`. Refer to [the configuration parameters reference](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/#redis) for Redis connection configuration options. For example:

```yaml
mimir:
  structuredConfig:
    blocks_storage:
      bucket_store:
        chunks_cache:
          backend: redis
          redis:
            endpoint: <redis-url>:6379
        index_cache:
          backend: redis
          redis:
            endpoint: <redis-url>:6379
        metadata_cache:
          backend: redis
          redis:
            endpoint: <redis-url>:6379
    frontend:
      cache_results: true
      results_cache:
        backend: redis
        redis:
          endpoint: <redis-url>:6379
```
