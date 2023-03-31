---
title: "Configure Redis cache"
menuTitle: "Redis cache"
description: "Learn how to configure Grafana Mimir to use external Redis as cache"
---

# Configure Redis cache

Beside built-in support for Memcached, Mimir also supports external Redis for chunks-cache, index-cache, results-cache and metadata-cache. To use Redis, we must disable the build-in Memcached
from `mimir-distributed` helm chart by making sure the cache configuration is not enabled. We can explicitly set `*-cache.enabled` to false or remove the whole `*-cache`
block from values file so that default setting disables Memcached.

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

Next we have to tell Mimir how to connect to the external Redis. Refer to [configuration parameters](docs/mimir/v2.7/references/configuration-parameters/) on available configuration options for Redis. We can use the Redis configuration block in `mimir.structuredConfig` as follows:

```yaml
mimir:
  structuredConfig:
    blocks_storage:
      bucket_store:
        chunks_cache:
          backend: redis
          redis:
            endpoint: <redis-url>:6379
            # and some more configs
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
