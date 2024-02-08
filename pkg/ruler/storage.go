// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/storage.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/ruler/rulestore/bucketclient"
	"github.com/grafana/mimir/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

// NewRuleStore returns a rule store backend client based on the provided cfg.
func NewRuleStore(ctx context.Context, cfg rulestore.Config, cfgProvider bucket.TenantConfigProvider, loader promRules.GroupLoader, cacheTTL time.Duration, logger log.Logger, reg prometheus.Registerer) (directStore, cachedStore rulestore.RuleStore, _ error) {
	if cfg.Backend == rulestore.BackendLocal {
		store, err := local.NewLocalRulesClient(cfg.Local, loader)
		if err != nil {
			return nil, nil, err
		}

		return store, store, nil
	}

	if cfg.Backend == bucket.Filesystem {
		level.Warn(logger).Log("msg", "-ruler-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem")
	}

	directBucketClient, err := bucket.NewClient(ctx, cfg.Config, "ruler-storage", logger, reg)
	if err != nil {
		return nil, nil, err
	}

	cachedBucketClient, err := wrapBucketWithCache(directBucketClient, cfg, cacheTTL, logger, reg)
	if err != nil {
		return nil, nil, err
	}

	directStore = bucketclient.NewBucketRuleStore(directBucketClient, cfgProvider, logger)
	cachedStore = bucketclient.NewBucketRuleStore(cachedBucketClient, cfgProvider, logger)

	return directStore, cachedStore, nil
}

func wrapBucketWithCache(bkt objstore.Bucket, cfg rulestore.Config, cacheTTL time.Duration, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	// Add the "component" label similarly to other components, so that metrics don't clash and have the same labels set
	// when running in monolithic mode.
	reg = prometheus.WrapRegistererWith(prometheus.Labels{"component": "ruler"}, reg)

	cacheCfg := bucketcache.NewCachingBucketConfig()

	cacheClient, err := cache.CreateClient("ruler-storage-cache", cfg.Cache, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, errors.Wrapf(err, "ruler-storage-cache")
	}

	// If the cache backend hasn't been configured the returned cache instance is nil.
	if cacheClient == nil {
		return bkt, nil
	}

	// Cache all bucket iterations except tenants listing, for two reasons:
	// 1. We want new tenants to be discovered asap to offer a better UX to tenants setting up their first rules.
	// 2. The number of API calls issued to list tenants is orders of magnitude lower than the ones issued to list
	//    per-tenant rule groups in a multi-tenant cluster.
	codec := bucketcache.SnappyIterCodec{IterCodec: bucketcache.JSONIterCodec{}}
	cacheCfg.CacheIter("iter", cacheClient, isNotTenantsDir, cacheTTL, codec)

	return bucketcache.NewCachingBucket("ruler", bkt, cacheCfg, logger, reg)
}

func isNotTenantsDir(name string) bool {
	return name != ""
}
