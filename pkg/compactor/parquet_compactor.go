// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package compactor

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ParquetCompactor struct {
	services.Service

	bucket objstore.InstrumentedBucket // TODO (jesus.vazquez) Compactor is using objstore.Bucket instead

	loader      *bucketindex.Loader
	Cfg         Config
	registerer  prometheus.Registerer
	logger      log.Logger
	limits      *validation.Overrides
	blockRanges []int64

	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher
}

func NewParquetCompactor(compactorCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides, ingestionReplicationFactor int) (*ParquetCompactor, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "parquet-compactor", logger, prometheus.DefaultRegisterer)

	if err != nil {
		return nil, err
	}
	indexLoaderConfig := bucketindex.LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
		UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
		IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
	}

	loader := bucketindex.NewLoader(indexLoaderConfig, bucketClient, limits, util_log.Logger, prometheus.DefaultRegisterer)

	c := &ParquetCompactor{
		Cfg:         compactorCfg,
		bucket:      bucketClient,
		loader:      loader,
		logger:      log.With(logger, "component", "parquet-compactor"),
		registerer:  registerer,
		blockRanges: compactorCfg.BlockRanges.ToMilliseconds(),
		limits:      limits,
	}

	c.Service = services.NewBasicService(c.starting, c.run, c.stopping)
	return c, nil
}

func (c *ParquetCompactor) starting(ctx context.Context) error {
	// TODO (jesus.vazquez) Implement the starting method.
	return nil
}

func (c *ParquetCompactor) run(ctx context.Context) error {
	// TODO (jesus.vazquez) Implement the run method.
	return nil
}

func (c *ParquetCompactor) stopping(_ error) error {
	// TODO (jesus.vazquez) Implement the stopping method.
	return nil
}
