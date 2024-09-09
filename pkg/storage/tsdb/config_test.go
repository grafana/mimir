// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"flag"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup       func(*BlocksStorageConfig, *activeseries.Config)
		expectedErr error
	}{
		"should pass on S3 backend": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.Bucket.Backend = "s3"
			},
			expectedErr: nil,
		},
		"should pass on GCS backend": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.Bucket.Backend = "gcs"
			},
			expectedErr: nil,
		},
		"should fail on unknown storage backend": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.Bucket.Backend = "unknown"
			},
			expectedErr: bucket.ErrUnsupportedStorageBackend,
		},
		"should fail on invalid ship concurrency": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.ShipConcurrency = 0
			},
			expectedErr: errInvalidShipConcurrency,
		},
		"should pass on invalid ship concurrency but shipping is disabled": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.ShipConcurrency = 0
				cfg.TSDB.ShipInterval = 0
			},
			expectedErr: nil,
		},
		"should fail on invalid compaction interval": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.HeadCompactionInterval = 0
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on too high compaction interval": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.HeadCompactionInterval = 20 * time.Minute
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on invalid compaction concurrency": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.HeadCompactionConcurrency = 0
			},
			expectedErr: errInvalidCompactionConcurrency,
		},
		"should pass on valid compaction concurrency": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.HeadCompactionConcurrency = 10
			},
			expectedErr: nil,
		},
		"should fail on negative stripe size": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.StripeSize = -2
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 0": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.StripeSize = 0
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 1": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.StripeSize = 1
			},
			expectedErr: errInvalidStripeSize,
		},
		"should pass on valid stripe size": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.StripeSize = 1 << 14
			},
			expectedErr: nil,
		},
		"should fail on empty block ranges": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.BlockRanges = nil
			},
			expectedErr: errEmptyBlockranges,
		},
		"should fail on invalid TSDB WAL segment size": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.WALSegmentSizeBytes = 0
			},
			expectedErr: errInvalidWALSegmentSizeBytes,
		},
		"should fail on invalid store-gateway streaming batch size": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.StreamingBatchSize = 0
			},
			expectedErr: errInvalidStreamingBatchSize,
		},
		"should fail if forced compaction is enabled but active series tracker is not": {
			setup: func(cfg *BlocksStorageConfig, activeSeriesCfg *activeseries.Config) {
				cfg.TSDB.EarlyHeadCompactionMinInMemorySeries = 1_000_000
				activeSeriesCfg.Enabled = false
			},
			expectedErr: errEarlyCompactionRequiresActiveSeries,
		},
		"should fail on invalid forced compaction min series reduction percentage": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 101
			},
			expectedErr: errInvalidEarlyHeadCompactionMinSeriesReduction,
		},
		"should not fail on 'ignore deletion marks while querying delay' less than 'ignore deletion marks in store-gateways delay'": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 120 * time.Minute
				cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay = 121 * time.Minute
			},
			expectedErr: nil,
		},
		"should fail on 'ignore deletion marks while querying delay' equal to 'ignore deletion marks in store-gateways delay'": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 2 * time.Hour
				cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay = 2 * time.Hour
			},
			expectedErr: errInvalidIgnoreDeletionMarksDelayConfig,
		},
		"should fail on 'ignore deletion marks while querying delay' greater than 'ignore deletion marks in store-gateways delay'": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 121 * time.Minute
				cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay = 120 * time.Minute
			},
			expectedErr: errInvalidIgnoreDeletionMarksDelayConfig,
		},
		"should not fail on 'ignore deletion marks while querying delay' greater than 3x sync interval": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.SyncInterval = 10 * time.Minute
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 31 * time.Minute
			},
			expectedErr: nil,
		},
		"should fail on 'ignore deletion marks while query delay' equal to 3x sync interval": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.SyncInterval = 10 * time.Minute
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 30 * time.Minute
			},
			expectedErr: errIgnoreDeletionMarksDelayTooShort,
		},
		"should fail on 'ignore deletion marks while query delay' less than 3x sync interval": {
			setup: func(cfg *BlocksStorageConfig, _ *activeseries.Config) {
				cfg.BucketStore.SyncInterval = 10 * time.Minute
				cfg.BucketStore.IgnoreDeletionMarksWhileQueryingDelay = 29 * time.Minute
			},
			expectedErr: errIgnoreDeletionMarksDelayTooShort,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			storageCfg := &BlocksStorageConfig{}
			activeSeriesCfg := &activeseries.Config{}

			flagext.DefaultValues(storageCfg)
			flagext.DefaultValues(activeSeriesCfg)
			testData.setup(storageCfg, activeSeriesCfg)

			actualErr := storageCfg.Validate(*activeSeriesCfg)
			assert.Equal(t, testData.expectedErr, actualErr)
		})
	}
}

func TestConfig_DurationList(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg            BlocksStorageConfig
		expectedRanges []int64
		f              func(*BlocksStorageConfig)
	}{
		"default to 2h": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
		"parse ranges correctly": {
			cfg: BlocksStorageConfig{
				TSDB: TSDBConfig{
					BlockRanges: []time.Duration{
						2 * time.Hour,
						10 * time.Hour,
						50 * time.Hour,
					},
				},
			},
			expectedRanges: []int64{7200000, 36000000, 180000000},
			f:              func(*BlocksStorageConfig) {},
		},
		"handle multiple flag parse": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
	}

	for name, data := range tests {
		testdata := data

		t.Run(name, func(t *testing.T) {
			testdata.f(&testdata.cfg)
			assert.Equal(t, testdata.expectedRanges, testdata.cfg.TSDB.BlockRanges.ToMilliseconds())
		})
	}
}
