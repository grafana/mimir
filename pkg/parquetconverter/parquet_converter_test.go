// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestParquetConverter(t *testing.T) {
	user := "testuser"
	t.Parallel()

	bucketDir := t.TempDir()
	bucketClient, err := filesystem.NewBucket(bucketDir)
	println(bucketDir)
	require.NoError(t, err)
	uBucket := bucket.NewPrefixedBucketClient(bucketClient, user)

	ctx := context.Background()

	bid := uploadTestBlock(ctx, t, uBucket)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfig(t)
	cfg.ShardingRing.Common.InstanceID = "converters-1"
	cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.Common.KVStore.Mock = ringStore

	c, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient))

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), c)) })

	test.Poll(t, 10*time.Second, true, func() interface{} {
		parquetFiles := 0
		err := uBucket.Iter(ctx, bid.String(), func(name string) error {
			if strings.HasSuffix(name, ".parquet") {
				parquetFiles++
			}
			return nil
		})
		require.NoError(t, err)
		mark, err := ReadConversionMark(ctx, bid, uBucket, log.NewNopLogger())
		require.NoError(t, err)
		return parquetFiles == 2 && mark.Version == CurrentVersion
	})

}

func prepare(t *testing.T, cfg Config, bucketClient objstore.Bucket) (*ParquetConverter, *prometheus.Registry) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	cfg.DataDir = t.TempDir()

	logs := &concurrency.SyncBuffer{}
	registry := prometheus.NewRegistry()

	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucketClient, nil
	}
	c, err := newParquetConverter(cfg, log.NewLogfmtLogger(logs), registry, bucketClientFactory, overrides, defaultBlockConverter{})
	require.NoError(t, err)

	return c, registry
}

func prepareConfig(t *testing.T) Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	// Do not wait for ring stability by default, in order to speed up tests.
	cfg.ShardingRing.WaitStabilityMinDuration = 0
	cfg.ShardingRing.WaitStabilityMaxDuration = 0

	// Set lower timeout for waiting on converter to become ACTIVE in the ring for unit tests
	cfg.ShardingRing.WaitActiveInstanceTimeout = 5 * time.Second

	// Inject default KV store. Must be overridden if "real" sharding is required.
	inmem, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })
	cfg.ShardingRing.Common.KVStore.Mock = inmem
	cfg.ShardingRing.Common.InstanceAddr = "localhost"

	// Speed up tests
	cfg.ConversionInterval = 100 * time.Millisecond
	cfg.DiscoveryInterval = 100 * time.Millisecond

	cfg.MinCompactionLevel = 0
	return cfg
}

func uploadTestBlock(ctx context.Context, t *testing.T, bucket objstore.Bucket) ulid.ULID {
	return uploadTestBlockWithLevel(ctx, t, bucket, 2) // Default level 2
}

func uploadTestBlockWithLevel(ctx context.Context, t *testing.T, bucket objstore.Bucket, level int) ulid.ULID {
	dir := t.TempDir()

	bid, err := block.CreateBlock(ctx, dir,
		[]labels.Labels{
			labels.FromStrings("test", "foo", "a", "1"),
			labels.FromStrings("test", "foo", "a", "2"),
			labels.FromStrings("test", "foo", "a", "3"),
		},
		10, 1000, 2000, labels.EmptyLabels())

	require.NoError(t, err)

	// Manually set the compaction level in the meta.json
	metaFile := filepath.Join(dir, bid.String(), "meta.json")
	metaBytes, err := os.ReadFile(metaFile)
	require.NoError(t, err)

	var meta block.Meta
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	meta.Compaction.Level = level

	updatedMetaBytes, err := json.Marshal(&meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metaFile, updatedMetaBytes, 0644))

	b, err := tsdb.OpenBlock(nil, fmt.Sprintf("%s/%s", dir, bid.String()), nil, nil)
	require.NoError(t, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bucket, b.Dir(), nil)
	require.NoError(t, err)
	return bid
}

func TestShouldProcessBlock(t *testing.T) {
	now := time.Now()
	baseTime := now.Add(-2 * time.Hour)

	meta := &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:       ulid.MustNew(uint64(baseTime.UnixMilli()), nil),
			MinTime:    baseTime.UnixMilli(),
			MaxTime:    baseTime.Add(1 * time.Hour).UnixMilli(),
			Compaction: tsdb.BlockMetaCompaction{Level: 2},
		},
	}

	tests := []struct {
		name     string
		cfg      func(Config) Config
		setup    func(*testing.T, *ParquetConverter, objstore.InstrumentedBucket, *block.Meta)
		wantSkip bool
	}{
		{
			name:     "no filtering",
			wantSkip: false,
		},
		{
			name: "should process",
			cfg: func(c Config) Config {
				c.MinCompactionLevel = meta.Compaction.Level - 1
				c.MinBlockDuration = time.Duration(meta.MaxTime-meta.MinTime) - time.Hour
				c.MaxBlockAge = time.Since(time.UnixMilli(meta.MinTime)) + time.Hour
				c.MinDataAge = time.Since(time.UnixMilli(meta.MinTime)) - time.Hour
				c.MinBlockTimestamp = meta.ULID.Time() - 1000
				return c
			},
			wantSkip: false,
		},
		{
			name: "compaction level too low",
			cfg: func(c Config) Config {
				c.MinCompactionLevel = meta.Compaction.Level + 1
				return c
			},
			wantSkip: true,
		},
		{
			name: "block duration too short",
			cfg: func(c Config) Config {
				c.MinBlockDuration = time.Duration(meta.MaxTime-meta.MinTime) + time.Hour
				return c
			},
			wantSkip: true,
		},
		{
			name: "block too old",
			cfg: func(c Config) Config {
				c.MaxBlockAge = time.Since(time.UnixMilli(meta.MinTime)) - time.Nanosecond
				return c
			},
			wantSkip: true,
		},
		{
			name: "data too recent",
			cfg: func(c Config) Config {
				c.MinDataAge = time.Since(time.UnixMilli(meta.MinTime)) + time.Hour
				return c
			},
			wantSkip: true,
		},
		{
			name: "already queued",
			setup: func(t *testing.T, c *ParquetConverter, bucket objstore.InstrumentedBucket, meta *block.Meta) {
				c.queuedBlocks.Store(meta.ULID, time.Now())
			},
			wantSkip: true,
		},
		{
			name: "already converted",
			setup: func(t *testing.T, c *ParquetConverter, bucket objstore.InstrumentedBucket, meta *block.Meta) {
				err := WriteConversionMark(context.Background(), meta.ULID, bucket)
				require.NoError(t, err)
			},
			wantSkip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup bucket
			bucketClient, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			bucket := bucket.NewUserBucketClient("test", objstore.WithNoopInstr(bucketClient), nil)

			// Setup ring (same pattern as TestParquetConverter)
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			defer closer.Close()

			cfg := prepareConfig(t)
			if tt.cfg != nil {
				cfg = tt.cfg(cfg)
			}
			cfg.ShardingRing.Common.InstanceID = "test-converter"
			cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
			cfg.ShardingRing.Common.KVStore.Mock = ringStore

			c, _ := prepare(t, cfg, bucketClient)

			// Start services for ring functionality
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
			defer func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), c)) }()

			// Apply test-specific setup
			if tt.setup != nil {
				tt.setup(t, c, bucket, meta)
			}

			// Test the function
			reason, err := c.shouldProcessBlock(context.Background(), meta, bucket)
			require.NoError(t, err)

			if tt.wantSkip {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}
