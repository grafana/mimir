// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// partitionStore is the storage of one (tenant, partition, epoch): a Prometheus TSDB or a seriesstore engine.
// partitionTSDB serializes its mutations and fences its appends around it.
type partitionStore interface {
	Appender(ctx context.Context) storage.Appender
	Querier(mint, maxt int64) (storage.Querier, error)
	ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)
	Head() partitionHead
	// Blocks are the persisted Prometheus blocks; the seriesstore engine has none.
	Blocks() []*tsdb.Block
	// BlockTimes is the span of the data that left the head, its max time exclusive like a block's.
	BlockTimes() (minTime, maxTime int64, ok bool)
	// CompactHeadRange moves the head's samples in [mint, maxt] out of memory.
	CompactHeadRange(mint, maxt int64) error
	CompactOOOHead(ctx context.Context) error
	// Compact is the regular compaction: what is due leaves the head, the live window stays.
	Compact(ctx context.Context) error
	ApplySettings(outOfOrderTimeWindowMs, maxExemplars int64) error
	Close() error
}

// partitionHead is the in-memory part of a partition's storage: the Prometheus TSDB's head and the seriesstore engine
// are both one.
type partitionHead interface {
	ingester.PushHead
	loadstats.SecondaryHashHead
	NumSeries() uint64
	MinTime() int64
	MaxTime() int64
	MaxOOOTime() int64
	Meta() tsdb.BlockMeta
}

var _ partitionHead = (*tsdb.Head)(nil)

// prometheusStore is a Prometheus TSDB.
type prometheusStore struct {
	*tsdb.DB
}

func (s prometheusStore) Head() partitionHead {
	return s.DB.Head()
}

func (s prometheusStore) BlockTimes() (minTime, maxTime int64, ok bool) {
	blocks := s.DB.Blocks()
	if len(blocks) == 0 {
		return 0, 0, false
	}
	minTime, maxTime = int64(math.MaxInt64), int64(math.MinInt64)
	for _, block := range blocks {
		meta := block.Meta()
		minTime, maxTime = min(minTime, meta.MinTime), max(maxTime, meta.MaxTime)
	}
	return minTime, maxTime, true
}

func (s prometheusStore) CompactHeadRange(mint, maxt int64) error {
	return s.DB.CompactHead(tsdb.NewRangeHead(s.DB.Head(), mint, maxt))
}

func (s prometheusStore) ApplySettings(outOfOrderTimeWindowMs, maxExemplars int64) error {
	return s.DB.ApplyConfig(&config.Config{
		StorageConfig: config.StorageConfig{
			ExemplarsConfig: &config.ExemplarsConfig{MaxExemplars: maxExemplars},
			TSDBConfig:      &config.TSDBConfig{OutOfOrderTimeWindow: outOfOrderTimeWindowMs},
		},
	})
}
