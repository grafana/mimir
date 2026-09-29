// SPDX-License-Identifier: AGPL-3.0-only

// Package cardgen turns a model.Model into level-1 TSDB blocks laid out the
// way Mimir's block-builder produces them: one block per (partition, hour),
// holding every series with a sample in that hour, so a downstream compactor
// run sees the same shape it would from real ingestion.
package cardgen

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/tools/headcount/model"
)

// Config controls block layout. Partitions must match the partition count
// the compactor run afterward is configured for, so that split-and-merge
// shard boundaries land the same way they would for real ingester output.
type Config struct {
	Partitions int
	// Seed makes the generated blocks' ULIDs deterministic across runs,
	// independent of the population's own seed.
	Seed int64
}

// Generate writes one level-1 block per (partition, hour) with at least one
// live series into userDir, and returns their metadata in write order.
// Hour boundaries with no live series in a partition are skipped: the real
// block-builder never uploads an empty block either.
func Generate(pop *model.Model, userDir string, cfg Config) ([]*block.Meta, error) {
	if cfg.Partitions <= 0 {
		return nil, fmt.Errorf("partitions must be positive, got %d", cfg.Partitions)
	}

	cfgTime := pop.Config()
	hourMs := time.Hour.Milliseconds()
	// ulid.Monotonic guarantees distinct IDs for the many blocks sharing an
	// hour's timestamp (one per partition), while staying a pure function of
	// cfg.Seed and call order, so re-running Generate reproduces the same
	// block IDs.
	entropy := ulid.Monotonic(rand.New(rand.NewSource(cfg.Seed)), 0)

	var metas []*block.Meta
	for hourStart := cfgTime.Start.UnixMilli(); hourStart < cfgTime.End.UnixMilli(); hourStart += hourMs {
		hourEnd := hourStart + hourMs
		byPartition := partitionSeries(pop.Series, hourStart, hourEnd, cfg.Partitions)

		for _, specs := range byPartition {
			if len(specs) == 0 {
				continue
			}
			id, err := ulid.New(uint64(hourStart), entropy)
			if err != nil {
				return nil, fmt.Errorf("generating block id: %w", err)
			}
			meta, err := writeBlock(userDir, id, specs)
			if err != nil {
				return nil, fmt.Errorf("writing block for hour %d: %w", hourStart, err)
			}
			metas = append(metas, meta)
		}
	}
	return metas, nil
}

// partitionSeries assigns every series live in [hourStart, hourEnd) to one
// of numPartitions buckets by labels.StableHash, the same function the
// compactor's own partitioning uses, and builds one chunk per series
// covering its live time within the hour.
func partitionSeries(series []model.Series, hourStart, hourEnd int64, numPartitions int) []block.SeriesSpecs {
	byPartition := make([]block.SeriesSpecs, numPartitions)
	for _, s := range series {
		c, ok := hourChunk(s, hourStart, hourEnd)
		if !ok {
			continue
		}
		p := labels.StableHash(s.Labels) % uint64(numPartitions)
		byPartition[p] = append(byPartition[p], &block.SeriesSpec{Labels: s.Labels, Chunks: []chunks.Meta{c}})
	}
	return byPartition
}

// hourChunk builds a two-sample XOR chunk spanning a series' live time
// within [hourStart, hourEnd), matching the design's "chunks carry exact
// times cheaply, sample values are never read" shape. It reports false if
// the series has no interval overlapping the hour.
func hourChunk(s model.Series, hourStart, hourEnd int64) (chunks.Meta, bool) {
	minT, maxT := int64(-1), int64(-1)
	for _, iv := range s.Intervals {
		if !iv.Overlaps(hourStart, hourEnd) {
			continue
		}
		lo, hi := max(iv.Start, hourStart), min(iv.End, hourEnd)-1
		if minT == -1 || lo < minT {
			minT = lo
		}
		maxT = max(maxT, hi)
	}
	if minT == -1 {
		return chunks.Meta{}, false
	}

	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		return chunks.Meta{}, false
	}
	app.Append(0, minT, 1)
	if maxT > minT {
		app.Append(0, maxT, 1)
	}
	return chunks.Meta{Chunk: c, MinTime: minT, MaxTime: maxT}, true
}

func blockDir(userDir string, id ulid.ULID) string { return filepath.Join(userDir, id.String()) }
