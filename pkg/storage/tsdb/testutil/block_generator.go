package testutil

import (
	"context"
	"math"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	thanos_metadata "github.com/thanos-io/thanos/pkg/block/metadata"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

type BlockSeriesSpec struct {
	Labels labels.Labels
	Chunks []chunks.Meta
}

type BlockSeriesSpecs []*BlockSeriesSpec

func (s BlockSeriesSpecs) MinTime() int64 {
	minTime := int64(math.MaxInt64)

	for _, series := range s {
		for _, c := range series.Chunks {
			if c.MinTime < minTime {
				minTime = c.MinTime
			}
		}
	}

	return minTime
}

func (s BlockSeriesSpecs) MaxTime() int64 {
	maxTime := int64(math.MinInt64)

	for _, series := range s {
		for _, c := range series.Chunks {
			if c.MaxTime > maxTime {
				maxTime = c.MaxTime
			}
		}
	}

	return maxTime
}

func GenerateBlockFromSpec(userID string, storageDir string, specs BlockSeriesSpecs) (ulid.ULID, error) {
	blockID := ulid.MustNew(uint64(time.Now().UnixMilli()), nil)
	blockDir := filepath.Join(storageDir, blockID.String())

	// Ensure series labels are sorted.
	for _, series := range specs {
		sort.Sort(series.Labels)
	}

	// Ensure series are sorted.
	sort.Slice(specs, func(i, j int) bool {
		return labels.Compare(specs[i].Labels, specs[j].Labels) < 0
	})

	// Build symbols.
	uniqueSymbols := map[string]struct{}{}
	for _, series := range specs {
		for _, l := range series.Labels {
			uniqueSymbols[l.Name] = struct{}{}
			uniqueSymbols[l.Value] = struct{}{}
		}
	}

	symbols := []string{}
	for s := range uniqueSymbols {
		symbols = append(symbols, s)
	}
	sort.Strings(symbols)

	// Write all chunks to segment files.
	chunkw, err := chunks.NewWriter(filepath.Join(blockDir, "chunks"))
	if err != nil {
		return blockID, err
	}

	// Updates the Ref on each chunk.
	for _, series := range specs {
		if err := chunkw.WriteChunks(series.Chunks...); err != nil {
			return blockID, err
		}
	}

	if err := chunkw.Close(); err != nil {
		return blockID, nil
	}

	// Write index.
	iw, err := index.NewWriter(context.Background(), filepath.Join(blockDir, "index"))
	if err != nil {
		return blockID, err
	}

	// Add symbols.
	for _, s := range symbols {
		if err := iw.AddSymbol(s); err != nil {
			return blockID, err
		}
	}

	// Add series.
	for i, series := range specs {
		if err := iw.AddSeries(storage.SeriesRef(i), series.Labels, series.Chunks...); err != nil {
			return blockID, err
		}
	}

	if err := iw.Close(); err != nil {
		return blockID, err
	}

	// Generate the meta.json file.
	meta := &thanos_metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:       blockID,
			MinTime:    specs.MinTime(),
			MaxTime:    specs.MaxTime() + 1, // Not included.
			Compaction: tsdb.BlockMetaCompaction{Level: 1},
			Version:    1,
		},
		Thanos: thanos_metadata.Thanos{
			Version: thanos_metadata.ThanosVersion1,
			Labels:  map[string]string{mimir_tsdb.TenantIDExternalLabel: userID},
		},
	}

	if err := meta.WriteToDir(log.NewNopLogger(), blockDir); err != nil {
		return blockID, err
	}

	return blockID, nil
}
