// SPDX-License-Identifier: AGPL-3.0-only

package testutil

import (
	"context"
	"crypto/rand"
	"math"
	"path/filepath"
	"sort"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
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

// GenerateBlockFromSpec generates a TSDB block with series and chunks provided by the input specs.
// This utility is intended just to be used for testing. Do not use it for any production code.
func GenerateBlockFromSpec(userID string, storageDir string, specs BlockSeriesSpecs) (_ *metadata.Meta, returnErr error) {
	blockID := ulid.MustNew(ulid.Now(), rand.Reader)
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
		return nil, err
	}

	// Ensure the chunk writer is always closed (even on error).
	chunkwClosed := false
	defer func() {
		if !chunkwClosed {
			if err := chunkw.Close(); err != nil && returnErr == nil {
				returnErr = err
			}
		}
	}()

	// Updates the Ref on each chunk.
	for _, series := range specs {
		// Ensure every chunk meta has chunk data.
		for _, c := range series.Chunks {
			if c.Chunk == nil {
				return nil, errors.Errorf("missing chunk data for series %s", series.Labels.String())
			}
		}

		if err := chunkw.WriteChunks(series.Chunks...); err != nil {
			return nil, err
		}
	}

	chunkwClosed = true
	if err := chunkw.Close(); err != nil {
		return nil, nil
	}

	// Write index.
	indexw, err := index.NewWriter(context.Background(), filepath.Join(blockDir, "index"))
	if err != nil {
		return nil, err
	}

	// Ensure the index writer is always closed (even on error).
	indexwClosed := false
	defer func() {
		if !indexwClosed {
			if err := indexw.Close(); err != nil && returnErr == nil {
				returnErr = err
			}
		}
	}()

	// Add symbols.
	for _, s := range symbols {
		if err := indexw.AddSymbol(s); err != nil {
			return nil, err
		}
	}

	// Add series.
	for i, series := range specs {
		if err := indexw.AddSeries(storage.SeriesRef(i), series.Labels, series.Chunks...); err != nil {
			return nil, err
		}
	}

	indexwClosed = true
	if err := indexw.Close(); err != nil {
		return nil, err
	}

	// Generate the meta.json file.
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			MinTime: specs.MinTime(),
			MaxTime: specs.MaxTime() + 1, // Not included.
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{blockID},
			},
			Version: 1,
		},
		Thanos: metadata.Thanos{
			Version: metadata.ThanosVersion1,
		},
	}

	return meta, meta.WriteToDir(log.NewNopLogger(), blockDir)
}
