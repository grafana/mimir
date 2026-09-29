// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/mimir/blob/main/pkg/storage/tsdb/block/block_generator.go
// Provenance-includes-license: AGPL-3.0-only
// Provenance-includes-copyright: The Mimir Authors.

package cardgen

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// writeBlock is block.GenerateBlockFromSpec with the block ID taken as a
// parameter instead of drawn from crypto/rand, so a caller can make block
// IDs (and therefore fixture layouts) reproducible from a seed. Keep this in
// sync with upstream if that function's on-disk format changes.
func writeBlock(userDir string, id ulid.ULID, specs block.SeriesSpecs) (_ *block.Meta, returnErr error) {
	dir := blockDir(userDir, id)
	stats := tsdb.BlockStats{}

	slices.SortFunc(specs, func(a, b *block.SeriesSpec) int {
		return labels.Compare(a.Labels, b.Labels)
	})

	uniqueSymbols := map[string]struct{}{}
	for _, series := range specs {
		series.Labels.Range(func(l labels.Label) {
			uniqueSymbols[l.Name] = struct{}{}
			uniqueSymbols[l.Value] = struct{}{}
		})
	}
	symbols := make([]string, 0, len(uniqueSymbols))
	for s := range uniqueSymbols {
		symbols = append(symbols, s)
	}
	slices.Sort(symbols)

	chunkw, err := chunks.NewWriter(filepath.Join(dir, "chunks"))
	if err != nil {
		return nil, err
	}
	chunkwClosed := false
	defer func() {
		if !chunkwClosed {
			if err := chunkw.Close(); err != nil && returnErr == nil {
				returnErr = err
			}
		}
	}()

	for _, series := range specs {
		for _, c := range series.Chunks {
			if c.Chunk == nil {
				return nil, errors.Errorf("missing chunk data for series %s", series.Labels.String())
			}
		}
		updateStats(&stats, series.Chunks)
		if err := chunkw.WriteChunks(series.Chunks...); err != nil {
			return nil, err
		}
	}
	chunkwClosed = true
	if err := chunkw.Close(); err != nil {
		return nil, err
	}

	indexw, err := index.NewWriter(context.Background(), filepath.Join(dir, "index"))
	if err != nil {
		return nil, err
	}
	indexwClosed := false
	defer func() {
		if !indexwClosed {
			if err := indexw.Close(); err != nil && returnErr == nil {
				returnErr = err
			}
		}
	}()

	for _, s := range symbols {
		if err := indexw.AddSymbol(s); err != nil {
			return nil, err
		}
	}
	for i, series := range specs {
		if err := indexw.AddSeries(storage.SeriesRef(i), series.Labels, series.Chunks...); err != nil {
			return nil, err
		}
	}
	indexwClosed = true
	if err := indexw.Close(); err != nil {
		return nil, err
	}

	meta := &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: specs.MinTime(),
			MaxTime: specs.MaxTime() + 1, // Not included.
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{id},
			},
			Version: 1,
			Stats:   stats,
		},
		Thanos: block.ThanosMeta{
			Version: block.ThanosVersion1,
		},
	}
	return meta, meta.WriteToDir(log.NewNopLogger(), dir)
}

// updateStats mirrors the unexported helper of the same name in
// pkg/storage/tsdb/block/index.go: it counts one series plus its samples and
// chunks into stats. All chunks here are XOR-encoded float samples.
func updateStats(stats *tsdb.BlockStats, chunkMetas []chunks.Meta) {
	stats.NumSeries++
	stats.NumChunks += uint64(len(chunkMetas))
	for _, c := range chunkMetas {
		n := uint64(c.Chunk.NumSamples())
		stats.NumSamples += n
		stats.NumFloatSamples += n
	}
}
