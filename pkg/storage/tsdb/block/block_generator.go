// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/testutil/e2eutil/prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"context"
	crypto_rand "crypto/rand"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/util/test"
)

type SeriesSpec struct {
	Labels labels.Labels
	Chunks []chunks.Meta
}

type SeriesSpecs []*SeriesSpec

func (s SeriesSpecs) MinTime() int64 {
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

func (s SeriesSpecs) MaxTime() int64 {
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
func GenerateBlockFromSpec(storageDir string, specs SeriesSpecs) (_ *Meta, returnErr error) {
	blockID := ulid.MustNew(ulid.Now(), crypto_rand.Reader)
	blockDir := filepath.Join(storageDir, blockID.String())

	stats := tsdb.BlockStats{}

	// Ensure series are sorted.
	sort.Slice(specs, func(i, j int) bool {
		return labels.Compare(specs[i].Labels, specs[j].Labels) < 0
	})

	// Build symbols.
	uniqueSymbols := map[string]struct{}{}
	for _, series := range specs {
		series.Labels.Range(func(l labels.Label) {
			uniqueSymbols[l.Name] = struct{}{}
			uniqueSymbols[l.Value] = struct{}{}
		})
	}

	symbols := []string{}
	for s := range uniqueSymbols {
		symbols = append(symbols, s)
	}
	slices.Sort(symbols)

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
		stats.NumSeries++

		// Ensure every chunk meta has chunk data.
		for _, c := range series.Chunks {
			if c.Chunk == nil {
				return nil, errors.Errorf("missing chunk data for series %s", series.Labels.String())
			}
			stats.NumChunks++
			stats.NumSamples += uint64(c.Chunk.NumSamples())
		}

		if err := chunkw.WriteChunks(series.Chunks...); err != nil {
			return nil, err
		}
	}

	chunkwClosed = true
	if err := chunkw.Close(); err != nil {
		return nil, err
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
	meta := &Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			MinTime: specs.MinTime(),
			MaxTime: specs.MaxTime() + 1, // Not included.
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{blockID},
			},
			Version: 1,
			Stats:   stats,
		},
		Thanos: ThanosMeta{
			Version: ThanosVersion1,
		},
	}

	return meta, meta.WriteToDir(log.NewNopLogger(), blockDir)
}

// CreateBlock writes a block with the given series and numSamples samples each.
// Timeseries i%3==0 will contain floats, i%3==1 will contain histograms and i%3==2 will contain float histograms
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
) (id ulid.ULID, err error) {
	if len(series) < 3 {
		return id, errors.Errorf("not enough series to test different value types: floats, histograms, float histograms")
	}

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = math.MaxInt64
	headOpts.EnableNativeHistograms.Store(true)
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	var g errgroup.Group
	var timeStepSize = (maxt - mint) / int64(numSamples+1)
	var batchSize = len(series) / runtime.GOMAXPROCS(0)

	// For calculating the next series value type (float, histogram, etc)
	batchOffset := 0
	// prep some histogram test data
	testHistograms := test.GenerateTestHistograms(numSamples)
	testFloatHistograms := test.GenerateTestFloatHistograms(numSamples)

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		batchValueTypeOffset := batchOffset
		batchOffset += batchSize
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender(ctx)

				for j, lset := range batch {
					var err error
					switch (batchValueTypeOffset + j) % 3 {
					case 0:
						_, err = app.Append(0, lset, t, rand.Float64()) //TODO check with dev team what this value is for and if it needs a CSPRNG
					case 1:
						_, err = app.AppendHistogram(0, lset, t, testHistograms[i], nil)
					case 2:
						_, err = app.AppendHistogram(0, lset, t, nil, testFloatHistograms[i])
					}
					if err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}

						return errors.Wrap(err, "add sample")
					}
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				t += timeStepSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	blocks, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if len(blocks) == 0 || (blocks[0] == ulid.ULID{}) {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}
	if len(blocks) > 1 {
		return id, errors.Errorf("expected one block, got %d, asked for %d samples", len(blocks), numSamples)
	}

	id = blocks[0]

	blockDir := filepath.Join(dir, id.String())

	if _, err = InjectThanosMeta(log.NewNopLogger(), blockDir, ThanosMeta{
		Labels: extLset.Map(),
		Source: TestSource,
		Files:  []File{},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
		return id, errors.Wrap(err, "remove tombstones")
	}

	return id, nil
}
