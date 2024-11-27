// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSplitBlocks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	cfg := config{
		outputDir:        t.TempDir(),
		blockConcurrency: 2,
		maxBlockDuration: 24 * time.Hour,
	}
	logger := log.NewNopLogger()

	startOfDay := time.Now().Truncate(24 * time.Hour)
	specs := buildSeriesSpec(startOfDay)

	blocksDir := t.TempDir()
	meta, err := block.GenerateBlockFromSpec(blocksDir, specs)
	require.NoError(t, err)

	err = block.Upload(context.Background(), logger, bkt, path.Join(blocksDir, meta.ULID.String()), meta)
	require.NoError(t, err)

	for _, dryRun := range []bool{true, false} {
		cfg.dryRun = dryRun
		err = splitBlocks(context.Background(), cfg, bkt, logger)
		require.NoError(t, err)

		entries, err := os.ReadDir(cfg.outputDir)
		require.NoError(t, err)
		expectedEntries := 3
		if dryRun {
			expectedEntries = 0
		}
		require.Len(t, entries, expectedEntries)
	}
}

func TestSplitLocalBlock(t *testing.T) {
	dir := t.TempDir()

	startOfDay := time.Now().Truncate(24 * time.Hour)

	specs := buildSeriesSpec(startOfDay)
	meta, err := block.GenerateBlockFromSpec(dir, specs)
	require.NoError(t, err)

	blocks, err := splitLocalBlock(context.Background(), dir, filepath.Join(dir, meta.ULID.String()), *meta, 24*time.Hour, true, log.NewNopLogger())
	require.NoError(t, err)

	// We expect 3 blocks
	require.Len(t, blocks, 3)

	// First block
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[0].String()))

		// Series 1 has 2 samples in this block
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, spec[0].Chunks[0].Chunk.NumSamples(), 2)
		require.Equal(t, startOfDay.Add(10*time.Minute).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(12*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has 2 samples in this block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, spec[1].Chunks[0].Chunk.NumSamples(), 2)
		require.Equal(t, startOfDay.UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(12*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 2)
	}

	// Second block has all three series.
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[1].String()))

		// series 1 has 2 samples in this block
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, spec[0].Chunks[0].Chunk.NumSamples(), 2)
		require.Equal(t, startOfDay.Add(24*time.Hour).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(36*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has 2 samples in this block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, spec[1].Chunks[0].Chunk.NumSamples(), 2)
		require.Equal(t, startOfDay.Add(24*time.Hour).UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(36*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// Series 3 has 3 samples in this block
		require.Equal(t, spec[2].Labels, labels.FromStrings("__name__", "3_series_with_samples_on_second_day"))
		require.Len(t, spec[2].Chunks, 1)
		require.Equal(t, spec[2].Chunks[0].Chunk.NumSamples(), 3)
		require.Equal(t, startOfDay.Add(24*time.Hour).UnixMilli(), spec[2].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(26*time.Hour).UnixMilli(), spec[2].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 3)
	}

	// Last block, only has series 1 and 2 again.
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[2].String()))

		// series 1 has 1 sample in this block
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, spec[0].Chunks[0].Chunk.NumSamples(), 1)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has 1 sample in this block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, spec[1].Chunks[0].Chunk.NumSamples(), 1)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 2)
	}
}

func buildSeriesSpec(startOfDay time.Time) []*block.SeriesSpec {
	return []*block.SeriesSpec{
		{
			Labels: labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					// Ends up in block 1
					newSample(startOfDay.Add(10*time.Minute).UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(12*time.Hour).UnixMilli(), 2, nil, nil),
					// Ends up in block 2
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 3, nil, nil),
					newSample(startOfDay.Add(36*time.Hour).UnixMilli(), 4, nil, nil),
					// Ends up in block 3
					newSample(startOfDay.Add(48*time.Hour).UnixMilli(), 5, nil, nil),
				})),
			},
		},

		{
			Labels: labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"),
			Chunks: []chunks.Meta{
				// Ends up in block 1
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(12*time.Hour).UnixMilli(), 2, nil, nil),
				})),
				// Ends up in block 2
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 3, nil, nil),
					newSample(startOfDay.Add(36*time.Hour).UnixMilli(), 4, nil, nil),
				})),
				// Ends up in block 3
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(48*time.Hour).UnixMilli(), 5, nil, nil),
				})),
			},
		},

		{
			Labels: labels.FromStrings("__name__", "3_series_with_samples_on_second_day"),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					// Ends up in block 2
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(25*time.Hour).UnixMilli(), 2, nil, nil),
					newSample(startOfDay.Add(26*time.Hour).UnixMilli(), 3, nil, nil),
				})),
			},
		},
	}
}

func listSeriesAndChunksFromBlock(t *testing.T, blockDir string) []*block.SeriesSpec {
	blk, err := tsdb.OpenBlock(promslog.NewNopLogger(), blockDir, nil)
	require.NoError(t, err)
	chunkReader, err := blk.Chunks()
	require.NoError(t, err)
	defer require.NoError(t, chunkReader.Close())

	allKey, allValue := index.AllPostingsKey()
	r, err := index.NewFileReader(filepath.Join(blockDir, block.IndexFilename))
	require.NoError(t, err)
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")
	it, err := r.Postings(context.Background(), allKey, allValue)
	require.NoError(t, err)

	result := []*block.SeriesSpec(nil)
	for it.Next() {
		ref := it.At()
		lbls := labels.ScratchBuilder{}
		chks := []chunks.Meta(nil)
		require.NoError(t, r.Series(ref, &lbls, &chks))

		ss := block.SeriesSpec{
			Labels: lbls.Labels(),
			Chunks: chks,
		}
		for i, c := range chks {
			chunk, _, err := chunkReader.ChunkOrIterable(c)
			require.NoError(t, err)
			chks[i].Chunk = chunk
		}
		result = append(result, &ss)
	}

	return result
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func newSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) chunks.Sample {
	return sample{t, v, h, fh}
}
func (s sample) T() int64                      { return s.t }
func (s sample) F() float64                    { return s.v }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	return sample{
		s.t,
		s.v,
		s.h.Copy(),
		s.fh.Copy(),
	}
}
