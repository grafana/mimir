package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSplitLocalBlock(t *testing.T) {
	dir := t.TempDir()

	startOfDay := time.Now().Truncate(24 * time.Hour)

	specs := []*block.SeriesSpec{
		{
			Labels: labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(10*time.Minute).UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(12*time.Hour).UnixMilli(), 2, nil, nil),
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 3, nil, nil),
					newSample(startOfDay.Add(36*time.Hour).UnixMilli(), 4, nil, nil),
					newSample(startOfDay.Add(48*time.Hour).UnixMilli(), 5, nil, nil),
				})),
			},
		},

		{
			Labels: labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(12*time.Hour).UnixMilli(), 2, nil, nil),
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 3, nil, nil),
					newSample(startOfDay.Add(36*time.Hour).UnixMilli(), 4, nil, nil),
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(48*time.Hour).UnixMilli(), 5, nil, nil),
				})),
			},
		},

		{
			Labels: labels.FromStrings("__name__", "3_series_with_samples_on_second_day"),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					newSample(startOfDay.Add(24*time.Hour).UnixMilli(), 1, nil, nil),
					newSample(startOfDay.Add(25*time.Hour).UnixMilli(), 2, nil, nil),
					newSample(startOfDay.Add(26*time.Hour).UnixMilli(), 3, nil, nil),
				})),
			},
		},
	}

	meta, err := block.GenerateBlockFromSpec(dir, specs)
	require.NoError(t, err)

	blocks, err := splitLocalBlock(context.Background(), dir, filepath.Join(dir, meta.ULID.String()), *meta, 24*time.Hour, log.NewNopLogger())
	require.NoError(t, err)

	// We expect 3 blocks
	require.Len(t, blocks, 3)

	// First block
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[0].String()))

		// series 1 has its entire chunk in the first block
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, startOfDay.Add(10*time.Minute).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has only first chunk in the first block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, startOfDay.UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(12*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 2)
	}

	// Second block has all three series.
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[1].String()))

		// series 1 has its entire chunk in the second block as well.
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, startOfDay.Add(10*time.Minute).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has only second chunk in the first block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, startOfDay.Add(24*time.Hour).UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(36*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// Series 3 has its chunk in this block too
		require.Equal(t, spec[2].Labels, labels.FromStrings("__name__", "3_series_with_samples_on_second_day"))
		require.Len(t, spec[2].Chunks, 1)
		require.Equal(t, startOfDay.Add(24*time.Hour).UnixMilli(), spec[2].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(26*time.Hour).UnixMilli(), spec[2].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 3)
	}

	// Last block, only has series 1 and 2 again.
	{
		spec := listSeriesAndChunksFromBlock(t, filepath.Join(dir, blocks[2].String()))

		// series 1 has its entire chunk in the last block too.
		require.Equal(t, spec[0].Labels, labels.FromStrings("__name__", "1_series_with_one_chunk_with_samples_covering_multiple_days"))
		require.Len(t, spec[0].Chunks, 1)
		require.Equal(t, startOfDay.Add(10*time.Minute).UnixMilli(), spec[0].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[0].Chunks[0].MaxTime)

		// Series 2 has only last chunk in the first block
		require.Equal(t, spec[1].Labels, labels.FromStrings("__name__", "2_series_with_multiple_chunks_not_crossing_24h_boundary"))
		require.Len(t, spec[1].Chunks, 1)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[1].Chunks[0].MinTime)
		require.Equal(t, startOfDay.Add(48*time.Hour).UnixMilli(), spec[1].Chunks[0].MaxTime)

		// No more series.
		require.Len(t, spec, 2)
	}
}

func listSeriesAndChunksFromBlock(t *testing.T, blockDir string) []*block.SeriesSpec {
	allKey, allValue := index.AllPostingsKey()
	r, err := index.NewFileReader(filepath.Join(blockDir, block.IndexFilename))
	require.NoError(t, err)
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")

	it, err := r.Postings(context.Background(), allKey, allValue)
	require.NoError(t, err)

	result := []*block.SeriesSpec(nil)

	for it.Next() {
		lbls := labels.ScratchBuilder{}
		chks := []chunks.Meta(nil)

		ref := it.At()

		require.NoError(t, r.Series(ref, &lbls, &chks))

		ss := block.SeriesSpec{
			Labels: lbls.Labels(),
			Chunks: chks,
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
