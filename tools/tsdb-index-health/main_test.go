// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"path"
	"testing"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestGatherIndexHealthStats(t *testing.T) {
	userID := "user"
	tmpDir := t.TempDir()

	spec1 := testutil.BlockSeriesSpec{
		Labels: labels.FromStrings(labels.MetricName, "asdf"),
		Chunks: []chunks.Meta{
			tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
				sample{10, 11, nil, nil},
				sample{20, 12, nil, nil},
				sample{30, 13, nil, nil},
			}),
		},
	}
	spec2 := testutil.BlockSeriesSpec{
		Labels: labels.FromStrings(labels.MetricName, "zxcv", "foo", "bar"),
		Chunks: []chunks.Meta{
			tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
				sample{40, 0, test.GenerateTestHistogram(1), nil},
				sample{50, 0, test.GenerateTestHistogram(2), nil},
				sample{60, 0, test.GenerateTestHistogram(3), nil},
			}),
			tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
				sample{70, 0, test.GenerateTestHistogram(4), nil},
				sample{80, 0, test.GenerateTestHistogram(5), nil},
				sample{90, 0, test.GenerateTestHistogram(6), nil},
			}),
		},
	}

	meta, err := testutil.GenerateBlockFromSpec(userID, tmpDir, []*testutil.BlockSeriesSpec{&spec1, &spec2})
	require.NoError(t, err)

	blockDir := path.Join(tmpDir, meta.ULID.String())
	stats, err := block.GatherBlockHealthStats(log.NewNopLogger(), blockDir, meta.MinTime, meta.MaxTime, true)
	require.NoError(t, err)

	require.Equal(t, int64(2), stats.TotalSeries)
	require.Equal(t, int64(1), stats.SeriesMinChunks)
	require.Equal(t, int64(1), stats.SeriesAvgChunks)
	require.Equal(t, int64(2), stats.SeriesMaxChunks)
	require.Equal(t, int64(3), stats.TotalChunks)
	require.Equal(t, int64(2), stats.LabelNamesCount)
	require.Equal(t, int64(2), stats.MetricLabelValuesCount)
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s sample) T() int64                      { return s.t }
func (s sample) V() float64                    { return s.v }
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
