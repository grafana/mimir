// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestGatherIndexHealthStats(t *testing.T) {
	tmpDir := t.TempDir()

	spec1 := block.SeriesSpec{
		Labels: labels.FromStrings(model.MetricNameLabel, "asdf"),
		Chunks: []chunks.Meta{
			must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: 10, Val: 11},
				test.Sample{TS: 20, Val: 12},
				test.Sample{TS: 30, Val: 13},
			})),
		},
	}
	spec2 := block.SeriesSpec{
		Labels: labels.FromStrings(model.MetricNameLabel, "zxcv", "foo", "bar"),
		Chunks: []chunks.Meta{
			must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: 40, Hist: test.GenerateTestHistogram(1)},
				test.Sample{TS: 50, Hist: test.GenerateTestHistogram(2)},
				test.Sample{TS: 60, Hist: test.GenerateTestHistogram(3)},
			})),
			must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: 70, Hist: test.GenerateTestHistogram(4)},
				test.Sample{TS: 80, Hist: test.GenerateTestHistogram(5)},
				test.Sample{TS: 90, Hist: test.GenerateTestHistogram(6)},
			})),
		},
	}

	meta, err := block.GenerateBlockFromSpec(tmpDir, []*block.SeriesSpec{&spec1, &spec2})
	require.NoError(t, err)

	blockDir := path.Join(tmpDir, meta.ULID.String())
	stats, err := block.GatherBlockHealthStats(context.Background(), log.NewNopLogger(), blockDir, meta.MinTime, meta.MaxTime, true)
	require.NoError(t, err)

	require.Equal(t, int64(2), stats.TotalSeries)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
