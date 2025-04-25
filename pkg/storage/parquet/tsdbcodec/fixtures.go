// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package tsdbcodec

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/dskit/runutil"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

const intervalSeconds = 10
const interval = intervalSeconds * time.Second

const NumIntervals = 10000 + int(time.Minute/interval) + 1 // The longest-range test we run has 10000 steps with a 1m range selector, so make sure we have slightly more data than that.

var DefaultMetricSizes = []int{1, 100, 2000}

const DefaultHistogramBuckets = 5

func GenerateTestLabelSets(labelCardinalities []int, sampleCount int) []labels.Labels {
	if len(labelCardinalities) == 0 {
		return nil
	}

	metricLabels := make([]labels.Labels, 0, sampleCount)

	for _, labelCount := range labelCardinalities {
		aMetricName := "a_" + strconv.Itoa(labelCount)
		bMetricName := "b_" + strconv.Itoa(labelCount)

		if labelCount == 1 {
			// metricLabels with one series only have the __name__ label
			metricLabels = append(metricLabels, labels.FromStrings("__name__", aMetricName))
			metricLabels = append(metricLabels, labels.FromStrings("__name__", bMetricName))
		} else {
			// metricLabels with more than one series have the __name__ label
			// and a one "l" label with `labelCount` different values
			for i := 0; i < labelCount; i++ {
				labelValue := strconv.Itoa(i)
				metricLabels = append(metricLabels, labels.FromStrings("__name__", aMetricName, "l", labelValue))
				metricLabels = append(metricLabels, labels.FromStrings("__name__", bMetricName, "l", labelValue))
			}
		}
	}
	return metricLabels
}

func GenerateTestStorageSeriesFromLabelSets(
	metricLabelSets []labels.Labels, labelCardinalities []int, minT int, sampleCount int,
) []storage.Series {
	maxT := minT + sampleCount

	totalSeries := 0
	for _, labelCount := range labelCardinalities {
		totalSeries += 2 * labelCount // one series each for a_ and b_ metric names
	}

	if len(labelCardinalities) == 0 {
		return nil
	}

	series := make([]storage.Series, totalSeries)
	for i, metricLabelSet := range metricLabelSets {
		samples := make([]chunks.Sample, 0, sampleCount)
		for t := minT; t < maxT; t++ {
			samples = append(samples, util_test.Sample{
				TS:  int64(t) * interval.Milliseconds(),
				Val: float64(t) + float64(i)/float64(totalSeries),
			})
		}
		series[i] = storage.NewListSeries(metricLabelSet, samples)
	}
	return series
}

func equalChunkIters(iter1, iter2 chunkenc.Iterator) bool {
	for {
		nextValType1 := iter1.Next()
		nextValType2 := iter2.Next()

		// if one iterator ends (returns ValNone) before the other, they are not equal
		if nextValType1 != nextValType2 {
			return false
		}

		// nextVals are same type;
		// if both iterators are ValNone and we did not fail previously,
		// then both are done and the series are equal
		if nextValType1 == chunkenc.ValNone {
			return true
		}

		switch nextValType1 {
		case chunkenc.ValFloat:
			t1, v1 := iter1.At()
			t2, v2 := iter2.At()
			if t1 != t2 || v1 != v2 {
				return false
			}
		case chunkenc.ValHistogram:
			t1, h1 := iter1.AtHistogram(nil)
			t2, h2 := iter2.AtHistogram(nil)
			if t1 != t2 || h1 != h2 {
				return false
			}
		case chunkenc.ValFloatHistogram:
			t1, fh1 := iter1.AtFloatHistogram(nil)
			t2, fh2 := iter2.AtFloatHistogram(nil)
			if t1 != t2 || fh1 != fh2 {
				return false
			}
		default:
			// unknown sample type
			return false
		}
	}
}

func CreateTSDBBlock(
	ctx context.Context,
	series []storage.Series,
	dir string,
) (id ulid.ULID, err error) {
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
	var batchSize = len(series) / runtime.GOMAXPROCS(0)
	batchOffset := 0
	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)
	mtx := &sync.Mutex{}
	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		batchOffset += batchSize
		series = series[l:]
		g.Go(func() error {
			lmint := int64(math.MaxInt64)
			lmaxt := int64(math.MinInt64)

			for _, s := range batch {
				app := h.Appender(ctx)
				lbls := s.Labels()
				iter := s.Iterator(nil)
				var err error
			LOOP:
				for {
					switch iter.Next() {
					case chunkenc.ValNone:
						break LOOP
					case chunkenc.ValFloat:
						t, v := iter.At()
						_, err = app.Append(0, lbls, t, v)
					case chunkenc.ValHistogram:
						t, h := iter.AtHistogram(nil)
						_, err = app.AppendHistogram(0, lbls, t, h, nil)
					case chunkenc.ValFloatHistogram:
						t, fh := iter.AtFloatHistogram(nil)
						_, err = app.AppendHistogram(0, lbls, t, nil, fh)
					}
					if err != nil {
						return errors.Wrap(err, "append")
					}
					lmint = min(lmint, iter.AtT())
					lmaxt = max(lmaxt, iter.AtT())
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
			}
			mtx.Lock()
			mint = min(mint, lmint)
			maxt = max(maxt, lmaxt)
			mtx.Unlock()
			return nil
		})

	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, promslog.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	blocks, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if len(blocks) != 1 {
		return id, errors.Errorf("expected one block, got %d", len(blocks))
	}

	id = blocks[0]
	blockDir := filepath.Join(dir, id.String())

	if _, err = block.InjectThanosMeta(log.NewNopLogger(), blockDir, block.ThanosMeta{
		Labels: nil,
		Source: block.TestSource,
		Files:  []block.File{},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
		return id, errors.Wrap(err, "remove tombstones")
	}
	return id, nil
}
