// SPDX-License-Identifier: AGPL-3.0-only

package parquet

import (
	"context"
	"sort"
	"sync"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
)

type queryableShard struct {
	shard       storage.ParquetShard
	m           *search.Materializer
	concurrency int
}

func newQueryableShard(opts *querierOpts, block storage.ParquetShard, d *schema.PrometheusParquetChunksDecoder, rowCountQuota *search.Quota, chunkBytesQuota *search.Quota, dataBytesQuota *search.Quota) (*queryableShard, error) {
	s, err := block.TSDBSchema()
	if err != nil {
		return nil, err
	}
<<<<<<< HEAD
	m, err := search.NewMaterializer(s, d, block, opts.concurrency, rowCountQuota, chunkBytesQuota, dataBytesQuota, opts.materializedSeriesCallback, opts.materializedLabelsFilterCallback)
=======
	m, err := search.NewMaterializer(s, d, block, opts.concurrency, rowCountQuota, chunkBytesQuota, dataBytesQuota, opts.materializedSeriesCallback)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	if err != nil {
		return nil, err
	}

	return &queryableShard{
		shard:       block,
		m:           m,
		concurrency: opts.concurrency,
	}, nil
}

func (b queryableShard) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.ChunkSeriesSet, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([]prom_storage.ChunkSeries, 0, 1024)
	rMtx := sync.Mutex{}

	for rgi := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
<<<<<<< HEAD
			cs, err := search.MatchersToConstraints(matchers...)
=======
			cs, err := search.MatchersToConstraint(matchers...)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}

			if len(rr) == 0 {
				return nil
			}

<<<<<<< HEAD
			series, err := b.m.Materialize(ctx, nil, rgi, mint, maxt, skipChunks, rr)
=======
			series, err := b.m.Materialize(ctx, rgi, mint, maxt, skipChunks, rr)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
			if err != nil {
				return err
			}
			rMtx.Lock()
			results = append(results, series...)
			rMtx.Unlock()
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	if sorted {
		sort.Sort(byLabels(results))
	}
	return convert.NewChunksSeriesSet(results), nil
}

func (b queryableShard) LabelNames(ctx context.Context, limit int64, matchers []*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return b.m.MaterializeAllLabelNames(), nil
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for rgi := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
<<<<<<< HEAD
			cs, err := search.MatchersToConstraints(matchers...)
=======
			cs, err := search.MatchersToConstraint(matchers...)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}
			series, err := b.m.MaterializeLabelNames(ctx, rgi, rr)
			if err != nil {
				return err
			}
			results[rgi] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

func (b queryableShard) LabelValues(ctx context.Context, name string, limit int64, matchers []*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return b.allLabelValues(ctx, name, limit)
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for rgi := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
<<<<<<< HEAD
			cs, err := search.MatchersToConstraints(matchers...)
=======
			cs, err := search.MatchersToConstraint(matchers...)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(ctx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}
			series, err := b.m.MaterializeLabelValues(ctx, name, rgi, rr)
			if err != nil {
				return err
			}
			results[rgi] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

func (b queryableShard) allLabelValues(ctx context.Context, name string, limit int64) ([]string, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))

	for i := range b.shard.LabelsFile().RowGroups() {
		errGroup.Go(func() error {
			series, err := b.m.MaterializeAllLabelValues(ctx, name, i)
			if err != nil {
				return err
			}
			results[i] = series
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return util.MergeUnsortedSlices(int(limit), results...), nil
}

type byLabels []prom_storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }
