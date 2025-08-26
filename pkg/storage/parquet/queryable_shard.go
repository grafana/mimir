// SPDX-License-Identifier: AGPL-3.0-only

package parquet

import (
	"context"
	"sort"
	"strings"

	"github.com/efficientgo/core/errors"
	"github.com/parquet-go/parquet-go"
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
	m, err := search.NewMaterializer(s, d, block, opts.concurrency, rowCountQuota, chunkBytesQuota, dataBytesQuota, opts.materializedSeriesCallback, opts.materializedLabelsFilterCallback)
	if err != nil {
		return nil, err
	}

	return &queryableShard{
		shard:       block,
		m:           m,
		concurrency: opts.concurrency,
	}, nil
}

func (b queryableShard) Query(ctx context.Context, sorted bool, sp *prom_storage.SelectHints, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.ChunkSeriesSet, error) {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	rowGroupCount := len(b.shard.LabelsFile().RowGroups())
	results := make([][]prom_storage.ChunkSeries, rowGroupCount)
	for i := range results {
		results[i] = make([]prom_storage.ChunkSeries, 0, 1024/rowGroupCount)
	}

	for rgi := range rowGroupCount {
		errGroup.Go(func() error {
			cs, err := search.MatchersToConstraints(matchers...)
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

			seriesSetIter, err := b.m.Materialize(ctx, sp, rgi, mint, maxt, skipChunks, rr)
			if err != nil {
				return err
			}
			defer func() { _ = seriesSetIter.Close() }()
			for seriesSetIter.Next() {
				results[rgi] = append(results[rgi], seriesSetIter.At())
			}
			if sorted {
				sort.Sort(byLabels(results[rgi]))
			}
			return seriesSetIter.Err()
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	totalResults := 0
	for _, res := range results {
		totalResults += len(res)
	}

	resultsFlattened := make([]prom_storage.ChunkSeries, 0, totalResults)
	for _, res := range results {
		resultsFlattened = append(resultsFlattened, res...)
	}
	if sorted {
		sort.Sort(byLabels(resultsFlattened))
	}

	return convert.NewChunksSeriesSet(resultsFlattened), nil
}

// QueryIter queries the Parquet shard for the given time range and matchers.
// seriesWithoutChunks iterates only labels, while seriesWithChunks includes chunk data.
// Their underlying structures are independent of each other.
// Although seriesWithoutChunks only has labels, we use ChunkSeriesSet to play nicely
// with other helpers. Arguably hacky.
func (b queryableShard) QueryIter(ctx context.Context, sorted bool, sp *prom_storage.SelectHints, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (
	seriesWithoutChunks prom_storage.ChunkSeriesSet,
	seriesWithChunks prom_storage.ChunkSeriesSet,
	err error,
) {
	errGroup, groupCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(b.concurrency)

	rowGroupCount := len(b.shard.LabelsFile().RowGroups())
	labelsSets := make([]prom_storage.ChunkSeriesSet, rowGroupCount)
	var seriesSets []prom_storage.ChunkSeriesSet
	if !skipChunks {
		seriesSets = make([]prom_storage.ChunkSeriesSet, rowGroupCount)
	}

	for rgi := range rowGroupCount {
		errGroup.Go(func() error {
			cs, err := search.MatchersToConstraints(matchers...)
			if err != nil {
				return err
			}
			err = search.Initialize(b.shard.LabelsFile(), cs...)
			if err != nil {
				return err
			}
			rr, err := search.Filter(groupCtx, b.shard, rgi, cs...)
			if err != nil {
				return err
			}

			if len(rr) == 0 {
				return nil
			}

			labelsSlices, err := b.m.MaterializeAllLabels(groupCtx, rgi, rr)
			if err != nil {
				return errors.Wrapf(err, "error materializing labels")
			}

			labels, rr := b.m.FilterSeriesLabels(groupCtx, sp, labelsSlices, rr)

			labelsSets[rgi] = search.NewNoChunksConcreteLabelsSeriesSet(labels)
			if skipChunks {
				// No need to query for chunks
				return nil
			}

			// MaterializeChunks is designed to materialize rows from top to bottom of the Parquet file. That
			// is not necessarily ordered by labels set, as rows are ordered by a particular subset of labels.
			// If we got them in that order, we'd have to load them all inmemory to then sort (because we need them sorted).
			// Instead we leverage the fact that rows are sorted by label set _after_ sorted by that particular
			// subset of columns (known as the 'sorting columns'). That means that if we were to materialize a set of rows
			// with equal sorting column values, they would be sorted by label set.
			// So the trick is: group our label sets in groups such that all series within a group have the same values
			// for the sorting columns. We can then materialize each of the groups separately and then merge them sorted
			// on the fly as we iterate, given each of them is sorted.
			labelsGroups, rrGroups, err := groupBySortingColumns(b.shard.LabelsFile().RowGroups()[rgi].SortingColumns(), labels, rr)
			if err != nil {
				return errors.Wrapf(err, "error grouping by sorting columns")
			}

			var groupSeriesSets []prom_storage.ChunkSeriesSet
			// TODO: this could be parallelizable, though the creation of iterators should not be too expensive nor i/o heavy
			for groupIdx, groupRR := range rrGroups {
				if len(groupRR) == 0 {
					continue
				}
				// TODO: closeable
				chunksIter, err := b.m.MaterializeChunks(ctx, rgi, mint, maxt, groupRR)
				if err != nil {
					return errors.Wrapf(err, "error materializing chunks for group %d", groupIdx)
				}

				// TODO: we are returning the chunks iterator without knowing if it has any values at all.
				// And, more importantly, labels for the series. In Prometheus world that violates the querier's
				// interface. For us, whether or not that is problematic depends on how Queriers handle responses
				// from Store Gateways that have labels for which there are no series. We'll see...
				groupSeriesSet := newConcreteLabelsChunkSeriesSet(labelsGroups[groupIdx], chunksIter)
				groupSeriesSets = append(groupSeriesSets, groupSeriesSet)
			}

			seriesSets[rgi] = prom_storage.NewMergeChunkSeriesSet(groupSeriesSets, 0, prom_storage.NewConcatenatingChunkSeriesMerger())
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, nil, err
	}

	return convert.NewMergeChunkSeriesSet(labelsSets, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger()),
		convert.NewMergeChunkSeriesSet(seriesSets, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger()), nil
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
			cs, err := search.MatchersToConstraints(matchers...)
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
			cs, err := search.MatchersToConstraints(matchers...)
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

// groupBySortingColumns groups the given labels and rows into groups where
// each group has the same values for the specified sorting columns. This is useful because
// Parquet series (rows) are sorted by label set for a fixed tuple of sorted columns. Consequently,
// each of the returned groups are guaranteed to be ordered by label set within the Parquet file.
func groupBySortingColumns(sortingColumns []parquet.SortingColumn, lbls []labels.Labels, rr []search.RowRange) ([][]labels.Labels, [][]search.RowRange, error) {
	sortingLabels := make([]string, 0, len(sortingColumns))
	for _, col := range sortingColumns {

		path := col.Path()
		column := path[len(path)-1]

		label, ok := schema.ExtractLabelFromColumn(column)
		if !ok {
			return nil, nil, errors.Newf("cannot extract label from sorting column %q", column)
		}
		sortingLabels = append(sortingLabels, label)
	}

	if len(sortingLabels) == 0 || len(lbls) == 0 {
		if len(lbls) == 0 {
			return nil, nil, nil
		}
		return [][]labels.Labels{lbls}, [][]search.RowRange{rr}, nil
	}

	groupMap := make(map[string]int)
	var labelsGroups [][]labels.Labels
	var rrGroups [][]search.RowRange
	keyValues := make([]string, len(sortingLabels)) // Reuse this slice to avoid allocations

	labelIdx := 0
	for _, rowRange := range rr {
		currentRow := rowRange.From
		rangeStart := currentRow
		var currentGroupKey string
		var currentGroupIndex int

		for i := int64(0); i < rowRange.Count; i++ {
			if labelIdx >= len(lbls) {
				return nil, nil, errors.Newf("label index %d exceeds labels length %d", labelIdx, len(lbls))
			}

			lbl := lbls[labelIdx]
			// Build group key
			for j, sortingLabel := range sortingLabels {
				keyValues[j] = lbl.Get(sortingLabel)
			}
			key := strings.Join(keyValues, "\x00")

			groupIndex, exists := groupMap[key]
			if !exists {
				// New group
				groupIndex = len(labelsGroups)
				groupMap[key] = groupIndex
				labelsGroups = append(labelsGroups, make([]labels.Labels, 0))
				rrGroups = append(rrGroups, make([]search.RowRange, 0))
			}

			if i == 0 {
				// First row in range - initialize group
				currentGroupKey = key
				currentGroupIndex = groupIndex
			} else if key != currentGroupKey {
				// Group change - finish current range and start new one
				rrGroups[currentGroupIndex] = append(rrGroups[currentGroupIndex], search.RowRange{
					From:  rangeStart,
					Count: currentRow - rangeStart,
				})

				// Start new group
				currentGroupKey = key
				currentGroupIndex = groupIndex
				rangeStart = currentRow
			}

			// Add label to current group
			labelsGroups[currentGroupIndex] = append(labelsGroups[currentGroupIndex], lbl)
			labelIdx++
			currentRow++
		}

		// Finish the last range in this RowRange
		rrGroups[currentGroupIndex] = append(rrGroups[currentGroupIndex], search.RowRange{
			From:  rangeStart,
			Count: currentRow - rangeStart,
		})
	}

	return labelsGroups, rrGroups, nil
}
