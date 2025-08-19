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
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
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

			b.shard.TSDBSchema()

			labelsSlices, err := b.m.MaterializeAllLabels(ctx, rgi, rr)
			if err != nil {
				return errors.Wrapf(err, "error materializing labels")
			}

			labels, rr := b.m.FilterSeriesLabels(ctx, sp, labelsSlices, rr)
			
			if skipChunks {
				// For skipChunks, just add series with labels (no need to group)
				noChunksSeriesSet := search.NewNoChunksConcreteLabelsSeriesSet(labels)
				defer func() { _ = noChunksSeriesSet.Close() }()
				for noChunksSeriesSet.Next() {
					results[rgi] = append(results[rgi], noChunksSeriesSet.At())
				}
				if err := noChunksSeriesSet.Err(); err != nil {
					return err
				}
			} else {
				// Group by sorting columns for chunk materialization
				labelsGroups, rrGroups, err := groupBySortingColumns(b.shard.LabelsFile().RowGroups()[rgi].SortingColumns(), labels, rr)
				if err != nil {
					return errors.Wrapf(err, "error grouping by sorting columns")
				}

				// Create ChunkSeriesSet for each group
				var groupSeriesSets []prom_storage.ChunkSeriesSet
				for groupIdx, groupRR := range rrGroups {
					if len(groupRR) == 0 {
						continue
					}

					groupLabels := labelsGroups[groupIdx]
					
					// Materialize chunks for this group
					chunksIter, err := b.m.MaterializeChunks(ctx, rgi, mint, maxt, groupRR)
					if err != nil {
						return errors.Wrapf(err, "error materializing chunks for group %d", groupIdx)
					}

					// Create simple series set that pairs chunks with labels without filtering
					groupSeriesSet := &simpleChunkSeriesSet{
						labels:     groupLabels,
						chunksIter: chunksIter,
					}
					groupSeriesSets = append(groupSeriesSets, groupSeriesSet)
				}

				// Merge all group iterators on-the-fly using NewMergeChunkSeriesSet
				if len(groupSeriesSets) > 0 {
					// Close all individual group series sets when done
					defer func() {
						for _, groupSet := range groupSeriesSets {
							if closer, ok := groupSet.(interface{ Close() error }); ok {
								_ = closer.Close()
							}
						}
					}()
					
					mergedSeriesSet := prom_storage.NewMergeChunkSeriesSet(groupSeriesSets, 0, prom_storage.NewConcatenatingChunkSeriesMerger())
					for mergedSeriesSet.Next() {
						results[rgi] = append(results[rgi], mergedSeriesSet.At())
					}
					if err := mergedSeriesSet.Err(); err != nil {
						return err
					}
				}
			}

			if sorted {
				sort.Sort(byLabels(results[rgi]))
			}
			return nil
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

// simpleChunkSeriesSet is a ChunkSeriesSet that pairs chunks with labels without filtering empty chunks
type simpleChunkSeriesSet struct {
	labels      []labels.Labels
	chunksIter  search.ChunksIteratorIterator
	labelIdx    int
	currentSeries prom_storage.ChunkSeries
	err         error
}

func (s *simpleChunkSeriesSet) Next() bool {
	if !s.chunksIter.Next() {
		s.err = s.chunksIter.Err()
		return false
	}
	
	if s.labelIdx >= len(s.labels) {
		s.err = errors.Newf("chunk/label mismatch: got chunk but no corresponding label (labelIdx=%d, labels=%d)", s.labelIdx, len(s.labels))
		return false
	}
	
	lbls := s.labels[s.labelIdx]
	chunkIter := s.chunksIter.At()
	
	s.currentSeries = &simpleChunkSeries{
		labels:      lbls,
		chunkIter:   chunkIter,
	}
	s.labelIdx++
	return true
}

func (s *simpleChunkSeriesSet) At() prom_storage.ChunkSeries {
	return s.currentSeries
}

func (s *simpleChunkSeriesSet) Err() error {
	return s.err
}

func (s *simpleChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *simpleChunkSeriesSet) Close() error {
	return s.chunksIter.Close()
}

// simpleChunkSeries is a simple ChunkSeries that pairs labels with a chunk iterator
type simpleChunkSeries struct {
	labels    labels.Labels
	chunkIter chunks.Iterator
}

func (s *simpleChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *simpleChunkSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return s.chunkIter
}

func (s *simpleChunkSeries) ChunkCount() (int, error) {
	// We don't have a way to count chunks without consuming the iterator,
	// so return -1 to indicate unknown count
	return -1, nil
}

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
