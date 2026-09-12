// SPDX-License-Identifier: AGPL-3.0-only

// This file exposes package-internal types for the sg-index-lookup-demo tool.
// It is compiled only with the sg_demo build tag and must not be included in
// production or test builds.

package storegateway

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimirtsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// DemoBucketBlock is a thin wrapper around bucketBlock that exposes
// ExpandedPostings for the demo tool.
type DemoBucketBlock struct {
	bb *bucketBlock
}

// NewBucketBlockForDemo constructs a minimal bucketBlock suitable for the demo:
// no chunk objects, no index cache, noop partitioners.
func NewBucketBlockForDemo(
	meta *block.Meta,
	bkt objstore.InstrumentedBucketReader,
	headerReader indexheader.Reader,
	planner index.LookupPlanner,
	logger log.Logger,
) *DemoBucketBlock {
	bb := &bucketBlock{
		userID:            "demo",
		logger:            logger,
		metrics:           NewBucketStoreMetrics(nil),
		bkt:               bkt,
		indexCache:        noopCache{},
		meta:              meta,
		indexHeaderReader: headerReader,
		lookupPlanner:     planner,
		partitioners:      newGapBasedPartitioners(mimirtsdb.DefaultPartitionerMaxGapSize, mimirtsdb.DefaultPartitionerMaxGapSize, mimirtsdb.DefaultPartitionerMaxGapSize, nil),
	}
	return &DemoBucketBlock{bb: bb}
}

// NoopLookupPlanner returns the noopLookupPlanner used by bucketBlock by default.
func NoopLookupPlanner() index.LookupPlanner {
	return noopLookupPlanner{}
}

// BuildCostBasedPlanner reads the full block index from blockDir, generates
// count-min sketch statistics in memory, and returns a CostBasedPlanner.
// Intended only for the demo tool — stat generation is synchronous and slow
// for large blocks.
func BuildCostBasedPlanner(blockDir string, meta *block.Meta, logger log.Logger) (index.LookupPlanner, error) {
	idxPath := blockDir + "/" + block.IndexFilename
	idxReader, err := index.NewFileReader(idxPath, index.DecodePostingsRaw)
	if err != nil {
		return nil, err
	}
	defer idxReader.Close()

	gen := lookupplan.NewStatisticsGenerator(logger)
	cfg := lookupplan.CostConfig{
		RetrievedPostingCost:              lookupplan.DefaultRetrievedPostingCost,
		RetrievedSeriesCost:               lookupplan.DefaultRetrievedSeriesCost,
		RetrievedPostingListCost:          lookupplan.DefaultRetrievedPostingListCost,
		MinSeriesPerBlockForQueryPlanning: 0, // no minimum — block may be small
		LabelCardinalityForLargerSketch:   lookupplan.DefaultLabelCardinalityForLargerSketch,
		LabelCardinalityForSmallerSketch:  lookupplan.DefaultLabelCardinalityForSmallerSketch,
	}
	stats, err := gen.Stats(meta.BlockMeta, &indexReaderAdapter{idxReader}, cfg.LabelCardinalityForSmallerSketch, cfg.LabelCardinalityForLargerSketch)
	if err != nil {
		return nil, err
	}

	metrics := lookupplan.NewMetrics(nil).ForUser("demo")
	return lookupplan.NewCostBasedPlanner(metrics, stats, cfg), nil
}

// indexReaderAdapter wraps index.Reader to satisfy the tsdb.IndexReader interface.
// Only the methods used by StatisticsGenerator.Stats are implemented.
type indexReaderAdapter struct {
	r *index.Reader
}

func (a *indexReaderAdapter) Symbols() index.StringIter { return a.r.Symbols() }
func (a *indexReaderAdapter) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	return a.r.LabelValues(ctx, name, nil, matchers...)
}
func (a *indexReaderAdapter) SortedLabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	return a.r.SortedLabelValues(ctx, name, nil, matchers...)
}
func (a *indexReaderAdapter) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	return a.r.Postings(ctx, name, values...)
}
func (a *indexReaderAdapter) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) index.Postings {
	return a.r.PostingsForLabelMatching(ctx, name, match)
}
func (a *indexReaderAdapter) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	return a.r.PostingsForAllLabelValues(ctx, name)
}
func (a *indexReaderAdapter) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error) {
	return a.r.LabelNames(ctx, matchers...)
}
func (a *indexReaderAdapter) LabelNamesFor(ctx context.Context, postings index.Postings) ([]string, error) {
	return a.r.LabelNamesFor(ctx, postings)
}
func (a *indexReaderAdapter) IndexLookupPlanner() index.LookupPlanner {
	return a.r.IndexLookupPlanner()
}
func (a *indexReaderAdapter) Close() error { return a.r.Close() }

func (a *indexReaderAdapter) PostingsForMatchers(_ context.Context, _ bool, _ ...*labels.Matcher) (index.Postings, error) {
	return nil, fmt.Errorf("not implemented")
}
func (a *indexReaderAdapter) SortedPostings(p index.Postings) index.Postings { return p }
func (a *indexReaderAdapter) ShardedPostings(p index.Postings, _, _ uint64) index.Postings {
	return p
}
func (a *indexReaderAdapter) Series(_ storage.SeriesRef, _ *labels.ScratchBuilder, _ *[]chunks.Meta) error {
	return fmt.Errorf("not implemented")
}

var _ tsdb.IndexReader = (*indexReaderAdapter)(nil)

// ExpandedPostings resolves matchers to a posting list using the block's index,
// returning the refs and any matchers the planner deferred to scan phase.
func (d *DemoBucketBlock) ExpandedPostings(ctx context.Context, ms []*labels.Matcher) ([]storage.SeriesRef, []*labels.Matcher, error) {
	// Use worstCaseFetchedDataStrategy with the default factor, matching production behaviour.
	strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}
	ir := d.bb.indexReader(strategy)
	defer ir.Close()

	stats := newSafeQueryStats()
	refs, pending, err := ir.ExpandedPostings(ctx, ms, stats)
	return refs, pending, err
}
