// SPDX-License-Identifier: AGPL-3.0-only

// This file exposes package-internal types for the sg-index-lookup-demo tool.
// It is compiled only with the sg_demo build tag and must not be included in
// production or test builds.

package storegateway

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

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
