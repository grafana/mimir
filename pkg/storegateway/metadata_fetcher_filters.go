// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/metadata_fetcher_filters.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

type MetadataFilterWithBucketIndex interface {
	// FilterWithBucketIndex is like Thanos MetadataFilter.Filter() but it provides in input the bucket index too.
	FilterWithBucketIndex(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, idx *bucketindex.Index, synced block.GaugeVec) error
}

// IgnoreDeletionMarkFilter is like the Thanos IgnoreDeletionMarkFilter, but it also implements
// the MetadataFilterWithBucketIndex interface.
type IgnoreDeletionMarkFilter struct {
	upstream *block.IgnoreDeletionMarkFilter

	delay           time.Duration
	deletionMarkMap map[ulid.ULID]*metadata.DeletionMark
}

// NewIgnoreDeletionMarkFilter creates IgnoreDeletionMarkFilter.
func NewIgnoreDeletionMarkFilter(logger log.Logger, bkt objstore.InstrumentedBucketReader, delay time.Duration, concurrency int) *IgnoreDeletionMarkFilter {
	return &IgnoreDeletionMarkFilter{
		upstream: block.NewIgnoreDeletionMarkFilter(logger, bkt, delay, concurrency),
		delay:    delay,
	}
}

// DeletionMarkBlocks returns blocks that were marked for deletion.
func (f *IgnoreDeletionMarkFilter) DeletionMarkBlocks() map[ulid.ULID]*metadata.DeletionMark {
	// If the cached deletion marks exist it means the filter function was called with the bucket
	// index, so it's safe to return it.
	if f.deletionMarkMap != nil {
		return f.deletionMarkMap
	}

	return f.upstream.DeletionMarkBlocks()
}

// Filter implements block.MetadataFilter.
func (f *IgnoreDeletionMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	return f.upstream.Filter(ctx, metas, synced, modified)
}

// FilterWithBucketIndex implements MetadataFilterWithBucketIndex.
func (f *IgnoreDeletionMarkFilter) FilterWithBucketIndex(_ context.Context, metas map[ulid.ULID]*metadata.Meta, idx *bucketindex.Index, synced block.GaugeVec) error {
	// Build a map of block deletion marks
	marks := make(map[ulid.ULID]*metadata.DeletionMark, len(idx.BlockDeletionMarks))
	for _, mark := range idx.BlockDeletionMarks {
		marks[mark.ID] = mark.ThanosDeletionMark()
	}

	// Keep it cached.
	f.deletionMarkMap = marks

	for _, mark := range marks {
		if _, ok := metas[mark.ID]; !ok {
			continue
		}

		if time.Since(time.Unix(mark.DeletionTime, 0)).Seconds() > f.delay.Seconds() {
			synced.WithLabelValues(block.MarkedForDeletionMeta).Inc()
			delete(metas, mark.ID)
		}
	}

	return nil
}

const minTimeExcludedMeta = "min-time-excluded"

// minTimeMetaFilter filters out blocks that contain the most recent data (based on block MinTime).
type minTimeMetaFilter struct {
	limit time.Duration
}

func newMinTimeMetaFilter(limit time.Duration) *minTimeMetaFilter {
	return &minTimeMetaFilter{limit: limit}
}

func (f *minTimeMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	if f.limit <= 0 {
		return nil
	}

	limitTime := timestamp.FromTime(time.Now().Add(-f.limit))

	for id, m := range metas {
		if m.MinTime < limitTime {
			continue
		}

		synced.WithLabelValues(minTimeExcludedMeta).Inc()
		delete(metas, id)
	}
	return nil
}
