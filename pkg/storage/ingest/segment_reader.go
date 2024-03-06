// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

// SegmentReader is an high-level client used to read a partition segments.
type SegmentReader struct {
	services.Service

	partitionID  int32
	lastOffsetID int64
	hedgeDelay   time.Duration
	readTimeout  time.Duration
	logger       log.Logger
	storage      *SegmentStorage
	metadata     *MetadataStore

	// segmentsBuffer holds a buffer of fetched segments, ready to be returned by WaitNextSegment().
	segmentsBuffer chan *Segment

	backoffConfig backoff.Config // Storing backoff config in the field allows us to override it in the test.
}

func NewSegmentReader(bucket objstore.InstrumentedBucket, metadata *MetadataStore, partitionID int32, lastOffsetID int64, bufferSize int, hedgeDelay, readTimeout time.Duration, reg prometheus.Registerer, logger log.Logger) *SegmentReader {
	c := &SegmentReader{
		partitionID:    partitionID,
		lastOffsetID:   lastOffsetID,
		hedgeDelay:     hedgeDelay,
		readTimeout:    readTimeout,
		logger:         logger,
		storage:        NewSegmentStorage(bucket, metadata, reg),
		metadata:       metadata,
		segmentsBuffer: make(chan *Segment, bufferSize),
	}

	c.Service = services.NewBasicService(nil, c.running, nil)
	c.backoffConfig = backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	}
	return c
}

const maxInMemorySegments = 128

func (c *SegmentReader) running(ctx context.Context) error {
	try := backoff.New(ctx, c.backoffConfig)

	var segments []*Segment
	for try.Ongoing() {
		refs := c.metadata.WatchSegments(ctx, c.partitionID, c.lastOffsetID)

		if len(refs) > maxInMemorySegments {
			refs = refs[:maxInMemorySegments]
		}

		var err error
		segments, err = c.fetchSegments(ctx, refs, segments)
		if err != nil {
			try.Wait()
			continue
		}
		try.Reset()

		for ix, segment := range segments {
			// Add the fetched segment to the buffer.
			select {
			case c.segmentsBuffer <- segment:
				// The segment has been successfully loaded, so we can update the last offset ID.
				c.lastOffsetID = segment.Ref.OffsetID
				// we don't need to keep reference to the segment anymore.
				segments[ix] = nil

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

const maxSegmentsFetchConcurrency = 16

// Fetches segments in parallel. Returns segments in the same order as refs.
// SegmentBuf may have some segments already from previous fetch. If refs are correct, such segments are not reused.
// If no error is returned, all segments were fetched.
func (c *SegmentReader) fetchSegments(ctx context.Context, segmentRefs []SegmentRef, segmentsBuf []*Segment) ([]*Segment, error) {
	if cap(segmentsBuf) < len(segmentRefs) {
		segmentsBuf = make([]*Segment, len(segmentRefs))
	}
	// We use cap, not len, because we want to clear the rest of the slice (after fetched segments) later.
	segmentsBuf = segmentsBuf[:cap(segmentsBuf)]

	err := concurrency.ForEachJob(ctx, len(segmentRefs), maxSegmentsFetchConcurrency, func(ctx context.Context, idx int) error {
		ref := segmentRefs[idx]
		if segmentsBuf[idx] != nil && segmentsBuf[idx].Ref == ref {
			// We already have the segment from previous fetch. No need to fetch it again.
			return nil
		}

		segment, err := c.storage.FetchSegmentWithRetries(ctx, ref, c.hedgeDelay, c.readTimeout)
		if err != nil {
			level.Warn(c.logger).Log("msg", "segment reader failed to fetch segment", "segment_ref", ref.String(), "err", err)
			return err
		}

		level.Debug(c.logger).Log("msg", "segment reader fetched segment", "segment_ref", ref.String())
		segmentsBuf[idx] = segment
		return nil
	})

	// Clear remaining segments in the buffer.
	for idx := len(segmentRefs); idx < cap(segmentsBuf); idx++ {
		segmentsBuf[idx] = nil
	}

	// Only return fetched segments. In case of error, caller will pass the same buffer, and we may find that some segments are already fetched.
	return segmentsBuf[:len(segmentRefs)], err
}

// WaitNextSegment blocks until the next segment has been fetched by the reader,
// or context is canceled.
func (c *SegmentReader) WaitNextSegment(ctx context.Context) (*Segment, error) {
	select {
	case segment := <-c.segmentsBuffer:
		return segment, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
