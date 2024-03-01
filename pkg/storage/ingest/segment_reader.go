// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"
)

// SegmentReader is an high-level client used to read a partition segments.
type SegmentReader struct {
	services.Service

	partitionID  int32
	lastOffsetID int64
	logger       log.Logger
	storage      *SegmentStorage
	metadata     *MetadataStore

	// segmentsBuffer holds a buffer of fetched segments, ready to be returned by WaitNextSegment().
	segmentsBuffer chan *Segment
}

func NewSegmentReader(bucket objstore.Bucket, metadata *MetadataStore, partitionID int32, lastOffsetID int64, bufferSize int, logger log.Logger) *SegmentReader {
	c := &SegmentReader{
		partitionID:    partitionID,
		lastOffsetID:   lastOffsetID,
		logger:         logger,
		storage:        NewSegmentStorage(bucket, metadata),
		metadata:       metadata,
		segmentsBuffer: make(chan *Segment, bufferSize),
	}

	c.Service = services.NewBasicService(nil, c.running, nil)
	return c
}

func (c *SegmentReader) running(ctx context.Context) error {
	try := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})

	for try.Ongoing() {
		refs := c.metadata.WatchSegments(ctx, c.partitionID, c.lastOffsetID)

		for _, ref := range refs {
			segment, err := c.storage.FetchSegmentWithRetries(ctx, ref)
			if err != nil {
				level.Warn(c.logger).Log("msg", "segment reader failed to fetch segment", "segment_ref", ref.String())

				try.Wait()
				break
			}

			// No error occurred, so we can reset the backoff.
			try.Reset()

			// Add the fetched segment to the buffer.
			select {
			case c.segmentsBuffer <- segment:
				// The segment has been successfully loaded, so we can update the last offset ID.
				c.lastOffsetID = segment.Ref.OffsetID

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
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
