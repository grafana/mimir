// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"iter"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMergeSourcesByTimestamp(t *testing.T) {
	// Asserts that records from multiple sources are interleaved into a single ascending-timestamp stream.
	t.Run("orders across sources", func(t *testing.T) {
		s0 := closedSource(0, rec(0, 10), rec(1, 30))
		s1 := closedSource(1, rec(0, 20), rec(1, 40))

		require.Equal(t, []int64{10, 20, 30, 40}, collectMerge(t, s0, s1))
	})

	// Asserts that the merge blocks on a slow source rather than emitting a faster source's later
	// records ahead of the slow source's earlier ones.
	t.Run("waits for a lagging source", func(t *testing.T) {
		// fast has both records available immediately (buffered + closed); slow feeds over an
		// unbuffered channel, so each send blocks until the merge pulls it. fast's late record
		// (t=40) is available the whole time, so if the merge failed to wait for slow it would
		// emit 40 ahead of slow's 20 and 30.
		fast := closedSource(0, rec(0, 10), rec(1, 40))
		slow := &kafkaClusterSource{clusterID: 1, records: make(chan *kgo.Record)}

		var got []int64
		done := make(chan error, 1)
		go func() {
			done <- mergeSourcesByTimestamp(context.Background(), []*kafkaClusterSource{fast, slow}, func(r *kgo.Record) error {
				got = append(got, r.Timestamp.UnixMilli())
				return nil
			})
		}()

		slow.records <- rec(0, 20)
		slow.records <- rec(1, 30)
		close(slow.records)

		require.NoError(t, <-done)
		require.Equal(t, []int64{10, 20, 30, 40}, got)
	})

	// Asserts that records from one source are emitted in their offset order even when their
	// timestamps are non-monotonic (the merge does not sort within a source).
	t.Run("preserves offset order within a source", func(t *testing.T) {
		// Kafka orders a partition by offset, not timestamp, so a single source can deliver
		// non-monotonic timestamps. The merge must consume every record and keep each source's
		// offset order; it does not (and cannot) globally sort disorder internal to a source —
		// that residual jitter is left to the TSDB out-of-order window. Here s0 delivers 50, 30,
		// 40 in offset order.
		s0 := closedSource(0, rec(0, 50), rec(1, 30), rec(2, 40))
		s1 := closedSource(1, rec(0, 20))

		// 20 (s1) sorts first; then s0's records pass through in their offset order.
		require.Equal(t, []int64{20, 50, 30, 40}, collectMerge(t, s0, s1))
	})

	// Asserts that records sharing a timestamp are emitted in cluster ID order, regardless of the
	// order the sources are passed in. (Ordering among records from a single source is covered by
	// the "preserves offset order within a source" case.)
	t.Run("tie-breaks equal timestamps by cluster ID", func(t *testing.T) {
		// Both records share a timestamp and s1 is passed before s0, but cluster 0 must emit first.
		s0 := closedSource(0, valuedRec(0, 50, "a"))
		s1 := closedSource(1, valuedRec(0, 50, "b"))

		var got []string
		err := mergeSourcesByTimestamp(context.Background(), []*kafkaClusterSource{s1, s0}, func(r *kgo.Record) error {
			got = append(got, string(r.Value))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, got)
	})

	// Asserts that a source yielding no records doesn't stall the merge or corrupt the output.
	t.Run("skips an empty source", func(t *testing.T) {
		require.Equal(t, []int64{10}, collectMerge(t, closedSource(0), closedSource(1, rec(0, 10))))
	})

	// Asserts that a source's terminal error surfaces from the merge, after its already-available
	// records are emitted.
	t.Run("propagates a source error", func(t *testing.T) {
		wantErr := errors.New("boom")
		bad := closedSource(1, rec(0, 20))
		bad.finalErr = wantErr

		// The good source's record is emitted, then draining the bad source surfaces its error.
		err := mergeSourcesByTimestamp(context.Background(), []*kafkaClusterSource{closedSource(0, rec(0, 10)), bad}, func(*kgo.Record) error {
			return nil
		})
		require.ErrorIs(t, err, wantErr)
	})

	// Asserts that an error returned by the emit callback aborts the merge and is returned to the caller.
	t.Run("propagates an emit error", func(t *testing.T) {
		wantErr := errors.New("emit failed")
		err := mergeSourcesByTimestamp(context.Background(), []*kafkaClusterSource{closedSource(0, rec(0, 10))}, func(*kgo.Record) error {
			return wantErr
		})
		require.ErrorIs(t, err, wantErr)
	})

	// Asserts that the merge returns the context error instead of blocking forever when a source
	// never produces.
	t.Run("honors context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// A source that never produces; without cancellation the merge would block forever.
		blocked := &kafkaClusterSource{clusterID: 0, records: make(chan *kgo.Record)}
		err := mergeSourcesByTimestamp(ctx, []*kafkaClusterSource{blocked}, func(*kgo.Record) error {
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}

// Asserts that next returns buffered records, then a nil record at EOF (carrying any terminal error
// the producer recorded), and the context error when cancelled.
func TestKafkaClusterSource_Next(t *testing.T) {
	t.Run("returns records then nil at clean EOF", func(t *testing.T) {
		s := closedSource(0, rec(0, 10))

		r, err := s.next(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(0), r.Offset)

		r, err = s.next(context.Background())
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("returns finalErr at EOF", func(t *testing.T) {
		wantErr := errors.New("producer failed")
		s := closedSource(0)
		s.finalErr = wantErr

		r, err := s.next(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("returns context error when cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s := &kafkaClusterSource{clusterID: 0, records: make(chan *kgo.Record)}

		r, err := s.next(ctx)
		require.Nil(t, r)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// captureConsumer records each Consume call's records as a separate batch.
type captureConsumer struct {
	batches [][]*kgo.Record
	err     error
}

func (c *captureConsumer) Consume(_ context.Context, recs iter.Seq[*kgo.Record]) error {
	if c.err != nil {
		return c.err
	}
	c.batches = append(c.batches, slices.Collect(recs))
	return nil
}

// Asserts that the batcher flushes to the consumer at maxBatch (reporting the flush), flushes the
// remainder on demand, treats an empty flush as a no-op, and propagates the consumer's error.
func TestRecordBatcher(t *testing.T) {
	ctx := context.Background()

	t.Run("flushes at maxBatch and reports it, then flushes the remainder", func(t *testing.T) {
		c := &captureConsumer{}
		b := newRecordBatcher(c, 2)

		flushed, err := b.add(ctx, rec(0, 10))
		require.NoError(t, err)
		require.False(t, flushed, "batch not yet full")

		flushed, err = b.add(ctx, rec(1, 20))
		require.NoError(t, err)
		require.True(t, flushed, "batch reached maxBatch")

		flushed, err = b.add(ctx, rec(2, 30))
		require.NoError(t, err)
		require.False(t, flushed)

		require.NoError(t, b.flush(ctx))

		require.Len(t, c.batches, 2)
		require.Len(t, c.batches[0], 2)
		require.Len(t, c.batches[1], 1)
		require.Equal(t, int64(2), c.batches[1][0].Offset)
	})

	t.Run("flush of an empty batch is a no-op", func(t *testing.T) {
		c := &captureConsumer{}
		b := newRecordBatcher(c, 2)
		require.NoError(t, b.flush(ctx))
		require.Empty(t, c.batches)
	})

	t.Run("propagates the consumer error on flush", func(t *testing.T) {
		wantErr := errors.New("consume failed")
		b := newRecordBatcher(&captureConsumer{err: wantErr}, 1)
		_, err := b.add(ctx, rec(0, 10))
		require.ErrorIs(t, err, wantErr)
	})
}

// Asserts that records are forwarded to the channel in order, and that the context error is returned
// when the send blocks on a full/unread channel.
func TestRecordChannelConsumer(t *testing.T) {
	t.Run("forwards records to the channel in order", func(t *testing.T) {
		records := make(chan *kgo.Record, 2)
		c := recordChannelConsumer{records: records}

		require.NoError(t, c.Consume(context.Background(), slices.Values([]*kgo.Record{rec(0, 10), rec(1, 20)})))
		close(records)

		var got []int64
		for r := range records {
			got = append(got, r.Offset)
		}
		require.Equal(t, []int64{0, 1}, got)
	})

	t.Run("returns the context error when the consumer is blocked", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// Unbuffered channel with no reader: the send blocks until ctx cancellation wins.
		c := recordChannelConsumer{records: make(chan *kgo.Record)}
		err := c.Consume(ctx, slices.Values([]*kgo.Record{rec(0, 10)}))
		require.ErrorIs(t, err, context.Canceled)
	})
}

// rec builds a record at the given offset whose timestamp is ms milliseconds past the epoch.
func rec(offset int64, ms int64) *kgo.Record {
	return &kgo.Record{Offset: offset, Timestamp: time.UnixMilli(ms)}
}

// valuedRec is rec with a Value, so tie-broken ordering can be asserted by payload.
func valuedRec(offset int64, ms int64, val string) *kgo.Record {
	r := rec(offset, ms)
	r.Value = []byte(val)
	return r
}

// closedSource returns a kafkaClusterSource pre-loaded with recs (in offset order) and
// already closed, so the merge can drain it without a producer goroutine.
func closedSource(clusterID int, recs ...*kgo.Record) *kafkaClusterSource {
	records := make(chan *kgo.Record, len(recs))
	for _, r := range recs {
		records <- r
	}
	close(records)
	return &kafkaClusterSource{clusterID: clusterID, records: records}
}

// collectMerge drains the merge of sources and returns the emitted record timestamps in
// milliseconds.
func collectMerge(t *testing.T, sources ...*kafkaClusterSource) []int64 {
	t.Helper()
	var got []int64
	err := mergeSourcesByTimestamp(context.Background(), sources, func(r *kgo.Record) error {
		got = append(got, r.Timestamp.UnixMilli())
		return nil
	})
	require.NoError(t, err)
	return got
}
