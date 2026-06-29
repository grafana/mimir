// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"iter"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// captureConsumer records each Consume call's records as a separate batch.
type captureConsumer struct {
	batches [][]*kgo.Record
}

func (c *captureConsumer) Consume(_ context.Context, recs iter.Seq[*kgo.Record]) error {
	var b []*kgo.Record
	for r := range recs {
		b = append(b, r)
	}
	c.batches = append(c.batches, b)
	return nil
}

// closedSource returns a writeCompartmentSource pre-loaded with recs (in offset
// order) and already closed, so the merge can drain it without a producer.
func closedSource(wcID int, recs ...*kgo.Record) *writeCompartmentSource {
	records := make(chan *kgo.Record, len(recs))
	for _, r := range recs {
		records <- r
	}
	close(records)
	return &writeCompartmentSource{wcID: wcID, records: records}
}

func rec(offset int64, ms int64) *kgo.Record {
	return &kgo.Record{Offset: offset, Timestamp: time.Unix(0, ms*int64(time.Millisecond))}
}

func collectMerge(t *testing.T, sources ...*writeCompartmentSource) []int64 {
	t.Helper()
	var got []int64
	err := mergeSourcesByTimestamp(context.Background(), sources, func(r *kgo.Record) error {
		got = append(got, r.Timestamp.UnixMilli())
		return nil
	})
	require.NoError(t, err)
	return got
}

func TestMergeSourcesByTimestamp_OrdersAcrossSources(t *testing.T) {
	s0 := closedSource(0, rec(0, 10), rec(1, 30))
	s1 := closedSource(1, rec(0, 20), rec(1, 40))

	got := collectMerge(t, s0, s1)

	require.Equal(t, []int64{10, 20, 30, 40}, got)
}

func TestMergeSourcesByTimestamp_WaitsForLaggingSourceToCatchUp(t *testing.T) {
	// fast has both its records available immediately (buffered + closed); slow
	// feeds over an unbuffered channel, so each send blocks until the merge pulls
	// it. fast's late record (t=40) is available the whole time, so if the merge
	// failed to wait for slow it would emit 40 ahead of slow's 20/30.
	fast := closedSource(0, rec(0, 10), rec(1, 40))
	slow := &writeCompartmentSource{wcID: 1, records: make(chan *kgo.Record)}

	var got []int64
	done := make(chan error, 1)
	go func() {
		done <- mergeSourcesByTimestamp(context.Background(), []*writeCompartmentSource{fast, slow}, func(r *kgo.Record) error {
			got = append(got, r.Timestamp.UnixMilli())
			return nil
		})
	}()

	slow.records <- rec(0, 20)
	slow.records <- rec(1, 30)
	close(slow.records)

	require.NoError(t, <-done)
	require.Equal(t, []int64{10, 20, 30, 40}, got)
}

func TestMergeSourcesByTimestamp_PassesThroughOutOfOrderTimestampsWithinASource(t *testing.T) {
	// Kafka orders a partition by offset, not timestamp, so a single WC stream can
	// deliver non-monotonic timestamps. The merge must consume every record and
	// keep each source's offset order; it does not (and cannot) globally sort
	// disorder internal to a source — that residual jitter goes to the TSDB OOO
	// window. Here wc0 delivers 50,30,40 in offset order.
	a := closedSource(0, rec(0, 50), rec(1, 30), rec(2, 40))
	b := closedSource(1, rec(0, 20))

	got := collectMerge(t, a, b)

	// 20 (wc1) sorts first; then wc0's records pass through in their offset order.
	require.Equal(t, []int64{20, 50, 30, 40}, got)
}

func tagged(offset int64, ms int64, val string) *kgo.Record {
	r := rec(offset, ms)
	r.Value = []byte(val)
	return r
}

func TestMergeSourcesByTimestamp_EqualTimestampsTieBreakByWCThenOffset(t *testing.T) {
	// All share a timestamp; order must be clusterID asc, then offset asc, regardless
	// of the source slice order (s1 is passed before s0).
	s0 := closedSource(0, tagged(0, 50, "a"), tagged(1, 50, "b"))
	s1 := closedSource(1, tagged(0, 50, "c"))

	var got []string
	err := mergeSourcesByTimestamp(context.Background(), []*writeCompartmentSource{s1, s0}, func(r *kgo.Record) error {
		got = append(got, string(r.Value))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, got)
}

func TestMergeSourcesByTimestamp_SingleSource(t *testing.T) {
	got := collectMerge(t, closedSource(0, rec(0, 10), rec(1, 20)))
	require.Equal(t, []int64{10, 20}, got)
}

func TestMergeSourcesByTimestamp_SkipsEmptySource(t *testing.T) {
	got := collectMerge(t, closedSource(0), closedSource(1, rec(0, 10)))
	require.Equal(t, []int64{10}, got)
}

func TestMergeSourcesByTimestamp_PropagatesSourceError(t *testing.T) {
	bad := closedSource(1, rec(0, 20))
	bad.finalErr = context.DeadlineExceeded

	err := mergeSourcesByTimestamp(context.Background(), []*writeCompartmentSource{closedSource(0, rec(0, 10)), bad}, func(*kgo.Record) error {
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRecordBatcher_FlushesAtMaxThenRemainder(t *testing.T) {
	c := &captureConsumer{}
	b := newRecordBatcher(c, 2)
	ctx := context.Background()

	for _, r := range []*kgo.Record{rec(0, 10), rec(1, 20), rec(2, 30)} {
		require.NoError(t, b.add(ctx, r))
	}
	require.NoError(t, b.flush(ctx))

	require.Len(t, c.batches, 2)
	require.Len(t, c.batches[0], 2)
	require.Len(t, c.batches[1], 1)
	require.Equal(t, int64(2), c.batches[1][0].Offset)
}

func TestRecordBatcher_FlushEmptyIsNoop(t *testing.T) {
	c := &captureConsumer{}
	b := newRecordBatcher(c, 2)
	require.NoError(t, b.flush(context.Background()))
	require.Empty(t, c.batches)
}

func TestRecordChannelConsumer_ForwardsRecordsToChannel(t *testing.T) {
	records := make(chan *kgo.Record, 2)
	c := recordChannelConsumer{records: records}

	err := c.Consume(context.Background(), slices.Values([]*kgo.Record{rec(0, 10), rec(1, 20)}))
	require.NoError(t, err)
	close(records)

	var got []int64
	for r := range records {
		got = append(got, r.Offset)
	}
	require.Equal(t, []int64{0, 1}, got)
}
