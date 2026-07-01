// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestInitialOffsetProbing(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		offsets        []*offsetTime
		start          int64
		resume         int64
		end            int64
		jobSize        time.Duration
		endTime        time.Time
		expectedRanges []*offsetTime
		minScanTime    time.Time
		msg            string
	}{
		"no new data": {
			// End offset is the one that was consumed last time.
			offsets: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 199*1000000, time.UTC)},
			},
			resume:         2000,
			end:            2000,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
		},
		"old data with single unconsumed record": {
			offsets: []*offsetTime{
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
			},
			resume:      2000,
			end:         2001,
			jobSize:     200 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 2001, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
		},
		"one record: no new data": {
			offsets: []*offsetTime{
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
			},
			resume:         2000,
			end:            2000,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
		},
		"empty partition: no data": {
			offsets:        []*offsetTime{},
			resume:         0,
			end:            0,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{{offset: 0, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
		},
		"data gaps wider than job size": {
			offsets: []*offsetTime{
				{offset: 999, time: time.Date(2025, 3, 1, 10, 0, 0, 99*1000000, time.UTC)},
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 103*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 104*1000000, time.UTC)},
				{offset: 1005, time: time.Date(2025, 3, 1, 10, 0, 0, 105*1000000, time.UTC)},
				{offset: 1006, time: time.Date(2025, 3, 1, 10, 0, 0, 106*1000000, time.UTC)},
				{offset: 1007, time: time.Date(2025, 3, 1, 10, 0, 0, 107*1000000, time.UTC)},
				{offset: 1008, time: time.Date(2025, 3, 1, 10, 0, 0, 108*1000000, time.UTC)},
				{offset: 1009, time: time.Date(2025, 3, 1, 10, 0, 0, 109*1000000, time.UTC)},
				{offset: 1010, time: time.Date(2025, 3, 1, 10, 0, 0, 110*1000000, time.UTC)},
				{offset: 1011, time: time.Date(2025, 3, 1, 10, 0, 0, 111*1000000, time.UTC)},
				{offset: 1012, time: time.Date(2025, 3, 1, 10, 0, 0, 112*1000000, time.UTC)},
				// (large gap that would produce duplicates in a naive implementation)
				{offset: 1013, time: time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC)},
				{offset: 1014, time: time.Date(2025, 3, 1, 10, 0, 0, 501*1000000, time.UTC)},
				{offset: 1015, time: time.Date(2025, 3, 1, 10, 0, 0, 502*1000000, time.UTC)},
				{offset: 1016, time: time.Date(2025, 3, 1, 10, 0, 0, 503*1000000, time.UTC)},
				{offset: 1017, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
			resume:      1000,
			end:         1020,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 99*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1013, time: time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC)},
				{offset: 1014, time: time.Date(2025, 3, 1, 10, 0, 0, 501*1000000, time.UTC)},
				{offset: 1017, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
				{offset: 1020, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
		},
		"records with duplicate timestamps": {
			offsets: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1005, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1006, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1007, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1008, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
			},
			resume:      1000,
			end:         1009,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1009, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
		},
		"resumption offset is before min scan time": {
			offsets: []*offsetTime{
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC)},
			},
			resume:      1001,
			end:         1004,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 0, 0, 150*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
		},
		"min scan time later than any data": {
			offsets: []*offsetTime{
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC)},
			},
			resume:         1001,
			end:            1004,
			jobSize:        100 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 900*1000000, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"resume < start and start == end": {
			offsets:        []*offsetTime{},
			start:          1004,
			resume:         1000,
			end:            1004,
			jobSize:        1 * time.Minute,
			endTime:        time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{{offset: 1004, time: time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC)}},
		},
		"resume < start < end": {
			offsets: []*offsetTime{
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 0, 0, time.UTC)},
			},
			start:       1003,
			resume:      1000,
			end:         1004,
			jobSize:     1 * time.Minute,
			endTime:     time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 0, 0, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC)},
			},
		},
		"resume == start == end": {
			offsets:        []*offsetTime{},
			start:          1003,
			resume:         1003,
			end:            1003,
			jobSize:        1 * time.Minute,
			endTime:        time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC)}},
		},
		"hour-based ranges when resume < start": {
			offsets: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 3000, time: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			start:       2000,
			resume:      100,
			end:         10001,
			jobSize:     1 * time.Hour,
			endTime:     time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC),
			minScanTime: time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 3000, time: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
				{offset: 10001, time: time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC)},
			},
			msg: "if resumption offset has fallen off the retention window, we should produce jobs beginning at start",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			f := &mockOffsetFinder{offsets: tt.offsets, end: tt.end, distinctTimes: make(map[time.Time]struct{})}
			scanner := newOffsetScanner(f, tt.endTime, tt.jobSize, tt.endTime.Sub(tt.minScanTime), promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}))
			j, err := scanner.probeInitialOffsets(ctx, partitionOffsets{topic: "topic", partition: 0, start: tt.start, resume: tt.resume, end: tt.end}, test.NewTestingLogger(t))
			assert.NoError(t, err)
			assert.EqualValues(t, tt.expectedRanges, j, tt.msg)
		})
	}
}

func TestScanProbeTimes(t *testing.T) {
	// jobSize of 1m means scanStep is jobSize/4 = 15s. Probes step back from
	// endTime by scanStep, stopping once the next step would be at or before
	// minScanTime.
	jobSize := 1 * time.Minute

	tests := map[string]struct {
		endTime     time.Time
		minScanTime time.Time
		expected    []time.Time
	}{
		"aligned to minScan time": {
			// Window is exactly 60s (4 * scanStep), so the descending grid
			// lands exactly on minScanTime, which is excluded.
			endTime:     time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 1, 50, 0, time.UTC),
			expected: []time.Time{
				time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 35, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 20, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 5, 0, time.UTC),
			},
		},
		"unaligned to both": {
			// Window is 65s (not a multiple of scanStep), so the lowest probe
			// sits 5s above minScanTime.
			endTime:     time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 1, 45, 0, time.UTC),
			expected: []time.Time{
				time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 35, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 20, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 2, 5, 0, time.UTC),
				time.Date(2025, 3, 1, 10, 1, 50, 0, time.UTC),
			},
		},
		"single step": {
			// Window is 10s, shorter than one scanStep, so only endTime is probed.
			endTime:     time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 2, 40, 0, time.UTC),
			expected: []time.Time{
				time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			},
		},
		"empty scan window": {
			// minScanTime at endTime yields no probes.
			endTime:     time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 2, 50, 0, time.UTC),
			expected:    nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, scanProbeTimes(tt.endTime, jobSize, tt.minScanTime))
		})
	}
}

// TestOffsetFinder_offsetAfterTime runs against a kfake cluster, so it
// exercises the method end-to-end including kadm's ListOffsetsAfterMilli.
func TestOffsetFinder_offsetAfterTime(t *testing.T) {
	ctx := context.Background()

	var vnet kfake.VirtualNetwork
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, "ingest", testkafka.WithVirtualNetwork(&vnet))
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.Dialer(vnet.DialContext),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(cli.Close)

	require.NoError(t, cli.ProduceSync(ctx, &kgo.Record{Value: []byte("v"), Topic: "ingest", Partition: 0}).FirstErr())

	finder := newOffsetFinder(kadm.NewClient(cli))

	// A time before the record returns a real record offset and timestamp.
	off, _, isEndOffset, err := finder.offsetAfterTime(ctx, "ingest", 0, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
	assert.False(t, isEndOffset, "a real record at or after the time is not the end offset")

	// A time after every record has no record after it, so ListOffsetsAfterMilli
	// falls back to the end offset with Kafka's -1 timestamp sentinel, which
	// offsetAfterTime reports as the end offset.
	off, _, isEndOffset, err = finder.offsetAfterTime(ctx, "ingest", 0, time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, int64(1), off, "should fall back to the end offset")
	assert.True(t, isEndOffset, "no record after the time must be reported as the end offset")
}

func TestOffsetFinder_offsetAfterTime_negativeTimestamp(t *testing.T) {
	ctx := context.Background()
	probeTime := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)

	// A -1 timestamp (no record at or after the time) is reported as the end
	// offset, never an error, so the partition is still seeded and its backlog
	// isn't dropped.
	finder := newOffsetFinder(nil)
	finder.offsets[probeTime] = kadm.ListedOffsets{
		"ingest": {0: {Topic: "ingest", Partition: 0, Offset: 42, Timestamp: -1}},
	}
	off, _, isEndOffset, err := finder.offsetAfterTime(ctx, "ingest", 0, probeTime)
	require.NoError(t, err)
	assert.Equal(t, int64(42), off)
	assert.True(t, isEndOffset, "a -1 timestamp must be reported as the end offset")

	// A timestamp below -1 should never be returned; it indicates a malformed
	// response, so we fail loudly.
	badFinder := newOffsetFinder(nil)
	badFinder.offsets[probeTime] = kadm.ListedOffsets{
		"ingest": {0: {Topic: "ingest", Partition: 0, Offset: 42, Timestamp: -2}},
	}
	assert.Panics(t, func() {
		_, _, _, _ = badFinder.offsetAfterTime(ctx, "ingest", 0, probeTime)
	})
}

// Create an offset finder that we can prepopulate with offset scenarios.
type mockOffsetFinder struct {
	offsets       []*offsetTime
	end           int64
	distinctTimes map[time.Time]struct{}
}

func (o *mockOffsetFinder) offsetAfterTime(_ context.Context, _ string, _ int32, t time.Time) (int64, time.Time, bool, error) {
	o.distinctTimes[t] = struct{}{}
	// scan the offsets slice and return the lowest offset whose time is after t.
	mint := time.Time{}
	maxt := time.Time{}
	off := int64(-1)
	for _, pair := range o.offsets {
		if pair.time.After(t) {
			if mint.IsZero() || mint.After(pair.time) {
				mint = pair.time
				off = pair.offset
			}
			if maxt.Before(pair.time) {
				maxt = pair.time
			}
		}
	}
	if off == -1 {
		// Like ListOffsetsAfterMilli, we return the end offset if we don't find any new data.
		return o.end, time.Time{}, true, nil
	}
	return off, mint, false, nil
}

var _ offsetStore = (*mockOffsetFinder)(nil)
