package usagetracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestTrackerStore_HappyCase(t *testing.T) {
	const defaultIdleTimeout = 20 * time.Minute
	const testUser1 = "user1"
	limits := limiterMock{testUser1: 3}

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker := newTrackerStore(defaultIdleTimeout, log.NewNopLogger(), limits, noopEvents{})
	{
		// Push 2 series, both are accepted.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{1, 2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(2), tracker.tenants[testUser1].series.Load())
	}
	now = now.Add(10 * time.Minute)
	{
		// Push 2 more series, one is accepted, one is rejected.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Equal(t, []uint64{4}, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	{
		// Cleanup now is a noop, nothing expired yet.
		tracker.cleanup(now)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	{
		// Push only series 2, series 1 will expire.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	now = now.Add(11 * time.Minute)
	{
		// Cleanup again will remove series 1.
		tracker.cleanup(now)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(2), tracker.tenants[testUser1].series.Load())
	}
	{
		// Pushing 3, 4 works now.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
}

type limiterMock map[string]uint64

func (l limiterMock) localSeriesLimit(userID string) uint64 { return l[userID] }

type noopEvents struct{}

func (n noopEvents) publishCreatedSeries(ctx context.Context, userID string, series []uint64) error {
	return nil
}

func TestMinutes(t *testing.T) {
	// t0 is at 4-hour boundary
	t0 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(30 * time.Minute)
	t2 := t0.Add(-30 * time.Minute)

	t.Run("sub", func(t *testing.T) {
		for _, tc := range []struct {
			a, b     minutes
			expected int
		}{
			{a: toMinutes(t0), b: toMinutes(t0.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t0), b: toMinutes(t0.Add(5 * time.Minute)), expected: -5},
			{a: toMinutes(t1), b: toMinutes(t1.Add(5 * time.Minute)), expected: -5},
			{a: toMinutes(t2), b: toMinutes(t2.Add(5 * time.Minute)), expected: -5},

			{a: toMinutes(t0), b: toMinutes(t0.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t0), b: toMinutes(t0.Add(59 * time.Minute)), expected: -59},
			{a: toMinutes(t1), b: toMinutes(t1.Add(59 * time.Minute)), expected: -59},
			{a: toMinutes(t2), b: toMinutes(t2.Add(59 * time.Minute)), expected: -59},
		} {
			t.Run(fmt.Sprintf("%s sub %s = %d", tc.a, tc.b, tc.expected), func(t *testing.T) {
				require.Equal(t, tc.expected, tc.a.sub(tc.b))
			})
		}
	})

	t.Run("greaterThan", func(t *testing.T) {
		for _, tc := range []struct {
			a, b     minutes
			expected bool
		}{
			{a: toMinutes(t0), b: toMinutes(t0.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t0), b: toMinutes(t0.Add(5 * time.Minute)), expected: false},
			{a: toMinutes(t1), b: toMinutes(t1.Add(5 * time.Minute)), expected: false},
			{a: toMinutes(t2), b: toMinutes(t2.Add(5 * time.Minute)), expected: false},

			{a: toMinutes(t0), b: toMinutes(t0.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t0), b: toMinutes(t0.Add(59 * time.Minute)), expected: false},
			{a: toMinutes(t1), b: toMinutes(t1.Add(59 * time.Minute)), expected: false},
			{a: toMinutes(t2), b: toMinutes(t2.Add(59 * time.Minute)), expected: false},
		} {
			t.Run(fmt.Sprintf("%s > %s = %t", tc.a, tc.b, tc.expected), func(t *testing.T) {
				require.Equal(t, tc.expected, tc.a.greaterThan(tc.b))
			})
		}
	})
}
