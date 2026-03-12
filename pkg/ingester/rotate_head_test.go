// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

func TestRotateHead_BasicQueryAcrossHeads(t *testing.T) {
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()

	// Append samples to the first head.
	app := db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("__name__", "metric1", "job", "test"), 100, 1.0)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("__name__", "metric1", "job", "test"), 200, 2.0)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("__name__", "metric2", "job", "test"), 150, 10.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Rotate the head.
	require.NoError(t, db.RotateHead())

	// Append samples to the new head.
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("__name__", "metric1", "job", "test"), 300, 3.0)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("__name__", "metric3", "job", "test"), 350, 99.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Query across both heads.
	q, err := db.Querier(0, 400)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", "metric.*"))

	var seriesLabels []labels.Labels
	sampleCounts := map[string]int{}
	for ss.Next() {
		s := ss.At()
		seriesLabels = append(seriesLabels, s.Labels())

		iter := s.Iterator(nil)
		count := 0
		for iter.Next() != 0 {
			count++
		}
		require.NoError(t, iter.Err())
		sampleCounts[s.Labels().String()] = count
	}
	require.NoError(t, ss.Err())

	require.Len(t, seriesLabels, 3, "expected 3 series across both heads")
	require.Equal(t, 3, sampleCounts[labels.FromStrings("__name__", "metric1", "job", "test").String()], "metric1 should have 3 samples across both heads")
	require.Equal(t, 1, sampleCounts[labels.FromStrings("__name__", "metric2", "job", "test").String()], "metric2 should have 1 sample from retired head")
	require.Equal(t, 1, sampleCounts[labels.FromStrings("__name__", "metric3", "job", "test").String()], "metric3 should have 1 sample from active head")
}

func TestRotateHead_MultipleRotations(t *testing.T) {
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()

	// Phase 1: append and rotate.
	app := db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("phase", "1"), 100, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, db.RotateHead())

	// Phase 2: append and rotate.
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("phase", "2"), 200, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, db.RotateHead())

	// Phase 3: append (active head).
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("phase", "3"), 300, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Query across all three heads.
	q, err := db.Querier(0, 400)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchRegexp, "phase", ".*"))
	count := 0
	for ss.Next() {
		count++
		s := ss.At()
		iter := s.Iterator(nil)
		sampleCount := 0
		for iter.Next() != 0 {
			sampleCount++
		}
		require.NoError(t, iter.Err())
		require.Equal(t, 1, sampleCount, "each phase series should have exactly 1 sample")
	}
	require.NoError(t, ss.Err())
	require.Equal(t, 3, count, "expected 3 series from 3 phases (2 retired + 1 active)")
}

func TestDropRetiredHeadsBefore(t *testing.T) {
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()

	// Append with early timestamps and rotate.
	app := db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("gen", "old"), 100, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, db.RotateHead())

	// Append with later timestamps and rotate.
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("gen", "new"), 5000, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, db.RotateHead())

	// Append to active head.
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("gen", "active"), 10000, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Verify all 3 series are queryable.
	q, err := db.Querier(0, math.MaxInt64)
	require.NoError(t, err)
	ss := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchRegexp, "gen", ".*"))
	count := 0
	for ss.Next() {
		count++
	}
	require.NoError(t, ss.Err())
	require.NoError(t, q.Close())
	require.Equal(t, 3, count)

	// Drop retired heads with maxT < 1000 (the first one has maxT=100).
	require.NoError(t, db.DropRetiredHeadsBefore(1000))

	// Now only 2 series should be queryable (old is dropped).
	q, err = db.Querier(0, math.MaxInt64)
	require.NoError(t, err)
	ss = q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchRegexp, "gen", ".*"))
	count = 0
	for ss.Next() {
		count++
	}
	require.NoError(t, ss.Err())
	require.NoError(t, q.Close())
	require.Equal(t, 2, count, "old retired head should have been dropped")
}

func TestRotateHead_LabelNamesAndValues(t *testing.T) {
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()

	// Append with a unique label in first head and rotate.
	app := db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("__name__", "m1", "env", "prod"), 100, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, db.RotateHead())

	// Append with a different unique label in second head.
	app = db.Appender(ctx)
	_, err = app.Append(0, labels.FromStrings("__name__", "m2", "env", "staging"), 200, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Query label values — should see both "prod" and "staging".
	q, err := db.Querier(0, 300)
	require.NoError(t, err)
	defer q.Close()

	vals, _, err := q.LabelValues(ctx, "env", nil)
	require.NoError(t, err)
	require.Contains(t, vals, "prod", "should contain 'prod' from retired head")
	require.Contains(t, vals, "staging", "should contain 'staging' from active head")

	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, names, "__name__")
	require.Contains(t, names, "env")
}

func TestRotateHead_EmptyHeadSkipped(t *testing.T) {
	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Rotating an empty head should be a no-op.
	require.NoError(t, db.RotateHead())

	// The head should still work for writes.
	app := db.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("x", "y"), 100, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
}
