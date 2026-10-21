// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// markAllInactive makes every series inactive, as time passing until the tracker's purge at now would: until the
// next sample of each.
func (u *userTSDB) markAllInactive(now time.Time) {
	if u.trackerActive() {
		u.activeSeries.Purge(now, nil)
		return
	}
	u.nativeActive.DeactivateAll()
}

// TestIngesterActiveSeriesOfLargeRequests pushes requests of many series, as distributors send them, of new series and of
// series the ingester has, and checks every series that was ingested is active.
func TestIngesterActiveSeriesOfLargeRequests(t *testing.T) {
	const (
		userID   = "user"
		series   = 3000
		requests = 3
	)
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	limits := defaultLimitsTestConfig()
	ing, r, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, validation.NewOverrides(limits, nil), nil, "", "", prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, ing, r)
	ctx := user.InjectOrgID(context.Background(), userID)

	push := func(from, to int, at time.Time) {
		req := &mimirpb.WriteRequest{Source: mimirpb.API}
		for n := from; n < to; n++ {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: fmt.Sprintf("metric_%d", n%50)}, {Name: "pod", Value: fmt.Sprintf("pod-%d", n)}},
				Samples: []mimirpb.Sample{{TimestampMs: at.UnixMilli(), Value: float64(n)}},
			}})
		}
		_, err := ing.Push(ctx, req)
		require.NoError(t, err)
	}

	now := time.Now()
	for round := range requests {
		// New series on the first round, and the same ones with later samples after.
		for from := 0; from < series; from += 1000 {
			push(from, from+1000, now.Add(time.Duration(round)*time.Second))
		}
		db := ing.getTSDB(userID)
		require.Equal(t, series, db.activeSeriesCounts(time.Now()).Total, "round %d", round)
	}
	// Series created by the following requests, and the earlier ones, with gaps.
	push(series, series+700, now.Add(10*time.Second))
	push(0, 500, now.Add(11*time.Second))
	push(series+700, series+1400, now.Add(12*time.Second))
	require.Equal(t, series+1400, ing.getTSDB(userID).activeSeriesCounts(time.Now()).Total)
}

// TestActiveSeriesBetweenPurgesAsTheTracker checks that, between two updates of the active series, the engine that
// keeps them tells the same series as the tracker: those that went idle since the last update are still active.
func TestActiveSeriesBetweenPurgesAsTheTracker(t *testing.T) {
	const idle = time.Hour
	activeAt := func(engine string) (purged, kept bool) {
		previous := testEngine
		testEngine = engine
		t.Cleanup(func() { testEngine = previous })

		cfg := defaultIngesterTestConfig(t)
		cfg.ActiveSeriesMetrics.Enabled = true
		cfg.ActiveSeriesMetrics.IdleTimeout = idle
		ing, r, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, validation.NewOverrides(defaultLimitsTestConfig(), nil), nil, "", "", prometheus.NewRegistry())
		require.NoError(t, err)
		startAndWaitHealthy(t, ing, r)
		ctx := user.InjectOrgID(context.Background(), "user")
		push := func(name string) (before, after time.Time) {
			before = time.Now()
			_, err := ing.Push(ctx, &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: name}},
				Samples: []mimirpb.Sample{{TimestampMs: before.UnixMilli(), Value: 1}},
			}}}})
			require.NoError(t, err)
			return before, time.Now()
		}
		_, firstDone := push("first")
		time.Sleep(10 * time.Millisecond)
		secondStart, secondDone := push("second")
		require.True(t, firstDone.Before(secondStart))

		db := ing.getTSDB("user")
		// An update that the first series went idle by, and the second not.
		db.activeSeriesCounts(firstDone.Add(idle + time.Millisecond))
		// Both are idle by now, but only the first was purged.
		refs := db.activeRefs(secondDone.Add(idle + time.Millisecond))
		ref := func(name string) storage.SeriesRef {
			idx := mustIndex(db.Head())
			defer func() { require.NoError(t, idx.Close()) }()
			postings, err := tsdb.PostingsForMatchers(ctx, idx, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name))
			require.NoError(t, err)
			require.True(t, postings.Next())
			return postings.At()
		}
		return refs.ContainsRef(ref("first")), refs.ContainsRef(ref("second"))
	}
	for _, engine := range []string{"", "seriesstore"} {
		purged, kept := activeAt(engine)
		require.False(t, purged, "engine %q", engine)
		require.True(t, kept, "engine %q", engine)
	}
}
