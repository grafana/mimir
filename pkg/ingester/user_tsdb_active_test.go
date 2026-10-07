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
