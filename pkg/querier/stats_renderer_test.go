// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"

	mimir_stats "github.com/grafana/mimir/pkg/querier/stats"
)

func TestStatsRenderer(t *testing.T) {

	testCases := map[string]struct {
		expr                 string
		expectedTotalSamples uint64
	}{
		"zero_series": {
			expr:                 `zero_series{}`,
			expectedTotalSamples: 0,
		},
		"dense_series": {
			expr:                 `dense_series{}`,
			expectedTotalSamples: 11,
		},
		"start_series": {
			expr:                 `start_series{}`,
			expectedTotalSamples: 6,
		},
		"end_series": {
			expr:                 `end_series{}`,
			expectedTotalSamples: 6,
		},
		"sparse_series": {
			expr:                 `sparse_series{}`,
			expectedTotalSamples: 5 + 4,
		},
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     nil,
		MaxSamples: 100,
		Timeout:    5 * time.Second,
	})

	start := timestamp.Time(0)
	end := start.Add(10 * time.Minute)
	step := 1 * time.Minute

	storage := promqltest.LoadedStorage(t, `
		load 1m
			dense_series  0 1 2 3 4 5 6 7 8 9 10
			start_series  0 1 _ _ _ _ _ _ _ _ _
			end_series    _ _ _ _ _ 5 6 7 8 9 10
			sparse_series 0 _ _ _ _ _ _ 7 _ _ _
	`)
	t.Cleanup(func() { storage.Close() })

	api := v1.NewAPI(
		engine,
		storage,
		nil,
		nil,
		func(context.Context) v1.ScrapePoolsRetriever { return &DummyTargetRetriever{} },
		func(context.Context) v1.TargetRetriever { return &DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{},
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		promslog.NewNopLogger(),
		func(context.Context) v1.RulesRetriever { return &DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		false, // Not an agent.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, nil },
		&v1.PrometheusVersion{},
		nil,
		nil,
		prometheus.DefaultGatherer,
		nil,
		StatsRenderer,
		false,
		nil,
		false,
		false,
		0,
	)
	promRouter := route.New().WithPrefix("/api/v1")

	api.Register(promRouter)

	runQuery := func(expr string) *mimir_stats.Stats {
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("GET", fmt.Sprintf("/api/v1/query_range?query=%s&start=%d&end=%d&step=%ds", expr, start.Unix(), end.Unix(), int(step.Seconds())), nil)
		ctx := context.Background()
		_, ctx = mimir_stats.ContextWithEmptyStats(ctx)
		req = req.WithContext(user.InjectOrgID(ctx, "test org"))

		promRouter.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		return mimir_stats.FromContext(ctx)
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			stats := runQuery(tc.expr)

			require.NotNil(t, stats)
			require.Equal(t, tc.expectedTotalSamples, stats.LoadSamplesProcessed())
		})
	}
}
