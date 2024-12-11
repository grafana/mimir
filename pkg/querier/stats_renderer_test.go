// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"

	mimir_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
)

func TestStatsRenderer(t *testing.T) {

	testCases := []struct {
		name    string
		samples uint64
	}{
		{name: "ZeroSamples", samples: 0},
		{name: "OneSample", samples: 1},
		{name: "TenSamples", samples: 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     nil,
				MaxSamples: 100,
				Timeout:    5 * time.Second,
			})

			now := model.Time(time.Now().UnixMilli())

			var seriesList []storage.Series

			for i := 0; i < int(tc.samples); i++ {
				seriesList = append(seriesList, series.NewConcreteSeries(
					labels.FromStrings("__name__", "test", "serial", strconv.Itoa(i)),
					[]model.SamplePair{{Timestamp: now, Value: model.SampleValue(i)}}, nil,
				))
			}

			q := &mockSampleAndChunkQueryable{
				queryableFn: func(_, _ int64) (storage.Querier, error) {
					return &mockQuerier{
						selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
							return series.NewConcreteSeriesSetFromUnsortedSeries(seriesList)
						},
					}, nil
				},
			}

			api := v1.NewAPI(
				engine,
				q,
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

			rec := httptest.NewRecorder()

			req := httptest.NewRequest("GET", "/api/v1/query?query=test", nil)
			ctx := context.Background()
			_, ctx = mimir_stats.ContextWithEmptyStats(ctx)
			req = req.WithContext(user.InjectOrgID(ctx, "test org"))

			promRouter.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			stats := mimir_stats.FromContext(ctx)
			require.NotNil(t, stats)
			require.Equal(t, tc.samples, stats.LoadSamplesProcessed())
		})
	}
}
