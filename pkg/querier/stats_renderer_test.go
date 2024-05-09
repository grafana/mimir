// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
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
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		Reg:                nil,
		ActiveQueryTracker: nil,
		MaxSamples:         100,
		Timeout:            5 * time.Second,
	})

	now := model.Time(time.Now().UnixMilli())

	q := &mockSampleAndChunkQueryable{
		queryableFn: func(int64, int64) (storage.Querier, error) {
			return &mockQuerier{
				seriesSet: series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
					series.NewConcreteSeries(
						labels.FromStrings("__name__", "test", "serial", "1"),
						[]model.SamplePair{{Timestamp: model.Time(now), Value: 41}}, nil,
					),
					series.NewConcreteSeries(
						labels.FromStrings("__name__", "test", "serial", "2"),
						[]model.SamplePair{{Timestamp: model.Time(now), Value: 42}}, nil,
					),
				}),
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
		log.NewNopLogger(),
		func(context.Context) v1.RulesRetriever { return &DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		false, // Not an agent.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
		prometheus.DefaultGatherer,
		nil,
		StatsRenderer,
		false,
		false,
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
	require.Equal(t, uint64(2), stats.LoadTotalSamples())
}
