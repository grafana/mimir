// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/series"                 //lint:ignore faillint series is fine
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions" //lint:ignore faillint requestoptions is fine
	"github.com/grafana/mimir/pkg/util/limiter"                   //lint:ignore faillint limiter is fine
	"github.com/grafana/mimir/pkg/util/promqlext"                 //lint:ignore faillint promqlext is fine
)

func TestPrometheusEngine_StartTimestamps(t *testing.T) {
	tests := []struct {
		name            string
		engineDefault   bool
		opts            promql.QueryOpts
		rangeQuery      bool
		expectedEnabled bool
	}{
		{
			name:            "instant default false absent",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, nil),
			rangeQuery:      false,
			expectedEnabled: false,
		},
		{
			name:            "instant default false disabled",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(false)),
			rangeQuery:      false,
			expectedEnabled: false,
		},
		{
			name:            "instant default false enabled",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(true)),
			rangeQuery:      false,
			expectedEnabled: true,
		},
		{
			name:            "instant default false nil options",
			engineDefault:   false,
			opts:            nil,
			rangeQuery:      false,
			expectedEnabled: false,
		},
		{
			name:            "instant default true absent",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, nil),
			rangeQuery:      false,
			expectedEnabled: true,
		},
		{
			name:            "instant default true disabled",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(false)),
			rangeQuery:      false,
			expectedEnabled: false,
		},
		{
			name:            "instant default true enabled",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(true)),
			rangeQuery:      false,
			expectedEnabled: true,
		},
		{
			name:            "instant default true nil options",
			engineDefault:   true,
			opts:            nil,
			rangeQuery:      false,
			expectedEnabled: true,
		},
		{
			name:            "range default false absent",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, nil),
			rangeQuery:      true,
			expectedEnabled: false,
		},
		{
			name:            "range default false disabled",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(false)),
			rangeQuery:      true,
			expectedEnabled: false,
		},
		{
			name:            "range default false enabled",
			engineDefault:   false,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(true)),
			rangeQuery:      true,
			expectedEnabled: true,
		},
		{
			name:            "range default false nil options",
			engineDefault:   false,
			opts:            nil,
			rangeQuery:      true,
			expectedEnabled: false,
		},
		{
			name:            "range default true absent",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, nil),
			rangeQuery:      true,
			expectedEnabled: true,
		},
		{
			name:            "range default true disabled",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(false)),
			rangeQuery:      true,
			expectedEnabled: false,
		},
		{
			name:            "range default true enabled",
			engineDefault:   true,
			opts:            promql.NewPrometheusQueryOpts(false, 0, new(true)),
			rangeQuery:      true,
			expectedEnabled: true,
		},
		{
			name:            "range default true nil options",
			engineDefault:   true,
			opts:            nil,
			rangeQuery:      true,
			expectedEnabled: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := NewPrometheusEngine(promql.EngineOpts{
				MaxSamples:         1000,
				Timeout:            time.Minute,
				UseStartTimestamps: tc.engineDefault,
				Parser:             promqlext.NewPromQLParser(),
			})

			encoded, err := chunkenc.NewEmptyChunk(chunkenc.EncXOR2)
			require.NoError(t, err)
			app, err := encoded.Appender()
			require.NoError(t, err)
			app.Append(100_000, 200_000, 42)

			// Observe what storage receives independently of the engine's result.
			var selected, collected, memoryTracked bool
			querier := &startTimestampQuerier{selectSeries: func(ctx context.Context) storage.SeriesSet {
				selected = true
				collected = requestoptions.CollectStartTimestampsFromContext(ctx)
				tracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
				memoryTracked = err == nil && tracker != nil
				return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{&storage.SeriesEntry{
					Lset:             labels.FromStrings("__name__", "counter"),
					SampleIteratorFn: func(reuse chunkenc.Iterator) chunkenc.Iterator { return encoded.Iterator(reuse) },
				}})
			}}
			queryable := &storage.MockQueryable{MockQuerier: querier}

			var query promql.Query
			if tc.rangeQuery {
				query, err = engine.NewRangeQuery(t.Context(), queryable, tc.opts, "start_timestamp(counter)", time.Unix(200, 0), time.Unix(210, 0), 10*time.Second)
			} else {
				query, err = engine.NewInstantQuery(t.Context(), queryable, tc.opts, "start_timestamp(counter)", time.Unix(200, 0))
			}
			require.NoError(t, err)
			defer query.Close()

			// Exec receives a different context with a stale collection setting.
			ctx := requestoptions.ContextWithStartTimestampCollection(t.Context(), !tc.expectedEnabled)

			result := query.Exec(ctx)
			require.NoError(t, result.Err)
			require.True(t, selected)
			require.True(t, memoryTracked)
			require.Equal(t, tc.expectedEnabled, collected)

			// The engine and storage must agree, including when the override is absent.
			expectedSamples := 0
			if tc.expectedEnabled {
				expectedSamples = 1
			}
			if tc.rangeQuery {
				matrix, err := result.Matrix()
				require.NoError(t, err)
				require.Len(t, matrix, expectedSamples)
				if tc.expectedEnabled {
					require.Len(t, matrix[0].Floats, 2)
					require.Equal(t, float64(100), matrix[0].Floats[0].F)
					require.Equal(t, float64(100), matrix[0].Floats[1].F)
				}
			} else {
				vector, err := result.Vector()
				require.NoError(t, err)
				require.Len(t, vector, expectedSamples)
				if tc.expectedEnabled {
					require.Equal(t, float64(100), vector[0].F)
				}
			}
		})
	}
}

type startTimestampQuerier struct {
	storage.MockQuerier
	selectSeries func(context.Context) storage.SeriesSet
}

func (q *startTimestampQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return q.selectSeries(ctx)
}

func TestPrometheusEngine_QueryCreationError(t *testing.T) {
	engine := NewPrometheusEngine(promql.EngineOpts{Timeout: time.Minute})
	query, err := engine.NewInstantQuery(t.Context(), nil, nil, "(", time.Now())
	require.Error(t, err)
	require.Nil(t, query)

	query, err = engine.NewRangeQuery(t.Context(), nil, nil, "(", time.Now(), time.Now(), time.Second)
	require.Error(t, err)
	require.Nil(t, query)
}
