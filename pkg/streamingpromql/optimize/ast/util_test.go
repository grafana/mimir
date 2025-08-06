// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
)

func testASTOptimizationPassWithData(t *testing.T, loadTemplate string, testCases map[string]string) {
	numSamples := 100
	replacer := strings.NewReplacer("<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(loadTemplate)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:               promslog.NewNopLogger(),
		Reg:                  nil,
		MaxSamples:           100000000,
		Timeout:              time.Minute,
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})

	for input, expected := range testCases {
		if input == expected {
			continue
		}
		t.Run(input, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")
			startTime := timestamp.Time(0)
			endTime := startTime.Add(time.Duration(numSamples) * time.Minute)

			qInput, err := engine.NewRangeQuery(ctx, queryable, nil, input, startTime, endTime, step)
			require.NoError(t, err)
			resInput := qInput.Exec(context.Background())
			require.NoError(t, resInput.Err)
			matrixInput, ok := resInput.Value.(promql.Matrix)
			require.True(t, ok)

			qRewritten, err := engine.NewRangeQuery(ctx, queryable, nil, expected, startTime, endTime, step)
			require.NoError(t, err)
			resRewritten := qRewritten.Exec(context.Background())
			require.NoError(t, resRewritten.Err)
			matrixRewritten, ok := resRewritten.Value.(promql.Matrix)
			require.True(t, ok)

			approximatelyEqualsMatrix(t, matrixInput, matrixRewritten)
		})
	}
}

func approximatelyEqualsMatrix(t *testing.T, a, b promql.Matrix) {
	require.Equal(t, len(a), len(b), "Matrix lengths do not match")

	// Sort both results by metric string to ensure consistent comparison
	sortMetrics := func(streams []promql.Series) {
		sort.Slice(streams, func(i, j int) bool {
			return streams[i].Metric.String() < streams[j].Metric.String()
		})
	}
	sortMetrics(a)
	sortMetrics(b)

	for i := range a {
		require.Equal(t, a[i].Metric, b[i].Metric, "Metric labels do not match for series %d", i)

		require.Equal(t, len(a[i].Floats), len(b[i].Floats), "Samples length does not match for series %d", i)
		for j := range a[i].Floats {
			require.Equal(t, a[i].Floats[j].T, b[i].Floats[j].T, "Sample timestamps do not match for series %d at index %d", i, j)
			require.InDelta(t, a[i].Floats[j].F, b[i].Floats[j].F, 1e-10, "Sample values do not match for series %d at index %d", i, j)
		}

		require.Equal(t, len(a[i].Histograms), len(b[i].Histograms), "Histograms length does not match for series %d", i)
		for j := range a[i].Histograms {
			require.Equal(t, a[i].Histograms[j].T, b[i].Histograms[j].T, "Histogram timestamps do not match for series %d at index %d", i, j)
			require.Equal(t, a[i].Histograms[j].H, b[i].Histograms[j].H, "Histogram values do not match for series %d at index %d", i, j)
		}
	}
}
