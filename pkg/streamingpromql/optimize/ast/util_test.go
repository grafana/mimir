// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/engineopts"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

func testASTOptimizationPassWithData(t *testing.T, loadTemplate string, testCases map[string]string) {
	numSamples := 100
	replacer := strings.NewReplacer("<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(loadTemplate)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	// Use Prometheus's query engine to execute the queries, ensuring that our query rewriting
	// produces the same results as the original queries without any added optimizations
	// on the query engine.
	engine := promql.NewEngine(engineopts.NewTestEngineOpts().CommonOpts)

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

			qRewritten, err := engine.NewRangeQuery(ctx, queryable, nil, expected, startTime, endTime, step)
			require.NoError(t, err)
			resRewritten := qRewritten.Exec(context.Background())
			require.NoError(t, resRewritten.Err)

			testutils.RequireEqualResults(t, "", resInput, resRewritten, true)
		})
	}
}
