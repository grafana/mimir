// SPDX-License-Identifier: AGPL-3.0-only

//go:build !goexperiment.simd

package aggregations

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func (g *SumAggregationGroup) accumulateFloatPoints(points []promql.FPoint, timeRange types.QueryTimeRange) {
	g.accumulateFloatsFallback(points, timeRange)
}
