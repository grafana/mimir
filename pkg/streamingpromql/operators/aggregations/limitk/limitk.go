// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitk

import (
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func New(
	inner types.InstantVectorOperator,
	param types.ScalarOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) types.InstantVectorOperator {
	if without {
		grouping = append(grouping, labels.MetricName)
	}

	slices.Sort(grouping)

	return &Query{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}
