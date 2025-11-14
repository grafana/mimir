// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestSubquery_CloseWithoutPrepare(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	inner := &TestOperator{
		Series:                   []labels.Labels{},
		Data:                     []types.InstantVectorSeriesData{},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	// Create time ranges
	parentQueryTimeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))

	subqueryTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)

	subquery, err := NewSubquery(
		inner,
		parentQueryTimeRange,
		subqueryTimeRange,
		nil,
		0,
		time.Minute,
		posrange.PositionRange{Start: 0, End: 10},
		memoryConsumptionTracker,
	)
	require.NoError(t, err)

	// Call Close() twice without calling Prepare() first
	// This should not panic or cause any issues
	subquery.Close()
	subquery.Close()
}
