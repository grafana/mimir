// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Most of the operator logic is exercised by the tests in pkg/streamingpromql/testdata/ours/multi_aggregation.test.
// The tests below cover behaviour that is difficult or impossible to exercise through PromQL test scripts.
func TestOperator_FinalizeAndCloseBehaviour(t *testing.T) {
	ctx := context.Background()
	inner := &operators.TestOperator{}
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	group := NewMultiAggregatorGroupEvaluator(inner, memoryConsumptionTracker)

	instance1 := group.AddInstance()
	instance2 := group.AddInstance()

	require.NoError(t, instance1.Finalize(ctx))
	require.False(t, inner.Finalized, "should only finalize inner operator after all instances have been finalized")
	require.NoError(t, instance1.Finalize(ctx))
	require.False(t, inner.Finalized, "should ignore second Finalize call from instance already finalized")
	require.NoError(t, instance2.Finalize(ctx))
	require.True(t, inner.Finalized, "should finalize inner operator after all instances have been finalized")

	instance1.Close()
	require.False(t, inner.Closed, "should only close inner operator after all instances have been closed")
	instance1.Close()
	require.False(t, inner.Closed, "should ignore second Close call from instance already closed")
	instance2.Close()
	require.True(t, inner.Closed, "should close inner operator after all instances have been closed")
}
