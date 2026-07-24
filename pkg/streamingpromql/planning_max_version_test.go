// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	frontendspinoff "github.com/grafana/mimir/pkg/frontend/querymiddleware/subqueryspinoff"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/subqueryspinoff"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestNewQueryPlan_NeverExceedsMaximumSupportedQueryPlanVersion is a bug reproduction: the planner must never
// dispatch plan nodes whose version exceeds the maximum supported version reported by its version provider,
// otherwise queriers that only support the lower version reject the plan (HTTP 400).
//
// The planner is configured like the query-frontend's remote execution planner (see createQueryFrontendQueryPlanner
// in pkg/mimir/modules.go), with a version provider reporting v17 (as if queriers still running an older version
// were present in the ring). The range query splitting pass wraps step-invariant scalar expressions in a scalar
// Duplicate node, which requires plan version 19, and the remote execution pass then dispatches it to queriers.
//
// This case covers a folded constant: the step-invariant scalar (2 * 3) survives constant folding as a
// step-invariant expression wrapping a bare NumberLiteral.
func TestNewQueryPlan_NeverExceedsMaximumSupportedQueryPlanVersion(t *testing.T) {
	maximumSupportedVersion := planning.QueryPlanV17
	planner := newRemoteExecutionMaxVersionPlanner(t, maximumSupportedVersion)

	expr := `some_metric > bool (2 * 3)`
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	timeRange := types.NewRangeQueryTimeRange(start, start.Add(6*time.Hour), time.Minute)

	p, err := planner.NewQueryPlan(t.Context(), expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	t.Logf("generated plan (version %v):\n%s", p.Version, p.String())

	// This is the plan currently generated (the broken one). The range query splitting pass wraps the
	// step-invariant scalar in a Duplicate node; a scalar Duplicate requires plan version 19, and the
	// remote execution pass dispatches it to queriers as part of the remote execution group. The bug is
	// asserted by the dispatched-subtree version check below, not by this plan shape: this golden just
	// documents the exact plan that triggers the leak.
	generatedPlan := `
		- TimeRangeSplit: interval 24h0m0s
			- Cache: split interval 24h0m0s
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup: eager load
						- node 0: DeduplicateAndMerge
							- BinaryExpression: LHS > bool RHS
								- LHS: VectorSelector: {__name__="some_metric"}
								- RHS: StepInvariantExpression
									- Duplicate
										- NumberLiteral: 6
	`
	require.Equal(t, testutils.TrimIndent(generatedPlan), p.String())

	requireNoDispatchedNodeExceedsMaxVersion(t, p, timeRange, maximumSupportedVersion)
}

// TestNewQueryPlan_NeverExceedsMaximumSupportedQueryPlanVersion_ScalarSelector covers the same bug for a
// step-invariant scalar that is not a bare literal: scalar(some_metric @ end()) is a scalar expression that
// contains a selector. The range query splitting pass still wraps it in a scalar Duplicate (requiring v19),
// but unlike the folded-constant case it cannot be fixed by unwrapping StepInvariantExpression(NumberLiteral):
// the inner is a real selector-bearing expression, so this is the case that needs the actual version gate.
func TestNewQueryPlan_NeverExceedsMaximumSupportedQueryPlanVersion_ScalarSelector(t *testing.T) {
	maximumSupportedVersion := planning.QueryPlanV17
	planner := newRemoteExecutionMaxVersionPlanner(t, maximumSupportedVersion)

	expr := `some_metric > bool scalar(some_metric @ end())`
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	timeRange := types.NewRangeQueryTimeRange(start, start.Add(6*time.Hour), time.Minute)

	p, err := planner.NewQueryPlan(t.Context(), expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	t.Logf("generated plan (version %v):\n%s", p.Version, p.String())

	generatedPlan := `
		- TimeRangeSplit: interval 24h0m0s
			- Cache: split interval 24h0m0s
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup: eager load
						- node 0: DeduplicateAndMerge
							- BinaryExpression: LHS > bool RHS
								- LHS: VectorSelector: {__name__="some_metric"}
								- RHS: StepInvariantExpression
									- Duplicate
										- FunctionCall: scalar(...)
											- VectorSelector: {__name__="some_metric"} @ 1704088800000 (2024-01-01T06:00:00Z)
	`
	require.Equal(t, testutils.TrimIndent(generatedPlan), p.String())

	requireNoDispatchedNodeExceedsMaxVersion(t, p, timeRange, maximumSupportedVersion)
}

// newRemoteExecutionMaxVersionPlanner builds a planner configured like the query-frontend's remote execution
// planner (remote execution + subquery spin-off passes, range query splitting and caching enabled), reporting
// maximumSupportedVersion as the ring-supported plan version.
func newRemoteExecutionMaxVersionPlanner(t *testing.T, maximumSupportedVersion planning.QueryPlanVersion) *streamingpromql.QueryPlanner {
	t.Helper()

	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeQuerySplittingAndCaching.SplitEnabled = true
	opts.RangeQuerySplittingAndCaching.SplitInterval = 24 * time.Hour
	opts.RangeQuerySplittingAndCaching.CacheEnabled = true

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewStaticQueryPlanVersionProvider(maximumSupportedVersion))
	require.NoError(t, err)

	planner.RegisterQueryPlanOptimizationPass(remoteexec.NewOptimizationPass())
	planner.RegisterASTOptimizationPass(subqueryspinoff.NewOptimizationPass(validation.MockDefaultOverrides(), opts.CommonOpts.NoStepSubqueryIntervalFn, frontendspinoff.Options{}, nil, opts.Logger))

	return planner
}

// requireNoDispatchedNodeExceedsMaxVersion asserts the real dispatch invariant enforced in production by
// sendRequest (pkg/frontend/v2/remoteexec.go): queriers validate the plan version computed from the nodes
// dispatched to them, not the whole plan's version. Nodes that only run in the query-frontend (eg. Cache,
// TimeRangeSplit) may legitimately require a higher version, so only the contents of remote execution groups
// are checked.
func requireNoDispatchedNodeExceedsMaxVersion(t *testing.T, p *planning.QueryPlan, timeRange types.QueryTimeRange, maximumSupportedVersion planning.QueryPlanVersion) {
	t.Helper()

	err := optimize.Walk(p.Root, optimize.VisitorFunc(func(node planning.Node, _ []planning.Node) (bool, error) {
		group, ok := node.(*remoteexec.RemoteExecutionGroup)
		if !ok {
			return true, nil
		}

		for _, dispatched := range group.Nodes {
			version, err := planning.MinimumRequiredPlanVersion(dispatched, timeRange)
			if err != nil {
				return false, err
			}

			require.LessOrEqualf(t, version, maximumSupportedVersion, "node dispatched to queriers requires plan version %v, but the maximum version supported by queriers is %v", version, maximumSupportedVersion)
		}

		return true, nil
	}))
	require.NoError(t, err)
}
