// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestInsertSyntheticInfoNameMatcher(t *testing.T) {
	// Note: the printer pulls all __name__ matchers to the front of the matcher
	// list, and the parser sorts __name__ matchers internally (so e.g.
	// `{__name__=~"...",__name__!="..."}` round-trips as `{__name__!="...",__name__=~"..."}`).
	// Expectations below reflect the printed form after the pass runs.
	testCases := map[string]string{
		// Only-negative __name__ matcher: synthetic .+_info appended.
		`info(metric, {__name__!~".+_info"})`:                 `info(metric, {__name__!~".+_info",__name__=~".+_info"})`,
		`info(metric, {__name__!="target_info"})`:             `info(metric, {__name__!="target_info",__name__=~".+_info"})`,
		`info(metric, {__name__!~".+_info", data=~".+"})`:     `info(metric, {__name__!~".+_info",__name__=~".+_info",data=~".+"})`,
		`info(metric, {__name__!="target_info", data=~".+"})`: `info(metric, {__name__!="target_info",__name__=~".+_info",data=~".+"})`,

		// Positive __name__ matcher present (with or without negatives): no change.
		`info(metric, {__name__=~".+_info"})`:                         `info(metric, {__name__=~".+_info"})`,
		`info(metric, {__name__="target_info"})`:                      `info(metric, {__name__="target_info"})`,
		`info(metric, {__name__=~".+_info",__name__!="target_info"})`: `info(metric, {__name__!="target_info",__name__=~".+_info"})`,

		// No __name__ matcher: handled by InsertOmittedTargetInfoSelector, this pass is a no-op.
		`info(metric, {data=~".+"})`: `info(metric, {data=~".+"})`,

		// info() with one arg: no second-arg selector to mutate.
		`info(metric)`: `info(metric)`,
	}

	ctx := context.Background()
	insertSyntheticInfoNameMatcher := &ast.InsertSyntheticInfoNameMatcher{}

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			result := runASTOptimizationPassWithoutMetrics(t, ctx, input, insertSyntheticInfoNameMatcher)
			require.Equal(t, expected, result.String())

			// Re-run the pass on its own output: it must be a no-op (idempotent).
			result2 := runASTOptimizationPassWithoutMetrics(t, ctx, result.String(), insertSyntheticInfoNameMatcher)
			require.Equal(t, expected, result2.String(), "pass should be idempotent")
		})
	}
}
