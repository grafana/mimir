// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestEnsureInfoHasPositiveNameMatcher(t *testing.T) {
	// Note: the printer pulls all __name__ matchers to the front of the matcher
	// list, and the parser sorts __name__ matchers internally (so e.g.
	// `{__name__=~"...",__name__!="..."}` round-trips as `{__name__!="...",__name__=~"..."}`).
	// Expectations below reflect the printed form after the pass runs.
	testCases := map[string]string{
		// Missing 2nd arg: default target_info selector inserted.
		`info(metric)`:      `info(metric, target_info)`,
		`info(metric @ 60)`: `info(metric @ 60.000, target_info)`,
		`1 + info(metric)`:  `1 + info(metric, target_info)`,
		`info(metric + 1)`:  `info(metric + 1, target_info)`,

		// 2nd arg with no __name__ matcher: target_info= appended.
		`info(metric, {env="prod"})`: `info(metric, {__name__="target_info",env="prod"})`,
		`info(metric, {data=~".+"})`: `info(metric, {__name__="target_info",data=~".+"})`,

		// 2nd arg with only-negative __name__ matchers: synthetic .+_info appended.
		`info(metric, {__name__!~".+_info"})`:                 `info(metric, {__name__!~".+_info",__name__=~".+_info"})`,
		`info(metric, {__name__!="target_info"})`:             `info(metric, {__name__!="target_info",__name__=~".+_info"})`,
		`info(metric, {__name__!~".+_info", data=~".+"})`:     `info(metric, {__name__!~".+_info",__name__=~".+_info",data=~".+"})`,
		`info(metric, {__name__!="target_info", data=~".+"})`: `info(metric, {__name__!="target_info",__name__=~".+_info",data=~".+"})`,

		// 2nd arg with at least one positive __name__ matcher: no change.
		`info(metric, {__name__=~".+_info"})`:                         `info(metric, {__name__=~".+_info"})`,
		`info(metric, {__name__="target_info"})`:                      `info(metric, {__name__="target_info"})`,
		`info(metric, {__name__=~".+_info",__name__!="target_info"})`: `info(metric, {__name__!="target_info",__name__=~".+_info"})`,
	}

	ctx := context.Background()
	pass := &ast.EnsureInfoHasPositiveNameMatcher{}

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			result := runASTOptimizationPassWithoutMetrics(t, ctx, input, pass)
			require.Equal(t, expected, result.String())

			// Re-apply the pass on the already-mutated AST: it must be a no-op
			// (idempotent). We can't round-trip through the parser here because
			// some valid post-pass forms (e.g. info(metric, target_info)) parse
			// only when emitted by an AST pass, not from raw query strings.
			result2, err := pass.Apply(ctx, result)
			require.NoError(t, err)
			require.Equal(t, expected, result2.String(), "pass should be idempotent")
		})
	}
}
