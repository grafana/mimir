// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestHandleInfoFunc(t *testing.T) {
	enableExperimentalFunctions := parser.EnableExperimentalFunctions
	t.Cleanup(func() {
		parser.EnableExperimentalFunctions = enableExperimentalFunctions
	})
	parser.EnableExperimentalFunctions = true

	testCases := map[string]string{
		"info(metric)":      "info(metric, target_info)",
		"info(metric @ 60)": "info(metric @ 60.000, target_info)",
		"1 + info(metric)":  "1 + info(metric, target_info)",
		"info(metric + 1)":  "info(metric + 1, target_info)",
	}

	ctx := context.Background()
	insertOmittedTargetInfoSelector := &ast.InsertOmittedTargetInfoSelector{}

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			result := runASTOptimizationPassWithoutMetrics(t, ctx, input, insertOmittedTargetInfoSelector)
			require.Equal(t, expected, result.String())
		})
	}
}
