// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestFunctionCall_Describe(t *testing.T) {
	t.Run("ordinary function call", func(t *testing.T) {
		f := &FunctionCall{
			FunctionCallDetails: &FunctionCallDetails{
				FunctionName: "foo",
			},
		}

		require.Equal(t, "foo(...)", f.Describe())
	})

	t.Run("absent()", func(t *testing.T) {
		f := &FunctionCall{
			FunctionCallDetails: &FunctionCallDetails{
				FunctionName: "absent",
				AbsentLabels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "foo", "env", "bar")),
			},
		}

		require.Equal(t, `absent(...) with labels foo{env="bar"}`, f.Describe())
	})

	t.Run("absent_over_time()", func(t *testing.T) {
		f := &FunctionCall{
			FunctionCallDetails: &FunctionCallDetails{
				FunctionName: "absent_over_time",
				AbsentLabels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "foo", "env", "bar")),
			},
		}

		require.Equal(t, `absent_over_time(...) with labels foo{env="bar"}`, f.Describe())
	})
}

func TestFunctionCall_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical without labels for absent()/absent_over_time()": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			expectEquivalent: true,
		},
		"identical with labels for absent()/absent_over_time()": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					AbsentLabels:       []mimirpb.LabelAdapter{{Name: "env", Value: "prod"}},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					AbsentLabels:       []mimirpb.LabelAdapter{{Name: "env", Value: "prod"}},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			expectEquivalent: true,
		},
		"different function": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "bar",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different children": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(13)},
			},
			expectEquivalent: false,
		},
		"different labels for absent()/absent_over_time()": {
			a: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					AbsentLabels:       []mimirpb.LabelAdapter{{Name: "env", Value: "prod"}},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			b: &FunctionCall{
				FunctionCallDetails: &FunctionCallDetails{
					FunctionName:       "foo",
					AbsentLabels:       []mimirpb.LabelAdapter{{Name: "env", Value: "test"}},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Args: []planning.Node{numberLiteralOf(12)},
			},
			expectEquivalent: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expectEquivalent, testCase.a.EquivalentTo(testCase.b))
			require.Equal(t, testCase.expectEquivalent, testCase.b.EquivalentTo(testCase.a))

			require.True(t, testCase.a.EquivalentTo(testCase.a))
			require.True(t, testCase.b.EquivalentTo(testCase.b))
		})
	}
}
