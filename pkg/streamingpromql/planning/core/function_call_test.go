// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"testing"
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
