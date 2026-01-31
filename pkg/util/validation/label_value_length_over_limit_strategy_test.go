// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLabelValueLengthOverLimitStrategy_Unmarshal(t *testing.T) {
	testCases := []struct {
		input   string
		want    LabelValueLengthOverLimitStrategy
		wantErr bool
	}{
		{
			input:   "invalid",
			wantErr: true,
		},
		{
			input: "",
			want:  LabelValueLengthOverLimitStrategyError,
		},
		{
			input: "error",
			want:  LabelValueLengthOverLimitStrategyError,
		},
		{
			input: "truncate",
			want:  LabelValueLengthOverLimitStrategyTruncate,
		},
		{
			input: "drop",
			want:  LabelValueLengthOverLimitStrategyDrop,
		},
	}
	for _, tc := range testCases {
		t.Run("input="+tc.input, func(t *testing.T) {
			for _, format := range []struct {
				name string
				set  func(*testing.T, *LabelValueLengthOverLimitStrategy, string) error
			}{
				{name: "json", set: func(t *testing.T, v *LabelValueLengthOverLimitStrategy, input string) error {
					t.Helper()
					b, err := json.Marshal(input)
					require.NoError(t, err)
					return json.Unmarshal(b, &v)
				}},
				{name: "yaml", set: func(t *testing.T, v *LabelValueLengthOverLimitStrategy, input string) error {
					t.Helper()
					b, err := yaml.Marshal(input)
					require.NoError(t, err)
					return yaml.Unmarshal(b, &v)
				}},
				{name: "flag", set: func(_ *testing.T, v *LabelValueLengthOverLimitStrategy, input string) error {
					return v.Set(input)
				}},
			} {
				t.Run(format.name, func(t *testing.T) {
					t.Parallel()

					var got LabelValueLengthOverLimitStrategy
					err := format.set(t, &got, tc.input)
					if tc.wantErr {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, got)

					expectedString := tc.input
					if expectedString == "" {
						expectedString = "error"
					}
					require.Equal(t, expectedString, got.String())
				})
			}
		})
	}
}
