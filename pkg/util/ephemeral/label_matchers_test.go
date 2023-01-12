// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestNewLabelMatchers(t *testing.T) {
	type testCase struct {
		name   string
		input  []string
		exp    LabelMatchers
		expErr bool
	}

	testCases := []testCase{
		{
			name:  "simple matcher for metric name",
			input: []string{"{__name__=\"foo\"}"},
			exp: LabelMatchers{
				source: []string{"{__name__=\"foo\"}"},
				config: []matcherSet{
					[]*labels.Matcher{
						{
							Type:  labels.MatchEqual,
							Name:  "__name__",
							Value: "foo",
						},
					},
				},
				string: "{__name__=\"foo\"}",
			},
			expErr: false,
		}, {
			name:  "two matcher sets with two matchers each",
			input: []string{"{__name__=\"foo\", testLabel1=\"testValue1\"}", "{__name__=\"bar\", testLabel2=\"testValue2\"}"},
			exp: LabelMatchers{
				source: []string{"{__name__=\"foo\", testLabel1=\"testValue1\"}", "{__name__=\"bar\", testLabel2=\"testValue2\"}"},
				config: []matcherSet{
					[]*labels.Matcher{
						{
							Type:  labels.MatchEqual,
							Name:  "__name__",
							Value: "foo",
						}, {
							Type:  labels.MatchEqual,
							Name:  "testLabel1",
							Value: "testValue1",
						},
					},
					{
						{
							Type:  labels.MatchEqual,
							Name:  "__name__",
							Value: "bar",
						}, {
							Type:  labels.MatchEqual,
							Name:  "testLabel2",
							Value: "testValue2",
						},
					},
				},
				string: "{__name__=\"foo\", testLabel1=\"testValue1\"};{__name__=\"bar\", testLabel2=\"testValue2\"}",
			},
			expErr: false,
		}, {
			name:  "two matcher sets with two matchers each",
			input: []string{"{__name__=\"foo\", testLabel1=\"testValue1\"}", "{__name__=\"bar\", testLabel2=\"testValue2\"}"},
			exp: LabelMatchers{
				source: []string{"{__name__=\"foo\", testLabel1=\"testValue1\"}", "{__name__=\"bar\", testLabel2=\"testValue2\"}"},
				config: []matcherSet{
					[]*labels.Matcher{
						{
							Type:  labels.MatchEqual,
							Name:  "__name__",
							Value: "foo",
						}, {
							Type:  labels.MatchEqual,
							Name:  "testLabel1",
							Value: "testValue1",
						},
					},
					{
						{
							Type:  labels.MatchEqual,
							Name:  "__name__",
							Value: "bar",
						}, {
							Type:  labels.MatchEqual,
							Name:  "testLabel2",
							Value: "testValue2",
						},
					},
				},
				string: "{__name__=\"foo\", testLabel1=\"testValue1\"};{__name__=\"bar\", testLabel2=\"testValue2\"}",
			},
			expErr: false,
		}, {
			name:  "invalid matcher",
			input: []string{"{__name__===\"foo\"}"},
			exp: LabelMatchers{
				source: []string{"{__name__===\"foo\"}"},
				config: []matcherSet{},
			},
			expErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := NewLabelMatchers(tc.input)
			if tc.expErr {
				require.Error(t, gotErr)
			} else {
				require.NoError(t, gotErr)
			}
			require.Equal(t, tc.exp, got)
		})
	}
}
