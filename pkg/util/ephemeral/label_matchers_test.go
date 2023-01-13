// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestParseLabelMatchers(t *testing.T) {
	type testCase struct {
		name           string
		inputStringArg string
		inputYamlBlob  string
		expect         LabelMatchers
		expectErr      bool
	}

	testCases := []testCase{
		{
			name:           "simple api matcher for metric name",
			inputStringArg: `api:{__name__="foo"}`,
			inputYamlBlob: `
api:
- '{__name__="foo"}'
`,
			expect: LabelMatchers{
				raw: map[source][]string{api: {`{__name__="foo"}`}},
				config: map[source][]matcherSet{api: {{{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "foo",
				}}}},
				string: "api:{__name__=\"foo\"}",
			},
			expectErr: false,
		}, {
			name:           "two matcher sets with two matchers each per source, unsorted",
			inputStringArg: `api:{__name__="bar_api", testLabel2="testValue2"};rule:{__name__="foo_rule", testLabel1="testValue1"};rule:{__name__="bar_rule", testLabel2="testValue2"};api:{__name__="foo_api", testLabel1="testValue1"};any:{__name__="foo_any", testLabel1="testValue1"};any:{__name__="bar_any", testLabel2="testValue2"}`,
			inputYamlBlob: `
api:
- '{__name__="bar_api", testLabel2="testValue2"}'
- '{__name__="foo_api", testLabel1="testValue1"}'
rule:
- '{__name__="foo_rule", testLabel1="testValue1"}'
- '{__name__="bar_rule", testLabel2="testValue2"}'
any:
- '{__name__="foo_any", testLabel1="testValue1"}'
- '{__name__="bar_any", testLabel2="testValue2"}'
`,
			expect: LabelMatchers{
				raw: map[source][]string{
					api:  {`{__name__="bar_api", testLabel2="testValue2"}`, `{__name__="foo_api", testLabel1="testValue1"}`},
					rule: {`{__name__="foo_rule", testLabel1="testValue1"}`, `{__name__="bar_rule", testLabel2="testValue2"}`},
					any:  {`{__name__="foo_any", testLabel1="testValue1"}`, `{__name__="bar_any", testLabel2="testValue2"}`},
				},
				config: map[source][]matcherSet{
					api: {
						{
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "bar_api",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel2",
								Value: "testValue2",
							},
						},
						{
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "foo_api",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel1",
								Value: "testValue1",
							},
						},
					},
					rule: {
						{
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "foo_rule",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel1",
								Value: "testValue1",
							},
						}, {
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "bar_rule",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel2",
								Value: "testValue2",
							},
						},
					},
					any: {
						{
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "foo_any",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel1",
								Value: "testValue1",
							},
						}, {
							{
								Type:  labels.MatchEqual,
								Name:  "__name__",
								Value: "bar_any",
							}, {
								Type:  labels.MatchEqual,
								Name:  "testLabel2",
								Value: "testValue2",
							},
						},
					},
				},
				string: `any:{__name__="foo_any", testLabel1="testValue1"};any:{__name__="bar_any", testLabel2="testValue2"};api:{__name__="bar_api", testLabel2="testValue2"};api:{__name__="foo_api", testLabel1="testValue1"};rule:{__name__="foo_rule", testLabel1="testValue1"};rule:{__name__="bar_rule", testLabel2="testValue2"}`,
			},
			expectErr: false,
		}, {
			name:           "invalid matcher",
			inputStringArg: `{__name__==="foo"}`,
			inputYamlBlob: `
api:
- '{__name__==="foo"}'
`,
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			check := func(t *testing.T, expect, got LabelMatchers, expectErr bool, gotErr error) {
				if expectErr {
					require.Error(t, gotErr)
				} else {
					require.NoError(t, gotErr)
					require.Equal(t, expect, got)
				}
			}

			t.Run("unmarshal yaml", func(t *testing.T) {
				got := LabelMatchers{}
				gotErr := yaml.Unmarshal([]byte(tc.inputYamlBlob), &got)
				check(t, tc.expect, got, tc.expectErr, gotErr)
			})

			t.Run("set string arg", func(t *testing.T) {
				got := LabelMatchers{}
				gotErr := got.Set(tc.inputStringArg)
				check(t, tc.expect, got, tc.expectErr, gotErr)
			})
		})
	}
}
