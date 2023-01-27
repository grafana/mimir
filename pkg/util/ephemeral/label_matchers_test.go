// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestParseLabelMatchers(t *testing.T) {
	type testCase struct {
		name           string
		inputStringArg string
		inputYamlBlob  string
		inputJSONBlob  string
		expect         LabelMatchers
		expectErr      bool
	}

	testCases := []testCase{
		{
			name:           "simple api matcher for metric name",
			inputStringArg: `api:{__name__="foo"}`,
			inputYamlBlob: `api:
    - '{__name__="foo"}'
`,
			inputJSONBlob: `{"api":["{__name__=\"foo\"}"]}`,
			expect: LabelMatchers{
				raw: map[Source][]string{API: {`{__name__="foo"}`}},
				bySource: map[Source]MatcherSetsForSource{
					ANY: {{{
						Type:  labels.MatchEqual,
						Name:  "__name__",
						Value: "foo",
					}}},
					API: {{{
						Type:  labels.MatchEqual,
						Name:  "__name__",
						Value: "foo",
					}}},
				},
				string: "api:{__name__=\"foo\"}",
			},
			expectErr: false,
		}, {
			name:           "two matcher sets with two matchers each per source, unsorted",
			inputStringArg: `api:{__name__="bar_api", testLabel2="testValue2"};rule:{__name__="foo_rule", testLabel1="testValue1"};rule:{__name__="bar_rule", testLabel2="testValue2"};api:{__name__="foo_api", testLabel1="testValue1"};any:{__name__="foo_any", testLabel1="testValue1"};any:{__name__="bar_any", testLabel2="testValue2"}`,
			inputYamlBlob: `any:
    - '{__name__="foo_any", testLabel1="testValue1"}'
    - '{__name__="bar_any", testLabel2="testValue2"}'
api:
    - '{__name__="bar_api", testLabel2="testValue2"}'
    - '{__name__="foo_api", testLabel1="testValue1"}'
rule:
    - '{__name__="foo_rule", testLabel1="testValue1"}'
    - '{__name__="bar_rule", testLabel2="testValue2"}'
`,
			inputJSONBlob: `{"any":["{__name__=\"foo_any\", testLabel1=\"testValue1\"}","{__name__=\"bar_any\", testLabel2=\"testValue2\"}"],"api":["{__name__=\"bar_api\", testLabel2=\"testValue2\"}","{__name__=\"foo_api\", testLabel1=\"testValue1\"}"],"rule":["{__name__=\"foo_rule\", testLabel1=\"testValue1\"}","{__name__=\"bar_rule\", testLabel2=\"testValue2\"}"]}`,
			expect: LabelMatchers{
				raw: map[Source][]string{
					API:  {`{__name__="bar_api", testLabel2="testValue2"}`, `{__name__="foo_api", testLabel1="testValue1"}`},
					RULE: {`{__name__="foo_rule", testLabel1="testValue1"}`, `{__name__="bar_rule", testLabel2="testValue2"}`},
					ANY:  {`{__name__="foo_any", testLabel1="testValue1"}`, `{__name__="bar_any", testLabel2="testValue2"}`},
				},
				bySource: map[Source]MatcherSetsForSource{
					API: {{
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_any"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_any"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_api"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_api"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}},
					RULE: {{
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_any"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_any"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_rule"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_rule"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}},
					ANY: {{
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_any"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_any"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_api"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_api"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "foo_rule"},
						{Type: labels.MatchEqual, Name: "testLabel1", Value: "testValue1"},
					}, {
						{Type: labels.MatchEqual, Name: "__name__", Value: "bar_rule"},
						{Type: labels.MatchEqual, Name: "testLabel2", Value: "testValue2"},
					}},
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

				if !tc.expectErr {
					t.Run("marshal yaml", func(t *testing.T) {
						gotYaml, err := yaml.Marshal(&got)
						require.NoError(t, err)
						require.Equal(t, tc.inputYamlBlob, string(gotYaml))
					})

					t.Run("marshal yaml (non-pointer)", func(t *testing.T) {
						gotYaml, err := yaml.Marshal(got)
						require.NoError(t, err)
						require.Equal(t, tc.inputYamlBlob, string(gotYaml))
					})
				}
			})

			t.Run("unmarshal json", func(t *testing.T) {
				got := LabelMatchers{}
				gotErr := json.Unmarshal([]byte(tc.inputJSONBlob), &got)
				check(t, tc.expect, got, tc.expectErr, gotErr)

				if !tc.expectErr {
					t.Run("marshal json", func(t *testing.T) {
						gotJSON, err := json.Marshal(&got)
						require.NoError(t, err)
						require.Equal(t, tc.inputJSONBlob, string(gotJSON))
					})

					t.Run("marshal json (non-pointer)", func(t *testing.T) {
						gotJSON, err := json.Marshal(got)
						require.NoError(t, err)
						require.Equal(t, tc.inputJSONBlob, string(gotJSON))
					})
				}
			})

			t.Run("set string arg", func(t *testing.T) {
				got := LabelMatchers{}
				gotErr := got.Set(tc.inputStringArg)
				check(t, tc.expect, got, tc.expectErr, gotErr)
			})
		})
	}
}

func TestIsEphemeral(t *testing.T) {
	labelBuilder := labels.NewBuilder(nil)
	labelBuilder.Set("__name__", "test_metric")
	labelBuilder.Set("testLabel1", "testValue1")
	testLabels := mimirpb.FromLabelsToLabelAdapters(labelBuilder.Labels(nil))

	type testCase struct {
		name         string
		matchers     string
		source       mimirpb.WriteRequest_SourceEnum
		seriesLabels []mimirpb.LabelAdapter
		expectResult bool
	}

	testCases := []testCase{
		{
			name:         "no matchers",
			matchers:     "",
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: false,
		}, {
			name:         "matching labels but with different source",
			matchers:     `api:{__name__="test_metric"}`,
			source:       mimirpb.RULE,
			seriesLabels: testLabels,
			expectResult: false,
		}, {
			name:         "matching source but with different labels",
			matchers:     `api:{__name__="different_metric"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: false,
		}, {
			name:         "matching source and labels, matching on metric name",
			matchers:     `api:{__name__="test_metric"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: true,
		}, {
			name:         "matching source and labels, matching on other label",
			matchers:     `api:{testLabel1="testValue1"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: true,
		}, {
			name:         "matching source and labels, matching on both labels",
			matchers:     `api:{__name__="test_metric", testLabel1="testValue1"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: true,
		}, {
			name:         "matching source and labels, matching on both labels, unsorted",
			matchers:     `api:{testLabel1="testValue1", __name__="test_metric"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: true,
		}, {
			name:         "matching rule for source 'any'",
			matchers:     `any:{testLabel1="testValue1", __name__="test_metric"}`,
			source:       mimirpb.API,
			seriesLabels: testLabels,
			expectResult: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lb LabelMatchers
			require.NoError(t, lb.Set(tc.matchers))

			lbForSource := lb.ForSource(tc.source)
			got := lbForSource.ShouldMarkEphemeral(tc.seriesLabels)
			require.Equal(t, tc.expectResult, got)
		})
	}
}
