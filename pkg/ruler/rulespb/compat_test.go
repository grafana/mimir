// SPDX-License-Identifier: AGPL-3.0-only

package rulespb

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirpb" //lint:ignore faillint allowed to import other protobuf
)

func TestProtoConversionShouldBeIdempotent(t *testing.T) {
	for name, group := range map[string]string{
		"no evaluation delay and no query offset": `
name: testrules
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))

    - alert: ThisIsBad
      expr: sum(rate(test_metric[2m]))
      for: 10m
`,

		"with evaluation delay": `
name: testrules
evaluation_delay: 3m
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with query offset": `
name: testrules
query_offset: 2m
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with evaluation delay and source tenants": `
name: testrules
evaluation_delay: 3m
source_tenants:
  - a
  - b
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with query offset and source tenants": `
name: testrules
query_offset: 2m
source_tenants:
  - a
  - b
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with evaluation delay and query offset": `
name: testrules
evaluation_delay: 3m
query_offset: 2m
source_tenants:
  - a
  - b
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,

		"with evaluation delay and source tenants and align of execution time": `
name: testrules
evaluation_delay: 3m
align_evaluation_time_on_interval: true
source_tenants:
  - a
  - b
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,
	} {
		t.Run(name, func(t *testing.T) {
			rg := rulefmt.RuleGroup{}
			require.NoError(t, yaml.Unmarshal([]byte(group), &rg))

			desc := ToProto("user", "namespace", rg)
			newRg := FromProto(desc)

			newYaml, err := yaml.Marshal(newRg)
			require.NoError(t, err)

			assert.YAMLEq(t, group, string(newYaml))
		})
	}
}

func TestToProto(t *testing.T) {
	const (
		user      = "user-1"
		namespace = "namespace"
	)

	tests := map[string]struct {
		input    rulefmt.RuleGroup
		expected *RuleGroupDesc
	}{
		"without evaluation delay and query offset": {
			input: rulefmt.RuleGroup{
				Name:     "group",
				Interval: model.Duration(60 * time.Second),
				Rules:    []rulefmt.Rule{},
				Labels:   map[string]string{},
			},
			expected: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 0,
				Rules:           []*RuleDesc{},
				Labels:          make([]mimirpb.LabelAdapter, 0),
			},
		},
		"with evaluation delay": {
			input: rulefmt.RuleGroup{
				Name:            "group",
				Interval:        model.Duration(60 * time.Second),
				EvaluationDelay: pointerOf(model.Duration(5 * time.Second)),
				Rules:           []rulefmt.Rule{},
				Labels:          map[string]string{},
			},
			expected: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 5 * time.Second,
				Rules:           []*RuleDesc{},
				Labels:          make([]mimirpb.LabelAdapter, 0),
			},
		},
		"with query offset": {
			input: rulefmt.RuleGroup{
				Name:        "group",
				Interval:    model.Duration(60 * time.Second),
				QueryOffset: pointerOf(model.Duration(2 * time.Second)),
				Rules:       []rulefmt.Rule{},
				Labels:      map[string]string{},
			},
			expected: &RuleGroupDesc{
				Name:        "group",
				Namespace:   namespace,
				Interval:    60 * time.Second,
				User:        user,
				QueryOffset: 2 * time.Second,
				Rules:       []*RuleDesc{},
				Labels:      make([]mimirpb.LabelAdapter, 0),
			},
		},
		"with both evaluation delay and query offset": {
			input: rulefmt.RuleGroup{
				Name:            "group",
				Interval:        model.Duration(60 * time.Second),
				EvaluationDelay: pointerOf(model.Duration(5 * time.Second)),
				QueryOffset:     pointerOf(model.Duration(2 * time.Second)),
				Rules:           []rulefmt.Rule{},
				Labels:          map[string]string{},
			},
			expected: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 5 * time.Second,
				QueryOffset:     2 * time.Second,
				Rules:           []*RuleDesc{},
				Labels:          make([]mimirpb.LabelAdapter, 0),
			},
		},
		"with limit": {
			input: rulefmt.RuleGroup{
				Name:     "group",
				Interval: model.Duration(60 * time.Second),
				Rules:    []rulefmt.Rule{},
				Labels:   map[string]string{},
				Limit:    10,
			},
			expected: &RuleGroupDesc{
				Name:      "group",
				Namespace: namespace,
				Interval:  60 * time.Second,
				User:      user,
				Rules:     []*RuleDesc{},
				Labels:    make([]mimirpb.LabelAdapter, 0),
				Limit:     10,
			},
		},
		"check grouplabels are properly propagated": {
			input: rulefmt.RuleGroup{
				Name: "group",
				Labels: map[string]string{
					"groupKey":  "groupValue",
					"groupKey2": "groupValue2",
				},
				Interval: model.Duration(60 * time.Second),
				Rules: []rulefmt.Rule{
					{
						Record: "test_metric:sum:rate1m",
						Expr:   "sum(rate(test_metric[1m]))",
						Labels: map[string]string{
							"recordKey": "recordValue",
						},
						Annotations: make(map[string]string),
					},
					{
						Alert: "alert_name",
						Expr:  "test_metric > 0",
						Labels: map[string]string{
							"alertKey": "alertValue",
						},
						Annotations: make(map[string]string),
					},
				},
			},
			expected: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 0,
				Labels: []mimirpb.LabelAdapter{
					{
						Name:  "groupKey",
						Value: "groupValue",
					},
					{
						Name:  "groupKey2",
						Value: "groupValue2",
					},
				},
				Rules: []*RuleDesc{
					{
						Record: "test_metric:sum:rate1m",
						Expr:   "sum(rate(test_metric[1m]))",
						Labels: []mimirpb.LabelAdapter{
							{
								Name:  "recordKey",
								Value: "recordValue",
							},
						},
						Annotations: []mimirpb.LabelAdapter{},
					},
					{
						Alert: "alert_name",
						Expr:  "test_metric > 0",
						Labels: []mimirpb.LabelAdapter{
							{
								Name:  "alertKey",
								Value: "alertValue",
							},
						},
						Annotations: []mimirpb.LabelAdapter{},
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := ToProto(user, namespace, testData.input)
			assert.Equal(t, testData.expected, actual)

			// Counter-proof: converting back to Prometheus model should return the input data.
			assert.Equal(t, testData.input, FromProto(actual))
		})
	}
}

func TestFromProto(t *testing.T) {
	const (
		user      = "user-1"
		namespace = "namespace"
	)

	tests := map[string]struct {
		input    *RuleGroupDesc
		expected rulefmt.RuleGroup
	}{
		"without evaluation delay and query offset": {
			input: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 0,
				Rules:           []*RuleDesc{},
				Labels:          []mimirpb.LabelAdapter{},
			},
			expected: rulefmt.RuleGroup{
				Name:     "group",
				Interval: model.Duration(60 * time.Second),
				Rules:    []rulefmt.Rule{},
				Labels:   map[string]string{},
			},
		},
		"with evaluation delay": {
			input: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 5 * time.Second,
				Rules:           []*RuleDesc{},
				Labels:          []mimirpb.LabelAdapter{},
			},
			expected: rulefmt.RuleGroup{
				Name:            "group",
				Interval:        model.Duration(60 * time.Second),
				EvaluationDelay: pointerOf(model.Duration(5 * time.Second)),
				Rules:           []rulefmt.Rule{},
				Labels:          map[string]string{},
			},
		},
		"with query offset": {
			input: &RuleGroupDesc{
				Name:        "group",
				Namespace:   namespace,
				Interval:    60 * time.Second,
				User:        user,
				QueryOffset: 2 * time.Second,
				Rules:       []*RuleDesc{},
				Labels:      []mimirpb.LabelAdapter{},
			},
			expected: rulefmt.RuleGroup{
				Name:        "group",
				Interval:    model.Duration(60 * time.Second),
				QueryOffset: pointerOf(model.Duration(2 * time.Second)),
				Rules:       []rulefmt.Rule{},
				Labels:      map[string]string{},
			},
		},
		"with limit": {
			input: &RuleGroupDesc{
				Name:      "group",
				Namespace: namespace,
				Interval:  60 * time.Second,
				User:      user,
				Rules:     []*RuleDesc{},
				Labels:    []mimirpb.LabelAdapter{},
				Limit:     10,
			},
			expected: rulefmt.RuleGroup{
				Name:     "group",
				Interval: model.Duration(60 * time.Second),
				Rules:    []rulefmt.Rule{},
				Labels:   map[string]string{},
				Limit:    10,
			},
		},
		"with both evaluation delay and query offset": {
			input: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 5 * time.Second,
				QueryOffset:     2 * time.Second,
				Rules:           []*RuleDesc{},
				Labels:          []mimirpb.LabelAdapter{},
			},
			expected: rulefmt.RuleGroup{
				Name:            "group",
				Interval:        model.Duration(60 * time.Second),
				EvaluationDelay: pointerOf(model.Duration(5 * time.Second)),
				QueryOffset:     pointerOf(model.Duration(2 * time.Second)),
				Rules:           []rulefmt.Rule{},
				Labels:          map[string]string{},
			},
		},
		"check grouplabels are properly propagated": {
			input: &RuleGroupDesc{
				Name:            "group",
				Namespace:       namespace,
				Interval:        60 * time.Second,
				User:            user,
				EvaluationDelay: 0,
				Labels: []mimirpb.LabelAdapter{
					{
						Name:  "groupKey",
						Value: "groupValue",
					},
				},
				Rules: []*RuleDesc{
					{
						Record: "test_metric:sum:rate1m",
						Expr:   "sum(rate(test_metric[1m]))",
						Labels: []mimirpb.LabelAdapter{
							{
								Name:  "recordKey",
								Value: "recordValue",
							},
						},
						Annotations: []mimirpb.LabelAdapter{},
					},
					{
						Alert: "alert_name",
						Expr:  "test_metric > 0",
						Labels: []mimirpb.LabelAdapter{
							{
								Name:  "alertKey",
								Value: "alertValue",
							},
						},
						Annotations: []mimirpb.LabelAdapter{},
					},
				},
			},
			expected: rulefmt.RuleGroup{
				Name: "group",
				Labels: map[string]string{
					"groupKey": "groupValue",
				},
				Interval: model.Duration(60 * time.Second),
				Rules: []rulefmt.Rule{
					{
						Record: "test_metric:sum:rate1m",
						Expr:   "sum(rate(test_metric[1m]))",
						Labels: map[string]string{
							"recordKey": "recordValue",
						},
						Annotations: make(map[string]string),
					},
					{
						Alert: "alert_name",
						Expr:  "test_metric > 0",
						Labels: map[string]string{
							"alertKey": "alertValue",
						},
						Annotations: make(map[string]string),
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := FromProto(testData.input)
			assert.Equal(t, testData.expected, actual)

			// Counter-proof: converting back to protobuf model should return the input data.
			assert.Equal(t, testData.input, ToProto(user, namespace, actual))
		})
	}
}

func TestFromProto_ZeroEvaluationDelayOrQueryOffsetIsIgnored(t *testing.T) {
	tests := map[string]struct {
		input string
	}{
		"zero evaluation delay": {
			input: `
name: testrules
evaluation_delay: 0s
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,
		},
		"zero query offset": {
			input: `
name: testrules
query_offset: 0s
rules:
    - record: test_metric:sum:rate1m
      expr: sum(rate(test_metric[1m]))
`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			rg := rulefmt.RuleGroup{}
			require.NoError(t, yaml.Unmarshal([]byte(testData.input), &rg))

			desc := ToProto("user", "namespace", rg)
			newRg := FromProto(desc)

			//nolint:staticcheck // We want to intentionally access a deprecated field
			assert.Nil(t, newRg.EvaluationDelay)
			assert.Nil(t, newRg.QueryOffset)
		})
	}
}
