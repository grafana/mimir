package kafka

import (
	"testing"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/stretchr/testify/require"
)

type kafkaKeyTestCase struct {
	name      string
	buf       []byte
	user      []byte
	labels    []mimirpb.LabelAdapter
	rules     validation.ForwardingRules
	expectKey []byte
}

var kafkaKeyTestCases = []kafkaKeyTestCase{
	{
		name: "simple happy case",
		buf:  nil,
		user: []byte("user"),
		labels: []mimirpb.LabelAdapter{
			{
				Name:  "__name__",
				Value: "test_metric",
			}, {
				Name:  "foo",
				Value: "bar",
			}, {
				Name:  "baz",
				Value: "qux",
			},
		},
		rules: validation.ForwardingRules{
			"test_metric": {
				DropLabels: []string{"baz"},
			},
		},
		expectKey: []byte("user\xfetest_metric{__dropped_labels__=\"baz\",foo=\"bar\"}"),
	}, {
		name: "more complicated happy case",
		buf:  nil,
		user: []byte("user"),
		labels: []mimirpb.LabelAdapter{
			{
				Name:  "label5",
				Value: "value5",
			}, {
				Name:  "label4",
				Value: "value4",
			}, {
				Name:  "__000__",
				Value: "special_label",
			}, {
				Name:  "label3",
				Value: "value3",
			}, {
				Name:  "__name__",
				Value: "test_metric",
			}, {
				Name:  "label2",
				Value: "value2",
			}, {
				Name:  "label1",
				Value: "value1",
			}, {
				Name:  "label0",
				Value: "value0",
			},
		},
		rules: validation.ForwardingRules{
			"test_metric": {
				DropLabels: []string{"label5", "label3", "label0"},
			},
		},
		expectKey: []byte("user\xfetest_metric{__000__=\"special_label\",__dropped_labels__=\"label0,label3,label5\",label1=\"value1\",label2=\"value2\",label4=\"value4\"}"),
	}, {
		name: "dropping all labels",
		buf:  nil,
		user: []byte("user"),
		labels: []mimirpb.LabelAdapter{
			{
				Name:  "__name__",
				Value: "test_metric",
			}, {
				Name:  "foo",
				Value: "bar",
			},
		},
		rules: validation.ForwardingRules{
			"test_metric": {
				DropLabels: []string{"foo"},
			},
		},
		expectKey: []byte("user\xfetest_metric{__dropped_labels__=\"foo\"}"),
	}, {
		name: "dropping multiple labels",
		buf:  nil,
		user: []byte("user"),
		labels: []mimirpb.LabelAdapter{
			{
				Name:  "__name__",
				Value: "test_metric",
			}, {
				Name:  "foo",
				Value: "bar",
			}, {
				Name:  "baz",
				Value: "qux",
			},
		},
		rules: validation.ForwardingRules{
			"test_metric": {
				DropLabels: []string{"foo", "baz"},
			},
		},
		expectKey: []byte("user\xfetest_metric{__dropped_labels__=\"baz,foo\"}"),
	},
}

func TestComposeKafkaKey(t *testing.T) {
	for _, tc := range kafkaKeyTestCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.rules = prepareRules(tc.rules)
			gotKey, err := ComposeKafkaKey(tc.buf, tc.user, tc.labels, tc.rules)
			require.NoError(t, err)
			require.Equal(t, string(tc.expectKey), string(gotKey))
		})
	}
}

func BenchmarkComposeKafkaKey(b *testing.B) {
	for _, tc := range kafkaKeyTestCases {
		tc.rules = prepareRules(tc.rules)

		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tc.buf, _ = ComposeKafkaKey(tc.buf, tc.user, tc.labels, tc.rules)

				// We validate for correctness in the test, no need to skew the benchmark to validate correctness again.
				// require.NoError(b, err)
				// require.Equal(b, tc.expectKey, tc.buf)
			}
		})
	}
}

func prepareRules(rules validation.ForwardingRules) validation.ForwardingRules {
	for metric, rule := range rules {
		rule.Prepare()
		rules[metric] = rule
	}
	return rules
}
