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
		name: "normal happy case",
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
		expectKey: []byte("user\xfe{__dropped_labels__=\"baz\", __name__=\"test_metric\", foo=\"bar\"}"),
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
		expectKey: []byte("user\xfe{__dropped_labels__=\"foo\", __name__=\"test_metric\"}"),
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
		expectKey: []byte("user\xfe{__dropped_labels__=\"baz,foo\", __name__=\"test_metric\"}"),
	},
}

func TestComposeKafkaKey(t *testing.T) {
	for _, tc := range kafkaKeyTestCases {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, err := ComposeKafkaKey(tc.buf, tc.user, tc.labels, tc.rules)
			require.NoError(t, err)
			require.Equal(t, tc.expectKey, gotKey)
		})
	}
}

func BenchmarkComposeKafkaKey(b *testing.B) {
	for _, tc := range kafkaKeyTestCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := ComposeKafkaKey(tc.buf, tc.user, tc.labels, tc.rules)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
