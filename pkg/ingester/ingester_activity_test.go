// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestRequestActivity(t *testing.T) {
	for _, tc := range []struct {
		request  interface{}
		expected string
	}{
		{
			request:  nil,
			expected: "test: user=\"\" trace=\"\" request=<nil>",
		},
		{
			request:  client.DefaultMetricsMetadataRequest(),
			expected: "test: user=\"\" trace=\"\" request=&MetricsMetadataRequest{Limit:-1,LimitPerMetric:-1,Metric:,}",
		},
		{
			request:  &client.LabelValuesCardinalityRequest{LabelNames: []string{"hello", "world"}, Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "test", Value: "value"}}, CountMethod: client.IN_MEMORY},
			expected: "test: user=\"\" trace=\"\" request=&LabelValuesCardinalityRequest{LabelNames:[hello world],Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:test,Value:value,},},CountMethod:IN_MEMORY,}",
		},
	} {
		assert.Equal(t, tc.expected, requestActivity(context.Background(), "test", tc.request))
	}
}

func TestQueryRequest_CustomStringer(t *testing.T) {
	tcs := map[string]struct {
		request *client.QueryRequest
	}{
		"nil request": {
			request: nil,
		},
		"empty request": {
			request: &client.QueryRequest{},
		},
		"no matchers": {
			request: &client.QueryRequest{
				StartTimestampMs: rand.Int63(),
				EndTimestampMs:   rand.Int63(),
			},
		},
		"one matcher": {
			request: &client.QueryRequest{
				StartTimestampMs: rand.Int63(),
				EndTimestampMs:   rand.Int63(),
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: "n_1", Value: "v_1"},
				},
			},
		},
		"multiple matchers": {
			request: &client.QueryRequest{
				StartTimestampMs: rand.Int63(),
				EndTimestampMs:   rand.Int63(),
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: "n_1", Value: "v_1"},
					{Type: client.EQUAL, Name: "n_2", Value: "v_2"},
					{Type: client.EQUAL, Name: "n_3", Value: "v_3"},
				},
			},
		},
		"one matcher projection includes": {
			request: &client.QueryRequest{
				StartTimestampMs: rand.Int63(),
				EndTimestampMs:   rand.Int63(),
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: "n_1", Value: "v_1"},
				},
				ProjectionInclude: true,
				ProjectionLabels:  []string{"env", "region"},
			},
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			sb := bytes.NewBuffer(nil)
			queryRequestToString(sb, tc.request)

			require.Equal(t, tc.request.String(), sb.String())
		})
	}
}

func BenchmarkRequestActivity(b *testing.B) {
	request := &client.QueryRequest{
		StartTimestampMs: rand.Int63(),
		EndTimestampMs:   rand.Int63(),
		Matchers: []*client.LabelMatcher{
			{Type: client.EQUAL, Name: "a_name", Value: "a_value"},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		requestActivity(context.Background(), "Ingester/QueryStream", request)
	}
}
