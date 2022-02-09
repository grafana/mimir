package ingester

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

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
			request:  &client.MetricsMetadataRequest{},
			expected: "test: user=\"\" trace=\"\" request=&MetricsMetadataRequest{}",
		},
		{
			request:  &client.LabelValuesCardinalityRequest{LabelNames: []string{"hello", "world"}, Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "test", Value: "value"}}},
			expected: "test: user=\"\" trace=\"\" request=&LabelValuesCardinalityRequest{LabelNames:[hello world],Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:test,Value:value,},},}",
		},
	} {
		assert.Equal(t, tc.expected, requestActivity(context.Background(), "test", tc.request))
	}
}
