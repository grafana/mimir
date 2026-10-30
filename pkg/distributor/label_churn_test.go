// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"

	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

func TestMergeLabelChurnResponses(t *testing.T) {
	partitionZeroResponse := &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 1000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 10, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
			{LabelName: "job", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
		},
	}
	partitionOneResponse := &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 2000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 4, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "other", SampledSeries: 10}, {MetricName: "up", SampledSeries: 3}}},
		},
	}

	merged := mergeLabelChurnResponses([][]zonedLabelChurnResponse{
		// Both zones of partition 0 respond with the same series, thus only one of the responses counts.
		{{zone: "zone-a", response: partitionZeroResponse}, {zone: "zone-b", response: partitionZeroResponse}},
		{{zone: "zone-b", response: partitionOneResponse}},
		{},
	})

	require.Equal(t, &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 2000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 14, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "other", SampledSeries: 10}, {MetricName: "up", SampledSeries: 8}}},
			{LabelName: "job", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
		},
	}, merged)
}
