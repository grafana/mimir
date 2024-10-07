// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAssertGatherAndCompare(t *testing.T) {
	g := fakeGatherer{
		metrics: []*dto.MetricFamily{
			{
				Name: proto.String("cortex_distributor_deduped_samples_total"),
				Help: proto.String("The total number of deduplicated samples."),
				Metric: []*dto.Metric{
					{
						Label: []*dto.LabelPair{
							{
								Name:  proto.String("environment"),
								Value: proto.String("test"),
							},
						},
						TimestampMs: proto.Int64(1000),
						Counter: &dto.Counter{
							Value: proto.Float64(1),
						},
					},
				},
			},
		},
	}

	t.Run("don't specify any metrics", func(t *testing.T) {
		// When not specifying any metrics, an error should be returned due to missing metric
		// cortex_distributor_latest_seen_sample_timestamp_seconds.
		err := gatherAndCompare(g, `
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter
		cortex_distributor_deduped_samples_total{environment="test"} 1 1000

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
		`)
		require.EqualError(t, err, ` # HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
 # TYPE cortex_distributor_deduped_samples_total counter
 cortex_distributor_deduped_samples_total{environment="test"} 1 1000
+# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
+# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
+cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
 `)
	})

	t.Run("specify required metric", func(t *testing.T) {
		// When specifying that cortex_distributor_deduped_samples_total as the one required metric,
		// cortex_distributor_latest_seen_sample_timestamp_seconds should be ignored even if it's missing.
		AssertGatherAndCompare(t, g, `
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter
		cortex_distributor_deduped_samples_total{environment="test"} 1 1000

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
		`, "cortex_distributor_deduped_samples_total")
	})

	t.Run("specify required metric which isn't there", func(t *testing.T) {
		// When specifying that cortex_distributor_deduped_samples_total as the one required metric,
		// cortex_distributor_latest_seen_sample_timestamp_seconds should be ignored even if it's missing.
		err := gatherAndCompare(g, `
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter
		cortex_distributor_deduped_samples_total{environment="test"} 1 1000

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
		`, "cortex_distributor_latest_seen_sample_timestamp_seconds")
		require.EqualError(t, err, "expected metric name(s) not found: [cortex_distributor_latest_seen_sample_timestamp_seconds]")
	})

	t.Run("specify required metric and absent metric", func(t *testing.T) {
		// Verify that cortex_distributor_deduped_samples_total is found among metrics returned by g,
		// and that conversely, cortex_distributor_non_ha_samples_received_total is not found among
		// metrics returned by g.
		// cortex_distributor_latest_seen_sample_timestamp_seconds is ignored, since it's not among
		// the specified metrics.
		AssertGatherAndCompare(t, g, `
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter
		cortex_distributor_deduped_samples_total{environment="test"} 1 1000

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
		`, "cortex_distributor_deduped_samples_total", "cortex_distributor_non_ha_samples_received_total")
	})

	t.Run("specify absent metric which is actually there", func(t *testing.T) {
		// Verify that cortex_distributor_deduped_samples_total is found among metrics returned by g,
		// and that conversely, cortex_distributor_non_ha_samples_received_total is not found among
		// metrics returned by g.
		// cortex_distributor_latest_seen_sample_timestamp_seconds is ignored, since it's not among
		// the specified metrics.
		err := gatherAndCompare(g, `
		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111
		`, "cortex_distributor_deduped_samples_total", "cortex_distributor_non_ha_samples_received_total")
		require.EqualError(t, err, "should be absent: metrics=cortex_distributor_deduped_samples_total")
	})
}

type fakeGatherer struct {
	metrics []*dto.MetricFamily
	err     error
}

func (g fakeGatherer) Gather() ([]*dto.MetricFamily, error) {
	return g.metrics, g.err
}
