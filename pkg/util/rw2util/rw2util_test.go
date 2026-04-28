// SPDX-License-Identifier: AGPL-3.0-only

package rw2util

import (
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMetadataOnly(t *testing.T) {
	rw2 := FromWriteRequest(&prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels:  []prompb.Label{{Name: "__name__", Value: "hasSamples"}},
			Samples: []prompb.Sample{{Value: 42, Timestamp: 123}},
		}},
		Metadata: []prompb.MetricMetadata{{
			MetricFamilyName: "noSamples",
			Type:             prompb.MetricMetadata_COUNTER,
			Help:             "Example help",
			Unit:             "eg",
		}},
	})

	data, err := rw2.Marshal()
	require.NoError(t, err)
	var got mimirpb.PreallocWriteRequest
	got.UnmarshalFromRW2 = true
	require.NoError(t, got.Unmarshal(data))

	require.Equal(t, []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
		Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: "hasSamples"}},
		Samples:   []mimirpb.Sample{{Value: 42, TimestampMs: 123}},
		Exemplars: []mimirpb.Exemplar{},
	}}, {TimeSeries: &mimirpb.TimeSeries{
		Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: "noSamples"}},
		Samples:   []mimirpb.Sample{},
		Exemplars: []mimirpb.Exemplar{},
	}}}, got.Timeseries)
	require.Equal(t, []*mimirpb.MetricMetadata{{
		MetricFamilyName: "noSamples",
		Type:             mimirpb.COUNTER,
		Help:             "Example help",
		Unit:             "eg",
	}}, got.Metadata)
}
