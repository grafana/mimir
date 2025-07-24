package otlpappender

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestCombinedAppender(t *testing.T) {
	testCases := map[string]struct {
		validIntervalCreatedTimestampZeroIngestion int64
		appends                                    func(*testing.T, *CombinedAppender)
		expectTimeseries                           []mimirpb.PreallocTimeseries
		expectTimeseriesNoCT                       []mimirpb.PreallocTimeseries
		expectMetadata                             []*mimirpb.MetricMetadata
	}{
		"no appends": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(_ *testing.T, _ *CombinedAppender) {
				// No appends to test.
			},
			expectTimeseries:     nil,
			expectTimeseriesNoCT: nil, // Same as expectTimeseries if nil.
			expectMetadata:       nil,
		},
		"single float sample": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					2000, 1000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
						},
						CreatedTimestamp: 1000,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid"}},
								Value:       27,
								TimestampMs: 1500,
							},
						},
					},
				},
			},
			expectMetadata: []*mimirpb.MetricMetadata{
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham").Get(model.MetricNameLabel),
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"multiple float samples, same series": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					2000, 1000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					3000, 1000, 52.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
							{TimestampMs: 3000, Value: 52.0},
						},
						CreatedTimestamp: 1000,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid"}},
								Value:       27,
								TimestampMs: 1500,
							},
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid2"}},
								Value:       45,
								TimestampMs: 2500,
							},
						},
					},
				},
			},
			expectMetadata: []*mimirpb.MetricMetadata{
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham").Get(model.MetricNameLabel),
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"multiple float samples, different series": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					2000, 1000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "cheese"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					3000, 1000, 52.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
						},
						CreatedTimestamp: 1000,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid"}},
								Value:       27,
								TimestampMs: 1500,
							},
						},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "cheese"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 3000, Value: 52.0},
						},
						CreatedTimestamp: 1000,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid2"}},
								Value:       45,
								TimestampMs: 2500,
							},
						},
					},
				},
			},
			expectMetadata: []*mimirpb.MetricMetadata{
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham").Get(model.MetricNameLabel),
					Help:             "help!",
					Unit:             "bytes",
				},
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: labels.FromStrings(model.MetricNameLabel, "spam", "a", "cheese").Get(model.MetricNameLabel),
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"multiple float samples, same series, but created time changed": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					2000, 1000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					3000, 2400, 52.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
						},
						CreatedTimestamp: 1000,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid"}},
								Value:       27,
								TimestampMs: 1500,
							},
						},
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 3000, Value: 52.0},
						},
						CreatedTimestamp: 2400,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid2"}},
								Value:       45,
								TimestampMs: 2500,
							},
						},
					},
				},
			},
			expectTimeseriesNoCT: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
							{TimestampMs: 3000, Value: 52.0},
						},
						CreatedTimestamp: 0,
						Exemplars: []mimirpb.Exemplar{
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid"}},
								Value:       27,
								TimestampMs: 1500,
							},
							{
								Labels:      []mimirpb.LabelAdapter{{Name: "traceId", Value: "myid2"}},
								Value:       45,
								TimestampMs: 2500,
							},
						},
					},
				},
			},
			expectMetadata: []*mimirpb.MetricMetadata{
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham").Get(model.MetricNameLabel),
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, enableCreatedTimestampZeroIngestion := range []bool{false, true} {
				tc := tc // capture range variable
				t.Run(fmt.Sprintf("enableCreatedTimestampZeroIngestion=%v", enableCreatedTimestampZeroIngestion), func(t *testing.T) {
					options := CombinedAppenderOptions{
						EnableCreatedTimestampZeroIngestion:        enableCreatedTimestampZeroIngestion,
						ValidIntervalCreatedTimestampZeroIngestion: tc.validIntervalCreatedTimestampZeroIngestion,
					}
					appender := NewCombinedAppender(options)
					tc.appends(t, appender)

					expectedTimeseries := tc.expectTimeseries

					if !enableCreatedTimestampZeroIngestion {
						if tc.expectTimeseriesNoCT != nil {
							expectedTimeseries = tc.expectTimeseriesNoCT
						} else {
							if tc.expectTimeseries != nil {
								expectedTimeseries = make([]mimirpb.PreallocTimeseries, len(tc.expectTimeseries))
								for i, ts := range tc.expectTimeseries {
									innerTs := *ts.TimeSeries    // Shallow copy to modify CreatedTimestamp.
									innerTs.CreatedTimestamp = 0 // Set CreatedTimestamp to 0 if the feature is disabled.
									expectedTimeseries[i].TimeSeries = &innerTs
								}
							}
						}
					}

					series, metadata, _ := appender.GetResult()
					require.Equal(t, expectedTimeseries, series)
					require.Equal(t, tc.expectMetadata, metadata)
				})
			}
		})
	}
}
