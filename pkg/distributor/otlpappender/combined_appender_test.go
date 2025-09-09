package otlpappender

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	otlpappender "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestCombinedAppender(t *testing.T) {
	testCases := map[string]struct {
		validIntervalCreatedTimestampZeroIngestion int64
		appends                                    func(*testing.T, *CombinedAppender)
		expectTimeseries                           []mimirpb.PreallocTimeseries
		expectTimeseriesNoCT                       []mimirpb.PreallocTimeseries // Same as expectTimeseries if nil.
		expectMetadata                             []*mimirpb.MetricMetadata
	}{
		"no appends": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(_ *testing.T, _ *CombinedAppender) {
				// No appends to test.
			},
			expectTimeseries: nil,
			expectMetadata:   nil,
		},
		"single float sample": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, 42.0,
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
					MetricFamilyName: "spam",
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
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 3000, 52.0,
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
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"multiple float samples, different series, same family": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "cheese"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 3000, 52.0,
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
					MetricFamilyName: "spam",
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
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					2400, 3000, 52.0,
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
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"single float sample, with created timestamp too old": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, defaultIntervalForStartTimestamps+2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, defaultIntervalForStartTimestamps+3000, 52.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: defaultIntervalForStartTimestamps + 2000, Value: 42.0},
							{TimestampMs: defaultIntervalForStartTimestamps + 3000, Value: 52.0},
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
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"single float sample, with created timestamp too old for some samples": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, defaultIntervalForStartTimestamps-2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, defaultIntervalForStartTimestamps+3000, 52.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: defaultIntervalForStartTimestamps - 2000, Value: 42.0},
							{TimestampMs: defaultIntervalForStartTimestamps + 3000, Value: 52.0},
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
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"single histogram sample": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendHistogram(
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, test.GenerateTestHistogram(1),
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam"}, {Name: "a", Value: "ham"}},
						Histograms: []mimirpb.Histogram{
							mimirpb.FromHistogramToHistogramProto(2000, test.GenerateTestHistogram(1)),
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
					Type:             mimirpb.HISTOGRAM,
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
			},
		},
		"mixed float and histogram samples, same series": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *CombinedAppender) {
				ca.AppendSample(
					labels.FromStrings(model.MetricNameLabel, "spam_count", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				ca.AppendHistogram(
					labels.FromStrings(model.MetricNameLabel, "spam_count", "a", "ham"),
					otlpappender.Metadata{
						Metadata:         metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "bytes", Help: "help!"},
						MetricFamilyName: "spam",
					},
					1000, 3000, test.GenerateTestHistogram(2),
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid2"), Value: 45, Ts: 2500, HasTs: true}})
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "spam_count"}, {Name: "a", Value: "ham"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 42.0},
						},
						Histograms: []mimirpb.Histogram{
							mimirpb.FromHistogramToHistogramProto(3000, test.GenerateTestHistogram(2)),
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
				// Yes, this is weird, but remote write does not have support for attaching metadata to samples.
				// Should we split the time series into two? Proably not worth it since the storage will just
				// overwrite the metadata anyway - krajorama.
				{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: "spam",
					Help:             "help!",
					Unit:             "bytes",
				},
				{
					Type:             mimirpb.HISTOGRAM,
					MetricFamilyName: "spam",
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
					appender := NewCombinedAppender().WithOptions(options)
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

					series, metadata := appender.GetResult()
					require.Equal(t, expectedTimeseries, series)
					require.Equal(t, tc.expectMetadata, metadata)
				})
			}
		})
	}
}
