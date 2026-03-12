// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/mimirpb/testutil"
	"github.com/grafana/mimir/pkg/util/test"
)

func appendSample(t *testing.T, ca *MimirAppender, ls labels.Labels, meta metadata.Metadata, familyName string, ct, ts int64, v float64, es []exemplar.Exemplar) {
	t.Helper()
	_, err := ca.Append(0, ls, ct, ts, v, nil, nil, storage.AppendV2Options{
		Metadata:         meta,
		MetricFamilyName: familyName,
		Exemplars:        es,
	})
	require.NoError(t, err)
}

func appendHistogram(t *testing.T, ca *MimirAppender, ls labels.Labels, meta metadata.Metadata, familyName string, ct, ts int64, h *histogram.Histogram, es []exemplar.Exemplar) {
	t.Helper()
	_, err := ca.Append(0, ls, ct, ts, 0, h, nil, storage.AppendV2Options{
		Metadata:         meta,
		MetricFamilyName: familyName,
		Exemplars:        es,
	})
	require.NoError(t, err)
}

func TestMimirAppender(t *testing.T) {
	collidingLabels1, collidingLabels2 := labelsWithHashCollision()

	testCases := map[string]struct {
		validIntervalCreatedTimestampZeroIngestion int64
		appends                                    func(*testing.T, *MimirAppender)
		expectTimeseries                           []mimirpb.PreallocTimeseries
		expectTimeseriesNoCT                       []mimirpb.PreallocTimeseries // Same as expectTimeseries if nil.
		expectMetadata                             []*mimirpb.MetricMetadata
		expectCollisions                           bool
	}{
		"no appends": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(_ *testing.T, _ *MimirAppender) {
				// No appends to test.
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{}, // Initialized from pool.
			expectMetadata:   nil,
		},
		"single float sample": {
			validIntervalCreatedTimestampZeroIngestion: defaultIntervalForStartTimestamps,
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "cheese"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, defaultIntervalForStartTimestamps+2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, defaultIntervalForStartTimestamps-2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendHistogram(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "bytes", Help: "help!"},
					"spam",
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
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam_count", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					1000, 2000, 42.0,
					[]exemplar.Exemplar{{Labels: labels.FromStrings("traceId", "myid"), Value: 27, Ts: 1500, HasTs: true}})
				appendHistogram(t, ca,
					labels.FromStrings(model.MetricNameLabel, "spam_count", "a", "ham"),
					metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "bytes", Help: "help!"},
					"spam",
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
		"colliding labels are tracked": {
			appends: func(t *testing.T, ca *MimirAppender) {
				appendSample(t, ca,
					collidingLabels1,
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					0, 1000, 42.0, nil)
				appendSample(t, ca,
					collidingLabels2,
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					0, 2000, 44.0, nil)
				appendSample(t, ca,
					collidingLabels1,
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					0, 3000, 46.0, nil)
				appendSample(t, ca,
					collidingLabels2,
					metadata.Metadata{Type: model.MetricTypeCounter, Unit: "bytes", Help: "help!"},
					"spam",
					0, 4000, 48.0, nil)
			},
			expectTimeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: mimirpb.FromLabelsToLabelAdapters(collidingLabels1),
						Samples: []mimirpb.Sample{
							{TimestampMs: 1000, Value: 42.0},
							{TimestampMs: 3000, Value: 46.0},
						},
						CreatedTimestamp: 0,
					},
				},
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: mimirpb.FromLabelsToLabelAdapters(collidingLabels2),
						Samples: []mimirpb.Sample{
							{TimestampMs: 2000, Value: 44.0},
							{TimestampMs: 4000, Value: 48.0},
						},
						CreatedTimestamp: 0,
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
			expectCollisions: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, enableCreatedTimestampZeroIngestion := range []bool{false, true} {
				t.Run(fmt.Sprintf("enableCreatedTimestampZeroIngestion=%v", enableCreatedTimestampZeroIngestion), func(t *testing.T) {
					appender := NewCombinedAppender()
					appender.EnableCreatedTimestampZeroIngestion = enableCreatedTimestampZeroIngestion
					appender.ValidIntervalCreatedTimestampZeroIngestion = tc.validIntervalCreatedTimestampZeroIngestion
					tc.appends(t, appender)

					expectedTimeseries := tc.expectTimeseries

					if !enableCreatedTimestampZeroIngestion {
						if tc.expectTimeseriesNoCT != nil {
							expectedTimeseries = tc.expectTimeseriesNoCT
						} else if tc.expectTimeseries != nil {
							expectedTimeseries = make([]mimirpb.PreallocTimeseries, len(tc.expectTimeseries))
							for i, ts := range tc.expectTimeseries {
								innerTs := *ts.TimeSeries    // Shallow copy to modify CreatedTimestamp.
								innerTs.CreatedTimestamp = 0 // Set CreatedTimestamp to 0 if the feature is disabled.
								expectedTimeseries[i].TimeSeries = &innerTs
							}
						}
					}

					series, metadata, _, _ := appender.GetResult()
					series = testutil.RemoveEmptyObjectFromSeries(series)
					require.Equal(t, expectedTimeseries, series)
					require.Equal(t, tc.expectMetadata, metadata)
					if tc.expectCollisions {
						require.Len(t, appender.collisionRefs, 1)
					} else {
						require.Empty(t, appender.collisionRefs)
					}
				})
			}
		})
	}
}

func TestMimirAppender_ResourceContext(t *testing.T) {
	testCases := map[string]struct {
		persistResourceAttributes bool
		resource                  *storage.ResourceContext
		appends                   func(*testing.T, *MimirAppender, *storage.ResourceContext)
		expectResourceAttrs       *mimirpb.ResourceAttributes
	}{
		"resource attributes disabled": {
			persistResourceAttributes: false,
			resource: &storage.ResourceContext{
				Identifying: map[string]string{"service.name": "myservice"},
				Descriptive: map[string]string{"host.name": "myhost"},
			},
			appends: func(t *testing.T, ca *MimirAppender, res *storage.ResourceContext) {
				_, err := ca.Append(0,
					labels.FromStrings(model.MetricNameLabel, "my_metric"),
					0, 1000, 42.0, nil, nil,
					storage.AppendV2Options{
						Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
						MetricFamilyName: "my_metric",
						Resource:         res,
					})
				require.NoError(t, err)
			},
			expectResourceAttrs: nil, // Disabled, so no attrs
		},
		"resource attributes enabled with identifying attrs": {
			persistResourceAttributes: true,
			resource: &storage.ResourceContext{
				Identifying: map[string]string{"service.name": "myservice"},
				Descriptive: map[string]string{"host.name": "myhost"},
			},
			appends: func(t *testing.T, ca *MimirAppender, res *storage.ResourceContext) {
				_, err := ca.Append(0,
					labels.FromStrings(model.MetricNameLabel, "my_metric"),
					0, 1000, 42.0, nil, nil,
					storage.AppendV2Options{
						Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
						MetricFamilyName: "my_metric",
						Resource:         res,
					})
				require.NoError(t, err)
			},
			expectResourceAttrs: &mimirpb.ResourceAttributes{
				Identifying: []mimirpb.AttributeEntry{
					{Key: "service.name", Value: "myservice"},
				},
				Descriptive: []mimirpb.AttributeEntry{
					{Key: "host.name", Value: "myhost"},
				},
				Timestamp: 1000,
			},
		},
		"target_info metric skips resource attributes": {
			persistResourceAttributes: true,
			resource: &storage.ResourceContext{
				Identifying: map[string]string{"service.name": "myservice"},
			},
			appends: func(t *testing.T, ca *MimirAppender, res *storage.ResourceContext) {
				_, err := ca.Append(0,
					labels.FromStrings(model.MetricNameLabel, "target_info"),
					0, 1000, 1.0, nil, nil,
					storage.AppendV2Options{
						Metadata:         metadata.Metadata{Type: model.MetricTypeInfo},
						MetricFamilyName: "target_info",
						Resource:         res,
					})
				require.NoError(t, err)
			},
			expectResourceAttrs: nil, // target_info should not have resource attrs
		},
		"nil resource context": {
			persistResourceAttributes: true,
			resource:                  nil,
			appends: func(t *testing.T, ca *MimirAppender, res *storage.ResourceContext) {
				_, err := ca.Append(0,
					labels.FromStrings(model.MetricNameLabel, "my_metric"),
					0, 1000, 42.0, nil, nil,
					storage.AppendV2Options{
						Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
						MetricFamilyName: "my_metric",
						Resource:         res,
					})
				require.NoError(t, err)
			},
			expectResourceAttrs: nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			appender := NewCombinedAppender()
			appender.PersistResourceAttributes = tc.persistResourceAttributes

			tc.appends(t, appender, tc.resource)

			series, _, resourceTable, _ := appender.GetResult()
			require.Len(t, series, 1)

			if tc.expectResourceAttrs == nil {
				require.Equal(t, int32(0), series[0].ResourceRef)
			} else {
				require.Greater(t, series[0].ResourceRef, int32(0))
				resAttrs := resourceTable[series[0].ResourceRef-1]
				// Check identifying attrs
				require.Equal(t, len(tc.expectResourceAttrs.Identifying), len(resAttrs.Identifying))
				for i, expected := range tc.expectResourceAttrs.Identifying {
					require.Equal(t, expected.Key, resAttrs.Identifying[i].Key)
					require.Equal(t, expected.Value, resAttrs.Identifying[i].Value)
				}
				// Check descriptive attrs
				require.Equal(t, len(tc.expectResourceAttrs.Descriptive), len(resAttrs.Descriptive))
				for i, expected := range tc.expectResourceAttrs.Descriptive {
					require.Equal(t, expected.Key, resAttrs.Descriptive[i].Key)
					require.Equal(t, expected.Value, resAttrs.Descriptive[i].Value)
				}
				require.Equal(t, tc.expectResourceAttrs.Timestamp, resAttrs.Timestamp)
			}
		})
	}
}

func TestMimirAppender_ScopeContext(t *testing.T) {
	testCases := map[string]struct {
		persistScopeAttributes bool
		scope                  *storage.ScopeContext
		expectScopeAttrs       *mimirpb.ScopeAttributes
	}{
		"scope attributes disabled": {
			persistScopeAttributes: false,
			scope: &storage.ScopeContext{
				Name:    "github.com/example/payment",
				Version: "1.2.0",
			},
			expectScopeAttrs: nil,
		},
		"scope attributes enabled with name and version": {
			persistScopeAttributes: true,
			scope: &storage.ScopeContext{
				Name:    "github.com/example/payment",
				Version: "1.2.0",
			},
			expectScopeAttrs: &mimirpb.ScopeAttributes{
				Name:      "github.com/example/payment",
				Version:   "1.2.0",
				Timestamp: 1000,
			},
		},
		"scope attributes enabled with all fields": {
			persistScopeAttributes: true,
			scope: &storage.ScopeContext{
				Name:      "github.com/example/payment",
				Version:   "1.2.0",
				SchemaURL: "https://opentelemetry.io/schemas/1.24.0",
				Attrs:     map[string]string{"library.language": "go"},
			},
			expectScopeAttrs: &mimirpb.ScopeAttributes{
				Name:      "github.com/example/payment",
				Version:   "1.2.0",
				SchemaURL: "https://opentelemetry.io/schemas/1.24.0",
				Attrs: []mimirpb.AttributeEntry{
					{Key: "library.language", Value: "go"},
				},
				Timestamp: 1000,
			},
		},
		"nil scope context": {
			persistScopeAttributes: true,
			scope:                  nil,
			expectScopeAttrs:       nil,
		},
		"empty scope context": {
			persistScopeAttributes: true,
			scope:                  &storage.ScopeContext{},
			expectScopeAttrs:       nil, // All fields empty, so not stored
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			appender := NewCombinedAppender()
			appender.PersistScopeAttributes = tc.persistScopeAttributes

			_, err := appender.Append(0,
				labels.FromStrings(model.MetricNameLabel, "my_metric"),
				0, 1000, 42.0, nil, nil,
				storage.AppendV2Options{
					Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
					MetricFamilyName: "my_metric",
					Scope:            tc.scope,
				})
			require.NoError(t, err)

			series, _, _, scopeTable := appender.GetResult()
			require.Len(t, series, 1)

			if tc.expectScopeAttrs == nil {
				require.Equal(t, int32(0), series[0].ScopeRef)
			} else {
				require.Greater(t, series[0].ScopeRef, int32(0))
				scopeAttrs := scopeTable[series[0].ScopeRef-1]
				require.Equal(t, tc.expectScopeAttrs.Name, scopeAttrs.Name)
				require.Equal(t, tc.expectScopeAttrs.Version, scopeAttrs.Version)
				require.Equal(t, tc.expectScopeAttrs.SchemaURL, scopeAttrs.SchemaURL)
				require.Equal(t, tc.expectScopeAttrs.Timestamp, scopeAttrs.Timestamp)
				require.Equal(t, len(tc.expectScopeAttrs.Attrs), len(scopeAttrs.Attrs))
				for i, expected := range tc.expectScopeAttrs.Attrs {
					require.Equal(t, expected.Key, scopeAttrs.Attrs[i].Key)
					require.Equal(t, expected.Value, scopeAttrs.Attrs[i].Value)
				}
			}
		})
	}
}

func TestMimirAppender_TableTimestampUpdatedToMax(t *testing.T) {
	resource := &storage.ResourceContext{
		Identifying: map[string]string{"service.name": "myservice"},
	}
	scope := &storage.ScopeContext{
		Name:    "github.com/example/payment",
		Version: "1.2.0",
	}

	appender := NewCombinedAppender()
	appender.PersistResourceAttributes = true
	appender.PersistScopeAttributes = true

	// Append first series at t=1000.
	_, err := appender.Append(0,
		labels.FromStrings(model.MetricNameLabel, "metric_a", "a", "1"),
		0, 1000, 1.0, nil, nil,
		storage.AppendV2Options{
			Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
			MetricFamilyName: "metric_a",
			Resource:         resource,
			Scope:            scope,
		})
	require.NoError(t, err)

	// Append second series at t=2000 with the same resource/scope pointers.
	_, err = appender.Append(0,
		labels.FromStrings(model.MetricNameLabel, "metric_b", "a", "1"),
		0, 2000, 2.0, nil, nil,
		storage.AppendV2Options{
			Metadata:         metadata.Metadata{Type: model.MetricTypeGauge},
			MetricFamilyName: "metric_b",
			Resource:         resource,
			Scope:            scope,
		})
	require.NoError(t, err)

	series, _, resourceTable, scopeTable := appender.GetResult()
	require.Len(t, series, 2)

	// Both series should reference the same table entries.
	require.Equal(t, series[0].ResourceRef, series[1].ResourceRef)
	require.Equal(t, series[0].ScopeRef, series[1].ScopeRef)

	// The table entry timestamps should be the max (2000).
	require.Equal(t, int64(2000), resourceTable[series[0].ResourceRef-1].Timestamp)
	require.Equal(t, int64(2000), scopeTable[series[0].ScopeRef-1].Timestamp)
}

// adapted from pkg/distributor/distributor_test.go
func labelsWithHashCollision() (labels.Labels, labels.Labels) {
	// These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
	ls1 := labels.FromStrings("__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y")
	ls2 := labels.FromStrings("__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF")

	if ls1.Hash() != ls2.Hash() {
		// These ones are the same when using -tags stringlabels
		ls1 = labels.FromStrings("__name__", "metric", "lbl", "HFnEaGl")
		ls2 = labels.FromStrings("__name__", "metric", "lbl", "RqcXatm")
	}

	if ls1.Hash() != ls2.Hash() {
		panic("This code needs to be updated: find new labels with colliding hash values.")
	}

	return ls1, ls2
}
