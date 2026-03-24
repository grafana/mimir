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

					series, metadata := appender.GetResult()
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
