// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	promRemote "github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	rw2util "github.com/grafana/mimir/pkg/util/test"
)

// Consts from https://github.com/prometheus/prometheus/blob/main/storage/remote/stats.go
const (
	rw20WrittenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	rw20WrittenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	rw20WrittenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

func TestDistributor(t *testing.T) {
	t.Run("caching_unmarshal_data_enabled", func(t *testing.T) {
		testDistributorWithCachingUnmarshalData(t, true)
	})

	t.Run("caching_unmarshal_data_disabled", func(t *testing.T) {
		testDistributorWithCachingUnmarshalData(t, false)
	})
}

type metadataResponse struct {
	Status string                            `json:"status"`
	Data   map[string][]metadataResponseItem `json:"data"`
}

type metadataResponseItem struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type distributorTestCase struct {
	rw1request      []prompb.WriteRequest
	rw2request      []promRW2.Request
	runtimeConfig   string
	queries         map[string]model.Matrix
	exemplarQueries map[string][]promv1.ExemplarQueryResult
	metadataQueries map[string]metadataResponse
	// Expected statistics for RW2 per request.
	expectedStats  []promRemote.WriteResponseStats
	shouldReject   bool
	expectedStatus int
}

func testDistributorWithCachingUnmarshalData(t *testing.T, cachingUnmarshalDataEnabled bool) {
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 5 * time.Minute

	overridesWithExemplars := func(maxExemplars int) string {
		return fmt.Sprintf("overrides:\n  \"%s\":\n    max_global_exemplars_per_user: %d\n", userID, maxExemplars)
	}

	testCases := map[string]distributorTestCase{
		"simple counter": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarC_total"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Metadata: []prompb.MetricMetadata{
						{
							MetricFamilyName: "foobarC_total",
							Help:             "some helpC",
							Unit:             "someunitC",
							Type:             prompb.MetricMetadata_COUNTER,
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_COUNTER,
								HelpRef: 3,
								UnitRef: 4,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarC_total", "some helpC", "someunitC"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarC_total": {{
					Metric: model.Metric{"__name__": "foobarC_total"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarC_total": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarC_total": {{
							Type: "counter",
							Help: "some helpC",
							Unit: "someunitC",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					Exemplars:  0,
				},
			},
		},

		"simple counter with created timestamp": {
			rw1request: nil, // Not supported in RW1
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.Add(1 * time.Second).UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_COUNTER,
								HelpRef: 3,
								UnitRef: 4,
							},
							CreatedTimestamp: queryStart.UnixMilli(),
						},
					},
					Symbols: []string{"", "__name__", "foobarC_CT_total", "some helpC_CT", "someunitC_CT"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarC_CT_total": {{
					Metric: model.Metric{"__name__": "foobarC_CT_total"},
					Values: []model.SamplePair{
						{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(0)},
						{Timestamp: model.Time(queryStart.Add(5 * time.Minute).UnixMilli()), Value: model.SampleValue(100)},
					},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarC_CT_total": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarC_CT_total": {{
							Type: "counter",
							Help: "some helpC_CT",
							Unit: "someunitC_CT",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					Exemplars:  0,
				},
			},
		},

		"simple gauge": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarG"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Metadata: []prompb.MetricMetadata{
						{
							MetricFamilyName: "foobarG",
							Help:             "some helpG",
							Unit:             "someunitG",
							Type:             prompb.MetricMetadata_GAUGE,
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_GAUGE,
								HelpRef: 3,
								UnitRef: 4,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarG", "some helpG", "someunitG"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarG": {{
					Metric: model.Metric{"__name__": "foobarG"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarG": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarG": {{
							Type: "gauge",
							Help: "some helpG",
							Unit: "someunitG",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					Exemplars:  0,
				},
			},
		},

		"simple histogram": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarH_bucket"}, {Name: "le", Value: "0.1"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarH_bucket"}, {Name: "le", Value: "+Inf"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
						},
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarH_count"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
						},
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarH_sum"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 1000}},
						},
					},
					Metadata: []prompb.MetricMetadata{
						{
							MetricFamilyName: "foobarH",
							Help:             "some helpH",
							Unit:             "someunitH",
							Type:             prompb.MetricMetadata_HISTOGRAM,
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
						{
							LabelsRefs: []uint32{1, 2, 3, 7},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
						{
							LabelsRefs: []uint32{1, 8},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
						{
							LabelsRefs: []uint32{1, 9},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 1000}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarH_bucket", "le", "0.1", "some helpH", "someunitH", "+Inf", "foobarH_count", "foobarH_sum"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarH_bucket": {
					{
						Metric: model.Metric{"__name__": "foobarH_bucket", "le": "0.1"},
						Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
					},
					{
						Metric: model.Metric{"__name__": "foobarH_bucket", "le": "+Inf"},
						Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(200)}},
					},
				},
				"foobarH_count": {{
					Metric: model.Metric{"__name__": "foobarH_count"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(200)}},
				}},
				"foobarH_sum": {{
					Metric: model.Metric{"__name__": "foobarH_sum"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(1000)}},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarH": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarH": {{
							Type: "histogram",
							Help: "some helpH",
							Unit: "someunitH",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    4,
					Histograms: 0,
					Exemplars:  0,
				},
			},
		},

		"simple summary": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarS"}, {Name: "quantile", Value: "0.5"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarS_count"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
						},
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobarS_sum"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 1000}},
						},
					},
					Metadata: []prompb.MetricMetadata{
						{
							MetricFamilyName: "foobarS",
							Help:             "some helpS",
							Unit:             "someunitS",
							Type:             prompb.MetricMetadata_SUMMARY,
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_SUMMARY,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
						{
							LabelsRefs: []uint32{1, 7},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 200}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_SUMMARY,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
						{
							LabelsRefs: []uint32{1, 8},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 1000}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_SUMMARY,
								HelpRef: 5,
								UnitRef: 6,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarS", "quantile", "0.5", "some helpS", "someunitS", "foobarS_count", "foobarS_sum"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarS": {{
					Metric: model.Metric{"__name__": "foobarS", "quantile": "0.5"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
				"foobarS_count": {{
					Metric: model.Metric{"__name__": "foobarS_count"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(200)}},
				}},
				"foobarS_sum": {{
					Metric: model.Metric{"__name__": "foobarS_sum"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(1000)}},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarS": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarS": {{
							Type: "summary",
							Help: "some helpS",
							Unit: "someunitS",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    3,
					Histograms: 0,
					Exemplars:  0,
				},
			},
		},

		"simple native histogram": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{{Name: "__name__", Value: "foobarNH"}},
							Histograms: []prompb.Histogram{
								{
									Count:  &prompb.Histogram_CountInt{CountInt: 200},
									Sum:    1000,
									Schema: -1,
									PositiveSpans: []prompb.BucketSpan{
										{
											Offset: 0,
											Length: 2,
										},
									},
									PositiveDeltas: []int64{150, -100},
									Timestamp:      queryStart.UnixMilli(),
								},
							},
						},
					},
					Metadata: []prompb.MetricMetadata{
						{
							MetricFamilyName: "foobarNH",
							Help:             "some helpNH",
							Unit:             "someunitNH",
							Type:             prompb.MetricMetadata_HISTOGRAM,
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Histograms: []promRW2.Histogram{
								{
									Count:  &promRW2.Histogram_CountInt{CountInt: 200},
									Sum:    1000,
									Schema: -1,
									PositiveSpans: []promRW2.BucketSpan{
										{
											Offset: 0,
											Length: 2,
										},
									},
									PositiveDeltas: []int64{150, -100},
									Timestamp:      queryStart.UnixMilli(),
								},
							},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 3,
								UnitRef: 4,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarNH", "some helpNH", "someunitNH"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarNH": {{
					Metric: model.Metric{"__name__": "foobarNH"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: &model.SampleHistogram{
								Count: 200,
								Sum:   1000,
								Buckets: model.HistogramBuckets{
									{
										Boundaries: 0,
										Lower:      0.25,
										Upper:      1,
										Count:      150,
									},
									{
										Boundaries: 0,
										Lower:      1,
										Upper:      4,
										Count:      50,
									},
								},
							},
						},
					},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarNH": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarNH": {{
							Type: "histogram",
							Help: "some helpNH",
							Unit: "someunitNH",
						}},
					},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    0,
					Histograms: 1,
					Exemplars:  0,
				},
			},
		},

		"simple nhcb histogram": {
			rw1request: nil, // Not supported in RW1
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Histograms: []promRW2.Histogram{
								{
									Count:  &promRW2.Histogram_CountInt{CountInt: 200},
									Sum:    1000,
									Schema: -53,
									PositiveSpans: []promRW2.BucketSpan{
										{
											Offset: 0,
											Length: 2,
										},
									},
									PositiveDeltas: []int64{150, -100},
									CustomValues:   []float64{0.1},
									Timestamp:      queryStart.UnixMilli(),
								},
							},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_HISTOGRAM,
								HelpRef: 3,
								UnitRef: 4,
							},
						},
					},
					Symbols: []string{"", "__name__", "foobarNHCB", "some helpNHCB", "someunitNHCB"},
				},
			},
			queries: map[string]model.Matrix{
				"foobarNHCB": {{
					Metric: model.Metric{"__name__": "foobarNHCB"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: &model.SampleHistogram{
								Count: 200,
								Sum:   1000,
								Buckets: model.HistogramBuckets{
									{
										Boundaries: 3,
										Lower:      model.FloatString(math.Inf(-1)),
										Upper:      0.1,
										Count:      150,
									},
									{
										Boundaries: 0,
										Lower:      0.1,
										Upper:      model.FloatString(math.Inf(1)),
										Count:      50,
									},
								},
							},
						},
					},
				}},
			},
			metadataQueries: map[string]metadataResponse{
				"foobarNHCB": {
					Status: "success",
					Data: map[string][]metadataResponseItem{
						"foobarNHCB": {{
							Type: "histogram",
							Help: "some helpNHCB",
							Unit: "someunitNHCB",
						}},
					},
				},
			},
		},

		"drop empty labels": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_empty_label"}, {Name: "empty", Value: ""}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series_with_empty_label", "empty", ""},
				},
			},
			queries: map[string]model.Matrix{
				"series_with_empty_label": {{
					Metric: model.Metric{"__name__": "series_with_empty_label"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
		},

		"wrong labels order": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_wrong_labels_order"}, {Name: "zzz", Value: "1"}, {Name: "aaa", Value: "2"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4, 5, 6},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series_with_wrong_labels_order", "zzz", "1", "aaa", "2"},
				},
			},
			queries: map[string]model.Matrix{
				"series_with_wrong_labels_order": {{
					Metric: model.Metric{"__name__": "series_with_wrong_labels_order", "aaa": "2", "zzz": "1"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
		},

		"deduplicated requests": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series1"}, {Name: "cluster", Value: "C"}, {Name: "replica", Value: "a"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series2"}, {Name: "cluster", Value: "C"}, {Name: "replica", Value: "b"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4, 5, 6},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series1", "cluster", "C", "replica", "a"},
				},
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4, 5, 6},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series2", "cluster", "C", "replica", "b"},
				},
			},
			queries: map[string]model.Matrix{
				"series1": {{
					Metric: model.Metric{"__name__": "series1", "cluster": "C"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
				"series2": {},
			},

			runtimeConfig: rw2util.Unindent(t, `
				overrides:
				  "`+userID+`":
				    ha_cluster_label: "cluster"
				    ha_replica_label: "replica"
			`),
		},

		"dropped labels": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_dropped_label"}, {Name: "dropped_label", Value: "some value"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series_with_dropped_label", "dropped_label", "some value"},
				},
			},
			queries: map[string]model.Matrix{
				"series_with_dropped_label": {{
					Metric: model.Metric{"__name__": "series_with_dropped_label"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
			runtimeConfig: rw2util.Unindent(t, `
				overrides:
				  "`+userID+`":
				    drop_labels:
				      - dropped_label
			`),
		},

		"relabeling test, using prometheus label": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_relabeling_applied"}, {Name: "prometheus", Value: "cluster/instance"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2, 3, 4},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
						},
					},
					Symbols: []string{"", "__name__", "series_with_relabeling_applied", "prometheus", "cluster/instance"},
				},
			},
			queries: map[string]model.Matrix{
				"series_with_relabeling_applied": {{
					Metric: model.Metric{
						"__name__":            "series_with_relabeling_applied",
						"prometheus":          "cluster/instance",
						"prometheus_instance": "instance", // label created by relabelling.
					},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},

			runtimeConfig: rw2util.Unindent(t, `
				overrides:
				  "`+userID+`":
				    metric_relabel_configs:
				      - source_labels: [prometheus]
				        regex: ".*/(.+)"
				        target_label: "prometheus_instance"
				        replacement: "$1"
				        action: replace
			`),
		},

		"series with exemplars": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:    []prompb.Label{{Name: "__name__", Value: "foobar_with_exemplars"}},
							Samples:   []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "test", Value: "test"}}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars:  []promRW2.Exemplar{{LabelsRefs: []uint32{3, 3}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
						},
					},
					Symbols: []string{"", "__name__", "foobar_with_exemplars", "test"},
				},
			},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_exemplars": {{
					SeriesLabels: model.LabelSet{
						"__name__": "foobar_with_exemplars",
					},
					Exemplars: []promv1.Exemplar{{
						Labels:    model.LabelSet{"test": "test"},
						Value:     123.0,
						Timestamp: model.Time(queryStart.UnixMilli()),
					}},
				}},
			},
			runtimeConfig: overridesWithExemplars(100),
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					Exemplars:  1,
				},
			},
		},

		"series with old exemplars": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  []prompb.Label{{Name: "__name__", Value: "foobar_with_old_exemplars"}},
							Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars: []prompb.Exemplar{{
								Labels:    []prompb.Label{{Name: "test", Value: "test"}},
								Value:     123.0,
								Timestamp: queryStart.Add(-10 * time.Minute).UnixMilli(), // Exemplars older than 10 minutes from oldest sample for the series in the request are dropped by distributor.
							}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars:  []promRW2.Exemplar{{LabelsRefs: []uint32{3, 3}, Value: 123.0, Timestamp: queryStart.Add(-10 * time.Minute).UnixMilli()}},
						},
					},
					Symbols: []string{"", "__name__", "foobar_with_old_exemplars", "test"},
				},
			},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_old_exemplars": {},
			},
			runtimeConfig: overridesWithExemplars(100),
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					// Mimir may silently drop outdated samples/histograms/exemplars during aggregation, due to age or
					// other middlewares running in distributors. Mimir can't differentiate if a data point was dropped
					// deliberately or accidentally. Hence, to avoid confusing clients we count every sample, histogram
					// and exemplar that was received, even if we didn't ingest it.
					Exemplars: 1,
				},
			},
		},

		"sending series with exemplars, when exemplars are disabled": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:    []prompb.Label{{Name: "__name__", Value: "foobar_with_exemplars_disabled"}},
							Samples:   []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "test", Value: "test"}}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Exemplars:  []promRW2.Exemplar{{LabelsRefs: []uint32{3, 3}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
						},
					},
					Symbols: []string{"", "__name__", "foobar_with_exemplars_disabled", "test"},
				},
			},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_exemplars_disabled": {},
			},
			// By disabling exemplars via runtime config, distributor will stop sending them to ingester.
			runtimeConfig: overridesWithExemplars(0),
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    1,
					Histograms: 0,
					// Mimir may silently drop outdated samples/histograms/exemplars during aggregation, due to age or
					// other middlewares running in distributors. Mimir can't differentiate if a data point was dropped
					// deliberately or accidentally. Hence, to avoid confusing clients we count every sample, histogram
					// and exemplar that was received, even if we didn't ingest it.
					Exemplars: 1,
				},
			},
		},

		"reduce native histogram buckets via down scaling": {
			rw1request: []prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: []prompb.Label{{Name: "__name__", Value: "histogram_down_scaling_series"}},
							Histograms: []prompb.Histogram{
								{
									Count:          &prompb.Histogram_CountInt{CountInt: 12},
									ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: 2},
									ZeroThreshold:  0.001,
									Sum:            18.4,
									Schema:         0,
									NegativeSpans:  []prompb.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
									NegativeDeltas: []int64{1, 1, -1, 0},
									PositiveSpans:  []prompb.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
									PositiveDeltas: []int64{1, 1, -1, 0},
									Timestamp:      queryStart.UnixMilli(),
								},
							},
						},
					},
				},
			},
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Histograms: []promRW2.Histogram{
								{
									Count:          &promRW2.Histogram_CountInt{CountInt: 12},
									ZeroCount:      &promRW2.Histogram_ZeroCountInt{ZeroCountInt: 2},
									ZeroThreshold:  0.001,
									Sum:            18.4,
									Schema:         0,
									NegativeSpans:  []promRW2.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
									NegativeDeltas: []int64{1, 1, -1, 0},
									PositiveSpans:  []promRW2.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
									PositiveDeltas: []int64{1, 1, -1, 0},
									Timestamp:      queryStart.UnixMilli(),
								},
							},
						},
					},
					Symbols: []string{"", "__name__", "histogram_down_scaling_series"},
				},
			},
			queries: map[string]model.Matrix{
				"histogram_down_scaling_series": {{
					Metric: model.Metric{
						"__name__": "histogram_down_scaling_series",
					},
					Histograms: []model.SampleHistogramPair{{Timestamp: model.Time(queryStart.UnixMilli()), Histogram: &model.SampleHistogram{
						Count: 12,
						Sum:   18.4,
						Buckets: model.HistogramBuckets{
							// This histogram has 3+3=6 buckets (without zero bucket), which was down scaled from 4+4=8 buckets.
							&model.HistogramBucket{Boundaries: 1, Lower: -16, Upper: -4, Count: 2},
							&model.HistogramBucket{Boundaries: 1, Lower: -4, Upper: -1, Count: 2},
							&model.HistogramBucket{Boundaries: 1, Lower: -1, Upper: -0.25, Count: 1},
							&model.HistogramBucket{Boundaries: 3, Lower: -0.001, Upper: 0.001, Count: 2},
							&model.HistogramBucket{Boundaries: 0, Lower: 0.25, Upper: 1, Count: 1},
							&model.HistogramBucket{Boundaries: 0, Lower: 1, Upper: 4, Count: 2},
							&model.HistogramBucket{Boundaries: 0, Lower: 4, Upper: 16, Count: 2},
						},
					},
					}},
				}},
			},
			runtimeConfig: rw2util.Unindent(t, `
				overrides:
				  "`+userID+`":
				    max_native_histogram_buckets: 7
			`),
		},
		"invalid rw2 symbols": {
			rw2request: []promRW2.Request{
				{
					Timeseries: []promRW2.TimeSeries{
						{
							LabelsRefs: []uint32{0, 1},
							Samples:    []promRW2.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
							Metadata: promRW2.Metadata{
								Type:    promRW2.Metadata_METRIC_TYPE_COUNTER,
								HelpRef: 2,
								UnitRef: 3,
							},
						},
					},
					Symbols: []string{"__name__", "invalid_series_bad_symbols", "some helpC", "someunitC"},
				},
			},
			expectedStats: []promRemote.WriteResponseStats{
				{
					Samples:    0,
					Histograms: 0,
					Exemplars:  0,
				},
			},
			expectedStatus: http.StatusBadRequest,
			shouldReject:   true,
		},
	}

	for _, rwVersion := range []string{"rw1", "rw2"} {
		t.Run(rwVersion, func(t *testing.T) {
			testDistributorCases(t, cachingUnmarshalDataEnabled, rwVersion, queryStart, queryEnd, queryStep, testCases)
		})
	}
}

func testDistributorCases(t *testing.T, cachingUnmarshalDataEnabled bool, rwVersion string, queryStart, queryEnd time.Time, queryStep time.Duration, testCases map[string]distributorTestCase) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	previousRuntimeConfig := ""
	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(previousRuntimeConfig)))

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	baseFlags := map[string]string{
		"-distributor.ingestion-tenant-shard-size":           "0",
		"-ingester.ring.heartbeat-period":                    "1s",
		"-distributor.ha-tracker.enable":                     "true",
		"-distributor.ha-tracker.enable-for-all-users":       "true",
		"-distributor.ha-tracker.store":                      "consul",
		"-distributor.ha-tracker.consul.hostname":            consul.NetworkHTTPEndpoint(),
		"-distributor.ha-tracker.prefix":                     "prom_ha/",
		"-timeseries-unmarshal-caching-optimization-enabled": strconv.FormatBool(cachingUnmarshalDataEnabled),
	}

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		baseFlags,
	)

	// We want only distributor to be reloading runtime config.
	distributorFlags := mergeFlags(flags, map[string]string{
		"-runtime-config.file":          filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),
		"-runtime-config.reload-period": "100ms",
		// Set non-zero default for number of exemplars. That way our values used in the test (0 and 100) will show up in runtime config diff.
		"-ingester.max-global-exemplars-per-user": "3",
	})

	// Ingester will not reload runtime config.
	ingesterFlags := mergeFlags(flags, map[string]string{
		// Ingester will always see exemplars enabled. We do this to avoid waiting for ingester to apply new setting to TSDB.
		"-ingester.max-global-exemplars-per-user": "100",
	})

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), distributorFlags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), ingesterFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until querier has updated the ring.
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	runtimeConfigURL := fmt.Sprintf("http://%s/runtime_config?mode=diff", distributor.HTTPEndpoint())

	requestCount := 0

	for testName, tc := range testCases {
		if rwVersion == "rw1" && tc.rw1request == nil || rwVersion == "rw2" && tc.rw2request == nil {
			// Skip test case if no remote write request for the the current rwVersion.
			continue
		}

		t.Run(testName, func(t *testing.T) {
			bodiesToClose := []io.ReadCloser{}
			t.Cleanup(func() {
				for _, b := range bodiesToClose {
					b.Close()
				}
			})

			if tc.runtimeConfig != previousRuntimeConfig {
				currentRuntimeConfig, err := getURL(runtimeConfigURL)
				require.NoError(t, err)

				// Write new runtime config
				require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(tc.runtimeConfig)))

				// Wait until distributor has reloaded runtime config.
				test.Poll(t, 1*time.Second, true, func() interface{} {
					newRuntimeConfig, err := getURL(runtimeConfigURL)
					require.NoError(t, err)
					return currentRuntimeConfig != newRuntimeConfig
				})

				previousRuntimeConfig = tc.runtimeConfig
			}

			switch rwVersion {
			case "rw1":
				for _, wreq := range tc.rw1request {
					res, err := client.PushRW1(&wreq)
					require.NoError(t, err)
					if tc.expectedStatus != 0 {
						require.Equal(t, tc.expectedStatus, res.StatusCode)
					} else {
						require.True(t, res.StatusCode == http.StatusOK || res.StatusCode == http.StatusAccepted, res.Status)
					}
					assertNoStats(t, res)
				}

				if !tc.shouldReject {
					requestCount += len(tc.rw1request)
				}
				err = distributor.WaitSumMetricsWithOptions(e2e.Equals(float64(requestCount)), []string{"cortex_distributor_requests_in_total"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "version", "1.0")))
				require.NoError(t, err)

			case "rw2":
				for i, wreq := range tc.rw2request {
					res, err := client.PushRW2(&wreq)
					require.NoError(t, err)
					if tc.expectedStatus != 0 {
						require.Equal(t, tc.expectedStatus, res.StatusCode)
					} else {
						require.True(t, res.StatusCode == http.StatusOK || res.StatusCode == http.StatusAccepted, res.Status)
					}
					if tc.expectedStats != nil {
						assertStats(t, tc.expectedStats[i], res)
					}
				}

				if !tc.shouldReject {
					requestCount += len(tc.rw2request)
				}
				err = distributor.WaitSumMetricsWithOptions(e2e.Equals(float64(requestCount)), []string{"cortex_distributor_requests_in_total"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "version", "2.0")))
				require.NoError(t, err)

			default:
				t.Fatalf("unknown rwVersion %s", rwVersion)
			}

			for q, res := range tc.queries {
				result, err := client.QueryRange(q, queryStart, queryEnd, queryStep)
				require.NoError(t, err)

				require.Equal(t, res.String(), result.String())
			}

			for q, expResult := range tc.exemplarQueries {
				result, err := client.QueryExemplars(q, queryStart, queryEnd)
				require.NoError(t, err)

				require.Equal(t, expResult, result)
			}

			for q, expected := range tc.metadataQueries {
				result, err := client.GetPrometheusMetadata(q)
				require.NoError(t, err)
				bodiesToClose = append(bodiesToClose, result.Body)
				require.Equal(t, http.StatusOK, result.StatusCode)
				data, err := io.ReadAll(result.Body)
				require.NoError(t, err)
				var actual metadataResponse
				err = json.Unmarshal(data, &actual)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func assertNoStats(t *testing.T, res *http.Response) {
	t.Helper()
	samplesH := res.Header.Get(rw20WrittenSamplesHeader)
	require.Empty(t, samplesH, "samples stats header should be empty")

	histogramsH := res.Header.Get(rw20WrittenHistogramsHeader)
	require.Empty(t, histogramsH, "histograms stats header should be empty")

	exemplarsH := res.Header.Get(rw20WrittenExemplarsHeader)
	require.Empty(t, exemplarsH, "exemplars stats header should be empty")
}

func assertStats(t *testing.T, expectedStats promRemote.WriteResponseStats, res *http.Response) {
	t.Helper()
	samplesH := res.Header.Get(rw20WrittenSamplesHeader)
	require.NotEmpty(t, samplesH, "missing samples stats header")
	samples, err := strconv.Atoi(samplesH)
	require.NoError(t, err, "samples stats header value should be an integer")
	require.Equal(t, expectedStats.Samples, samples, "wrong samples stats header value")

	histogramsH := res.Header.Get(rw20WrittenHistogramsHeader)
	require.NotEmpty(t, histogramsH, "missing histograms stats header")
	histograms, err := strconv.Atoi(histogramsH)
	require.NoError(t, err, "histograms stats header value should be an integer")
	require.Equal(t, expectedStats.Histograms, histograms, "wrong histograms stats header value")

	exemplarsH := res.Header.Get(rw20WrittenExemplarsHeader)
	require.NotEmpty(t, exemplarsH, "missing exemplars stats header")
	exemplars, err := strconv.Atoi(exemplarsH)
	require.NoError(t, err, "exemplars stats header value should be an integer")
	require.Equal(t, expectedStats.Exemplars, exemplars, "wrong exemplars stats header value")
}
