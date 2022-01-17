package bench

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestWorkload_generateTimeSeries(t *testing.T) {
	testCases := []struct {
		name         string
		workloadDesc WorkloadDesc
		numSeries    int
	}{
		{
			name: "basic",
			workloadDesc: WorkloadDesc{
				Replicas: 1,
				Series: []SeriesDesc{
					{
						Name:         "test_series",
						Type:         GaugeZero,
						StaticLabels: map[string]string{"constant_label": "true"},
						Labels: []LabelDesc{
							{
								Name:         "test_label_one",
								ValuePrefix:  "test_prefix",
								UniqueValues: 5,
							},
						},
					},
				},
			},
			numSeries: 5,
		},
		{
			name: "multiple replicas",
			workloadDesc: WorkloadDesc{
				Replicas: 5,
				Series: []SeriesDesc{
					{
						Name:         "test_series",
						Type:         GaugeZero,
						StaticLabels: map[string]string{"constant_label": "true"},
						Labels: []LabelDesc{
							{
								Name:         "test_label_one",
								ValuePrefix:  "test_prefix",
								UniqueValues: 2,
							},
						},
					},
				},
			},
			numSeries: 10,
		},
		{
			name: "multiple labels",
			workloadDesc: WorkloadDesc{
				Replicas: 1,
				Series: []SeriesDesc{
					{
						Name:         "test_series",
						Type:         GaugeZero,
						StaticLabels: map[string]string{"constant_label": "true"},
						Labels: []LabelDesc{
							{
								Name:         "test_label_one",
								ValuePrefix:  "test_prefix_one",
								UniqueValues: 2,
							},
							{
								Name:         "test_label_two",
								ValuePrefix:  "test_prefix_two",
								UniqueValues: 3,
							},
						},
					},
				},
			},
			numSeries: 6,
		},
		{
			name: "multiple series",
			workloadDesc: WorkloadDesc{
				Replicas: 1,
				Series: []SeriesDesc{
					{
						Name:         "test_series_one",
						Type:         GaugeZero,
						StaticLabels: map[string]string{"constant_label": "true"},
						Labels: []LabelDesc{
							{
								Name:         "test_label_one",
								ValuePrefix:  "test_prefix_one",
								UniqueValues: 2,
							},
							{
								Name:         "test_label_two",
								ValuePrefix:  "test_prefix_two",
								UniqueValues: 3,
							},
						},
					},
					{
						Name:         "test_series_two",
						Type:         GaugeZero,
						StaticLabels: map[string]string{"constant_label": "true"},
						Labels: []LabelDesc{
							{
								Name:         "test_label_one",
								ValuePrefix:  "test_prefix_one",
								UniqueValues: 2,
							},
							{
								Name:         "test_label_two",
								ValuePrefix:  "test_prefix_two",
								UniqueValues: 2,
							},
						},
					},
				},
			},
			numSeries: 10,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			w := newWriteWorkload(testCase.workloadDesc, prometheus.NewRegistry())
			generatedSeries := w.GenerateTimeSeries("test-id", time.Now())
			require.Equal(t, testCase.numSeries, countUniqueTimeSeries(generatedSeries))
		})
	}
}

func countUniqueTimeSeries(series []prompb.TimeSeries) int {
	seriesMap := map[string]bool{}

	for _, ts := range series {
		seriesMap[ts.String()] = true
	}

	return len(seriesMap)
}
