// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"cmp"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

// TestIngesterStartTimestampZeroSamples checks the zero samples that the ingester appends for the per-sample start
// timestamps of remote write 2.0, with the default zero_sample start timestamp strategy.
func TestIngesterStartTimestampZeroSamples(t *testing.T) {
	const (
		tenantOOODisabled = "st-ooo-disabled"
		tenantOOOEnabled  = "st-ooo-enabled"
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	runtimeConfig := fmt.Sprintf("overrides:\n  %q:\n    out_of_order_time_window: 1h\n", tenantOOOEnabled)
	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(runtimeConfig)))

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingester.ring.heartbeat-period": "1s",
			"-runtime-config.file":            filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),
		},
	)

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	}

	// All timestamps in the test cases are offsets from base, in milliseconds.
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Second).UnixMilli()
	float := func(st, ts int64, v float64) promRW2.Sample {
		return promRW2.Sample{StartTimestamp: base + st, Timestamp: base + ts, Value: v}
	}
	histogram := func(st, ts int64, count uint64) promRW2.Histogram {
		return promRW2.Histogram{
			Count:          &promRW2.Histogram_CountInt{CountInt: count},
			Sum:            float64(count),
			PositiveSpans:  []promRW2.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{int64(count)},
			StartTimestamp: base + st,
			Timestamp:      base + ts,
		}
	}

	testCases := map[string]struct {
		// Each entry is a separate time series in the request, all with the same labels.
		timeseries []promRW2.TimeSeries
		expected   []string
		// Defaults to expected.
		expectedOOOEnabled []string
	}{
		"float samples": {
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1), float(500, 2000, 2)}},
			},
			expected: []string{"float@500=0", "float@1000=1", "float@2000=2"},
		},
		"float samples with a changing start timestamp": {
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1), float(1500, 2000, 2)}},
			},
			expected: []string{"float@500=0", "float@1000=1", "float@1500=0", "float@2000=2"},
		},
		"histogram samples": {
			timeseries: []promRW2.TimeSeries{
				{Histograms: []promRW2.Histogram{histogram(500, 1000, 1), histogram(500, 2000, 2)}},
			},
			expected: []string{"histogram@500=0", "histogram@1000=1", "histogram@2000=2"},
		},
		"histogram samples with a changing start timestamp": {
			timeseries: []promRW2.TimeSeries{
				{Histograms: []promRW2.Histogram{histogram(500, 1000, 1), histogram(1500, 2000, 2)}},
			},
			expected: []string{"histogram@500=0", "histogram@1000=1", "histogram@1500=0", "histogram@2000=2"},
		},
		"float samples with a start timestamp before an earlier sample with a higher timestamp": {
			// The zero sample at 2500 would go to the out-of-order head, because of the sample at 3000.
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1), float(500, 3000, 3), float(500, 2000, 2), float(2500, 4000, 4)}},
			},
			expected:           []string{"float@500=0", "float@1000=1", "float@3000=3", "float@4000=4"},
			expectedOOOEnabled: []string{"float@500=0", "float@1000=1", "float@2000=2", "float@3000=3", "float@4000=4"},
		},
		"histogram samples with a start timestamp before an earlier sample with a higher timestamp": {
			// The zero histogram at 2500 would go to the out-of-order head, because of the histogram at 3000.
			timeseries: []promRW2.TimeSeries{
				{Histograms: []promRW2.Histogram{histogram(500, 1000, 1), histogram(500, 3000, 3), histogram(500, 2000, 2), histogram(2500, 4000, 4)}},
			},
			expected:           []string{"histogram@500=0", "histogram@1000=1", "histogram@3000=3", "histogram@4000=4"},
			expectedOOOEnabled: []string{"histogram@500=0", "histogram@1000=1", "histogram@2000=2", "histogram@3000=3", "histogram@4000=4"},
		},
		// Remote write 2.0 doesn't allow samples and histograms in the same time series, but Mimir accepts them.
		"float and histogram with the same start timestamp in one time series, float first": {
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1)}, Histograms: []promRW2.Histogram{histogram(500, 2000, 2)}},
			},
			expected: []string{"float@500=0", "float@1000=1", "histogram@2000=2"},
		},
		"float and histogram with the same start timestamp in one time series, histogram first": {
			// Floats are appended before histograms, so the histogram is out-of-order and doesn't get a zero sample.
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 2000, 2)}, Histograms: []promRW2.Histogram{histogram(500, 1000, 1)}},
			},
			expected:           []string{"float@500=0", "float@2000=2"},
			expectedOOOEnabled: []string{"float@500=0", "histogram@1000=1", "float@2000=2"},
		},
		// Each time series appends its own zero sample. The second one is before the samples of the first time series,
		// so it's dropped, or with the out-of-order window enabled, it's returned instead of the first one.
		"float and histogram with the same start timestamp in separate time series, float first": {
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1)}},
				{Histograms: []promRW2.Histogram{histogram(500, 2000, 2)}},
			},
			expected:           []string{"float@500=0", "float@1000=1", "histogram@2000=2"},
			expectedOOOEnabled: []string{"histogram@500=0", "float@1000=1", "histogram@2000=2"},
		},
		"float and histogram with the same start timestamp in separate time series, histogram first": {
			timeseries: []promRW2.TimeSeries{
				{Histograms: []promRW2.Histogram{histogram(500, 1000, 1)}},
				{Samples: []promRW2.Sample{float(500, 2000, 2)}},
			},
			expected:           []string{"histogram@500=0", "histogram@1000=1", "float@2000=2"},
			expectedOOOEnabled: []string{"float@500=0", "histogram@1000=1", "float@2000=2"},
		},
		"float samples with the same start timestamp in separate time series": {
			timeseries: []promRW2.TimeSeries{
				{Samples: []promRW2.Sample{float(500, 1000, 1)}},
				{Samples: []promRW2.Sample{float(500, 2000, 2)}},
			},
			expected: []string{"float@500=0", "float@1000=1", "float@2000=2"},
		},
	}

	caseNumber := 0
	for name, tc := range testCases {
		caseNumber++
		metricName := fmt.Sprintf("start_timestamp_%d", caseNumber)
		for _, tenant := range []string{tenantOOODisabled, tenantOOOEnabled} {
			t.Run(fmt.Sprintf("%s/%s", name, tenant), func(t *testing.T) {
				client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", tenant)
				require.NoError(t, err)

				req := &promRW2.Request{
					Symbols:    []string{"", model.MetricNameLabel, metricName},
					Timeseries: slices.Clone(tc.timeseries),
				}
				for i := range req.Timeseries {
					req.Timeseries[i].LabelsRefs = []uint32{1, 2}
				}
				res, err := client.PushRW2(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)

				result, _, _, err := client.Query(fmt.Sprintf("%s[1h]", metricName), time.Now())
				require.NoError(t, err)
				matrix, ok := result.(model.Matrix)
				require.True(t, ok, "unexpected result type %T", result)
				require.Len(t, matrix, 1)

				type sample struct {
					ts  int64
					str string
				}
				var actual []sample
				for _, f := range matrix[0].Values {
					ts := int64(f.Timestamp) - base
					actual = append(actual, sample{ts, fmt.Sprintf("float@%d=%v", ts, f.Value)})
				}
				for _, h := range matrix[0].Histograms {
					ts := int64(h.Timestamp) - base
					actual = append(actual, sample{ts, fmt.Sprintf("histogram@%d=%v", ts, h.Histogram.Count)})
				}
				slices.SortStableFunc(actual, func(a, b sample) int { return cmp.Compare(a.ts, b.ts) })
				actualStrings := make([]string, 0, len(actual))
				for _, smpl := range actual {
					actualStrings = append(actualStrings, smpl.str)
				}

				expected := tc.expected
				if tenant == tenantOOOEnabled && tc.expectedOOOEnabled != nil {
					expected = tc.expectedOOOEnabled
				}
				require.Equal(t, expected, actualStrings)
			})
		}
	}
}
