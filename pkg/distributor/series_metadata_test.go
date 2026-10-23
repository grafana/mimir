// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/rw2util"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Each series' own metric metadata, from a Remote Write 2.0 or an OTLP push, reaches the records with the series,
// within the metric metadata limits.
func TestSeriesMetadataReachesRecords(t *testing.T) {
	const (
		numPartitions     = 4
		maxMetadataLength = 32
	)
	now := time.Now()
	longHelp := strings.Repeat("h", maxMetadataLength+8)

	tests := map[string]struct {
		handler        func(push PushFunc, limits *validation.Overrides) http.Handler
		request        func(t *testing.T) *http.Request
		expectedStatus int
		expected       map[string]mimirpb.SeriesMetadata // By metric name.
		// The metric families' metadata with a unit that's too long, counted once each.
		expectedUnitTooLong int
	}{
		"remote write 2.0": {
			handler: func(push PushFunc, limits *validation.Overrides) http.Handler {
				return Handler(100000, nil, nil, false, false, limits, RetryConfig{}, push, nil, log.NewNopLogger())
			},
			request: func(t *testing.T) *http.Request {
				samples := []promRW2.Sample{{Value: 1, Timestamp: now.UnixMilli()}}
				var rw2 *promRW2.Request
				for n := range 8 {
					rw2 = rw2util.AddFloatSeries(rw2, labels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprintf("pod-%d", n)), samples, promRW2.Metadata_METRIC_TYPE_COUNTER, "Requests.", "requests", nil)
				}
				rw2 = rw2util.AddFloatSeries(rw2, labels.FromStrings(model.MetricNameLabel, "long_help_total"), samples, promRW2.Metadata_METRIC_TYPE_COUNTER, longHelp, "", nil)
				rw2 = rw2util.AddFloatSeries(rw2, labels.FromStrings(model.MetricNameLabel, "long_unit_total"), samples, promRW2.Metadata_METRIC_TYPE_COUNTER, "Long unit.", strings.Repeat("u", maxMetadataLength+1), nil)
				exemplars := []exemplar.Exemplar{{Labels: labels.FromStrings("trace_id", "abc"), Value: 1, Ts: now.UnixMilli()}}
				rw2 = rw2util.AddFloatSeries(rw2, labels.FromStrings(model.MetricNameLabel, "exemplars_total", "pod", "pod-0"), nil, promRW2.Metadata_METRIC_TYPE_GAUGE, "With exemplars.", "", exemplars)
				body, err := rw2.Marshal()
				require.NoError(t, err)
				req := createRequest(t, body)
				req.Header.Set("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request")
				req.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")
				return req
			},
			// The metric family's metadata with the unit that's too long is a partial error, as before.
			expectedStatus: http.StatusBadRequest,
			expected: map[string]mimirpb.SeriesMetadata{
				"requests_total":  {Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"},
				"long_help_total": {Type: mimirpb.COUNTER, Help: longHelp[:maxMetadataLength]},
				// A unit that's too long drops the series' metadata, though not the series.
				"long_unit_total": {},
				"exemplars_total": {Type: mimirpb.GAUGE, Help: "With exemplars."},
			},
			expectedUnitTooLong: 1,
		},
		"OTLP": {
			handler: func(push PushFunc, limits *validation.Overrides) http.Handler {
				return OTLPHandler(100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil, push, nil, nil, log.NewNopLogger())
			},
			request: func(t *testing.T) *http.Request {
				md := pmetric.NewMetrics()
				resource := md.ResourceMetrics().AppendEmpty()
				resource.Resource().Attributes().PutStr("service.name", "service")
				metrics := resource.ScopeMetrics().AppendEmpty().Metrics()
				timestamp := pcommon.NewTimestampFromTime(now)

				requests := metrics.AppendEmpty()
				requests.SetName("requests")
				requests.SetDescription("Requests.")
				sum := requests.SetEmptySum()
				sum.SetIsMonotonic(true)
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				for n := range 8 {
					point := sum.DataPoints().AppendEmpty()
					point.SetTimestamp(timestamp)
					point.SetDoubleValue(1)
					point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", n))
				}

				latency := metrics.AppendEmpty()
				latency.SetName("latency")
				latency.SetDescription("Latency.")
				latency.SetUnit("s")
				point := latency.SetEmptyGauge().DataPoints().AppendEmpty()
				point.SetTimestamp(timestamp)
				point.SetDoubleValue(1)

				duration := metrics.AppendEmpty()
				duration.SetName("duration")
				duration.SetDescription("Duration.")
				duration.SetUnit("s")
				histogram := duration.SetEmptyHistogram()
				histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				histogramPoint := histogram.DataPoints().AppendEmpty()
				histogramPoint.SetTimestamp(timestamp)
				histogramPoint.ExplicitBounds().FromRaw([]float64{0.1, 1})
				histogramPoint.BucketCounts().FromRaw([]uint64{1, 1, 1})
				histogramPoint.SetCount(3)
				histogramPoint.SetSum(2)

				return createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
			},
			expectedStatus: http.StatusOK,
			// The metric names have no suffixes, as the default translation strategy has it, but the units are
			// translated.
			expected: map[string]mimirpb.SeriesMetadata{
				"requests":        {Type: mimirpb.COUNTER, Help: "Requests."},
				"latency":         {Type: mimirpb.GAUGE, Help: "Latency.", Unit: "seconds"},
				"duration_bucket": {Type: mimirpb.HISTOGRAM, Help: "Duration.", Unit: "seconds"},
				"duration_sum":    {Type: mimirpb.HISTOGRAM, Help: "Duration.", Unit: "seconds"},
				"duration_count":  {Type: mimirpb.HISTOGRAM, Help: "Duration.", Unit: "seconds"},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.MaxMetadataLength = maxMetadataLength
			limits.MaxGlobalExemplarsPerUser = 100
			kafkaCluster, _ := testkafka.CreateCluster(t, numPartitions, kafkaTopic)
			distributors, _, _, _ := prepare(t, prepConfig{
				numDistributors:         1,
				numIngesters:            numPartitions,
				happyIngesters:          numPartitions,
				replicationFactor:       1,
				ingesterIngestionType:   ingesterIngestionTypeGRPC,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: numPartitions,
				ingestStorageKafka:      kafkaCluster,
				limits:                  limits,
			})
			response := httptest.NewRecorder()
			test.handler(distributors[0].PushWithMiddlewares, validation.NewOverrides(*limits, nil)).ServeHTTP(response, test.request(t))
			require.Equal(t, test.expectedStatus, response.Code, response.Body.String())

			actual := map[string]mimirpb.SeriesMetadata{}
			for _, record := range readAllRecordsFromKafka(t, kafkaCluster.ListenAddrs(), numPartitions, time.Second) {
				require.Equal(t, 2, ingest.ParseRecordVersion(record))
				var request mimirpb.PreallocWriteRequest
				require.NoError(t, ingest.DeserializeRecordContent(record.Value, &request, 2))
				for _, ts := range request.Timeseries {
					if len(ts.Samples) == 0 && len(ts.Histograms) == 0 && len(ts.Exemplars) == 0 {
						// A series without data stands in for a metric family's metadata.
						continue
					}
					metricName, err := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
					require.NoError(t, err)
					if seen, ok := actual[metricName]; ok {
						require.Equal(t, seen, ts.SeriesMetadata, "series of %s", metricName)
						continue
					}
					actual[metricName] = ts.SeriesMetadata
				}
			}
			require.Equal(t, test.expected, actual)
			require.Equal(t, float64(test.expectedUnitTooLong), testutil.ToFloat64(distributors[0].metadataValidationMetrics.unitTooLong.WithLabelValues("test")))
		})
	}
}
