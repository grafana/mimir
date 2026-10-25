// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// recordResources returns, by series, the attributes of the resource that each series with data refers to in the
// records, as "key=value" strings, in their order. It fails on a series whose ref differs between records.
func recordResources(t *testing.T, kafkaCluster *kfake.Cluster, numPartitions int32) map[string][]string {
	t.Helper()
	resources := map[string][]string{}
	for _, record := range readAllRecordsFromKafka(t, kafkaCluster.ListenAddrs(), numPartitions, time.Second) {
		require.Equal(t, 2, ingest.ParseRecordVersion(record))
		var request mimirpb.PreallocWriteRequest
		require.NoError(t, ingest.DeserializeRecordContent(record.Value, &request, 2))
		for _, ts := range request.Timeseries {
			if len(ts.Samples) == 0 && len(ts.Histograms) == 0 && len(ts.Exemplars) == 0 {
				continue
			}
			series := mimirpb.FromLabelAdaptersToString(ts.Labels)
			var attributes []string
			if ts.ResourceRef != 0 {
				for _, a := range request.ResourceTable[ts.ResourceRef-1].Attributes {
					value := a.StringValue
					switch a.Type {
					case mimirpb.RESOURCE_ATTRIBUTE_INT:
						value = fmt.Sprint(a.IntValue)
					case mimirpb.RESOURCE_ATTRIBUTE_BOOL:
						value = fmt.Sprint(a.BoolValue)
					}
					attributes = append(attributes, a.Key+"="+value)
				}
			}
			resources[series] = attributes
		}
	}
	return resources
}

// Each OTLP series' resource reaches the records with it, within the limits on resource shape, for a tenant with
// native resources; and a Remote Write client can't give series resources.
func TestResourcesReachRecords(t *testing.T) {
	const numPartitions = 4
	now := time.Now()
	longValue := strings.Repeat("v", 40)

	otlpRequest := func(t *testing.T) *http.Request {
		md := pmetric.NewMetrics()
		addResource := func(service string, attributes map[string]string, cores int64) {
			resource := md.ResourceMetrics().AppendEmpty()
			resource.Resource().Attributes().PutStr("service.name", service)
			resource.Resource().Attributes().PutStr("service.instance.id", "1")
			resource.Resource().Attributes().PutInt("host.cores", cores)
			for k, v := range attributes {
				resource.Resource().Attributes().PutStr(k, v)
			}
			metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("requests")
			gauge := metric.SetEmptyGauge()
			for n := range 8 {
				point := gauge.DataPoints().AppendEmpty()
				point.SetTimestamp(pcommon.NewTimestampFromTime(now))
				point.SetDoubleValue(1)
				point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", n))
			}
		}
		addResource("frontend", map[string]string{"host.name": "node-1"}, 8)
		addResource("backend", map[string]string{"host.name": "node-2", "process.command_line": longValue}, 4)
		addResource(longValue, nil, 2)
		return createOTLPProtoRequest(t, pmetricotlp.NewExportRequestFromMetrics(md), "")
	}
	otlpHandler := func(push PushFunc, limits *validation.Overrides) http.Handler {
		return OTLPHandler(100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil, push, nil, nil, log.NewNopLogger())
	}

	tests := map[string]struct {
		handler         func(push PushFunc, limits *validation.Overrides) http.Handler
		request         func(t *testing.T) *http.Request
		nativeResources bool
		expected        func(series string) []string
		// Of the series with data.
		expectedWithResources int
	}{
		"OTLP": {
			handler:         otlpHandler,
			request:         otlpRequest,
			nativeResources: true,
			expected: func(series string) []string {
				switch {
				case strings.HasPrefix(series, "target_info{"):
					// target_info carries no resource.
					return nil
				case strings.Contains(series, `job="frontend"`):
					return []string{"service.name=frontend", "service.instance.id=1", "host.cores=8", "host.name=node-1"}
				case strings.Contains(series, `job="backend"`):
					// The long descriptive value is dropped.
					return []string{"service.name=backend", "service.instance.id=1", "host.cores=4", "host.name=node-2"}
				default:
					// A long service attribute drops the resource.
					return nil
				}
			},
			expectedWithResources: 16,
		},
		"OTLP without native resources": {
			handler:  otlpHandler,
			request:  otlpRequest,
			expected: func(string) []string { return nil },
		},
		"remote write 2.0 with a client's resources": {
			handler: func(push PushFunc, limits *validation.Overrides) http.Handler {
				return Handler(100000, nil, nil, false, false, limits, RetryConfig{}, push, nil, log.NewNopLogger())
			},
			request: func(t *testing.T) *http.Request {
				req := &mimirpb.WriteRequest{ResourceTable: []mimirpb.ResourceEntry{{Attributes: []mimirpb.ResourceAttribute{{Key: "service.name", StringValue: "injected"}}}}}
				for n := range 8 {
					req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
						Labels:      []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "requests"}, {Name: "pod", Value: fmt.Sprintf("pod-%d", n)}},
						Samples:     []mimirpb.Sample{{TimestampMs: now.UnixMilli(), Value: 1}},
						ResourceRef: 1,
					}})
				}
				rw2, err := mimirpb.FromWriteRequestToRW2Request(req, nil, 0)
				require.NoError(t, err)
				require.Len(t, rw2.ResourceTable, 1)
				body, err := rw2.Marshal()
				require.NoError(t, err)
				httpReq := createRequest(t, body)
				httpReq.Header.Set("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request")
				httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")
				return httpReq
			},
			nativeResources: true,
			expected:        func(string) []string { return nil },
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.NativeResourcesEnabled = test.nativeResources
			limits.NativeResourcesMaxAttributeValueLength = 32
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
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())

			resources := recordResources(t, kafkaCluster, numPartitions)
			require.NotEmpty(t, resources)
			withResources := 0
			for series, attributes := range resources {
				require.Equal(t, test.expected(series), attributes, series)
				if attributes != nil {
					withResources++
				}
			}
			require.Equal(t, test.expectedWithResources, withResources)
		})
	}
}

// The distributor's CPU per sample to convert OTLP, with native resources off and on, for resources with attributes
// like a Kubernetes workload's.
func BenchmarkOTLPConversionNativeResources(b *testing.B) {
	const (
		numResources       = 20
		seriesPerResource  = 100
		descriptivePerKind = 6
	)
	md := pmetric.NewMetrics()
	now := pcommon.NewTimestampFromTime(time.Now())
	for r := range numResources {
		resource := md.ResourceMetrics().AppendEmpty()
		attributes := resource.Resource().Attributes()
		attributes.PutStr("service.name", fmt.Sprintf("service-%d", r))
		attributes.PutStr("service.namespace", "shop")
		attributes.PutStr("service.instance.id", fmt.Sprintf("instance-%d", r))
		for i := range descriptivePerKind {
			attributes.PutStr(fmt.Sprintf("k8s.label.%d", i), fmt.Sprintf("value-%d-%d", r, i))
			attributes.PutStr(fmt.Sprintf("host.attribute.%d", i), fmt.Sprintf("host-%d-%d", r, i))
			attributes.PutInt(fmt.Sprintf("process.number.%d", i), int64(r*i))
		}
		metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		metric.SetName("requests")
		gauge := metric.SetEmptyGauge()
		for s := range seriesPerResource {
			point := gauge.DataPoints().AppendEmpty()
			point.SetTimestamp(now)
			point.SetDoubleValue(1)
			point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", s))
		}
	}
	const samples = numResources * seriesPerResource

	for _, nativeResources := range []bool{false, true} {
		b.Run(fmt.Sprintf("native resources=%t", nativeResources), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
				series, _, table, _, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, md, conversionOptions{nativeResources: nativeResources}, log.NewNopLogger())
				if err != nil {
					b.Fatal(err)
				}
				if nativeResources != (len(table) == numResources) {
					b.Fatalf("got %d resources", len(table))
				}
				mimirpb.ReuseSlice(series)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*samples), "ns/sample")
		})
	}
}
