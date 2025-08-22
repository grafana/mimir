package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestConvert(t *testing.T) {

}

func TestProtobufVsJson(t *testing.T) {
	numDps := 10000000
	dps := make([]*metricsv1.NumberDataPoint, numDps)
	for i := 0; i < numDps; i++ {
		ts := uint64(100 * i)
		dps[i] = &metricsv1.NumberDataPoint{
			TimeUnixNano: ts,
			Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0},
		}
	}

	md := &metricsv1.MetricsData{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key: "namespace",
							Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{
									StringValue: "test",
								},
							},
						},
					},
				},
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Scope: &commonv1.InstrumentationScope{},
						Metrics: []*metricsv1.Metric{
							{
								Name: "metric1",
								Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{
									DataPoints: dps,
								}},
							},
						},
					},
				},
			},
		},
	}

	jsonBuf, err := protojson.Marshal(md)
	require.NoError(t, err)

	jsonGZ, err := gzipBuf(jsonBuf)
	require.NoError(t, err)

	fmt.Printf("gzipped json: %d\n", len(jsonGZ))

	protoBuf, err := proto.Marshal(md)
	require.NoError(t, err)

	protoGZ, err := gzipBuf(protoBuf)
	require.NoError(t, err)

	fmt.Printf("gzipped proto: %d\n", len(protoGZ))
}

func gzipBuf(buf []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(buf); err != nil {
		return nil, fmt.Errorf("write gzip: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close gzip: %w", err)
	}

	return b.Bytes(), nil
}
