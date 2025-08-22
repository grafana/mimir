package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestConvert(t *testing.T) {
	ctx := context.Background()

	blocksDir := t.TempDir()
	destDir := t.TempDir()

	blockID, err := block.CreateBlock(ctx, blocksDir, []labels.Labels{
		labels.FromStrings("__name__", "foobar", "service", "test-service", "series", "series-1"), // float series
		labels.FromStrings("__name__", "foobar", "service", "test-service", "series", "series-2"), // histogram series
		labels.FromStrings("__name__", "foobar", "service", "test-service", "series", "series-3"), // float histogram series
		labels.FromStrings("__name__", "foobar", "service", "test-service", "series", "series-4"), // float series
	}, 199, 0, 200_000, labels.EmptyLabels()) // 1 sample/sec per series, 199 samples per series over 200 seconds.
	require.NoError(t, err)

	cfg := config{
		block:                   filepath.Join(blocksDir, blockID.String()),
		dest:                    destDir,
		outputFormat:            "json",
		resourceAttributeLabels: defaultResourceAttributeLabels,
		batchSize:               1,                 // Each series gets its own batch.
		chunkSize:               100 * time.Second, // 2 100-second chunks.
	}

	require.NoError(t, convertBlock(ctx, cfg, log.NewNopLogger()))

	chunkDirs, err := os.ReadDir(destDir)
	require.NoError(t, err)
	require.Len(t, chunkDirs, 2)
	requireFilenamesEqual(t, chunkDirs, []string{
		"0",
		"100000",
	})

	chunk0Path := filepath.Join(destDir, "0")

	chunk0, err := os.ReadDir(chunk0Path)
	require.NoError(t, err)

	require.Len(t, chunk0, 4)
	requireFilenamesEqual(t, chunk0, []string{
		"batch-0",
		"batch-1",
		"batch-2",
		"batch-3",
	})

	chunk0Batch0 := readJsonFile(t, filepath.Join(chunk0Path, "batch-0"))
	require.Len(t, chunk0Batch0.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk0Batch0.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk0Batch0.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk0Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk0Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-1", chunk0Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_Gauge{}, chunk0Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	gauge := chunk0Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_Gauge)
	require.Len(t, gauge.Gauge.DataPoints, 100)
	require.Equal(t, uint64(0), gauge.Gauge.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((99 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[len(gauge.Gauge.DataPoints)-1].TimeUnixNano)

	chunk0Batch1 := readJsonFile(t, filepath.Join(chunk0Path, "batch-1"))
	require.Len(t, chunk0Batch1.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk0Batch1.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk0Batch1.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk0Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk0Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-2", chunk0Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_ExponentialHistogram{}, chunk0Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	histo := chunk0Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_ExponentialHistogram)
	require.Len(t, histo.ExponentialHistogram.DataPoints, 100)
	require.Equal(t, uint64(0), histo.ExponentialHistogram.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((99 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[len(histo.ExponentialHistogram.DataPoints)-1].TimeUnixNano)

	chunk0Batch2 := readJsonFile(t, filepath.Join(chunk0Path, "batch-2"))
	require.Len(t, chunk0Batch2.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk0Batch2.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk0Batch2.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk0Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk0Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-3", chunk0Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_ExponentialHistogram{}, chunk0Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	histo = chunk0Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_ExponentialHistogram)
	require.Len(t, histo.ExponentialHistogram.DataPoints, 100)
	require.Equal(t, uint64(0), histo.ExponentialHistogram.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((99 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[len(histo.ExponentialHistogram.DataPoints)-1].TimeUnixNano)

	chunk0Batch3 := readJsonFile(t, filepath.Join(chunk0Path, "batch-3"))
	require.Len(t, chunk0Batch3.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk0Batch3.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk0Batch3.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk0Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk0Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-4", chunk0Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_Gauge{}, chunk0Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	gauge = chunk0Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_Gauge)
	require.Len(t, gauge.Gauge.DataPoints, 100)
	require.Equal(t, uint64(0), gauge.Gauge.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((99 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[len(gauge.Gauge.DataPoints)-1].TimeUnixNano)

	chunk1Path := filepath.Join(destDir, "100000")

	chunk1, err := os.ReadDir(chunk1Path)
	require.NoError(t, err)

	require.Len(t, chunk1, 4)
	requireFilenamesEqual(t, chunk1, []string{
		"batch-0",
		"batch-1",
		"batch-2",
		"batch-3",
	})

	chunk1Batch0 := readJsonFile(t, filepath.Join(chunk1Path, "batch-0"))
	require.Len(t, chunk1Batch0.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk1Batch0.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk1Batch0.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk1Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk1Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-1", chunk1Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_Gauge{}, chunk1Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	gauge = chunk1Batch0.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_Gauge)
	require.Len(t, gauge.Gauge.DataPoints, 99)
	require.Equal(t, uint64((100 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((198 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[len(gauge.Gauge.DataPoints)-1].TimeUnixNano)

	chunk1Batch1 := readJsonFile(t, filepath.Join(chunk1Path, "batch-1"))
	require.Len(t, chunk1Batch1.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk1Batch1.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk1Batch1.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk1Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk1Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-2", chunk1Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_ExponentialHistogram{}, chunk1Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	histo = chunk1Batch1.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_ExponentialHistogram)
	require.Len(t, histo.ExponentialHistogram.DataPoints, 99)
	require.Equal(t, uint64((100 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((198 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[len(histo.ExponentialHistogram.DataPoints)-1].TimeUnixNano)

	chunk1Batch2 := readJsonFile(t, filepath.Join(chunk1Path, "batch-2"))
	require.Len(t, chunk1Batch2.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk1Batch2.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk1Batch2.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk1Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk1Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-3", chunk1Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_ExponentialHistogram{}, chunk1Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	histo = chunk1Batch2.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_ExponentialHistogram)
	require.Len(t, histo.ExponentialHistogram.DataPoints, 99)
	require.Equal(t, uint64((100 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((198 * time.Second).Nanoseconds()), histo.ExponentialHistogram.DataPoints[len(histo.ExponentialHistogram.DataPoints)-1].TimeUnixNano)

	chunk1Batch3 := readJsonFile(t, filepath.Join(chunk1Path, "batch-3"))
	require.Len(t, chunk1Batch3.ResourceMetrics, 1)
	requireAttributesEqual(t, "service=test-service", chunk1Batch3.ResourceMetrics[0].Resource.Attributes)
	require.Len(t, chunk1Batch3.ResourceMetrics[0].ScopeMetrics, 1)
	require.Len(t, chunk1Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "foobar", chunk1Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Name)
	requireAttributesEqual(t, "series=series-4", chunk1Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Metadata)
	require.IsType(t, &metricsv1.Metric_Gauge{}, chunk1Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data)
	gauge = chunk1Batch3.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].Data.(*metricsv1.Metric_Gauge)
	require.Len(t, gauge.Gauge.DataPoints, 99)
	require.Equal(t, uint64((100 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[0].TimeUnixNano)
	require.Equal(t, uint64((198 * time.Second).Nanoseconds()), gauge.Gauge.DataPoints[len(gauge.Gauge.DataPoints)-1].TimeUnixNano)
}

func requireAttributesEqual(t *testing.T, expected string, actual []*commonv1.KeyValue) {
	t.Helper()
	require.Equal(t, expected, attributesString(actual))
}

func attributesString(attrs []*commonv1.KeyValue) string {
	var sb strings.Builder

	for i, attr := range attrs {
		sb.WriteString(attr.Key)
		sb.WriteString("=")
		sb.WriteString(attr.Value.GetStringValue())

		if i < len(attrs)-1 {
			sb.WriteString(",")
		}
	}

	return sb.String()
}

func requireFilenamesEqual(t *testing.T, dirEntries []os.DirEntry, expectedFilenames []string) {
	t.Helper()

	actualFilenames := make([]string, len(dirEntries))
	for i, entry := range dirEntries {
		actualFilenames[i] = entry.Name()
	}

	require.Equal(t, expectedFilenames, actualFilenames)
}

func readJsonFile(t *testing.T, path string) *metricsv1.MetricsData {
	t.Helper()

	var res metricsv1.MetricsData

	buf, err := os.ReadFile(path)
	require.NoError(t, err)

	require.NoError(t, protojson.Unmarshal(buf, &res))

	return &res
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
