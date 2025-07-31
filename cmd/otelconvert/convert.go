package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	utillog "github.com/grafana/mimir/pkg/util/log"
)

func validateConvertConfig(cfg config) error {
	if cfg.block == "" {
		return fmt.Errorf("missing --block")
	}
	if cfg.dest == "" && !cfg.count {
		return fmt.Errorf("must use either --dest or --count")
	}

	return nil
}

func convertBlock(ctx context.Context, orig, dest string, count bool, chunkSize int, logger gokitlog.Logger) error {
	b, err := tsdb.OpenBlock(utillog.SlogFromGoKit(logger), orig, nil, nil)
	if err != nil {
		return fmt.Errorf("open block: %w", err)
	}

	cr, err := b.Chunks()
	if err != nil {
		return fmt.Errorf("get chunks reader: %w", err)
	}
	defer func() { _ = cr.Close() }()

	ir, err := b.Index()
	if err != nil {
		return fmt.Errorf("get index reader: %w", err)
	}
	defer func() { _ = ir.Close() }()

	p := ir.PostingsForAllLabelValues(ctx, "__name__")
	p = ir.SortedPostings(p)

	var builder labels.ScratchBuilder

	md := &metricsv1.MetricsData{}
	md.Reset()

	chunkCount := 0

	for p.Next() {
		if err = p.Err(); err != nil {
			return fmt.Errorf("iterate postings: %w", err)
		}

		// Flush a full chunk to disk.
		if proto.Size(md) > chunkSize {
			chunkCount, err = writeMetricsDataChunkToFile(md, dest, count, chunkCount)
			if err != nil {
				return fmt.Errorf("write metrics data to file: %w", err)
			}

			md.Reset()
		}

		chkMetas := []chunks.Meta(nil)
		err = ir.Series(p.At(), &builder, &chkMetas)
		if err != nil {
			return fmt.Errorf("populate series chunk metas: %w", err)
		}

		metricName, resourceAttrs := lblsToResourceAttributes(builder)

		var (
			gauge = &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{}}
			histo = &metricsv1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsv1.ExponentialHistogram{}}
		)

		for _, chkMeta := range chkMetas {
			chk, itr, err := cr.ChunkOrIterable(chkMeta)
			if err != nil {
				return fmt.Errorf("access chunk: %w", err)
			}

			if itr != nil {
				return fmt.Errorf("unexpected chunk iterator")
			}

			chkItr := chk.Iterator(nil)
			for valType := chkItr.Next(); valType != chunkenc.ValNone; valType = chkItr.Next() {
				switch valType {
				case chunkenc.ValFloat:
					ts, val := chkItr.At()
					gauge.Gauge.DataPoints = append(gauge.Gauge.DataPoints, &metricsv1.NumberDataPoint{
						TimeUnixNano: uint64(time.UnixMilli(ts).UnixNano()),
						Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: val},
					})
				case chunkenc.ValHistogram:
					ts, h := chkItr.AtHistogram(nil)
					histo.ExponentialHistogram.DataPoints = append(histo.ExponentialHistogram.DataPoints, &metricsv1.ExponentialHistogramDataPoint{
						TimeUnixNano:  uint64(time.UnixMilli(ts).UnixNano()),
						Count:         h.Count,
						Sum:           &h.Sum,
						Scale:         h.Schema,
						ZeroCount:     h.ZeroCount,
						ZeroThreshold: h.ZeroThreshold,
						Positive:      translateToOTELHistogramBuckets(h.PositiveSpans, h.PositiveBucketIterator()),
						Negative:      translateToOTELHistogramBuckets(h.NegativeSpans, h.NegativeBucketIterator()),
					})
				case chunkenc.ValFloatHistogram:
					ts, fh := chkItr.AtFloatHistogram(nil)
					histo.ExponentialHistogram.DataPoints = append(histo.ExponentialHistogram.DataPoints, &metricsv1.ExponentialHistogramDataPoint{
						TimeUnixNano:  uint64(time.UnixMilli(ts).UnixNano()),
						Count:         uint64(fh.Count),
						Sum:           &fh.Sum,
						Scale:         fh.Schema,
						ZeroCount:     uint64(fh.ZeroCount),
						ZeroThreshold: fh.ZeroThreshold,
						Positive:      translateToOTELFloatHistogramBuckets(fh.PositiveSpans, fh.PositiveBucketIterator()),
						Negative:      translateToOTELFloatHistogramBuckets(fh.NegativeSpans, fh.NegativeBucketIterator()),
					})
				default:
					return fmt.Errorf("unexpected value type: %s", valType)
				}
			}
		}

		rm := &metricsv1.ResourceMetrics{
			Resource: &resourcev1.Resource{
				Attributes: resourceAttrs,
			},
			ScopeMetrics: []*metricsv1.ScopeMetrics{
				{
					Metrics: []*metricsv1.Metric{
						{
							Name: metricName,
						},
					},
				},
			},
		}

		if len(gauge.Gauge.DataPoints) > 0 && len(histo.ExponentialHistogram.DataPoints) > 0 {
			return fmt.Errorf("both float and histogram values associated with metric: %s", metricName)
		} else if len(gauge.Gauge.DataPoints) > 0 {
			rm.ScopeMetrics[0].Metrics[0].Data = gauge
			md.ResourceMetrics = append(md.ResourceMetrics, rm)
		} else if len(histo.ExponentialHistogram.DataPoints) > 0 {
			rm.ScopeMetrics[0].Metrics[0].Data = histo
			md.ResourceMetrics = append(md.ResourceMetrics, rm)
		}

		if verbose {
			log.Printf("current size: %d\n", proto.Size(md))
		}
	}

	// Flush any remaining partial chunk to disk.
	if proto.Size(md) > 0 {
		_, err = writeMetricsDataChunkToFile(md, dest, count, chunkCount)
		if err != nil {
			return fmt.Errorf("write metrics data to file: %w", err)
		}
	}

	return nil
}

func writeMetricsDataChunkToFile(md *metricsv1.MetricsData, destDir string, countOnly bool, chunkCount int) (int, error) {
	if countOnly {
		n := proto.Size(md)
		writtenBytesCount += n
		log.Printf("registered %d bytes for chunk %d (%d bytes overall)\n", n, chunkCount, writtenBytesCount)

		return chunkCount + 1, nil
	}

	buf, err := proto.Marshal(md)
	if err != nil {
		return chunkCount, fmt.Errorf("marshal protobuf message: %w", err)
	}

	fp := filepath.Join(destDir, strconv.Itoa(chunkCount))
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return chunkCount, fmt.Errorf("open file '%s' for writing: %w", fp, err)
	}

	n, err := f.Write(buf)
	if err != nil {
		return chunkCount, fmt.Errorf("write to file '%s': %w", fp, err)
	}
	defer func() { _ = f.Close() }()

	log.Printf("wrote chunk %d to disk (%d bytes)\n", chunkCount, n)

	return chunkCount + 1, nil
}

func lblsToResourceAttributes(builder labels.ScratchBuilder) (string, []*commonv1.KeyValue) {
	builder.Sort()
	lbls := builder.Labels()

	if verbose {
		log.Printf("processing series %s...\n", lbls.String())
	}

	res := make([]*commonv1.KeyValue, lbls.Len())
	var name string

	i := 0
	lbls.Range(func(lbl labels.Label) {
		res[i] = &commonv1.KeyValue{
			Key: lbl.Name,
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: lbl.Value},
			},
		}

		if lbl.Name == "__name__" {
			name = lbl.Value
		}

		i++
	})

	return name, res
}

func translateToOTELHistogramBuckets(spans []histogram.Span, itr histogram.BucketIterator[uint64]) *metricsv1.ExponentialHistogramDataPoint_Buckets {
	numBktCounts := 0
	for spanIdx, span := range spans {
		if spanIdx > 0 {
			numBktCounts += int(span.Offset)
		}
		numBktCounts += int(span.Length)
	}

	bkts := &metricsv1.ExponentialHistogramDataPoint_Buckets{
		Offset:       spans[0].Offset - 1, // -1 because otel offset is for the lower bound not the upper bound.
		BucketCounts: make([]uint64, numBktCounts),
	}

	i := 0
	for itr.Next() {
		bkts.BucketCounts[i] = itr.At().Count
		i++
	}

	return bkts
}

func translateToOTELFloatHistogramBuckets(spans []histogram.Span, itr histogram.BucketIterator[float64]) *metricsv1.ExponentialHistogramDataPoint_Buckets {
	numBktCounts := 0
	for spanIdx, span := range spans {
		if spanIdx > 0 {
			numBktCounts += int(span.Offset)
		}
		numBktCounts += int(span.Length)
	}

	bkts := &metricsv1.ExponentialHistogramDataPoint_Buckets{
		Offset:       spans[0].Offset - 1, // -1 because otel offset is for the lower bound not the upper bound.
		BucketCounts: make([]uint64, numBktCounts),
	}

	i := 0
	for itr.Next() {
		bkts.BucketCounts[i] = uint64(itr.At().Count)
		i++
	}

	return bkts
}
