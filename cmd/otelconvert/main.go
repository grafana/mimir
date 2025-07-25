package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	utillog "github.com/grafana/mimir/pkg/util/log"
)

const (
	mdChunkSizeBytes = 500_000_000
)

var (
	writtenBytesCount = 0
)

type config struct {
	bucket  bucket.Config
	userID  string
	blockID string
	dest    string
	count   bool
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.userID, "user", "", "The user (tenant) that owns the blocks to be listed")
	flag.StringVar(&cfg.blockID, "block", "", "The block ID of the block to convert")
	flag.StringVar(&cfg.dest, "dest", "", "The path to write the resulting files to")
	flag.BoolVar(&cfg.count, "count", true, "Only count the number of bytes that would've been written to disk")

	// Parse CLI flags.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}
	if cfg.blockID == "" {
		log.Fatalln("no block specified")
	}
	if cfg.dest == "" && !cfg.count {
		log.Fatalln("no destination specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	logger := gokitlog.NewNopLogger()

	overallBktClient, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket client:", err)
	}

	bkt := bucket.NewPrefixedBucketClient(overallBktClient, cfg.userID)

	blockID, err := ulid.Parse(cfg.blockID)
	if err != nil {
		log.Fatalln("failed to parse block ID:", err)
	}

	blockDir := os.TempDir()
	defer func() { _ = os.RemoveAll(blockDir) }()

	fmt.Printf("downloading block %s to %s...\n", cfg.blockID, blockDir)

	err = block.Download(ctx, logger, bkt, blockID, blockDir)
	if err != nil {
		log.Fatalln("failed to download block:", err)
	}

	b, err := tsdb.OpenBlock(utillog.SlogFromGoKit(logger), blockDir, nil, nil)
	if err != nil {
		log.Fatalln("failed to open block:", err)
	}

	cr, err := b.Chunks()
	if err != nil {
		log.Fatalln("failed to get chunks reader:", err)
	}
	defer func() { _ = cr.Close() }()

	ir, err := b.Index()
	if err != nil {
		log.Fatalln("failed to get index reader:", err)
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
			log.Fatalln("failed to iterate postings:", err)
		}

		// Flush a full chunk to disk.
		if proto.Size(md) > mdChunkSizeBytes {
			chunkCount, err = writeMetricsDataChunkToFile(md, cfg.dest, cfg.count, chunkCount)
			if err != nil {
				log.Fatalln("failed to write metrics data to file:", err)
			}

			md.Reset()
		}

		chkMetas := []chunks.Meta(nil)
		err = ir.Series(p.At(), &builder, &chkMetas)
		if err != nil {
			log.Fatalln("failed to populate series chunk metas:", err)
		}

		metricName, resourceAttrs := lblsToResourceAttributes(builder)

		var (
			gauge = &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{}}
			histo = &metricsv1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsv1.ExponentialHistogram{}}
		)

		for _, chkMeta := range chkMetas {
			chk, itr, err := cr.ChunkOrIterable(chkMeta)
			if err != nil {
				log.Fatalln("failed to access chunk:", err)
			}

			if itr != nil {
				log.Fatalln("unexpected chunk iterator")
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
					log.Fatalln("unexpected value type:", valType)
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
			log.Fatalln("both float and histogram values associated with metric:", metricName)
		} else if len(gauge.Gauge.DataPoints) > 0 {
			rm.ScopeMetrics[0].Metrics[0].Data = gauge
			md.ResourceMetrics = append(md.ResourceMetrics, rm)
		} else if len(histo.ExponentialHistogram.DataPoints) > 0 {
			rm.ScopeMetrics[0].Metrics[0].Data = histo
			md.ResourceMetrics = append(md.ResourceMetrics, rm)
		}
	}

	// Flush any remaining partial chunk to disk.
	if proto.Size(md) > 0 {
		_, err = writeMetricsDataChunkToFile(md, cfg.dest, cfg.count, chunkCount)
		if err != nil {
			log.Fatalln("failed to write metrics data to file:", err)
		}
	}
}

func writeMetricsDataChunkToFile(md *metricsv1.MetricsData, destDir string, countOnly bool, chunkCount int) (int, error) {
	if countOnly {
		n := proto.Size(md)
		writtenBytesCount += n
		fmt.Printf("registered %d bytes for chunk %d (%d bytes overall)\n", n, chunkCount, writtenBytesCount)

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

	fmt.Printf("wrote chunk %d to disk (%d bytes)\n", chunkCount, n)

	return chunkCount + 1, nil
}

func lblsToResourceAttributes(builder labels.ScratchBuilder) (string, []*commonv1.KeyValue) {
	builder.Sort()
	lbls := builder.Labels()

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
