package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/cmd/otelconvert/arrow"
	utillog "github.com/grafana/mimir/pkg/util/log"
)

func validateConvertConfig(cfg config) error {
	if cfg.block == "" {
		return fmt.Errorf("missing --block")
	}
	if cfg.dest == "" && !cfg.count {
		return fmt.Errorf("must use either --dest or --count")
	}
	if cfg.outputFormat != "protobuf" && cfg.outputFormat != "json" && cfg.outputFormat != "arrow" && cfg.outputFormat != "parquet" {
		return fmt.Errorf("--output-format must be one of 'json', 'protobuf', 'arrow', or 'parquet'")
	}

	return nil
}

type chunkedMetricsData struct {
	resourceAttributeLabels map[string]struct{}
	data                    map[int64]map[string]*uncomposedMetric
}

type uncomposedMetric struct {
	metricName    string
	resourceAttrs []*commonv1.KeyValue
	scopeAttrs    []*commonv1.KeyValue
	metricAttrs   []*commonv1.KeyValue
	gauges        []*metricsv1.NumberDataPoint
	histograms    []*metricsv1.ExponentialHistogramDataPoint
}

func (m *uncomposedMetric) toResourceMetrics() (*metricsv1.ResourceMetrics, error) {
	rm := &metricsv1.ResourceMetrics{
		Resource: &resourcev1.Resource{
			Attributes: m.resourceAttrs,
		},
		ScopeMetrics: []*metricsv1.ScopeMetrics{
			{
				Scope: &commonv1.InstrumentationScope{
					Attributes: m.scopeAttrs,
				},
				Metrics: []*metricsv1.Metric{
					{
						Name:     m.metricName,
						Metadata: m.metricAttrs,
					},
				},
			},
		},
	}

	if m.histograms != nil {
		rm.ScopeMetrics[0].Metrics[0].Data = &metricsv1.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricsv1.ExponentialHistogram{
				DataPoints: m.histograms,
			},
		}
	}

	if m.gauges != nil {
		rm.ScopeMetrics[0].Metrics[0].Data = &metricsv1.Metric_Gauge{
			Gauge: &metricsv1.Gauge{
				DataPoints: m.gauges,
			},
		}
	}

	return rm, nil
}

func newChunkedMetricsData(resourceAttributeLabels map[string]struct{}) *chunkedMetricsData {
	return &chunkedMetricsData{
		resourceAttributeLabels: resourceAttributeLabels,
		data:                    make(map[int64]map[string]*uncomposedMetric),
	}
}

func (cmd *chunkedMetricsData) Chunks() (map[int64]*metricsv1.MetricsData, error) {
	res := make(map[int64]*metricsv1.MetricsData, len(cmd.data))

	for interval, postings := range cmd.data {
		res[interval] = &metricsv1.MetricsData{}
		res[interval].Reset()

		for _, m := range postings {
			rms, err := m.toResourceMetrics()
			if err != nil {
				return nil, fmt.Errorf("construct resource metrics: %w", err)
			}

			res[interval].ResourceMetrics = append(res[interval].ResourceMetrics, rms)
		}
	}

	return res, nil
}

func (cmd *chunkedMetricsData) Size() (int, error) {
	if len(cmd.data) == 0 {
		return 0, nil
	}

	chks, err := cmd.Chunks()
	if err != nil {
		return 0, fmt.Errorf("build chunks: %w", err)
	}

	size := 0

	for _, md := range chks {
		size += proto.Size(md)
	}

	return size, nil
}

func (cmd *chunkedMetricsData) Reset() {
	for interval := range cmd.data {
		delete(cmd.data, interval)
	}
}

func (cmd *chunkedMetricsData) AppendGauges(lbls labels.ScratchBuilder, interval int64, gauges []*metricsv1.NumberDataPoint) {
	if _, ok := cmd.data[interval]; !ok {
		cmd.data[interval] = make(map[string]*uncomposedMetric)
	}

	lbls.Sort()
	sortedLabels := lbls.Labels().String()

	if _, ok := cmd.data[interval][sortedLabels]; !ok {
		metricName, resourceAttrs, scopeAttrs, metricAttrs := labelsToAttributes(lbls, cmd.resourceAttributeLabels)
		cmd.data[interval][sortedLabels] = &uncomposedMetric{
			metricName:    metricName,
			resourceAttrs: resourceAttrs,
			scopeAttrs:    scopeAttrs,
			metricAttrs:   metricAttrs,
		}
	}

	cmd.data[interval][sortedLabels].gauges = append(cmd.data[interval][sortedLabels].gauges, gauges...)
}

func (cmd *chunkedMetricsData) AppendHistograms(lbls labels.ScratchBuilder, interval int64, histograms []*metricsv1.ExponentialHistogramDataPoint) {
	if _, ok := cmd.data[interval]; !ok {
		cmd.data[interval] = make(map[string]*uncomposedMetric)
	}

	lbls.Sort()
	sortedLabels := lbls.Labels().String()

	if _, ok := cmd.data[interval][sortedLabels]; !ok {
		metricName, resourceAttrs, scopeAttrs, metricAttrs := labelsToAttributes(lbls, cmd.resourceAttributeLabels)
		cmd.data[interval][sortedLabels] = &uncomposedMetric{
			metricName:    metricName,
			resourceAttrs: resourceAttrs,
			scopeAttrs:    scopeAttrs,
			metricAttrs:   metricAttrs,
		}
	}

	cmd.data[interval][sortedLabels].histograms = append(cmd.data[interval][sortedLabels].histograms, histograms...)
}

func convertBlock(ctx context.Context, cfg config, logger gokitlog.Logger) error {
	b, err := tsdb.OpenBlock(utillog.SlogFromGoKit(logger), cfg.block, nil, nil)
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

	mds := newChunkedMetricsData(commaStringToMap(cfg.resourceAttributeLabels))

	batchCount := 0
	postingsCount := 0
	totalPostings := 0

	parquetWriteManager := parquet.NewWriteManager(1_000_000, uint64(cfg.batchSize))
	for p.Next() {
		if err = p.Err(); err != nil {
			return fmt.Errorf("iterate postings: %w", err)
		}

		// Flush a full batch to disk.
		if postingsCount >= cfg.batchSize {
			if totalPostings%1000 == 0 {
				log.Printf("processed %d postings", totalPostings)
			}

			batchCount, err = writeBatchChunks(mds, cfg, batchCount, parquetWriteManager)
			if err != nil {
				return fmt.Errorf("write metrics data to file: %w", err)
			}

			mds.Reset()
			postingsCount = 0
		}

		if cfg.numBatches > 0 && batchCount >= cfg.numBatches {
			log.Printf("%d batches reached; exiting early\n", batchCount)
			break
		}

		chkMetas := []chunks.Meta(nil)
		err = ir.Series(p.At(), &builder, &chkMetas)
		if err != nil {
			return fmt.Errorf("populate series chunk metas: %w", err)
		}

		var (
			intervalMS = cfg.chunkSize.Milliseconds()

			gauges = make(map[int64][]*metricsv1.NumberDataPoint)
			histos = make(map[int64][]*metricsv1.ExponentialHistogramDataPoint)

			chkItr chunkenc.Iterator
		)

		for _, chkMeta := range chkMetas {
			chk, itr, err := cr.ChunkOrIterable(chkMeta)
			if err != nil {
				return fmt.Errorf("access chunk: %w", err)
			}

			if itr != nil {
				return fmt.Errorf("unexpected chunk iterator")
			}

			chkItr = chk.Iterator(chkItr)

			for valType := chkItr.Next(); valType != chunkenc.ValNone; valType = chkItr.Next() {
				switch valType {
				case chunkenc.ValFloat:
					ts, val := chkItr.At()

					interval := snapToInterval(ts, intervalMS)

					gauges[interval] = append(gauges[interval], &metricsv1.NumberDataPoint{
						TimeUnixNano: uint64(time.UnixMilli(ts).UnixNano()),
						Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: val},
					})
				case chunkenc.ValHistogram:
					ts, h := chkItr.AtHistogram(nil)

					interval := snapToInterval(ts, intervalMS)

					histos[interval] = append(histos[interval], &metricsv1.ExponentialHistogramDataPoint{
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

					interval := snapToInterval(ts, intervalMS)

					histos[interval] = append(histos[interval], &metricsv1.ExponentialHistogramDataPoint{
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

		if len(gauges) > 0 {
			for interval, g := range gauges {
				mds.AppendGauges(builder, interval, g)
			}
		}

		if len(histos) > 0 {
			for interval, h := range histos {
				mds.AppendHistograms(builder, interval, h)
			}
		}

		if verbose {
			if mdsSize, err := mds.Size(); err != nil {
				log.Printf("current batch size: %d\n", mdsSize)
			}
		}

		postingsCount++
		totalPostings++
		builder.Reset()
	}

	// Flush any remaining partial batch to disk.
	mdsSize, err := mds.Size()
	if err != nil {
		return fmt.Errorf("get size: %w", err)
	}
	if mdsSize > 0 {
		_, err = writeBatchChunks(mds, cfg, batchCount, parquetWriteManager)
		if err != nil {
			return fmt.Errorf("write metrics data to file: %w", err)
		}
	}

	parquetWriteManager.Stop()

	return nil
}

func commaStringToMap(s string) map[string]struct{} {
	res := make(map[string]struct{})

	for _, lbl := range strings.Split(s, ",") {
		res[lbl] = struct{}{}
	}

	return res
}

func writeBatchChunks(mds *chunkedMetricsData, cfg config, batchCount int, wm *parquet.WriteManager) (int, error) {
	chks, err := mds.Chunks()
	if err != nil {
		return batchCount, fmt.Errorf("build chunks: %w", err)
	}

	for interval, md := range chks {
		if err := writeBatchChunk(md, cfg, interval, batchCount, wm); err != nil {
			return batchCount, fmt.Errorf("write batch chunk: %w", err)
		}
	}

	if cfg.count {
		log.Printf("batch %d: %d bytes written overall", batchCount, writtenBytesCount)
	}

	return batchCount + 1, nil
}

func writeBatchChunk(md *metricsv1.MetricsData, cfg config, chunkInterval int64, batchCount int, wm *parquet.WriteManager) error {
	sizeBeforeDedupe := proto.Size(md)
	if cfg.dedupe {
		deduplicate(md)
	}
	sizeAfterDedupe := proto.Size(md)
	if cfg.dedupe && verbose {
		log.Printf("size before dedupe: %d bytes, size after dedupe: %d bytes\n", sizeBeforeDedupe, sizeAfterDedupe)
	}

	var (
		buf []byte
		err error
	)

	switch cfg.outputFormat {
	case "json":
		buf, err = protojson.Marshal(md)
		if err != nil {
			return fmt.Errorf("marshal json message: %w", err)
		}
	case "protobuf":
		buf, err = proto.Marshal(md)
		if err != nil {
			return fmt.Errorf("marshal protobuf message: %w", err)
		}
	case "arrow":
		buf, err = arrow.Marshal(md)
		if err != nil {
			return fmt.Errorf("marshal arrow message: %w", err)
		}
	case "parquet":
		err = wm.Write(md, cfg.dest)
		if err != nil {
			return fmt.Errorf("write parquet files: %w", err)
		}

		//log.Printf("wrote batch %d of chunk %d to disk\n", batchCount, chunkInterval)
		// we're already writing a file
		return nil
	default:
		return fmt.Errorf("unsupported output format: %s", cfg.outputFormat)
	}

	switch cfg.compressionType {
	case "":
		// No compression.
	case "gzip":
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err = w.Write(buf); err != nil {
			return fmt.Errorf("write gzip: %w", err)
		}
		if err = w.Close(); err != nil {
			return fmt.Errorf("flush gzip writer: %w", err)
		}

		buf = b.Bytes()
	default:
		return fmt.Errorf("unsupported compression type: %s", cfg.compressionType)
	}

	if cfg.count {
		n := len(buf)
		writtenBytesCount += n
		if verbose {
			log.Printf("registered %d bytes for batch %d of chunk %d (%d bytes overall)\n", n, batchCount, chunkInterval, writtenBytesCount)
		}

		return nil
	}

	chunkDirPath := filepath.Join(cfg.dest, strconv.Itoa(int(chunkInterval)))
	if err = os.MkdirAll(chunkDirPath, os.ModePerm); err != nil {
		return fmt.Errorf("create chunk directory '%s': %w", chunkDirPath, err)
	}

	fp := filepath.Join(chunkDirPath, fmt.Sprintf("batch-%d", batchCount))
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("open file '%s' for writing: %w", fp, err)
	}

	n, err := f.Write(buf)
	if err != nil {
		return fmt.Errorf("write to file '%s': %w", fp, err)
	}
	defer func() { _ = f.Close() }()

	if verbose {
		log.Printf("wrote batch %d of chunk %d to disk (%d bytes)\n", batchCount, chunkInterval, n)
	}

	return nil
}

func labelsToAttributes(builder labels.ScratchBuilder, resourceAttributeLabels map[string]struct{}) (string, []*commonv1.KeyValue, []*commonv1.KeyValue, []*commonv1.KeyValue) {
	builder.Sort()
	lbls := builder.Labels()

	if verbose {
		log.Printf("processing series %s...\n", lbls.String())
	}

	var (
		name          string
		resourceAttrs []*commonv1.KeyValue
		scopeAttrs    []*commonv1.KeyValue
		metricAttrs   []*commonv1.KeyValue
	)

	lbls.Range(func(lbl labels.Label) {
		if _, ok := resourceAttributeLabels[lbl.Name]; ok {
			resourceAttrs = append(resourceAttrs, &commonv1.KeyValue{
				Key: lbl.Name,
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: lbl.Value},
				},
			})
			return
		} else if strings.HasPrefix(lbl.Name, "otel_scope_") {
			scopeAttrs = append(scopeAttrs, &commonv1.KeyValue{
				Key: strings.TrimPrefix(lbl.Name, "otel_scope_"),
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: lbl.Value},
				},
			})
			return
		}

		metricAttrs = append(metricAttrs, &commonv1.KeyValue{
			Key: lbl.Name,
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: lbl.Value},
			},
		})

		if lbl.Name == "__name__" {
			name = lbl.Value
		}
	})

	return name, resourceAttrs, scopeAttrs, metricAttrs
}

func translateToOTELHistogramBuckets(spans []histogram.Span, itr histogram.BucketIterator[uint64]) *metricsv1.ExponentialHistogramDataPoint_Buckets {
	if len(spans) == 0 {
		return nil
	}

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

func snapToInterval(unixMS, intervalMS int64) int64 {
	return unixMS - (unixMS % intervalMS)
}
