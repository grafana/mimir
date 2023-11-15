// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	prometheustranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/multierr"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"

	otelParseError = "otlp_parse_error"
	maxErrMsgLen   = 1024
)

func OTLPHandler(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	enableOtelMetadataStorage bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	reg prometheus.Registerer,
	push PushFunc,
) http.Handler {
	discardedDueToOtelParseError := validation.DiscardedSamplesCounter(reg, otelParseError)

	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, limits, retryCfg, push, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, _ []byte, req *mimirpb.PreallocWriteRequest) ([]byte, error) {
		var decoderFunc func(buf []byte) (pmetricotlp.ExportRequest, error)

		logger := log.WithContext(ctx, log.Logger)

		contentType := r.Header.Get("Content-Type")
		switch contentType {
		case pbContentType:
			decoderFunc = func(buf []byte) (pmetricotlp.ExportRequest, error) {
				req := pmetricotlp.NewExportRequest()
				return req, req.UnmarshalProto(buf)
			}

		case jsonContentType:
			decoderFunc = func(buf []byte) (pmetricotlp.ExportRequest, error) {
				req := pmetricotlp.NewExportRequest()
				return req, req.UnmarshalJSON(buf)
			}

		default:
			return nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
		}

		if r.ContentLength > int64(maxRecvMsgSize) {
			return nil, httpgrpc.Errorf(http.StatusRequestEntityTooLarge, distributorMaxWriteMessageSizeErr{actual: int(r.ContentLength), limit: maxRecvMsgSize}.Error())
		}

		reader := r.Body
		// Handle compression.
		contentEncoding := r.Header.Get("Content-Encoding")
		switch contentEncoding {
		case "gzip":
			gr, err := gzip.NewReader(reader)
			if err != nil {
				return nil, err
			}
			reader = gr

		case "":
			// No compression.

		default:
			return nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\" or no compression supported", contentEncoding)
		}

		// Protect against a large input.
		reader = http.MaxBytesReader(nil, reader, int64(maxRecvMsgSize))

		body, err := io.ReadAll(reader)
		if err != nil {
			r.Body.Close()

			if util.IsRequestBodyTooLarge(err) {
				return body, httpgrpc.Errorf(http.StatusRequestEntityTooLarge, distributorMaxWriteMessageSizeErr{actual: -1, limit: maxRecvMsgSize}.Error())
			}

			return body, err
		}

		if err = r.Body.Close(); err != nil {
			return body, err
		}

		log, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.OTLPHandler.decodeAndConvert")
		defer log.Span.Finish()

		log.SetTag("content_type", contentType)
		log.SetTag("content_encoding", contentEncoding)
		log.SetTag("content_length", r.ContentLength)

		otlpReq, err := decoderFunc(body)
		if err != nil {
			return body, err
		}

		level.Debug(log).Log("msg", "decoding complete, starting conversion")

		metrics, err := otelMetricsToTimeseries(ctx, limits, discardedDueToOtelParseError, logger, otlpReq.Metrics())
		if err != nil {
			return body, err
		}

		metricCount := len(metrics)
		sampleCount := 0
		histogramCount := 0
		exemplarCount := 0

		for _, m := range metrics {
			sampleCount += len(m.Samples)
			histogramCount += len(m.Histograms)
			exemplarCount += len(m.Exemplars)
		}

		level.Debug(log).Log(
			"msg", "OTLP to Prometheus conversion complete",
			"metric_count", metricCount,
			"sample_count", sampleCount,
			"histogram_count", histogramCount,
			"exemplar_count", exemplarCount,
		)

		req.Timeseries = metrics

		if enableOtelMetadataStorage {
			metadata, err := otelMetricsToMetadata(ctx, limits, otlpReq.Metrics())
			if err != nil {
				return nil, err
			}
			req.Metadata = metadata
		}

		return body, nil
	})
}

func otelMetricTypeToMimirMetricType(otelMetric pmetric.Metric) mimirpb.MetricMetadata_MetricType {
	switch otelMetric.Type() {
	case pmetric.MetricTypeGauge:
		return mimirpb.GAUGE
	case pmetric.MetricTypeSum:
		metricType := mimirpb.GAUGE
		if otelMetric.Sum().IsMonotonic() {
			metricType = mimirpb.COUNTER
		}
		return metricType
	case pmetric.MetricTypeHistogram:
		return mimirpb.HISTOGRAM
	case pmetric.MetricTypeSummary:
		return mimirpb.SUMMARY
	case pmetric.MetricTypeExponentialHistogram:
		return mimirpb.HISTOGRAM
	}
	return mimirpb.UNKNOWN
}

func otelMetricsToMetadata(ctx context.Context, limits *validation.Overrides, md pmetric.Metrics) ([]*mimirpb.MetricMetadata, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	addSuffixes := limits.OTelMetricSuffixesEnabled(tenantID)

	resourceMetricsSlice := md.ResourceMetrics()

	metadataLength := 0
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		scopeMetricsSlice := resourceMetricsSlice.At(i).ScopeMetrics()
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			metadataLength += scopeMetricsSlice.At(j).Metrics().Len()
		}
	}

	metadata := make([]*mimirpb.MetricMetadata, 0, metadataLength)
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		scopeMetricsSlice := resourceMetricsSlice.At(i).ScopeMetrics()
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			scopeMetrics := scopeMetricsSlice.At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)
				entry := mimirpb.MetricMetadata{
					Type:             otelMetricTypeToMimirMetricType(metric),
					MetricFamilyName: prometheustranslator.BuildCompliantName(metric, "", addSuffixes),
					Help:             metric.Description(),
					Unit:             metric.Unit(),
				}
				metadata = append(metadata, &entry)
			}
		}
	}

	return metadata, nil
}

func otelMetricsToTimeseries(ctx context.Context, limits *validation.Overrides, discardedDueToOtelParseError *prometheus.CounterVec, logger kitlog.Logger, md pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	tsMap, errs := prometheusremotewrite.FromMetrics(md, prometheusremotewrite.Settings{
		AddMetricSuffixes: limits.OTelMetricSuffixesEnabled(tenantID),
	})
	if errs != nil {
		dropped := len(multierr.Errors(errs))
		discardedDueToOtelParseError.WithLabelValues(tenantID, "").Add(float64(dropped)) // Group is empty here as metrics couldn't be parsed

		parseErrs := errs.Error()
		if len(parseErrs) > maxErrMsgLen {
			parseErrs = parseErrs[:maxErrMsgLen]
		}

		if len(tsMap) == 0 {
			return nil, errors.New(parseErrs)
		}

		level.Warn(logger).Log("msg", "OTLP parse error", "err", parseErrs)
	}

	mimirTs := mimirpb.PreallocTimeseriesSliceFromPool()
	for _, promTs := range tsMap {
		mimirTs = append(mimirTs, promToMimirTimeseries(promTs))
	}

	return mimirTs, nil
}

func promToMimirTimeseries(promTs *prompb.TimeSeries) mimirpb.PreallocTimeseries {
	labels := make([]mimirpb.LabelAdapter, 0, len(promTs.Labels))
	for _, label := range promTs.Labels {
		labels = append(labels, mimirpb.LabelAdapter{
			Name:  label.Name,
			Value: label.Value,
		})
	}

	samples := make([]mimirpb.Sample, 0, len(promTs.Samples))
	for _, sample := range promTs.Samples {
		samples = append(samples, mimirpb.Sample{
			TimestampMs: sample.Timestamp,
			Value:       sample.Value,
		})
	}

	histograms := make([]mimirpb.Histogram, 0, len(promTs.Histograms))
	for idx := range promTs.Histograms {
		histograms = append(histograms, promToMimirHistogram(&promTs.Histograms[idx]))
	}

	exemplars := make([]mimirpb.Exemplar, 0, len(promTs.Exemplars))
	for _, exemplar := range promTs.Exemplars {
		labels := make([]mimirpb.LabelAdapter, 0, len(exemplar.Labels))
		for _, label := range exemplar.Labels {
			labels = append(labels, mimirpb.LabelAdapter{
				Name:  label.Name,
				Value: label.Value,
			})
		}

		exemplars = append(exemplars, mimirpb.Exemplar{
			Labels:      labels,
			Value:       exemplar.Value,
			TimestampMs: exemplar.Timestamp,
		})
	}

	ts := mimirpb.TimeseriesFromPool()
	ts.Labels = labels
	ts.Samples = samples
	ts.Histograms = histograms
	ts.Exemplars = exemplars

	return mimirpb.PreallocTimeseries{TimeSeries: ts}
}

func promToMimirHistogram(h *prompb.Histogram) mimirpb.Histogram {
	pSpans := make([]mimirpb.BucketSpan, 0, len(h.PositiveSpans))
	for _, span := range h.PositiveSpans {
		pSpans = append(
			pSpans, mimirpb.BucketSpan{
				Offset: span.Offset,
				Length: span.Length,
			},
		)
	}
	nSpans := make([]mimirpb.BucketSpan, 0, len(h.NegativeSpans))
	for _, span := range h.NegativeSpans {
		nSpans = append(
			nSpans, mimirpb.BucketSpan{
				Offset: span.Offset,
				Length: span.Length,
			},
		)
	}

	return mimirpb.Histogram{
		Count:          &mimirpb.Histogram_CountInt{CountInt: h.GetCountInt()},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: h.GetZeroCountInt()},
		NegativeSpans:  nSpans,
		NegativeDeltas: h.NegativeDeltas,
		NegativeCounts: h.NegativeCounts,
		PositiveSpans:  pSpans,
		PositiveDeltas: h.PositiveDeltas,
		PositiveCounts: h.PositiveCounts,
		Timestamp:      h.Timestamp,
		ResetHint:      mimirpb.Histogram_ResetHint(h.ResetHint),
	}
}

// TimeseriesToOTLPRequest is used in tests.
func TimeseriesToOTLPRequest(timeseries []prompb.TimeSeries, metadata []mimirpb.MetricMetadata) pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	for i, ts := range timeseries {
		name := ""
		attributes := pcommon.NewMap()

		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				name = l.Value
				continue
			}

			attributes.PutStr(l.Name, l.Value)
		}

		rm := d.ResourceMetrics()
		sm := rm.AppendEmpty().ScopeMetrics()

		if len(ts.Samples) > 0 {
			metric := sm.AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(name)
			metric.SetEmptyGauge()
			if metadata != nil {
				metric.SetDescription(metadata[i].GetHelp())
				metric.SetUnit(metadata[i].GetUnit())
			}
			for _, sample := range ts.Samples {
				datapoint := metric.Gauge().DataPoints().AppendEmpty()
				datapoint.SetTimestamp(pcommon.Timestamp(sample.Timestamp * time.Millisecond.Nanoseconds()))
				datapoint.SetDoubleValue(sample.Value)
				attributes.CopyTo(datapoint.Attributes())
			}
		}

		if len(ts.Histograms) > 0 {
			metric := sm.AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(name)
			metric.SetEmptyExponentialHistogram()
			if metadata != nil {
				metric.SetDescription(metadata[i].GetHelp())
				metric.SetUnit(metadata[i].GetUnit())
			}
			metric.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, histogram := range ts.Histograms {
				datapoint := metric.ExponentialHistogram().DataPoints().AppendEmpty()
				datapoint.SetTimestamp(pcommon.Timestamp(histogram.Timestamp * time.Millisecond.Nanoseconds()))
				datapoint.SetScale(histogram.Schema)
				datapoint.SetCount(histogram.GetCountInt())

				offset, counts := translateBucketsLayout(histogram.PositiveSpans, histogram.PositiveDeltas)
				datapoint.Positive().SetOffset(offset)
				datapoint.Positive().BucketCounts().FromRaw(counts)

				offset, counts = translateBucketsLayout(histogram.NegativeSpans, histogram.NegativeDeltas)
				datapoint.Negative().SetOffset(offset)
				datapoint.Negative().BucketCounts().FromRaw(counts)

				datapoint.SetSum(histogram.GetSum())
				datapoint.SetZeroCount(histogram.GetZeroCountInt())
				attributes.CopyTo(datapoint.Attributes())
			}
		}
	}

	return pmetricotlp.NewExportRequestFromMetrics(d)
}

// translateBucketLayout the test function that translates the Prometheus native histograms buckets
// layout to the OTel exponential histograms sparse buckets layout. It is the inverse function to
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/47471382940a0d794a387b06c99413520f0a68f8/pkg/translator/prometheusremotewrite/histograms.go#L118
func translateBucketsLayout(spans []prompb.BucketSpan, deltas []int64) (int32, []uint64) {
	if len(spans) == 0 {
		return 0, []uint64{}
	}

	firstSpan := spans[0]
	bucketsCount := int(firstSpan.Length)
	for i := 1; i < len(spans); i++ {
		bucketsCount += int(spans[i].Offset) + int(spans[i].Length)
	}
	buckets := make([]uint64, bucketsCount)

	bucketIdx := 0
	deltaIdx := 0
	currCount := int64(0)

	// set offset of the first span to 0 to simplify translation
	spans[0].Offset = 0
	for _, span := range spans {
		bucketIdx += int(span.Offset)
		for i := 0; i < int(span.GetLength()); i++ {
			currCount += deltas[deltaIdx]
			buckets[bucketIdx] = uint64(currCount)
			deltaIdx++
			bucketIdx++
		}
	}

	return firstSpan.Offset - 1, buckets
}
