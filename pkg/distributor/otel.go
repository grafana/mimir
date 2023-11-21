// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/multierr"

	"github.com/grafana/mimir/pkg/distributor/otlp"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"

	otelParseError = "otlp_parse_error"
	maxErrMsgLen   = 1024
)

// OTLPHandler is an http.Handler accepting OTLP write requests.
func OTLPHandler(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	enableOtelMetadataStorage bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	reg prometheus.Registerer,
	push PushFunc,
	logger log.Logger,
) http.Handler {
	//discardedDueToOtelParseError := validation.DiscardedSamplesCounter(reg, otelParseError)

	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, limits, retryCfg, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, dst []byte, logger log.Logger) (writeRequest, []byte, error) {
		var decoderFunc func(io.Reader) (pmetricotlp.ExportRequest, []byte, error)

		contentType := r.Header.Get("Content-Type")
		switch contentType {
		case pbContentType:
			decoderFunc = func(reader io.Reader) (pmetricotlp.ExportRequest, []byte, error) {
				er := pmetricotlp.NewExportRequest()
				buf, err := util.ParseProtoReader(ctx, reader, int(r.ContentLength), maxRecvMsgSize, dst, er, util.NoCompression)
				return er, buf, err
			}

			/*
				case jsonContentType:
					decoderFunc = func(buf []byte) (pmetricotlp.ExportRequest, error) {
						req := pmetricotlp.NewExportRequest()
						return req, req.UnmarshalJSON(buf)
					}
			*/

		default:
			return nil, nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
		}

		if r.ContentLength > int64(maxRecvMsgSize) {
			return nil, nil, httpgrpc.Errorf(http.StatusRequestEntityTooLarge, distributorMaxWriteMessageSizeErr{actual: int(r.ContentLength), limit: maxRecvMsgSize}.Error())
		}

		reader := r.Body
		// Handle compression.
		contentEncoding := r.Header.Get("Content-Encoding")
		switch contentEncoding {
		case "gzip":
			gr, err := gzip.NewReader(reader)
			if err != nil {
				return nil, nil, err
			}
			reader = gr

		case "":
			// No compression.

		default:
			return nil, nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\" or no compression supported", contentEncoding)
		}

		spanLogger, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.OTLPHandler.decodeAndConvert")
		defer spanLogger.Span.Finish()

		spanLogger.SetTag("content_type", contentType)
		spanLogger.SetTag("content_encoding", contentEncoding)
		spanLogger.SetTag("content_length", r.ContentLength)

		exportReq, buf, err := decoderFunc(reader)
		if err != nil {
			return nil, nil, err
		}

		return otlpRequest{
			wrapped: exportReq,
		}, buf, nil

		/*
			level.Debug(spanLogger).Log("msg", "decoding complete, starting conversion")

			metrics, err := otelMetricsToTimeseries(ctx, discardedDueToOtelParseError, logger, otlpReq.Metrics())
			if err != nil {
				return buf, err
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

			level.Debug(spanLogger).Log(
				"msg", "OTLP to Prometheus conversion complete",
				"metric_count", metricCount,
				"sample_count", sampleCount,
				"histogram_count", histogramCount,
				"exemplar_count", exemplarCount,
			)

			req.Timeseries = metrics

			if enableOtelMetadataStorage {
				metadata := otelMetricsToMetadata(otlpReq.Metrics())
				req.Metadata = metadata
			}
		*/
	})
}

// otlpRequest implements writeRequest.
type otlpRequest struct {
	wrapped pmetricotlp.ExportRequest
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

func otelMetricsToMetadata(addSuffixes bool, md pmetric.Metrics) []*mimirpb.MetricMetadata {
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

	return metadata
}

func otelMetricsToTimeseries(tenantID string, addSuffixes bool, discardedDueToOtelParseError *prometheus.CounterVec, logger log.Logger, md pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	ts, errs := otlp.FromMetrics(md, otlp.Settings{
		AddMetricSuffixes: addSuffixes,
	})
	if errs != nil {
		dropped := len(multierr.Errors(errs))
		discardedDueToOtelParseError.WithLabelValues(tenantID, "").Add(float64(dropped)) // Group is empty here as metrics couldn't be parsed

		parseErrs := errs.Error()
		if len(parseErrs) > maxErrMsgLen {
			parseErrs = parseErrs[:maxErrMsgLen]
		}

		if len(ts) == 0 {
			return nil, errors.New(parseErrs)
		}

		level.Warn(logger).Log("msg", "OTLP parse error", "err", parseErrs)
	}

	return ts, nil
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
