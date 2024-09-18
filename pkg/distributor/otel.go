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
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/distributor/otlp"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"

	otelParseError = "otlp_parse_error"
	maxErrMsgLen   = 1024
)

type OTLPHandlerLimits interface {
	OTelMetricSuffixesEnabled(id string) bool
	OTelCreatedTimestampZeroIngestionEnabled(id string) bool
}

// OTLPHandler is an http.Handler accepting OTLP write requests.
func OTLPHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits OTLPHandlerLimits,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	reg prometheus.Registerer,
	logger log.Logger,
	directTranslation bool,
) http.Handler {
	discardedDueToOtelParseError := validation.DiscardedSamplesCounter(reg, otelParseError)

	return otlpHandler(maxRecvMsgSize, requestBufferPool, sourceIPs, retryCfg, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error {
		contentType := r.Header.Get("Content-Type")
		contentEncoding := r.Header.Get("Content-Encoding")
		var compression util.CompressionType
		switch contentEncoding {
		case "gzip":
			compression = util.Gzip
		case "":
			compression = util.NoCompression
		default:
			return httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\" or no compression supported", contentEncoding)
		}

		var decoderFunc func(io.ReadCloser) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error)
		switch contentType {
		case pbContentType:
			decoderFunc = func(reader io.ReadCloser) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
				exportReq := pmetricotlp.NewExportRequest()
				unmarshaler := otlpProtoUnmarshaler{
					request: &exportReq,
				}
				protoBodySize, err := util.ParseProtoReader(ctx, reader, int(r.ContentLength), maxRecvMsgSize, buffers, unmarshaler, compression)
				var tooLargeErr util.MsgSizeTooLargeErr
				if errors.As(err, &tooLargeErr) {
					return exportReq, 0, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
						actual: tooLargeErr.Actual,
						limit:  tooLargeErr.Limit,
					}.Error())
				}
				return exportReq, protoBodySize, err
			}

		case jsonContentType:
			decoderFunc = func(reader io.ReadCloser) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
				exportReq := pmetricotlp.NewExportRequest()
				sz := int(r.ContentLength)
				if sz > 0 {
					// Extra space guarantees no reallocation
					sz += bytes.MinRead
				}
				buf := buffers.Get(sz)
				if compression == util.Gzip {
					var err error
					reader, err = gzip.NewReader(reader)
					if err != nil {
						return exportReq, 0, errors.Wrap(err, "create gzip reader")
					}
				}

				reader = http.MaxBytesReader(nil, reader, int64(maxRecvMsgSize))
				if _, err := buf.ReadFrom(reader); err != nil {
					if util.IsRequestBodyTooLarge(err) {
						return exportReq, 0, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
							actual: -1,
							limit:  maxRecvMsgSize,
						}.Error())
					}

					return exportReq, 0, errors.Wrap(err, "read write request")
				}

				return exportReq, buf.Len(), exportReq.UnmarshalJSON(buf.Bytes())
			}

		default:
			return httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
		}

		// Check the request size against the message size limit, regardless of whether the request is compressed.
		// If the request is compressed and its compressed length already exceeds the size limit, there's no need to decompress it.
		if r.ContentLength > int64(maxRecvMsgSize) {
			return httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
				actual: int(r.ContentLength),
				limit:  maxRecvMsgSize,
			}.Error())
		}

		spanLogger, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.OTLPHandler.decodeAndConvert")
		defer spanLogger.Span.Finish()

		spanLogger.SetTag("content_type", contentType)
		spanLogger.SetTag("content_encoding", contentEncoding)
		spanLogger.SetTag("content_length", r.ContentLength)

		otlpReq, uncompressedBodySize, err := decoderFunc(r.Body)
		if err != nil {
			return err
		}

		level.Debug(spanLogger).Log("msg", "decoding complete, starting conversion")

		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}
		addSuffixes := limits.OTelMetricSuffixesEnabled(tenantID)
		enableCTZeroIngestion := limits.OTelCreatedTimestampZeroIngestionEnabled(tenantID)

		pushMetrics.IncOTLPRequest(tenantID)
		pushMetrics.ObserveUncompressedBodySize(tenantID, float64(uncompressedBodySize))

		var metrics []mimirpb.PreallocTimeseries
		if directTranslation {
			metrics, err = otelMetricsToTimeseries(ctx, tenantID, addSuffixes, enableCTZeroIngestion, discardedDueToOtelParseError, spanLogger, otlpReq.Metrics())
			if err != nil {
				return err
			}
		} else {
			metrics, err = otelMetricsToTimeseriesOld(ctx, tenantID, addSuffixes, enableCTZeroIngestion, discardedDueToOtelParseError, spanLogger, otlpReq.Metrics())
			if err != nil {
				return err
			}
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
		req.Metadata = otelMetricsToMetadata(addSuffixes, otlpReq.Metrics())

		return nil
	})
}

func otlpHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	retryCfg RetryConfig,
	push PushFunc,
	logger log.Logger,
	parser parserFunc,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				logger = utillog.WithSourceIPs(source, logger)
			}
		}
		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest
			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Error(http.StatusBadRequest, err.Error())
				}

				rb.CleanUp()
				return nil, nil, err
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				rb.CleanUp()
			}
			return &req.WriteRequest, cleanup, nil
		}
		req := newRequest(supplier)
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				writeErrorToHTTPResponseBody(r.Context(), w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
				return
			}
			var (
				httpCode int
				grpcCode codes.Code
				errorMsg string
			)
			if st, ok := grpcutil.ErrorToStatus(err); ok {
				// This code is needed for a correct handling of errors returned by the supplier function.
				// These errors are created by using the httpgrpc package.
				httpCode = httpRetryableToOTLPRetryable(int(st.Code()))
				grpcCode = st.Code()
				errorMsg = st.Message()
			} else {
				grpcCode, httpCode = toOtlpGRPCHTTPStatus(err)
				errorMsg = err.Error()
			}
			if httpCode != 202 {
				// This error message is consistent with error message in Prometheus remote-write handler, and ingester's ingest-storage pushToStorage method.
				msgs := []interface{}{"msg", "detected an error while ingesting OTLP metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", err}
				if httpCode/100 == 4 {
					msgs = append(msgs, "insight", true)
				}
				level.Error(logger).Log(msgs...)
			}
			addHeaders(w, err, r, httpCode, retryCfg)
			writeErrorToHTTPResponseBody(r.Context(), w, httpCode, grpcCode, errorMsg, logger)
		}
	})
}

// toOtlpGRPCHTTPStatus is utilized by the OTLP endpoint.
func toOtlpGRPCHTTPStatus(pushErr error) (codes.Code, int) {
	var distributorErr Error
	if errors.Is(pushErr, context.DeadlineExceeded) || !errors.As(pushErr, &distributorErr) {
		return codes.Internal, http.StatusServiceUnavailable
	}

	grpcStatusCode := errorCauseToGRPCStatusCode(distributorErr.Cause(), false)
	httpStatusCode := errorCauseToHTTPStatusCode(distributorErr.Cause(), false)
	otlpHTTPStatusCode := httpRetryableToOTLPRetryable(httpStatusCode)
	return grpcStatusCode, otlpHTTPStatusCode
}

// httpRetryableToOTLPRetryable maps non-retryable 5xx HTTP status codes according
// to the OTLP specifications (https://opentelemetry.io/docs/specs/otlp/#failures-1)
// to http.StatusServiceUnavailable. In case of a non-retryable HTTP status code,
// httpRetryableToOTLPRetryable returns the HTTP status code itself.
// Unlike Prometheus, which retries 429 and all 5xx HTTP status codes,
// the OTLP client only retries on HTTP status codes 429, 502, 503, and 504.
func httpRetryableToOTLPRetryable(httpStatusCode int) int {
	if httpStatusCode/100 == 5 {
		mask := httpStatusCode % 100
		// We map all 5xx except 502, 503 and 504 into 503.
		if mask <= 1 || mask > 4 {
			return http.StatusServiceUnavailable
		}
	}
	return httpStatusCode
}

// writeErrorToHTTPResponseBody converts the given error into a grpc status and marshals it into a byte slice, in order to be written to the response body.
// See doc https://opentelemetry.io/docs/specs/otlp/#failures-1
func writeErrorToHTTPResponseBody(reqCtx context.Context, w http.ResponseWriter, httpCode int, grpcCode codes.Code, msg string, logger log.Logger) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if server.IsHandledByHttpgrpcServer(reqCtx) {
		w.Header().Set(server.ErrorMessageHeaderKey, msg) // If httpgrpc Server wants to convert this HTTP response into error, use this error message, instead of using response body.
	}
	w.WriteHeader(httpCode)

	respBytes, err := proto.Marshal(status.New(grpcCode, msg).Proto())
	if err != nil {
		level.Error(logger).Log("msg", "otlp response marshal failed", "err", err)
		writeResponseFailedBody, _ := proto.Marshal(status.New(codes.Internal, "failed to marshal OTLP response").Proto())
		_, _ = w.Write(writeResponseFailedBody)
		return
	}

	_, err = w.Write(respBytes)
	if err != nil {
		level.Error(logger).Log("msg", "write error to otlp response failed", "err", err)
	}
}

// otlpProtoUnmarshaler implements proto.Message wrapping pmetricotlp.ExportRequest.
type otlpProtoUnmarshaler struct {
	request *pmetricotlp.ExportRequest
}

func (o otlpProtoUnmarshaler) ProtoMessage() {}

func (o otlpProtoUnmarshaler) Reset() {}

func (o otlpProtoUnmarshaler) String() string {
	return ""
}

func (o otlpProtoUnmarshaler) Unmarshal(data []byte) error {
	return o.request.UnmarshalProto(data)
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

func otelMetricsToTimeseries(ctx context.Context, tenantID string, addSuffixes, enableCTZeroIngestion bool, discardedDueToOtelParseError *prometheus.CounterVec, logger log.Logger, md pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	converter := otlp.NewMimirConverter()
	_, errs := converter.FromMetrics(ctx, md, otlp.Settings{
		AddMetricSuffixes:                   addSuffixes,
		EnableCreatedTimestampZeroIngestion: enableCTZeroIngestion,
	}, logger)
	mimirTS := converter.TimeSeries()
	if errs != nil {
		dropped := len(multierr.Errors(errs))
		discardedDueToOtelParseError.WithLabelValues(tenantID, "").Add(float64(dropped)) // Group is empty here as metrics couldn't be parsed

		parseErrs := errs.Error()
		if len(parseErrs) > maxErrMsgLen {
			parseErrs = parseErrs[:maxErrMsgLen]
		}

		if len(mimirTS) == 0 {
			return nil, errors.New(parseErrs)
		}

		level.Warn(logger).Log("msg", "OTLP parse error", "err", parseErrs)
	}

	return mimirTS, nil
}

// Old, less efficient, version of otelMetricsToTimeseries.
func otelMetricsToTimeseriesOld(ctx context.Context, tenantID string, addSuffixes, enableCTZeroIngestion bool, discardedDueToOtelParseError *prometheus.CounterVec, logger log.Logger, md pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	converter := prometheusremotewrite.NewPrometheusConverter()
	annots, errs := converter.FromMetrics(ctx, md, prometheusremotewrite.Settings{
		AddMetricSuffixes:                   addSuffixes,
		EnableCreatedTimestampZeroIngestion: enableCTZeroIngestion,
	}, logger)
	promTS := converter.TimeSeries()
	if errs != nil {
		dropped := len(multierr.Errors(errs))
		discardedDueToOtelParseError.WithLabelValues(tenantID, "").Add(float64(dropped)) // Group is empty here as metrics couldn't be parsed

		parseErrs := errs.Error()
		if len(parseErrs) > maxErrMsgLen {
			parseErrs = parseErrs[:maxErrMsgLen]
		}

		if len(promTS) == 0 {
			return nil, errors.New(parseErrs)
		}

		level.Warn(logger).Log("msg", "OTLP parse error", "err", parseErrs)
	}
	ws, _ := annots.AsStrings("", 0, 0)
	if len(ws) > 0 {
		level.Warn(logger).Log("msg", "Warnings translating OTLP metrics to Prometheus write request", "warnings", ws)
	}

	mimirTS := mimirpb.PreallocTimeseriesSliceFromPool()
	for _, ts := range promTS {
		mimirTS = append(mimirTS, promToMimirTimeseries(&ts))
	}

	return mimirTS, nil
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
