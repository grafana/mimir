// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/tenant"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
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
	PromoteOTelResourceAttributes(id string) []string
	OTelKeepIdentifyingResourceAttributes(id string) bool
	OTelConvertHistogramsToNHCB(id string) bool
	OTelPromoteScopeMetadata(id string) bool
	OTelNativeDeltaIngestion(id string) bool
	OTelTranslationStrategy(id string) otlptranslator.TranslationStrategyOption
	NameValidationScheme(id string) model.ValidationScheme
	OTelLabelNameUnderscoreSanitization(string) bool
	OTelLabelNamePreserveMultipleUnderscores(string) bool
}

type OTLPPushMiddleware func(ctx context.Context, req *pmetricotlp.ExportRequest) error

// OTLPHandler is an http.Handler accepting OTLP write requests.
func (d *Distributor) OTLPHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	retryCfg RetryConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	push PushFunc,
	pushMetrics *PushMetrics,
	reg prometheus.Registerer,
	logger log.Logger,
) http.Handler {
	discardedDueToOTelParseError := validation.DiscardedSamplesCounter(reg, otelParseError)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				logger = utillog.WithSourceIPs(source, logger)
			}
		}

		otlpConverter := newOTLPMimirConverter()
		parser := d.newOTLPParser(limits, resourceAttributePromotionConfig, otlpConverter, enableStartTimeQuietZero, pushMetrics, discardedDueToOTelParseError)

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
		req.contentLength = r.ContentLength

		pushErr := push(ctx, req)
		if pushErr == nil {
			if otlpErr := otlpConverter.Err(); otlpErr != nil {
				// Push was successful, but OTLP converter left out some samples. We let the client know about it by replying with 4xx (and an insight log).
				pushErr = httpgrpc.Error(http.StatusBadRequest, otlpErr.Error())
			} else {
				// Respond as per spec:
				// https://opentelemetry.io/docs/specs/otlp/#otlphttp-response.
				var expResp colmetricpb.ExportMetricsServiceResponse
				addSuccessHeaders(w, req.artificialDelay)
				writeOTLPResponse(r, w, http.StatusOK, &expResp, logger)
				return
			}
		}

		if errors.Is(pushErr, context.Canceled) {
			level.Warn(logger).Log("msg", "push request canceled", "err", pushErr)
			writeErrorToHTTPResponseBody(r, w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
			return
		}
		if labelValueTooLongErr := (labelValueTooLongError{}); errors.As(pushErr, &labelValueTooLongErr) {
			// Translate from Mimir to OTel domain terminology
			pushErr = newValidationError(otelAttributeValueTooLongError{labelValueTooLongErr})
		}
		var (
			httpCode int
			grpcCode codes.Code
			errorMsg string
		)
		if st, ok := grpcutil.ErrorToStatus(pushErr); ok {
			grpcCode = st.Code()
			errorMsg = st.Message()

			// This code is needed for a correct handling of errors returned by the supplier function.
			// These errors are usually created by using the httpgrpc package.
			// However, distributor's write path is complex and has a lot of dependencies, so sometimes it's not.
			if util.IsHTTPStatusCode(grpcCode) {
				httpCode = httpRetryableToOTLPRetryable(int(grpcCode))
			} else {
				httpCode = http.StatusServiceUnavailable
			}
		} else {
			var isSoft bool
			grpcCode, httpCode, isSoft = toOtlpGRPCHTTPStatus(pushErr)
			if isSoft {
				handlePartialOTLPPush(pushErr, w, r, req, logger)
				return
			}

			errorMsg = pushErr.Error()
		}
		if httpCode != 202 {
			// This error message is consistent with error message in Prometheus remote-write handler, and ingester's ingest-storage pushToStorage method.
			msgs := []any{"msg", "detected an error while ingesting OTLP metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", pushErr}
			if httpCode/100 == 4 {
				msgs = append(msgs, "insight", true)
				logLevel = level.Warn
			}
			logLevel(logger).Log(msgs...)
		}
		addErrorHeaders(w, pushErr, r, httpCode, retryCfg)
		writeErrorToHTTPResponseBody(r, w, httpCode, grpcCode, errorMsg, logger)
	})
}

type otlpDecoderFunc = func(io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error)

func (d *Distributor) getOTLPDecoderFunc(
	ctx context.Context, r *http.Request, contentType, contentEncoding string, maxRecvMsgSize int, buffers *util.RequestBuffers, logger log.Logger,
) (otlpDecoderFunc, error) {
	var compression util.CompressionType
	switch contentEncoding {
	case "gzip":
		compression = util.Gzip
	case "lz4":
		compression = util.Lz4
	case "":
		compression = util.NoCompression
	default:
		return nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\", \"lz4\", or no compression supported", contentEncoding)
	}

	switch contentType {
	case pbContentType:
		return func(reader io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
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
		}, nil
	case jsonContentType:
		return func(reader io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
			exportReq := pmetricotlp.NewExportRequest()
			sz := int(r.ContentLength)
			if sz > 0 {
				// Extra space guarantees no reallocation
				sz += bytes.MinRead
			}
			buf := buffers.Get(sz)
			switch compression {
			case util.Gzip:
				gzReader, err := gzip.NewReader(reader)
				if err != nil {
					return exportReq, 0, errors.Wrap(err, "create gzip reader")
				}
				defer runutil.CloseWithLogOnErr(logger, gzReader, "close gzip reader")
				reader = gzReader
			case util.Lz4:
				reader = io.NopCloser(lz4.NewReader(reader))
			}

			reader = http.MaxBytesReader(nil, io.NopCloser(reader), int64(maxRecvMsgSize))
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
		}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
	}
}

func (d *Distributor) newOTLPParser(
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	otlpConverter *otlpMimirConverter,
	pushMetrics *PushMetrics,
	discardedDueToOTelParseError *prometheus.CounterVec,
) parserFunc {
	if resourceAttributePromotionConfig == nil {
		resourceAttributePromotionConfig = limits
	}
	if keepIdentifyingOTelResourceAttributesConfig == nil {
		keepIdentifyingOTelResourceAttributesConfig = limits
	}
	return func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error {
		var payloadSize int64
		if buf, ok := util.TryBufferFromReader(r.Body); ok {
			payloadSize = int64(buf.Len())
		} else {
			payloadSize = r.ContentLength
		}
		// Check the request size against the message size limit, regardless of whether the request is compressed.
		// If the request is compressed and its compressed length already exceeds the size limit, there's no need to decompress it.
		if payloadSize > int64(maxRecvMsgSize) {
			return httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
				actual: int(payloadSize),
				limit:  maxRecvMsgSize,
			}.Error())
		}

		contentType := r.Header.Get("Content-Type")
		contentEncoding := r.Header.Get("Content-Encoding")

		decoderFunc, err := d.getOTLPDecoderFunc(ctx, r, contentType, contentEncoding, maxRecvMsgSize, buffers, logger)
		if err != nil {
			return err
		}

		spanLogger, ctx := spanlogger.New(ctx, logger, tracer, "Distributor.OTLPHandler.decodeAndConvert")
		defer spanLogger.Finish()

		spanLogger.SetTag("content_type", contentType)
		spanLogger.SetTag("content_encoding", contentEncoding)
		spanLogger.SetTag("content_length", r.ContentLength)

		// Record an in-flight push request with the compressed size, to avoid exceeding the memory limit
		// while decompressing.
		ctx, err = d.StartPushRequest(ctx, payloadSize)
		if err != nil {
			return err
		}
		defer d.FinishPushRequest(ctx)

		otlpReq, uncompressedBodySize, err := decoderFunc(r.Body)
		if err != nil {
			return err
		}

		level.Debug(spanLogger).Log("msg", "decoding complete, starting conversion")

		for _, middleware := range OTLPPushMiddlewares {
			err := middleware(ctx, &otlpReq)
			if err != nil {
				return err
			}
		}

		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}
		enableCTZeroIngestion := limits.OTelCreatedTimestampZeroIngestionEnabled(tenantID)
		promoteResourceAttributes := resourceAttributePromotionConfig.PromoteOTelResourceAttributes(tenantID)
		keepIdentifyingResourceAttributes := keepIdentifyingOTelResourceAttributesConfig.OTelKeepIdentifyingResourceAttributes(tenantID)
		convertHistogramsToNHCB := limits.OTelConvertHistogramsToNHCB(tenantID)
		promoteScopeMetadata := limits.OTelPromoteScopeMetadata(tenantID)
		allowDeltaTemporality := limits.OTelNativeDeltaIngestion(tenantID)
		translationStrategy := limits.OTelTranslationStrategy(tenantID)
		validateTranslationStrategy(translationStrategy, limits, tenantID)

		pushMetrics.IncOTLPRequest(tenantID)
		pushMetrics.ObserveUncompressedBodySize(tenantID, "otlp", float64(uncompressedBodySize))
		pushMetrics.IncOTLPContentType(contentType)
		observeOTLPFieldsCount(pushMetrics, otlpReq)

		convOpts := conversionOptions{
			addSuffixes:                       translationStrategy.ShouldAddSuffixes(),
			enableCTZeroIngestion:             enableCTZeroIngestion,
			keepIdentifyingResourceAttributes: keepIdentifyingResourceAttributes,
			convertHistogramsToNHCB:           convertHistogramsToNHCB,
			promoteScopeMetadata:              promoteScopeMetadata,
			promoteResourceAttributes:         promoteResourceAttributes,
			allowDeltaTemporality:             allowDeltaTemporality,
			allowUTF8:                         !translationStrategy.ShouldEscape(),
			underscoreSanitization:            limits.OTelLabelNameUnderscoreSanitization(tenantID),
			preserveMultipleUnderscores:       limits.OTelLabelNamePreserveMultipleUnderscores(tenantID),
		}
		metrics, metadata, metricsDropped, err := otelMetricsToSeriesAndMetadata(
			ctx,
			otlpConverter,
			otlpReq.Metrics(),
			convOpts,
			spanLogger,
		)
		if metricsDropped > 0 {
			discardedDueToOTelParseError.WithLabelValues(tenantID, "").Add(float64(metricsDropped)) // "group" label is empty here as metrics couldn't be parsed
		}
		if err != nil {
			return err
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
			"metrics_dropped", metricsDropped,
			"sample_count", sampleCount,
			"histogram_count", histogramCount,
			"exemplar_count", exemplarCount,
			"promoted_resource_attributes", promoteResourceAttributes,
		)

		req.Source = mimirpb.OTLP
		req.Timeseries = metrics
		req.Metadata = metadata
		return nil
	}
}

func (d *Distributor) checkOTLPRequestSize(requestSize int64) error {
	inflightBytes := d.inflightPushRequestsBytes.Add(requestSize)
	if err := d.checkInflightBytes(inflightBytes); err != nil {
		return httpgrpc.Error(http.StatusRequestEntityTooLarge, err.Error())
	}
	return nil
}

// toOtlpGRPCHTTPStatus is utilized by the OTLP endpoint.
func toOtlpGRPCHTTPStatus(pushErr error) (codes.Code, int, bool) {
	var distributorErr Error
	if errors.Is(pushErr, context.DeadlineExceeded) || !errors.As(pushErr, &distributorErr) {
		return codes.Internal, http.StatusServiceUnavailable, false
	}

	grpcStatusCode := errorCauseToGRPCStatusCode(distributorErr.Cause())
	httpStatusCode := errorCauseToHTTPStatusCode(distributorErr.Cause())
	return grpcStatusCode, httpRetryableToOTLPRetryable(httpStatusCode), distributorErr.IsSoft()
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

// writeErrorToHTTPResponseBody converts the given error into a gRPC status and marshals it into a byte slice, in order to be written to the response body.
// See doc https://opentelemetry.io/docs/specs/otlp/#failures-1.
func writeErrorToHTTPResponseBody(r *http.Request, w http.ResponseWriter, httpCode int, grpcCode codes.Code, msg string, logger log.Logger) {
	validUTF8Msg := validUTF8Message(msg)
	if server.IsHandledByHttpgrpcServer(r.Context()) {
		w.Header().Set(server.ErrorMessageHeaderKey, validUTF8Msg) // If httpgrpc Server wants to convert this HTTP response into error, use this error message, instead of using response body.
	}

	st := status.New(grpcCode, validUTF8Msg).Proto()
	writeOTLPResponse(r, w, httpCode, st, logger)
}

func writeOTLPResponse(r *http.Request, w http.ResponseWriter, httpCode int, payload proto.Message, logger log.Logger) {
	// Per OTLP spec (see: https://opentelemetry.io/docs/specs/otlp/#otlphttp-response), the server MUST mirror
	// the request Content-Type in responses. The parser validates Content-Type and only accepts
	// application/json or application/x-protobuf. For requests with unsupported Content-Type that are rejected by the parser,
	// we default to application/x-protobuf.
	contentType := r.Header.Get("Content-Type")
	switch contentType {
	case jsonContentType, pbContentType:
		// Valid Content-Type, mirror it in the response.
	default:
		// Unsupported Content-Type, default to protobuf.
		contentType = pbContentType
	}

	w.Header().Set("X-Content-Type-Options", "nosniff")

	body, _, err := marshal(payload, contentType)
	if err != nil {
		httpCode = http.StatusInternalServerError
		level.Error(logger).Log("msg", "failed to marshal payload", "err", err, "payload", payload, "content_type", contentType)
		// Retry with a minimal Status message. If this also fails, marshaling is fundamentally broken.
		var format string
		body, format, err = marshal(status.New(codes.Internal, "failed to marshal OTLP response").Proto(), contentType)
		err = errors.Wrapf(err, "marshalling %T to %s", payload, format)
	}
	if err != nil {
		level.Error(logger).Log("msg", "OTLP response marshal failed, responding without payload", "err", err, "content_type", contentType)
		// If marshalling both the original and fallback message fails, return empty body with text/plain to signal the failure
		contentType = "text/plain"
		body = nil
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(httpCode)
	if len(body) == 0 {
		return
	}
	if _, err := w.Write(body); err != nil {
		level.Error(logger).Log("msg", "failed to write OTLP error response", "err", err, "content_type", contentType)
	}
}

func marshal(payload proto.Message, contentType string) ([]byte, string, error) {
	if contentType == jsonContentType {
		data, err := json.Marshal(payload)
		return data, "JSON", err
	}

	data, err := proto.Marshal(payload)
	return data, "protobuf", err
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

type conversionOptions struct {
	addSuffixes                       bool
	enableCTZeroIngestion             bool
	keepIdentifyingResourceAttributes bool
	convertHistogramsToNHCB           bool
	promoteScopeMetadata              bool
	promoteResourceAttributes         []string
	allowDeltaTemporality             bool
	allowUTF8                         bool
	underscoreSanitization            bool
	preserveMultipleUnderscores       bool
}

func otelMetricsToSeriesAndMetadata(
	ctx context.Context,
	converter *otlpMimirConverter,
	md pmetric.Metrics,
	opts conversionOptions,
	logger log.Logger,
) ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata, int, error) {
	settings := prometheusremotewrite.Settings{
		AddMetricSuffixes:                    opts.addSuffixes,
		PromoteResourceAttributes:            prometheusremotewrite.NewPromoteResourceAttributes(config.OTLPConfig{PromoteResourceAttributes: opts.promoteResourceAttributes}),
		KeepIdentifyingResourceAttributes:    opts.keepIdentifyingResourceAttributes,
		ConvertHistogramsToNHCB:              opts.convertHistogramsToNHCB,
		PromoteScopeMetadata:                 opts.promoteScopeMetadata,
		AllowDeltaTemporality:                opts.allowDeltaTemporality,
		AllowUTF8:                            opts.allowUTF8,
		LabelNameUnderscoreSanitization:      opts.underscoreSanitization,
		LabelNamePreserveMultipleUnderscores: opts.preserveMultipleUnderscores,
	}
	converter.appender.EnableCreatedTimestampZeroIngestion = opts.enableCTZeroIngestion
	mimirTS, metadata := converter.ToSeriesAndMetadata(ctx, md, settings, logger)

	dropped := converter.DroppedTotal()
	if len(mimirTS) == 0 && dropped > 0 {
		return nil, nil, dropped, converter.Err()
	}
	return mimirTS, metadata, dropped, nil
}

type otlpMimirConverter struct {
	appender  *otlpappender.MimirAppender
	converter *prometheusremotewrite.PrometheusConverter
	// err holds OTLP parse errors
	err error
}

func newOTLPMimirConverter(appender *otlpappender.MimirAppender) *otlpMimirConverter {
	return &otlpMimirConverter{
		appender:  appender,
		converter: prometheusremotewrite.NewPrometheusConverter(appender),
	}
}

func (c *otlpMimirConverter) ToSeriesAndMetadata(ctx context.Context, md pmetric.Metrics, settings prometheusremotewrite.Settings, logger log.Logger) ([]mimirpb.PreallocTimeseries, []*mimirpb.MetricMetadata) {
	if c.err != nil {
		return nil, nil
	}

	_, c.err = c.converter.FromMetrics(ctx, md, settings)

	timeseries, metadata := c.appender.GetResult()
	return timeseries, metadata
}

func (c *otlpMimirConverter) DroppedTotal() int {
	if c.err != nil {
		return len(multierr.Errors(c.err))
	}
	return 0
}

func (c *otlpMimirConverter) Err() error {
	if c.err != nil {
		errMsg := c.err.Error()
		if len(errMsg) > maxErrMsgLen {
			errMsg = errMsg[:maxErrMsgLen]
		}
		return fmt.Errorf("otlp parse error: %s", errMsg)
	}
	return nil
}

// TimeseriesToOTLPRequest is used in tests.
// If you provide exemplars they will be placed on the first float or
// histogram sample.
func TimeseriesToOTLPRequest(timeseries []prompb.TimeSeries, metadata []mimirpb.MetricMetadata) pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	for _, ts := range timeseries {
		name := ""
		attributes := pcommon.NewMap()

		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				name = l.Value
				continue
			}

			attributes.PutStr(l.Name, l.Value)
		}

		rms := d.ResourceMetrics()
		rm := rms.AppendEmpty()
		rm.Resource().Attributes().PutStr("resource.attr", "value")
		sm := rm.ScopeMetrics()

		if len(ts.Samples) > 0 {
			metric := sm.AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(name)
			metric.SetEmptyGauge()
			for _, m := range metadata {
				if m.MetricFamilyName == name {
					metric.SetDescription(m.GetHelp())
					metric.SetUnit(m.GetUnit())
				}
			}
			for i, sample := range ts.Samples {
				datapoint := metric.Gauge().DataPoints().AppendEmpty()
				datapoint.SetTimestamp(pcommon.Timestamp(sample.Timestamp * time.Millisecond.Nanoseconds()))
				datapoint.SetDoubleValue(sample.Value)
				attributes.CopyTo(datapoint.Attributes())
				if i == 0 {
					for _, tsEx := range ts.Exemplars {
						ex := datapoint.Exemplars().AppendEmpty()
						ex.SetDoubleValue(tsEx.Value)
						ex.SetTimestamp(pcommon.Timestamp(tsEx.Timestamp * time.Millisecond.Nanoseconds()))
						ex.FilteredAttributes().EnsureCapacity(len(tsEx.Labels))
						for _, label := range tsEx.Labels {
							ex.FilteredAttributes().PutStr(label.Name, label.Value)
						}
					}
				}
			}
		}

		if len(ts.Histograms) > 0 {
			metric := sm.AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(name)
			metric.SetEmptyExponentialHistogram()
			for _, m := range metadata {
				if m.MetricFamilyName == name {
					metric.SetDescription(m.GetHelp())
					metric.SetUnit(m.GetUnit())
				}
			}
			metric.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for i, histogram := range ts.Histograms {
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
				if i == 0 {
					for _, tsEx := range ts.Exemplars {
						ex := datapoint.Exemplars().AppendEmpty()
						ex.SetDoubleValue(tsEx.Value)
						ex.SetTimestamp(pcommon.Timestamp(tsEx.Timestamp * time.Millisecond.Nanoseconds()))
						ex.FilteredAttributes().EnsureCapacity(len(tsEx.Labels))
						for _, label := range tsEx.Labels {
							ex.FilteredAttributes().PutStr(label.Name, label.Value)
						}
					}
				}
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

type otelAttributeValueTooLongError struct {
	labelValueTooLongError
}

func (e otelAttributeValueTooLongError) Error() string {
	return fmt.Sprintf(
		"received a metric whose attribute value length of %d exceeds the limit of %d, attribute: '%s', value: '%.200s' (truncated) metric: '%.200s'. See: https://grafana.com/docs/grafana-cloud/send-data/otlp/otlp-format-considerations/#metrics-ingestion-limits",
		len(e.Label.Value), e.Limit, e.Label.Name, e.Label.Value, e.Series,
	)
}
