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

// protoUnmarshalerFactory creates a proto.Message unmarshaler for an ExportRequest.
// This type is used to eliminate per-request branching - the factory is created once
// at handler creation time and passed to the parser.
type protoUnmarshalerFactory func(*pmetricotlp.ExportRequest) proto.Message

// OTLPHandler is an http.Handler accepting OTLP write requests.
// It dispatches to specialized handlers at creation time to eliminate per-request branching.
func OTLPHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	retryCfg RetryConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	enableOTLPLazyDeserializing bool,
	enableBatchedStreaming bool,
	batchedStreamingBatchSize int,
	push PushFunc,
	pushMetrics *PushMetrics,
	reg prometheus.Registerer,
	logger log.Logger,
) http.Handler {
	discardedDueToOtelParseError := validation.DiscardedSamplesCounter(reg, otelParseError)

	// Dispatch to specialized handlers at creation time to eliminate per-request branching.
	if enableBatchedStreaming {
		return newOTLPBatchedHandler(
			maxRecvMsgSize, requestBufferPool, sourceIPs, limits,
			resourceAttributePromotionConfig, keepIdentifyingOTelResourceAttributesConfig,
			retryCfg, OTLPPushMiddlewares, enableOTLPLazyDeserializing, batchedStreamingBatchSize,
			push, pushMetrics, discardedDueToOtelParseError, logger,
		)
	}

	return newOTLPDefaultHandler(
		maxRecvMsgSize, requestBufferPool, sourceIPs, limits,
		resourceAttributePromotionConfig, keepIdentifyingOTelResourceAttributesConfig,
		retryCfg, OTLPPushMiddlewares, enableOTLPLazyDeserializing,
		push, pushMetrics, discardedDueToOtelParseError, logger,
	)
}

// newOTLPDefaultHandler creates the default OTLP handler for single-shot processing.
func newOTLPDefaultHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	retryCfg RetryConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	enableOTLPLazyDeserializing bool,
	push PushFunc,
	pushMetrics *PushMetrics,
	discardedDueToOtelParseError *prometheus.CounterVec,
	logger log.Logger,
) http.Handler {
	// Create unmarshaler factory at handler creation time to avoid per-request overhead.
	var createUnmarshaler protoUnmarshalerFactory
	if enableOTLPLazyDeserializing {
		createUnmarshaler = func(req *pmetricotlp.ExportRequest) proto.Message {
			return otlpProtoUnmarshalerLazy{request: req}
		}
	} else {
		createUnmarshaler = func(req *pmetricotlp.ExportRequest) proto.Message {
			return otlpProtoUnmarshaler{request: req}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		reqLogger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				reqLogger = utillog.WithSourceIPs(source, reqLogger)
			}
		}

		otlpConverter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())

		parser := newOTLPParser(
			limits, resourceAttributePromotionConfig, keepIdentifyingOTelResourceAttributesConfig,
			otlpConverter, pushMetrics, discardedDueToOtelParseError,
			OTLPPushMiddlewares, createUnmarshaler,
		)

		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest
			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, reqLogger); err != nil {
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
		handleOTLPPushResponse(w, r, req, pushErr, otlpConverter, retryCfg, reqLogger)
	})
}

// newOTLPBatchedHandler creates the batched streaming OTLP handler.
func newOTLPBatchedHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	retryCfg RetryConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	enableOTLPLazyDeserializing bool,
	batchSize int,
	push PushFunc,
	pushMetrics *PushMetrics,
	discardedDueToOtelParseError *prometheus.CounterVec,
	logger log.Logger,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		reqLogger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				reqLogger = utillog.WithSourceIPs(source, reqLogger)
			}
		}

		handleOTLPBatchedStreaming(
			ctx, w, r, maxRecvMsgSize, requestBufferPool, limits,
			resourceAttributePromotionConfig, keepIdentifyingOTelResourceAttributesConfig,
			retryCfg, OTLPPushMiddlewares, enableOTLPLazyDeserializing, batchSize,
			push, pushMetrics, discardedDueToOtelParseError, reqLogger,
		)
	})
}

// handleOTLPPushResponse handles the response after pushing OTLP metrics.
func handleOTLPPushResponse(w http.ResponseWriter, r *http.Request, req *Request, pushErr error, otlpConverter *otlpMimirConverter, retryCfg RetryConfig, logger log.Logger) {
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
		msgs := []interface{}{"msg", "detected an error while ingesting OTLP metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", pushErr}
		logLevel := level.Error
		if httpCode/100 == 4 {
			msgs = append(msgs, "insight", true)
			logLevel = level.Warn
		}
		logLevel(logger).Log(msgs...)
	}
	addErrorHeaders(w, pushErr, r, httpCode, retryCfg)
	writeErrorToHTTPResponseBody(r, w, httpCode, grpcCode, errorMsg, logger)
}

func handlePartialOTLPPush(pushErr error, w http.ResponseWriter, r *http.Request, req *Request, logger log.Logger) {
	// Respond as per spec:
	// https://opentelemetry.io/docs/specs/otlp/#otlphttp-response.
	expResp := colmetricpb.ExportMetricsServiceResponse{
		PartialSuccess: &colmetricpb.ExportMetricsPartialSuccess{
			RejectedDataPoints: 0,
			ErrorMessage:       pushErr.Error(),
		},
	}
	addSuccessHeaders(w, req.artificialDelay)
	writeOTLPResponse(r, w, http.StatusOK, &expResp, logger)
}

// handleOTLPBatchedStreaming handles OTLP requests using batched streaming to reduce peak memory.
// It processes metrics in batches, overlapping conversion and push in a pipeline.
func handleOTLPBatchedStreaming(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	retryCfg RetryConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	enableOTLPLazyDeserializing bool,
	batchSize int,
	push PushFunc,
	pushMetrics *PushMetrics,
	discardedDueToOtelParseError *prometheus.CounterVec,
	logger log.Logger,
) {
	// Parse OTLP request (decompression + protobuf/JSON parsing)
	rb := util.NewRequestBuffers(requestBufferPool)
	otlpReq, rawBytes, uncompressedBodySize, tenantID, convOpts, err := parseOTLPRequestForBatching(
		ctx, r, maxRecvMsgSize, rb, limits,
		resourceAttributePromotionConfig, keepIdentifyingOTelResourceAttributesConfig,
		OTLPPushMiddlewares, enableOTLPLazyDeserializing, pushMetrics, logger,
	)
	if err != nil {
		rb.CleanUp()
		handleOTLPParseError(w, r, err, retryCfg, logger)
		return
	}

	// Record metrics
	pushMetrics.IncOTLPRequest(tenantID)
	pushMetrics.ObserveUncompressedBodySize(tenantID, "otlp", float64(uncompressedBodySize))
	pushMetrics.IncOTLPContentType(r.Header.Get("Content-Type"))
	// Only observe field counts when we have a parsed request.
	// Streaming skips parsing when rawBytes != nil and no middlewares.
	if rawBytes == nil || len(OTLPPushMiddlewares) > 0 {
		observeOTLPFieldsCount(pushMetrics, otlpReq)
	}

	// Create batch processor
	processor := &otlpBatchProcessor{
		limits:                  limits,
		resourceAttrConfig:      resourceAttributePromotionConfig,
		keepIdentifyingConfig:   keepIdentifyingOTelResourceAttributesConfig,
		pushMetrics:             pushMetrics,
		discardedCounter:        discardedDueToOtelParseError,
		middlewares:             OTLPPushMiddlewares,
		enableLazyDeserializing: enableOTLPLazyDeserializing,
		push:                    push,
		logger:                  logger,
		batchSize:               batchSize,
	}

	// Process batches - use streaming iteration for protobuf if rawBytes available
	softErrors, hardErr := processor.processBatched(ctx, otlpReq, rawBytes, tenantID, convOpts)

	// Cleanup buffer after all batches complete (important for lazy deserialization)
	rb.CleanUp()

	// Handle response
	if hardErr != nil {
		handleOTLPBatchError(w, r, hardErr, retryCfg, logger)
		return
	}

	if len(softErrors) > 0 {
		// Partial success - some batches failed with soft errors
		handleOTLPBatchPartialSuccess(w, r, softErrors, logger)
		return
	}

	// Full success
	var expResp colmetricpb.ExportMetricsServiceResponse
	addSuccessHeaders(w, -1) // No artificial delay tracked for batched streaming
	writeOTLPResponse(r, w, http.StatusOK, &expResp, logger)
}

// parseOTLPRequestForBatching parses an OTLP request without converting to mimirpb.
// It returns the parsed request, raw bytes (for protobuf streaming), tenant ID, and conversion options.
// rawBytes is non-nil only for protobuf requests and can be used with streaming parsers.
func parseOTLPRequestForBatching(
	ctx context.Context,
	r *http.Request,
	maxRecvMsgSize int,
	buffers *util.RequestBuffers,
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	enableOTLPLazyDeserializing bool,
	pushMetrics *PushMetrics,
	logger log.Logger,
) (pmetricotlp.ExportRequest, []byte, int, string, conversionOptions, error) {
	var emptyReq pmetricotlp.ExportRequest
	var emptyOpts conversionOptions

	if resourceAttributePromotionConfig == nil {
		resourceAttributePromotionConfig = limits
	}
	if keepIdentifyingOTelResourceAttributesConfig == nil {
		keepIdentifyingOTelResourceAttributesConfig = limits
	}

	contentType := r.Header.Get("Content-Type")
	contentEncoding := r.Header.Get("Content-Encoding")
	var compression util.CompressionType
	switch contentEncoding {
	case "gzip":
		compression = util.Gzip
	case "lz4":
		compression = util.Lz4
	case "zstd":
		compression = util.Zstd
	case "":
		compression = util.NoCompression
	default:
		return emptyReq, nil, 0, "", emptyOpts, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\", \"lz4\", \"zstd\", or no compression supported", contentEncoding)
	}

	// Check content length before decompression
	if r.ContentLength > int64(maxRecvMsgSize) {
		return emptyReq, nil, 0, "", emptyOpts, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
			actual: int(r.ContentLength),
			limit:  maxRecvMsgSize,
		}.Error())
	}

	// Decode the request
	var otlpReq pmetricotlp.ExportRequest
	var rawBytes []byte // Raw protobuf bytes for streaming (nil for JSON)
	var uncompressedBodySize int
	var err error

	switch contentType {
	case pbContentType:
		// First decompress to get raw bytes for streaming
		rawBytes, err = util.DecompressRequest(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, buffers, compression)
		var tooLargeErr util.MsgSizeTooLargeErr
		if errors.As(err, &tooLargeErr) {
			return emptyReq, nil, 0, "", emptyOpts, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
				compressed: tooLargeErr.Compressed,
				actual:     tooLargeErr.Actual,
				limit:      tooLargeErr.Limit,
			}.Error())
		}
		if err != nil {
			return emptyReq, nil, 0, "", emptyOpts, err
		}
		uncompressedBodySize = len(rawBytes)

		// Only parse if middlewares need the full request.
		// Streaming will parse rawBytes directly, avoiding double-parsing.
		if len(OTLPPushMiddlewares) > 0 {
			exportReq := pmetricotlp.NewExportRequest()
			var unmarshaler proto.Unmarshaler
			if enableOTLPLazyDeserializing {
				unmarshaler = otlpProtoUnmarshalerLazy{request: &exportReq}
			} else {
				unmarshaler = otlpProtoUnmarshaler{request: &exportReq}
			}
			if err = unmarshaler.Unmarshal(rawBytes); err != nil {
				return emptyReq, nil, 0, "", emptyOpts, err
			}
			otlpReq = exportReq
		}
		// If no middlewares, otlpReq stays empty - streaming will use rawBytes directly

	case jsonContentType:
		// JSON doesn't support streaming parsing, so rawBytes stays nil
		exportReq := pmetricotlp.NewExportRequest()
		sz := int(r.ContentLength)
		if sz > 0 {
			sz += bytes.MinRead
		}
		buf := buffers.Get(sz)
		var reader io.Reader = r.Body
		switch compression {
		case util.Gzip:
			gzReader, err := gzip.NewReader(reader)
			if err != nil {
				return emptyReq, nil, 0, "", emptyOpts, errors.Wrap(err, "create gzip reader")
			}
			defer runutil.CloseWithLogOnErr(logger, gzReader, "close gzip reader")
			reader = gzReader
		case util.Lz4:
			reader = io.NopCloser(lz4.NewReader(reader))
		case util.Zstd:
			reader, err = zstd.NewReader(reader)
			if err != nil {
				return emptyReq, nil, 0, "", emptyOpts, errors.Wrap(err, "create zstd reader")
			}
		}

		reader = http.MaxBytesReader(nil, io.NopCloser(reader), int64(maxRecvMsgSize))
		if _, err := buf.ReadFrom(reader); err != nil {
			if util.IsRequestBodyTooLarge(err) {
				return emptyReq, nil, 0, "", emptyOpts, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
					actual: -1,
					limit:  maxRecvMsgSize,
				}.Error())
			}
			return emptyReq, nil, 0, "", emptyOpts, errors.Wrap(err, "read write request")
		}
		if err := exportReq.UnmarshalJSON(buf.Bytes()); err != nil {
			return emptyReq, nil, 0, "", emptyOpts, err
		}
		uncompressedBodySize = buf.Len()
		otlpReq = exportReq

	default:
		return emptyReq, nil, 0, "", emptyOpts, httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
	}

	// Run middlewares
	for _, middleware := range OTLPPushMiddlewares {
		if err := middleware(ctx, &otlpReq); err != nil {
			return emptyReq, nil, 0, "", emptyOpts, err
		}
	}

	// Get tenant ID and configuration
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return emptyReq, nil, 0, "", emptyOpts, err
	}

	translationStrategy := limits.OTelTranslationStrategy(tenantID)
	validateTranslationStrategy(translationStrategy, limits, tenantID)

	convOpts := conversionOptions{
		addSuffixes:                       translationStrategy.ShouldAddSuffixes(),
		enableCTZeroIngestion:             limits.OTelCreatedTimestampZeroIngestionEnabled(tenantID),
		keepIdentifyingResourceAttributes: keepIdentifyingOTelResourceAttributesConfig.OTelKeepIdentifyingResourceAttributes(tenantID),
		convertHistogramsToNHCB:           limits.OTelConvertHistogramsToNHCB(tenantID),
		promoteScopeMetadata:              limits.OTelPromoteScopeMetadata(tenantID),
		promoteResourceAttributes:         resourceAttributePromotionConfig.PromoteOTelResourceAttributes(tenantID),
		allowDeltaTemporality:             limits.OTelNativeDeltaIngestion(tenantID),
		allowUTF8:                         !translationStrategy.ShouldEscape(),
		underscoreSanitization:            limits.OTelLabelNameUnderscoreSanitization(tenantID),
		preserveMultipleUnderscores:       limits.OTelLabelNamePreserveMultipleUnderscores(tenantID),
	}

	return otlpReq, rawBytes, uncompressedBodySize, tenantID, convOpts, nil
}

// handleOTLPParseError handles errors that occur during OTLP request parsing.
func handleOTLPParseError(w http.ResponseWriter, r *http.Request, err error, retryCfg RetryConfig, logger log.Logger) {
	if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
		err = httpgrpc.Error(http.StatusBadRequest, err.Error())
	}

	st, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		writeErrorToHTTPResponseBody(r, w, http.StatusBadRequest, codes.InvalidArgument, err.Error(), logger)
		return
	}

	grpcCode := st.Code()
	httpCode := http.StatusBadRequest
	if util.IsHTTPStatusCode(grpcCode) {
		httpCode = httpRetryableToOTLPRetryable(int(grpcCode))
	}

	addErrorHeaders(w, err, r, httpCode, retryCfg)
	writeErrorToHTTPResponseBody(r, w, httpCode, grpcCode, st.Message(), logger)
}

// handleOTLPBatchError handles hard errors from batch processing.
func handleOTLPBatchError(w http.ResponseWriter, r *http.Request, err error, retryCfg RetryConfig, logger log.Logger) {
	if errors.Is(err, context.Canceled) {
		level.Warn(logger).Log("msg", "push request canceled", "err", err)
		writeErrorToHTTPResponseBody(r, w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
		return
	}

	grpcCode, httpCode, _ := toOtlpGRPCHTTPStatus(err)
	errorMsg := err.Error()

	msgs := []interface{}{"msg", "detected an error while ingesting OTLP metrics request (batched streaming)", "httpCode", httpCode, "err", err}
	logLevel := level.Error
	if httpCode/100 == 4 {
		msgs = append(msgs, "insight", true)
		logLevel = level.Warn
	}
	logLevel(logger).Log(msgs...)

	addErrorHeaders(w, err, r, httpCode, retryCfg)
	writeErrorToHTTPResponseBody(r, w, httpCode, grpcCode, errorMsg, logger)
}

// handleOTLPBatchPartialSuccess handles soft errors from batch processing.
func handleOTLPBatchPartialSuccess(w http.ResponseWriter, r *http.Request, softErrors []error, logger log.Logger) {
	errorMsg := aggregateBatchErrors(softErrors)
	level.Warn(logger).Log("msg", "partial success in OTLP batched streaming", "errors", len(softErrors), "insight", true)

	expResp := colmetricpb.ExportMetricsServiceResponse{
		PartialSuccess: &colmetricpb.ExportMetricsPartialSuccess{
			RejectedDataPoints: 0, // We don't track exact count per batch
			ErrorMessage:       errorMsg,
		},
	}
	addSuccessHeaders(w, -1)
	writeOTLPResponse(r, w, http.StatusOK, &expResp, logger)
}

func observeOTLPFieldsCount(pushMetrics *PushMetrics, req pmetricotlp.ExportRequest) {
	resourceMetricsSlice := req.Metrics().ResourceMetrics()
	pushMetrics.ObserveOTLPArrayLengths("resource_metrics", resourceMetricsSlice.Len())
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		resourceMetrics := resourceMetricsSlice.At(i)
		scopeMetricsSlice := resourceMetrics.ScopeMetrics()
		pushMetrics.ObserveOTLPArrayLengths("scope_metrics", scopeMetricsSlice.Len())
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			scopeMetrics := scopeMetricsSlice.At(j)
			metricSlice := scopeMetrics.Metrics()
			pushMetrics.ObserveOTLPArrayLengths("metrics", metricSlice.Len())
		}
	}
}

func newOTLPParser(
	limits OTLPHandlerLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	otlpConverter *otlpMimirConverter,
	pushMetrics *PushMetrics,
	discardedDueToOtelParseError *prometheus.CounterVec,
	OTLPPushMiddlewares []OTLPPushMiddleware,
	createUnmarshaler protoUnmarshalerFactory,
) parserFunc {
	if resourceAttributePromotionConfig == nil {
		resourceAttributePromotionConfig = limits
	}
	if keepIdentifyingOTelResourceAttributesConfig == nil {
		keepIdentifyingOTelResourceAttributesConfig = limits
	}
	return func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error {
		contentType := r.Header.Get("Content-Type")
		contentEncoding := r.Header.Get("Content-Encoding")
		var compression util.CompressionType
		switch contentEncoding {
		case "gzip":
			compression = util.Gzip
		case "lz4":
			compression = util.Lz4
		case "zstd":
			compression = util.Zstd
		case "":
			compression = util.NoCompression
		default:
			return httpgrpc.Errorf(http.StatusUnsupportedMediaType, "unsupported compression: %s. Only \"gzip\", \"lz4\", \"zstd\", or no compression supported", contentEncoding)
		}

		var decoderFunc func(io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error)
		switch contentType {
		case pbContentType:
			decoderFunc = func(reader io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
				exportReq := pmetricotlp.NewExportRequest()
				unmarshaler := createUnmarshaler(&exportReq)
				protoBodySize, err := util.ParseProtoReader(ctx, reader, int(r.ContentLength), maxRecvMsgSize, buffers, unmarshaler, compression)
				var tooLargeErr util.MsgSizeTooLargeErr
				if errors.As(err, &tooLargeErr) {
					return exportReq, 0, httpgrpc.Error(http.StatusRequestEntityTooLarge, distributorMaxOTLPRequestSizeErr{
						compressed: tooLargeErr.Compressed,
						actual:     tooLargeErr.Actual,
						limit:      tooLargeErr.Limit,
					}.Error())
				}
				return exportReq, protoBodySize, err
			}

		case jsonContentType:
			decoderFunc = func(reader io.Reader) (req pmetricotlp.ExportRequest, uncompressedBodySize int, err error) {
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
				case util.Zstd:
					reader, err = zstd.NewReader(reader)
					if err != nil {
						return exportReq, 0, errors.Wrap(err, "create zstd reader")
					}
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

		spanLogger, ctx := spanlogger.New(ctx, logger, tracer, "Distributor.OTLPHandler.decodeAndConvert")
		defer spanLogger.Finish()

		spanLogger.SetTag("content_type", contentType)
		spanLogger.SetTag("content_encoding", contentEncoding)
		spanLogger.SetTag("content_length", r.ContentLength)

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
			discardedDueToOtelParseError.WithLabelValues(tenantID, "").Add(float64(metricsDropped)) // "group" label is empty here as metrics couldn't be parsed
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

// validateTranslationStrategy ensures consistency between name translation strategy and name validation scheme and metric name suffix enablement.
// Any inconsistency at this point indicates a programming error, so we panic on errors.
func validateTranslationStrategy(translationStrategy otlptranslator.TranslationStrategyOption, limits OTLPHandlerLimits, tenantID string) {
	validationScheme := limits.NameValidationScheme(tenantID)
	switch validationScheme {
	case model.LegacyValidation:
		if !translationStrategy.ShouldEscape() {
			panic(fmt.Errorf(
				"metric and label name validation scheme is %s, but incompatible OTel translation strategy: %s",
				validationScheme, translationStrategy,
			))
		}
	case model.UTF8Validation:
		if translationStrategy.ShouldEscape() {
			panic(fmt.Errorf(
				"metric and label name validation scheme is %s, but incompatible OTel translation strategy: %s",
				validationScheme, translationStrategy,
			))
		}
	default:
		panic(fmt.Errorf("unhandled name validation scheme: %s", validationScheme))
	}

	addSuffixes := limits.OTelMetricSuffixesEnabled(tenantID)
	if addSuffixes && !translationStrategy.ShouldAddSuffixes() {
		panic(fmt.Errorf("OTel metric suffixes are enabled, but incompatible OTel translation strategy: %s", translationStrategy))
	} else if !addSuffixes && translationStrategy.ShouldAddSuffixes() {
		panic(fmt.Errorf("OTel metric suffixes are disabled, but incompatible OTel translation strategy: %s", translationStrategy))
	}
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
// otlpProtoUnmarshaler implements proto.Message wrapping pmetricotlp.ExportRequest
// using standard (eager) deserialization.
type otlpProtoUnmarshaler struct {
	request *pmetricotlp.ExportRequest
}

func (o otlpProtoUnmarshaler) ProtoMessage() {}
func (o otlpProtoUnmarshaler) Reset()        {}
func (o otlpProtoUnmarshaler) String() string { return "" }

func (o otlpProtoUnmarshaler) Unmarshal(data []byte) error {
	return o.request.UnmarshalProto(data)
}

// otlpProtoUnmarshalerLazy implements proto.Message wrapping pmetricotlp.ExportRequest
// using lazy deserialization. This avoids parsing nested messages until they're accessed.
type otlpProtoUnmarshalerLazy struct {
	request *pmetricotlp.ExportRequest
}

func (o otlpProtoUnmarshalerLazy) ProtoMessage() {}
func (o otlpProtoUnmarshalerLazy) Reset()        {}
func (o otlpProtoUnmarshalerLazy) String() string { return "" }

func (o otlpProtoUnmarshalerLazy) Unmarshal(data []byte) error {
	return o.request.UnmarshalProtoLazy(data)
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

// otlpBatchProcessor processes OTLP metrics in batches at the ResourceMetrics level
// to reduce peak memory usage.
type otlpBatchProcessor struct {
	limits                  OTLPHandlerLimits
	resourceAttrConfig      OTelResourceAttributePromotionConfig
	keepIdentifyingConfig   KeepIdentifyingOTelResourceAttributesConfig
	pushMetrics             *PushMetrics
	discardedCounter        *prometheus.CounterVec
	middlewares             []OTLPPushMiddleware
	enableLazyDeserializing bool
	push                    PushFunc
	logger                  log.Logger
	batchSize               int // Number of ResourceMetrics to process per batch
}

// processBatched processes OTLP metrics in batches sequentially.
// It uses a shared appender across all batches to maintain series deduplication
// while reducing peak memory by pushing batches incrementally.
// If rawBytes is non-nil (protobuf), it uses streaming parsing to reduce peak memory.
// If rawBytes is nil (JSON), it falls back to iterating over the parsed request.
func (p *otlpBatchProcessor) processBatched(
	ctx context.Context,
	otlpReq pmetricotlp.ExportRequest,
	rawBytes []byte,
	tenantID string,
	convOpts conversionOptions,
) (softErrors []error, hardErr error) {
	// SHARED appender for entire request - maintains series deduplication
	appender := otlpappender.NewCombinedAppender()
	appender.EnableCreatedTimestampZeroIngestion = convOpts.enableCTZeroIngestion
	converter := newOTLPMimirConverter(appender)

	// Get conversion settings
	settings := prometheusremotewrite.Settings{
		AddMetricSuffixes:                    convOpts.addSuffixes,
		PromoteResourceAttributes:            prometheusremotewrite.NewPromoteResourceAttributes(config.OTLPConfig{PromoteResourceAttributes: convOpts.promoteResourceAttributes}),
		KeepIdentifyingResourceAttributes:    convOpts.keepIdentifyingResourceAttributes,
		ConvertHistogramsToNHCB:              convOpts.convertHistogramsToNHCB,
		PromoteScopeMetadata:                 convOpts.promoteScopeMetadata,
		AllowDeltaTemporality:                convOpts.allowDeltaTemporality,
		AllowUTF8:                            convOpts.allowUTF8,
		LabelNameUnderscoreSanitization:      convOpts.underscoreSanitization,
		LabelNamePreserveMultipleUnderscores: convOpts.preserveMultipleUnderscores,
	}

	// Determine batch size - default to 1 if not set
	batchSize := p.batchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	// Use streaming iteration for protobuf, fall back to parsed request for JSON
	if rawBytes != nil {
		return p.processBatchedStreaming(ctx, rawBytes, converter, settings, appender, batchSize)
	}
	return p.processBatchedParsed(ctx, otlpReq, converter, settings, appender, batchSize)
}

// processBatchedStreaming processes batches using streaming iteration over raw protobuf bytes.
// This reduces peak memory by:
// 1. Parsing ResourceMetrics one at a time (releasing input memory between batches)
// 2. Clearing the appender after each push (releasing output memory between batches)
func (p *otlpBatchProcessor) processBatchedStreaming(
	ctx context.Context,
	rawBytes []byte,
	converter *otlpMimirConverter,
	settings prometheusremotewrite.Settings,
	appender *otlpappender.MimirAppender,
	batchSize int,
) (softErrors []error, hardErr error) {
	// Create streaming iterator over raw protobuf bytes
	iter := pmetric.NewResourceMetricsIterator(rawBytes, nil)
	defer iter.Release()

	batchCount := 0

	for iter.Next() {
		// Check for context cancellation
		if ctx.Err() != nil {
			return softErrors, ctx.Err()
		}

		rm := iter.Current()
		_, err := converter.converter.FromResourceMetrics(ctx, rm, settings)
		if err != nil {
			// Conversion errors are soft - continue with next ResourceMetrics
			softErrors = append(softErrors, err)
		}

		batchCount++

		// Push batch when we've accumulated enough ResourceMetrics
		if batchCount >= batchSize {
			softErrs, hardErr := p.pushBatch(ctx, appender)
			softErrors = append(softErrors, softErrs...)
			if hardErr != nil {
				return softErrors, hardErr
			}
			batchCount = 0
		}
	}

	if err := iter.Err(); err != nil {
		return softErrors, err
	}

	// Push any remaining series from the last partial batch
	if batchCount > 0 {
		softErrs, hardErr := p.pushBatch(ctx, appender)
		softErrors = append(softErrors, softErrs...)
		if hardErr != nil {
			return softErrors, hardErr
		}
	}

	return softErrors, nil
}

// processBatchedParsed processes batches using the pre-parsed request (for JSON).
// This reduces peak memory by clearing the appender after each push.
func (p *otlpBatchProcessor) processBatchedParsed(
	ctx context.Context,
	otlpReq pmetricotlp.ExportRequest,
	converter *otlpMimirConverter,
	settings prometheusremotewrite.Settings,
	appender *otlpappender.MimirAppender,
	batchSize int,
) (softErrors []error, hardErr error) {
	resourceMetricsSlice := otlpReq.Metrics().ResourceMetrics()
	n := resourceMetricsSlice.Len()

	if n == 0 {
		return nil, nil
	}

	// Process batches sequentially
	for i := 0; i < n; i += batchSize {
		// Check for context cancellation
		if ctx.Err() != nil {
			return softErrors, ctx.Err()
		}

		// Calculate end index for this batch
		end := i + batchSize
		if end > n {
			end = n
		}

		// Convert batch through shared appender using FromResourceMetrics directly
		for j := i; j < end; j++ {
			rm := resourceMetricsSlice.At(j)
			_, err := converter.converter.FromResourceMetrics(ctx, rm, settings)
			if err != nil {
				// Conversion errors are soft - continue with next ResourceMetrics
				softErrors = append(softErrors, err)
			}
		}

		// Push the batch and clear appender to release memory
		softErrs, hardErr := p.pushBatch(ctx, appender)
		softErrors = append(softErrors, softErrs...)
		if hardErr != nil {
			return softErrors, hardErr
		}
	}

	return softErrors, nil
}

// pushBatch extracts all series from the appender, pushes them, and clears the appender.
// This releases memory after each batch to reduce peak memory usage.
func (p *otlpBatchProcessor) pushBatch(
	ctx context.Context,
	appender *otlpappender.MimirAppender,
) (softErrors []error, hardErr error) {
	series, metadata := appender.GetResult()

	// Skip if no series in this batch
	if len(series) == 0 {
		return nil, nil
	}

	// Create and push batch request
	req := p.createBatchRequest(series, metadata)
	pushErr := p.push(ctx, req)

	// Clean up the request (but not the series - appender.Clear() will handle that)
	req.CleanUp()

	// Clear the appender to release memory before the next batch
	appender.Clear()

	if pushErr != nil {
		if isOTLPHardError(pushErr) {
			return nil, pushErr
		}
		softErrors = append(softErrors, pushErr)
	}

	return softErrors, nil
}

// createBatchRequest creates a Request from converted metrics and metadata.
func (p *otlpBatchProcessor) createBatchRequest(metrics []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata) *Request {
	writeReq := &mimirpb.WriteRequest{
		Timeseries: metrics,
		Metadata:   metadata,
		Source:     mimirpb.OTLP,
	}
	return NewParsedRequest(writeReq)
}


// isOTLPHardError returns true if the error should stop batch processing.
// Hard errors include context cancellation, deadlines, and critical system errors.
func isOTLPHardError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var distributorErr Error
	if errors.As(err, &distributorErr) {
		cause := distributorErr.Cause()
		// Circuit breaker and instance limits are hard errors - the system is overloaded
		return cause == mimirpb.ERROR_CAUSE_CIRCUIT_BREAKER_OPEN || cause == mimirpb.ERROR_CAUSE_INSTANCE_LIMIT
	}

	return false
}

// aggregateBatchErrors creates an error message from multiple batch errors.
func aggregateBatchErrors(errs []error) string {
	if len(errs) == 0 {
		return ""
	}
	if len(errs) == 1 {
		return errs[0].Error()
	}

	// Limit message length
	const maxLen = 1024
	msg := fmt.Sprintf("%d batches failed: ", len(errs))
	for i, err := range errs {
		if i > 0 {
			msg += "; "
		}
		errMsg := err.Error()
		if len(msg)+len(errMsg) > maxLen {
			msg += "..."
			break
		}
		msg += errMsg
	}
	return msg
}
