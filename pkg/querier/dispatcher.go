// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var evaluateQueryRequestMessageName = proto.MessageName(&querierpb.EvaluateQueryRequest{})

var errMQENotEnabled = errors.New("MQE is not enabled on this querier")
var errZeroBatchSize = errors.New("requested batch size cannot be 0 for an instant vector result")

type Dispatcher struct {
	engine    *streamingpromql.Engine
	queryable storage.Queryable
	extractor propagation.Extractor
	logger    log.Logger

	// We need to report two kinds of metrics, to mirror those emitted by HTTP requests:
	// cortex_querier_... (eg. cortex_querier_inflight_requests), and
	// cortex_... (eg. cortex_inflight_requests).
	querierMetrics *RequestMetrics
	serverMetrics  *server.Metrics

	// This will usually be time.Now(), but is replaced in some tests.
	timeNow func() time.Time
}

func NewDispatcher(engine *streamingpromql.Engine, queryable storage.Queryable, querierMetrics *RequestMetrics, serverMetrics *server.Metrics, extractor propagation.Extractor, logger log.Logger) *Dispatcher {
	return &Dispatcher{
		engine:         engine,
		queryable:      queryable,
		extractor:      extractor,
		logger:         logger,
		querierMetrics: querierMetrics,
		serverMetrics:  serverMetrics,
		timeNow:        time.Now,
	}
}

func (d *Dispatcher) HandleProtobuf(ctx context.Context, req *prototypes.Any, metadata propagation.Carrier, stream frontendv2pb.QueryResultStream) {
	logger := spanlogger.FromContext(ctx, d.logger)
	writer := newQueryResponseWriter(stream, d.querierMetrics, d.serverMetrics, logger)
	messageName, err := prototypes.AnyMessageName(req)
	writer.Start(messageName, len(req.Value)) // We deliberately call this before checking the error below, so that we still get stats for malformed requests.
	defer writer.Finish()

	if err != nil {
		writer.WriteError(ctx, apierror.TypeBadData, fmt.Errorf("malformed query request type %q: %w", req.TypeUrl, err))
		return
	}

	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		writer.WriteError(ctx, apierror.TypeBadData, err)
		return
	}

	ctx, err = d.extractor.ExtractFromCarrier(ctx, metadata)
	if err != nil {
		writer.WriteError(ctx, apierror.TypeBadData, err)
		return
	}

	writer.tenantID = tenantID

	switch messageName {
	case evaluateQueryRequestMessageName:
		d.evaluateQuery(ctx, req.Value, writer)

	default:
		writer.WriteError(ctx, apierror.TypeBadData, fmt.Errorf("unknown query request type %q", req.TypeUrl))
	}
}

func (d *Dispatcher) evaluateQuery(ctx context.Context, body []byte, resp *queryResponseWriter) {
	startTime := d.timeNow()
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("request.type", evaluateQueryRequestMessageName))

	if d.engine == nil {
		resp.WriteError(ctx, apierror.TypeNotFound, errMQENotEnabled)
		return
	}

	req := &querierpb.EvaluateQueryRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		resp.WriteError(ctx, apierror.TypeInternal, fmt.Errorf("could not read request body: %w", err))
		return
	}

	d.querierMetrics.PlansReceived.WithLabelValues(req.Plan.Version.String()).Inc()

	if len(req.Nodes) != 1 {
		resp.WriteError(ctx, apierror.TypeBadData, fmt.Errorf("this querier only supports evaluating exactly one node, got %d", len(req.Nodes)))
		return
	}

	evaluationNode := req.Nodes[0]

	plan, nodes, err := req.Plan.ToDecodedPlan(evaluationNode.NodeIndex)
	if err != nil {
		resp.WriteError(ctx, apierror.TypeBadData, fmt.Errorf("could not decode plan: %w", err))
		return
	}

	opts := promql.NewPrometheusQueryOpts(false, 0)
	e, err := d.engine.NewEvaluator(ctx, d.queryable, opts, plan, nodes[0], evaluationNode.TimeRange.ToDecodedTimeRange())
	if err != nil {
		resp.WriteError(ctx, apierror.TypeBadData, fmt.Errorf("could not materialize query: %w", err))
		return
	}

	defer e.Close()

	observer := &evaluationObserver{
		w:                  resp,
		nodeIndex:          evaluationNode.NodeIndex,
		startTime:          startTime,
		timeNow:            d.timeNow,
		originalExpression: req.Plan.OriginalExpression,
		batchSize:          req.BatchSize,
	}

	if err := e.Evaluate(ctx, observer); err != nil {
		resp.WriteError(ctx, apierror.TypeExec, err)
		return
	}

	span.SetStatus(codes.Ok, "evaluation completed successfully")
}

func errorTypeForError(err error, fallback apierror.Type) mimirpb.QueryErrorType {
	apiErrorType := apierror.TypeForError(err, fallback)
	t, conversionErr := mimirpb.ErrorTypeFromAPIErrorType(apiErrorType)

	// ErrorTypeFromAPIErrorType should never fail, as the APIError and QueryErrorType enums should remain
	// in sync (and this is enforced with a test).
	// If this does fail, it's a bug and should be fixed.
	if conversionErr != nil {
		return mimirpb.QUERY_ERROR_TYPE_INTERNAL
	}

	return t
}

type queryResponseWriter struct {
	stream         frontendv2pb.QueryResultStream
	querierMetrics *RequestMetrics
	serverMetrics  *server.Metrics
	logger         *spanlogger.SpanLogger

	querierInflightRequests     prometheus.Gauge
	serverInflightRequests      prometheus.Gauge
	querierResponseMessageBytes prometheus.Observer
	serverResponseMessageBytes  prometheus.Observer
	startTime                   time.Time
	routeName                   string
	status                      string
	tenantID                    string
}

func newQueryResponseWriter(stream frontendv2pb.QueryResultStream, querierMetrics *RequestMetrics, serverMetrics *server.Metrics, logger *spanlogger.SpanLogger) *queryResponseWriter {
	return &queryResponseWriter{
		stream:         stream,
		querierMetrics: querierMetrics,
		serverMetrics:  serverMetrics,
		logger:         logger,
	}
}

// Start emits metrics at the start of request processing.
// It should only be called once per instance.
func (w *queryResponseWriter) Start(routeName string, payloadSize int) {
	if routeName == "" {
		routeName = "<invalid>"
	}

	w.routeName = routeName
	w.status = "OK"
	w.startTime = time.Now()

	w.querierInflightRequests = w.querierMetrics.InflightRequests.WithLabelValues("gRPC", routeName)
	w.querierInflightRequests.Inc()
	w.serverInflightRequests = w.serverMetrics.InflightRequests.WithLabelValues("gRPC", routeName)
	w.serverInflightRequests.Inc()

	w.querierMetrics.ReceivedMessageSize.WithLabelValues("gRPC", routeName).Observe(float64(payloadSize))
	w.serverMetrics.ReceivedMessageSize.WithLabelValues("gRPC", routeName).Observe(float64(payloadSize))
	w.querierResponseMessageBytes = w.querierMetrics.SentMessageSize.WithLabelValues("gRPC", routeName)
	w.serverResponseMessageBytes = w.serverMetrics.SentMessageSize.WithLabelValues("gRPC", routeName)
}

// Finish emits metrics at the end of request processing.
// It should only be called after Start is called, and should only be called once per instance.
func (w *queryResponseWriter) Finish() {
	duration := time.Since(w.startTime)
	w.querierMetrics.RequestDuration.WithLabelValues("gRPC", w.routeName, w.status, "false").Observe(duration.Seconds())
	w.serverMetrics.RequestDuration.WithLabelValues("gRPC", w.routeName, w.status, "false").Observe(duration.Seconds())
	w.serverMetrics.PerTenantRequestDuration.WithLabelValues("gRPC", w.routeName, w.status, "false", w.tenantID).Observe(duration.Seconds())
	w.serverMetrics.PerTenantRequestTotal.WithLabelValues("gRPC", w.routeName, w.status, "false", w.tenantID).Inc()

	w.querierInflightRequests.Dec()
	w.serverInflightRequests.Dec()
}

func (w *queryResponseWriter) Write(ctx context.Context, r querierpb.EvaluateQueryResponse) error {
	resp := &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &r,
		},
	}

	if err := w.write(ctx, resp); err != nil {
		if grpcutil.IsCanceled(err) {
			level.Debug(w.logger).Log("msg", "failed to write message to query-frontend stream because the request was canceled", "err", err)
		} else {
			level.Warn(w.logger).Log("msg", "failed to write message to query-frontend stream", "err", err)
		}

		return err
	}

	return nil
}

func (w *queryResponseWriter) WriteError(ctx context.Context, fallbackType apierror.Type, err error) {
	typ := errorTypeForError(err, fallbackType)
	msg := err.Error()

	if typ == mimirpb.QUERY_ERROR_TYPE_CANCELED {
		level.Debug(w.logger).Log("msg", "returning cancelled status", "type", typ.String(), "msg", msg)
	} else {
		span := trace.SpanFromContext(ctx)
		span.SetStatus(codes.Error, msg)

		level.Warn(w.logger).Log("msg", "returning error", "type", typ.String(), "msg", msg)
	}

	w.status = "ERROR_" + strings.TrimPrefix(typ.String(), "QUERY_ERROR_TYPE_")

	resp := &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_Error{
			Error: &querierpb.Error{
				Type:    typ,
				Message: msg,
			},
		},
	}

	if err := w.write(ctx, resp); err != nil {
		if grpcutil.IsCanceled(err) {
			level.Debug(w.logger).Log("msg", "failed to write error to query-frontend stream because the request was canceled", "writeErr", err, "originalErr", msg)
		} else {
			level.Warn(w.logger).Log("msg", "failed to write error to query-frontend stream", "writeErr", err, "originalErr", msg)
		}
	}
}

func (w *queryResponseWriter) write(ctx context.Context, resp *frontendv2pb.QueryResultStreamRequest) error {
	size := float64(resp.Size())
	w.querierResponseMessageBytes.Observe(size)
	w.serverResponseMessageBytes.Observe(size)

	return w.stream.Write(ctx, resp)
}

type evaluationObserver struct {
	w         *queryResponseWriter
	nodeIndex int64 // FIXME: remove this once Evaluator supports multiple nodes and passes the node index to the methods below
	batchSize uint64
	startTime time.Time
	timeNow   func() time.Time

	currentInstantVectorSeriesDataBatch []types.InstantVectorSeriesData

	originalExpression string
}

func (o *evaluationObserver) SeriesMetadataEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, series []types.SeriesMetadata) error {
	defer types.SeriesMetadataSlicePool.Put(&series, evaluator.MemoryConsumptionTracker)

	protoSeries := make([]querierpb.SeriesMetadata, 0, len(series))

	for _, s := range series {
		protoSeries = append(protoSeries, querierpb.SeriesMetadata{
			Labels:   mimirpb.FromLabelsToLabelAdapters(s.Labels),
			DropName: s.DropName,
		})
	}

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
			SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
				NodeIndex: o.nodeIndex,
				Series:    protoSeries,
			},
		},
	})
}

func (o *evaluationObserver) InstantVectorSeriesDataEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, seriesIndex int, seriesData types.InstantVectorSeriesData) error {
	if o.batchSize == 0 {
		return errZeroBatchSize
	}

	if cap(o.currentInstantVectorSeriesDataBatch) == 0 {
		o.currentInstantVectorSeriesDataBatch = make([]types.InstantVectorSeriesData, 0, o.batchSize)
	}

	o.currentInstantVectorSeriesDataBatch = append(o.currentInstantVectorSeriesDataBatch, seriesData)

	if len(o.currentInstantVectorSeriesDataBatch) < int(o.batchSize) {
		return nil
	}

	return o.sendInstantVectorSeriesDataBatch(ctx, evaluator)
}

func (o *evaluationObserver) sendInstantVectorSeriesDataBatch(ctx context.Context, evaluator *streamingpromql.Evaluator) error {
	series := make([]querierpb.InstantVectorSeriesData, 0, len(o.currentInstantVectorSeriesDataBatch))

	for _, d := range o.currentInstantVectorSeriesDataBatch {
		series = append(series, querierpb.InstantVectorSeriesData{
			// The methods below do unsafe casts and do not copy the data from the slices, but this is OK as we're immediately
			// serializing the message and sending it before the deferred return to the pool occurs above.
			Floats:     mimirpb.FromFPointsToSamples(d.Floats),
			Histograms: mimirpb.FromHPointsToHistograms(d.Histograms),
		})
	}

	msg := querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
			InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
				NodeIndex: o.nodeIndex,
				Series:    series,
			},
		},
	}

	if err := o.w.Write(ctx, msg); err != nil {
		return err
	}

	for _, d := range o.currentInstantVectorSeriesDataBatch {
		types.PutInstantVectorSeriesData(d, evaluator.MemoryConsumptionTracker)
	}

	o.currentInstantVectorSeriesDataBatch = o.currentInstantVectorSeriesDataBatch[:0]
	return nil
}

func (o *evaluationObserver) RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	// We do not need to return anything to the pool here: the ring buffers in stepData are reused for subsequent steps, or returned to the pool elsewhere if not needed.

	floatsHead, floatsTail := stepData.Floats.UnsafePoints()
	floats, cleanup, err := combineSlices(floatsHead, floatsTail, types.FPointSlicePool, evaluator.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	if cleanup != nil {
		defer cleanup()
	}

	histogramsHead, histogramsTail := stepData.Histograms.UnsafePoints()
	histograms, cleanup, err := combineSlices(histogramsHead, histogramsTail, types.HPointSlicePool, evaluator.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	if cleanup != nil {
		defer cleanup()
	}

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
			RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
				NodeIndex:   o.nodeIndex,
				SeriesIndex: int64(seriesIndex),
				StepT:       stepData.StepT,
				RangeStart:  stepData.RangeStart,
				RangeEnd:    stepData.RangeEnd,

				// The methods below do unsafe casts and do not copy the data from the slices, but this is OK as we're immediately
				// serializing the message and sending it before returning (and therefore before anything else can modify the slices).
				Floats:     mimirpb.FromFPointsToSamples(floats),
				Histograms: mimirpb.FromHPointsToHistograms(histograms),
			},
		},
	})
}

func combineSlices[T any](head, tail []T, pool *types.LimitingBucketedPool[[]T, T], memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]T, func(), error) {
	if len(head) == 0 {
		return tail, nil, nil
	}

	if len(tail) == 0 {
		return head, nil, nil
	}

	// We can't mutate the head or tail slice, so create a new temporary slice with all points.
	combined, err := pool.Get(len(head)+len(tail), memoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined, func() {
		pool.Put(&combined, memoryConsumptionTracker)
	}, nil
}

func (o *evaluationObserver) ScalarEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, data types.ScalarData) error {
	defer types.FPointSlicePool.Put(&data.Samples, evaluator.MemoryConsumptionTracker)

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_ScalarValue{
			ScalarValue: &querierpb.EvaluateQueryResponseScalarValue{
				NodeIndex: o.nodeIndex,

				// The method below does and unsafe cast and does not copy the data from the slice, but this is OK as we're immediately
				// serializing the message and sending it before the deferred return to the pool occurs above.
				Values: mimirpb.FromFPointsToSamples(data.Samples),
			},
		},
	})
}

func (o *evaluationObserver) StringEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, data string) error {
	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_StringValue{
			StringValue: &querierpb.EvaluateQueryResponseStringValue{
				NodeIndex: o.nodeIndex,
				Value:     data,
			},
		},
	})
}

func (o *evaluationObserver) EvaluationCompleted(ctx context.Context, evaluator *streamingpromql.Evaluator, annotations *annotations.Annotations, evaluationStats *types.QueryStats) error {
	if len(o.currentInstantVectorSeriesDataBatch) > 0 {
		// Send any outstanding data now.
		if err := o.sendInstantVectorSeriesDataBatch(ctx, evaluator); err != nil {
			return err
		}
	}

	var annos querierpb.Annotations

	if annotations != nil {
		annos.Warnings, annos.Infos = annotations.AsStrings(o.originalExpression, 0, 0)
	}

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
			EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
				Annotations: annos,
				Stats:       o.populateStats(ctx, evaluationStats),
			},
		},
	})
}

func (o *evaluationObserver) populateStats(ctx context.Context, evaluationStats *types.QueryStats) querier_stats.Stats {
	querierStats := querier_stats.FromContext(ctx)
	if querierStats == nil {
		return querier_stats.Stats{}
	}

	querierStats.AddSamplesProcessed(uint64(evaluationStats.TotalSamples))
	querierStats.AddWallTime(o.timeNow().Sub(o.startTime))

	// Return a copy of the stats to avoid race conditions if anything is still modifying the
	// stats after we return them for serialization.
	return querierStats.Copy().Stats
}
