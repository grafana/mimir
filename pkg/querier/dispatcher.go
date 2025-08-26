// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querier

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var evaluateQueryRequestMessageName = proto.MessageName(&querierpb.EvaluateQueryRequest{})

type Dispatcher struct {
	engine    *streamingpromql.Engine
	queryable storage.Queryable
	logger    log.Logger
}

func NewDispatcher(logger log.Logger, engine *streamingpromql.Engine, queryable storage.Queryable) *Dispatcher {
	return &Dispatcher{
		engine:    engine,
		queryable: queryable,
		logger:    logger,
	}
}

// ServeHTTP responds to requests made to the evaluation HTTP endpoint.
// This is primarily used for debugging: most requests will arrive from query-frontends via
// the query-scheduler over gRPC and therefore be handled by HandleProtobuf.
func (d *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	contentType := r.Header.Get("Content-Type")

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if mediaType != "application/protobuf" {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return
	}

	protoType := params["proto"]
	if protoType == "" {
		http.Error(w, "missing proto parameter in Content-Type header", http.StatusBadRequest)
		return
	}

	switch protoType {
	case evaluateQueryRequestMessageName:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		stream := &httpResultStream{w: w}
		writer := &queryResponseWriter{
			stream: stream,
			logger: d.logger,
		}
		d.evaluateQuery(r.Context(), body, writer)
	default:
		http.Error(w, "unknown request type", http.StatusUnsupportedMediaType)
	}
}

// Why do we call this method "HandleProtobuf", and the other "ServeHTTP"?
// We want to satisfy the http.Handler interface, which requires a method called ServeHTTP,
// and the querier/worker.RequestHandler interface that uses the verb "handle"
// in its method name, so we've made the querier/worker.ProtobufRequestHandler interface use "handle" as well.
func (d *Dispatcher) HandleProtobuf(ctx context.Context, req *prototypes.Any, stream frontendv2pb.QueryResultStream) {
	messageName, err := prototypes.AnyMessageName(req)
	if err != nil {
		writeErrorToStream(ctx, stream, mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("malformed query request type %q: %v", req.TypeUrl, err), d.logger)
		return
	}

	switch messageName {
	case evaluateQueryRequestMessageName:
		writer := &queryResponseWriter{
			stream: stream,
			logger: d.logger,
		}

		d.evaluateQuery(ctx, req.Value, writer)

	default:
		writeErrorToStream(ctx, stream, mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("unknown query request type %q", req.TypeUrl), d.logger)
	}
}

func writeErrorToStream(ctx context.Context, stream frontendv2pb.QueryResultStream, t mimirpb.QueryErrorType, msg string, logger log.Logger) {
	err := stream.Write(ctx, &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_Error{
			Error: &querierpb.Error{
				Type:    t,
				Message: msg,
			},
		},
	})

	if err != nil {
		level.Debug(logger).Log("msg", "failed to write error", "writeErr", err, "originalErr", msg)
	}
}

func (d *Dispatcher) evaluateQuery(ctx context.Context, body []byte, resp *queryResponseWriter) {
	if d.engine == nil {
		resp.WriteError(ctx, mimirpb.QUERY_ERROR_TYPE_NOT_FOUND, "MQE is not enabled on this querier")
		return
	}

	req := &querierpb.EvaluateQueryRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		resp.WriteError(ctx, mimirpb.QUERY_ERROR_TYPE_INTERNAL, fmt.Sprintf("could not read request body: %s", err.Error()))
		return
	}

	if len(req.Nodes) != 1 {
		resp.WriteError(ctx, mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("this querier only supports evaluating exactly one node, got %d", len(req.Nodes)))
		return
	}

	evaluationNode := req.Nodes[0]

	plan, nodes, err := req.Plan.ToDecodedPlan(evaluationNode.NodeIndex)
	if err != nil {
		resp.WriteError(ctx, mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("could not decode plan: %s", err.Error()))
		return
	}

	e, err := d.engine.NewEvaluator(ctx, d.queryable, nil, plan, nodes[0], evaluationNode.TimeRange.ToDecodedTimeRange())
	if err != nil {
		resp.WriteError(ctx, mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("could not materialize query: %s", err.Error()))
		return
	}

	defer e.Close()

	if err := e.Evaluate(ctx, &evaluationObserver{resp, evaluationNode.NodeIndex, req.Plan.OriginalExpression}); err != nil {
		resp.WriteError(ctx, errorTypeForError(err), err.Error())
	}
}

func errorTypeForError(err error) mimirpb.QueryErrorType {
	// This method mirrors the behaviour of Prometheus' returnAPIError (https://github.com/prometheus/prometheus/blob/1ada3ced5a91fb4a6e5df473ac360ad99e62209e/web/api/v1/api.go#L682).
	if errors.Is(err, context.Canceled) {
		return mimirpb.QUERY_ERROR_TYPE_CANCELED
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return mimirpb.QUERY_ERROR_TYPE_TIMEOUT
	}

	var queryCanceledErr promql.ErrQueryCanceled
	if errors.As(err, &queryCanceledErr) {
		return mimirpb.QUERY_ERROR_TYPE_CANCELED
	}

	var queryTimeoutErr promql.ErrQueryTimeout
	if errors.As(err, &queryTimeoutErr) {
		return mimirpb.QUERY_ERROR_TYPE_TIMEOUT
	}

	var storageError promql.ErrStorage
	if errors.As(err, &storageError) {
		return mimirpb.QUERY_ERROR_TYPE_INTERNAL
	}

	return mimirpb.QUERY_ERROR_TYPE_EXECUTION
}

type httpResultStream struct {
	w http.ResponseWriter
}

func (s *httpResultStream) Write(ctx context.Context, r *frontendv2pb.QueryResultStreamRequest) error {
	b, err := r.Marshal()
	if err != nil {
		return err
	}

	if err := binary.Write(s.w, binary.LittleEndian, uint64(len(b))); err != nil {
		return err
	}

	if _, err := s.w.Write(b); err != nil {
		return err
	}

	return nil
}

type queryResponseWriter struct {
	stream frontendv2pb.QueryResultStream
	logger log.Logger
}

func (w *queryResponseWriter) Write(ctx context.Context, r querierpb.EvaluateQueryResponse) error {
	return w.stream.Write(ctx, &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &r,
		},
	})
}

func (w *queryResponseWriter) WriteError(ctx context.Context, typ mimirpb.QueryErrorType, msg string) {
	writeErrorToStream(ctx, w.stream, typ, msg, w.logger)
}

type evaluationObserver struct {
	w         *queryResponseWriter
	nodeIndex int64 // FIXME: remove this once Evaluator supports multiple nodes and passes the node index to the methods below

	originalExpression string
}

func (o *evaluationObserver) SeriesMetadataEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, series []types.SeriesMetadata) error {
	defer types.SeriesMetadataSlicePool.Put(&series, evaluator.MemoryConsumptionTracker)

	protoSeries := make([]querierpb.SeriesMetadata, 0, len(series))

	for _, s := range series {
		protoSeries = append(protoSeries, querierpb.SeriesMetadata{
			Labels: mimirpb.FromLabelsToLabelAdapters(s.Labels),
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
	defer types.PutInstantVectorSeriesData(seriesData, evaluator.MemoryConsumptionTracker)

	// TODO: batch up series to return, rather than sending each series one at a time?

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
			InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
				NodeIndex: o.nodeIndex,

				// The methods below do unsafe casts and do not copy the data from the slices, but this is OK as we're immediately
				// serializing the message and sending it before the deferred return to the pool occurs above.
				Floats:     mimirpb.FromFPointsToSamples(seriesData.Floats),
				Histograms: mimirpb.FromHPointsToHistograms(seriesData.Histograms),
			},
		},
	})
}

func (o *evaluationObserver) RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *streamingpromql.Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	// We do not need to return anything to the pool here: the ring buffers in stepData are reused for subsequent steps, or returned to the pool elsewhere if not needed.

	// TODO: batch up series / steps to return, rather than sending each step one at a time?

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

func (o *evaluationObserver) EvaluationCompleted(ctx context.Context, evaluator *streamingpromql.Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error {
	var annos querierpb.Annotations

	if annotations != nil {
		annos.Warnings, annos.Infos = annotations.AsStrings(o.originalExpression, 0, 0)
	}

	return o.w.Write(ctx, querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
			EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
				Annotations: annos,
				Stats: querierpb.QueryStats{
					TotalSamples:        stats.TotalSamples,
					TotalSamplesPerStep: stats.TotalSamplesPerStep,
				},
			},
		},
	})
}
