// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var evaluateQueryRequestType = proto.MessageName(&querierpb.EvaluateQueryRequest{})

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
// the query-scheduler over gRPC and therefore be handled by ServeGRPC.
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
	case evaluateQueryRequestType:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		writer := &httpResponseWriter{w: w, logger: d.logger}
		d.evaluateQuery(r.Context(), body, writer)
	default:
		http.Error(w, "unknown request type", http.StatusUnsupportedMediaType)
	}
}

func (d *Dispatcher) evaluateQuery(ctx context.Context, body []byte, resp queryResponseWriter) {
	req := &querierpb.EvaluateQueryRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		resp.WriteError(mimirpb.QUERY_ERROR_TYPE_INTERNAL, fmt.Sprintf("could not read request body: %s", err.Error()))
		return
	}

	if len(req.Nodes) != 1 {
		resp.WriteError(mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("this querier only supports evaluating exactly one node, got %d", len(req.Nodes)))
		return
	}

	evaluationNode := req.Nodes[0]

	plan, nodes, err := req.Plan.ToDecodedPlan(evaluationNode.NodeIndex)
	if err != nil {
		resp.WriteError(mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("could not decode plan: %s", err.Error()))
		return
	}

	e, err := d.engine.NewEvaluator(ctx, d.queryable, nil, plan, nodes[0], evaluationNode.TimeRange.ToDecodedTimeRange())
	if err != nil {
		resp.WriteError(mimirpb.QUERY_ERROR_TYPE_BAD_DATA, fmt.Sprintf("could not materialize query: %s", err.Error()))
		return
	}

	defer e.Close()

	if err := e.Evaluate(ctx, &evaluationObserver{resp, evaluationNode.NodeIndex, req.Plan.OriginalExpression}); err != nil {
		// TODO: translate error like https://github.com/prometheus/prometheus/blob/1ada3ced5a91fb4a6e5df473ac360ad99e62209e/web/api/v1/api.go#L682
		resp.WriteError(mimirpb.QUERY_ERROR_TYPE_INTERNAL, err.Error())
	}
}

type queryResponseWriter interface {
	WriteError(typ mimirpb.QueryErrorType, msg string)
	Write(m querierpb.EvaluateQueryResponse) error
}

type httpResponseWriter struct {
	w      http.ResponseWriter
	logger log.Logger
}

func (w *httpResponseWriter) Write(m querierpb.EvaluateQueryResponse) error {
	b, err := m.Marshal()
	if err != nil {
		return err
	}

	if err := binary.Write(w.w, binary.LittleEndian, uint64(len(b))); err != nil {
		return err
	}

	if _, err := w.w.Write(b); err != nil {
		return err
	}

	return nil
}

func (w *httpResponseWriter) WriteError(typ mimirpb.QueryErrorType, msg string) {
	err := w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_Error{
			Error: &querierpb.Error{
				Type:    typ,
				Message: msg,
			},
		},
	})

	if err != nil {
		level.Debug(w.logger).Log("msg", "could not write response message", "err", err)
	}
}

type evaluationObserver struct {
	w         queryResponseWriter
	nodeIndex int64 // FIXME: remove this once Evaluator supports multiple nodes and passes the node index to the methods below

	originalExpression string
}

func (o *evaluationObserver) SeriesMetadataEvaluated(evaluator *streamingpromql.Evaluator, series []types.SeriesMetadata) error {
	defer types.SeriesMetadataSlicePool.Put(&series, evaluator.MemoryConsumptionTracker)

	protoSeries := make([]querierpb.SeriesMetadata, 0, len(series))

	for _, s := range series {
		protoSeries = append(protoSeries, querierpb.SeriesMetadata{
			Labels: mimirpb.FromLabelsToLabelAdapters(s.Labels),
		})
	}

	return o.w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
			SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
				NodeIndex: o.nodeIndex,
				Series:    protoSeries,
			},
		},
	})
}

func (o *evaluationObserver) InstantVectorSeriesDataEvaluated(evaluator *streamingpromql.Evaluator, seriesIndex int, seriesData types.InstantVectorSeriesData) error {
	defer types.PutInstantVectorSeriesData(seriesData, evaluator.MemoryConsumptionTracker)

	// TODO: batch up series to return, rather than sending each series one at a time?

	return o.w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
			InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
				NodeIndex:  o.nodeIndex,
				Floats:     mimirpb.FromFPointsToSamples(seriesData.Floats),
				Histograms: mimirpb.FromHPointsToHistograms(seriesData.Histograms),
			},
		},
	})
}

func (o *evaluationObserver) RangeVectorStepSamplesEvaluated(evaluator *streamingpromql.Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	// We do not need to return anything to the pool here: the ring buffers in stepData are reused for subsequent steps, or returned to the pool elsewhere if not needed.

	// TODO: batch up series / steps to return, rather than sending each step one at a time?

	floatsHead, floatsTail := stepData.Floats.UnsafePoints()
	floats, needToReturnFloats, err := combineSlices(floatsHead, floatsTail, types.FPointSlicePool, evaluator.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	if needToReturnFloats {
		defer types.FPointSlicePool.Put(&floats, evaluator.MemoryConsumptionTracker)
	}

	histogramsHead, histogramsTail := stepData.Histograms.UnsafePoints()
	histograms, needToReturnHistograms, err := combineSlices(histogramsHead, histogramsTail, types.HPointSlicePool, evaluator.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	if needToReturnHistograms {
		defer types.HPointSlicePool.Put(&histograms, evaluator.MemoryConsumptionTracker)
	}

	return o.w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
			RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
				NodeIndex:   o.nodeIndex,
				SeriesIndex: int64(seriesIndex),
				StepT:       stepData.StepT,
				RangeStart:  stepData.RangeStart,
				RangeEnd:    stepData.RangeEnd,
				Floats:      mimirpb.FromFPointsToSamples(floats),
				Histograms:  mimirpb.FromHPointsToHistograms(histograms),
			},
		},
	})
}

func combineSlices[T any](head, tail []T, pool *types.LimitingBucketedPool[[]T, T], memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]T, bool, error) {
	if len(head) == 0 {
		return tail, false, nil
	}

	if len(tail) == 0 {
		return head, false, nil
	}

	// We can't mutate the head or tail slice, so create a new temporary slice with all points.
	combined, err := pool.Get(len(head)+len(tail), memoryConsumptionTracker)
	if err != nil {
		return nil, false, err
	}

	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined, true, nil
}

func (o *evaluationObserver) ScalarEvaluated(evaluator *streamingpromql.Evaluator, data types.ScalarData) error {
	defer types.FPointSlicePool.Put(&data.Samples, evaluator.MemoryConsumptionTracker)

	return o.w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_ScalarValue{
			ScalarValue: &querierpb.EvaluateQueryResponseScalarValue{
				NodeIndex: o.nodeIndex,
				Values:    mimirpb.FromFPointsToSamples(data.Samples),
			},
		},
	})
}

func (o *evaluationObserver) StringEvaluated(evaluator *streamingpromql.Evaluator, data string) error {
	return o.w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_StringValue{
			StringValue: &querierpb.EvaluateQueryResponseStringValue{
				NodeIndex: o.nodeIndex,
				Value:     data,
			},
		},
	})
}

func (o *evaluationObserver) EvaluationCompleted(evaluator *streamingpromql.Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error {
	var annos querierpb.Annotations

	if annotations != nil {
		annos.Warnings, annos.Infos = annotations.AsStrings(o.originalExpression, 0, 0)
	}

	return o.w.Write(querierpb.EvaluateQueryResponse{
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
