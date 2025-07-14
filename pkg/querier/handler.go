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

	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql"
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
		resp.WriteError(fmt.Sprintf("could not read request body: %s", err.Error()))
		return
	}

	if len(req.Nodes) != 1 {
		resp.WriteError(fmt.Sprintf("this querier only supports evaluating exactly one node, got %d", len(req.Nodes)))
		return
	}

	plan, err := req.Plan.ToDecodedPlan()
	if err != nil {
		resp.WriteError(fmt.Sprintf("could not decode plan: %s", err.Error()))
		return
	}

	// TODO: materialize requested node, not root
	q, err := d.engine.MaterializeNode(ctx, plan, d.queryable, nil, plan.Root, plan.TimeRange)
	if err != nil {
		resp.WriteError(fmt.Sprintf("could not materialize query: %s", err.Error()))
		return
	}

	defer q.Close()

	// TODO: start sending messages
	resp.WriteError(fmt.Sprintf("TODO"))
}

type queryResponseWriter interface {
	WriteError(msg string)
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

func (w *httpResponseWriter) WriteError(msg string) {
	err := w.Write(querierpb.EvaluateQueryResponse{
		Message: &querierpb.EvaluateQueryResponse_Error{
			Error: &querierpb.Error{
				Message: msg,
			},
		},
	})

	if err != nil {
		level.Debug(w.logger).Log("msg", "could not write response message", "err", err)
	}
}
