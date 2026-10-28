// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package scheduler

import (
	"context"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/rootqueryid"
)

type RequestKey struct {
	frontendAddr string
	queryID      uint64
}

func NewSchedulerRequestKey(frontendAddr string, queryID uint64) RequestKey {
	return RequestKey{
		frontendAddr: frontendAddr,
		queryID:      queryID,
	}
}

type SchedulerRequest struct {
	FrontendAddr string
	UserID       string
	QueryID      uint64
	// RootQueryID identifies the user query this request is a sub-request of, as reported by the
	// frontend. It is a random UUID, so it is globally unique. Empty means unknown.
	RootQueryID               string
	HttpRequest               *httpgrpc.HTTPRequest
	ProtobufRequest           *schedulerpb.ProtobufRequest
	StatsEnabled              bool
	AdditionalQueueDimensions []string

	EnqueueTime time.Time
	// Dispatched is set once the request has sent to a querier for execution.
	Dispatched atomic.Bool

	Ctx        context.Context
	CancelFunc context.CancelCauseFunc
	QueueSpan  trace.Span

	ParentSpanContext trace.SpanContext

	// reservation is set when the request is a sub-request of a queue reservation. The success
	// function of the enqueue sets it on the dispatcher goroutine, before a querier can receive the
	// request.
	reservation *queue.Reservation
}

// finishReservedSubRequest tells the request's reservation that the request is finished or
// skipped. QuerierLoop calls it once for each dequeued request.
func (sr *SchedulerRequest) finishReservedSubRequest() {
	if sr.reservation != nil {
		sr.reservation.ItemFinished(time.Now())
	}
}

func (sr *SchedulerRequest) Key() RequestKey {
	return RequestKey{
		frontendAddr: sr.FrontendAddr,
		queryID:      sr.QueryID,
	}
}

// LogFields returns the fields that identify this request, for inclusion in request-scoped log
// lines.
func (sr *SchedulerRequest) LogFields() []any {
	return requestLogFields(sr.UserID, sr.QueryID, sr.RootQueryID)
}

// requestLogFields returns the fields that identify a request, for inclusion in request-scoped log
// lines. rootQueryID is omitted when it is empty.
func requestLogFields(userID string, queryID uint64, rootQueryID string) []any {
	return rootqueryid.AppendLogFields([]any{"user", userID, "query_id", queryID}, rootQueryID)
}

// ExpectedQueryComponentName parses the expected query component from annotations by the frontend.
func (sr *SchedulerRequest) ExpectedQueryComponentName() string {
	if len(sr.AdditionalQueueDimensions) > 0 {
		return sr.AdditionalQueueDimensions[0]
	}
	return queue.UnknownDimension
}
