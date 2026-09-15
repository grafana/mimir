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
	"github.com/grafana/mimir/pkg/util/parentquery"
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
	// ParentQueryID identifies the user query this request is a sub-request of, as reported by the
	// frontend. It is unique within a single frontend only. Zero means unknown.
	ParentQueryID             uint64
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
	return requestLogFields(sr.UserID, sr.QueryID, sr.ParentQueryID)
}

// requestLogFields returns the fields that identify a request, for inclusion in request-scoped log
// lines. parentQueryID is omitted when it is zero, so that an unknown parent is not reported as
// query 0.
func requestLogFields(userID string, queryID, parentQueryID uint64) []any {
	return parentquery.AppendLogFields([]any{"user", userID, "query_id", queryID}, parentQueryID)
}

// ExpectedQueryComponentName parses the expected query component from annotations by the frontend.
func (sr *SchedulerRequest) ExpectedQueryComponentName() string {
	if len(sr.AdditionalQueueDimensions) > 0 {
		return sr.AdditionalQueueDimensions[0]
	}
	return queue.UnknownDimension
}
