// SPDX-License-Identifier: AGPL-3.0-only

package spanlogger

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/spanlogger"
	"github.com/weaveworks/common/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/dskit/tenant"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// TenantIDsTagName is the tenant IDs tag name.
	TenantIDsTagName = spanlogger.TenantIDsTagName
)

type loggerCtxMarker struct{}

// SpanLogger unifies tracing and logging, to reduce repetition.
type SpanLogger struct {
	log.Logger
	trace.Span
	sampled bool
}

// defaultTenantResolver is used to include tenant IDs in spans automatically from the context.
var (
	defaultTenantResolver = tenant.NewMultiResolver()
	loggerCtxKey          = &loggerCtxMarker{}
)

// New makes a new SpanLogger with a log.Logger to send logs to. The provided context will have the logger attached
// to it and can be retrieved with FromContext.
func New(ctx context.Context, method string, kvps ...interface{}) (*SpanLogger, context.Context) {
	return NewWithLogger(ctx, util_log.Logger, method, kvps...)
}

// NewWithLogger is like New but allows to pass a logger.
func NewWithLogger(ctx context.Context, logger log.Logger, method string, kvps ...interface{}) (*SpanLogger, context.Context) {
	ctx, sp := otel.Tracer("").Start(ctx, method)
	defer sp.End()
	if ids, err := defaultTenantResolver.TenantIDs(ctx); err == nil && len(ids) > 0 {
		sp.SetAttributes(attribute.StringSlice(TenantIDsTagName, ids))
	}
	lwc, sampled := withContext(ctx, logger, defaultTenantResolver)
	l := &SpanLogger{
		Logger:  log.With(lwc, "method", method),
		Span:    sp,
		sampled: sampled,
	}
	if len(kvps) > 0 {
		level.Debug(l).Log(kvps...)
	}

	ctx = context.WithValue(ctx, loggerCtxKey, logger)
	return l, ctx
}

// SpanLogger unifies tracing and logging, to reduce repetition.
func withContext(ctx context.Context, logger log.Logger, resolver *tenant.MultiResolver) (log.Logger, bool) {
	userID, err := resolver.TenantID(ctx)
	if err == nil && userID != "" {
		logger = log.With(logger, "user", userID)
	}
	traceID, ok := tracing.ExtractSampledTraceID(ctx)
	if !ok {
		return logger, false
	}

	return log.With(logger, "traceID", traceID), true
}

// FromContext returns a SpanLogger using the current parent span.
// If there is no parent span, the SpanLogger will only log to the logger
// within the context. If the context doesn't have a logger, the fallback
// logger is used.
func FromContext(ctx context.Context, fallback log.Logger) *SpanLogger {
	logger, ok := ctx.Value(loggerCtxKey).(log.Logger)
	if !ok {
		logger = fallback
	}
	sp := trace.SpanFromContext(ctx)
	lwc, sampled := withContext(ctx, logger, defaultTenantResolver)
	return &SpanLogger{
		Logger:  lwc,
		Span:    sp,
		sampled: sampled,
	}
}

func (s *SpanLogger) Error(err error) error {
	if err == nil || !s.sampled {
		return err
	}
	s.Span.SetStatus(codes.Error, "")
	s.Span.RecordError(err)
	return err
}
