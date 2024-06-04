package backend

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

const (
	endpointCallResource    = "callResource"
	endpointCheckHealth     = "checkHealth"
	endpointCollectMetrics  = "collectMetrics"
	endpointQueryData       = "queryData"
	endpointRunStream       = "runStream"
	endpointSubscribeStream = "subscribeStream"
	endpointPublishStream   = "publishStream"
)

// Logger is the default logger instance. This can be used directly to log from
// your plugin to grafana-server with calls like backend.Logger.Debug(...).
var Logger = log.DefaultLogger

// NewLoggerWith creates a new logger with the given arguments.
var NewLoggerWith = func(args ...interface{}) log.Logger {
	return log.New().With(args...)
}

func withContextualLogAttributes(ctx context.Context, pCtx PluginContext, endpoint string) context.Context {
	args := []any{"pluginID", pCtx.PluginID, "endpoint", endpoint}
	if tid := trace.SpanContextFromContext(ctx).TraceID(); tid.IsValid() {
		args = append(args, "traceID", tid.String())
	}
	if pCtx.DataSourceInstanceSettings != nil {
		args = append(
			args,
			"dsName", pCtx.DataSourceInstanceSettings.Name,
			"dsUID", pCtx.DataSourceInstanceSettings.UID,
		)
		if pCtx.User != nil {
			args = append(args, "uname", pCtx.User.Name)
		}
	}
	ctx = log.WithContextualAttributes(ctx, args)
	return ctx
}
