package alertmanager

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	alertingLogging "github.com/grafana/alerting/logging"
)

// alertingLogger implements the alertingLogging.Logger interface.
type alertingLogger struct {
	kitLogger log.Logger
}

func loggerFactory(logger log.Logger) alertingLogging.LoggerFactory {
	return func(loggerName string, ctx ...any) alertingLogging.Logger {
		keyvals := append([]any{"logger", loggerName}, ctx...)
		return &alertingLogger{kitLogger: log.With(logger, keyvals)}
	}
}

func (l *alertingLogger) New(ctx ...any) alertingLogging.Logger {
	return &alertingLogger{log.With(l.kitLogger, ctx...)}
}

func (l *alertingLogger) Log(keyvals ...any) error {
	return l.kitLogger.Log(keyvals)
}

func (l *alertingLogger) Debug(msg string, ctx ...any) {
	level.Debug(l.kitLogger).Log(buildArgs(msg, ctx))
}

func (l *alertingLogger) Info(msg string, ctx ...any) {
	level.Info(l.kitLogger).Log(buildArgs(msg, ctx))
}

func (l *alertingLogger) Warn(msg string, ctx ...any) {
	level.Warn(l.kitLogger).Log(buildArgs(msg, ctx))
}

func (l *alertingLogger) Error(msg string, ctx ...any) {
	level.Error(l.kitLogger).Log(buildArgs(msg, ctx))
}

// buildArgs builds the keyvals for the log message.
// It adds "msg" and the message string as the first two elements.
func buildArgs(msg string, ctx ...any) []any {
	return append([]any{"msg", msg}, ctx...)
}
