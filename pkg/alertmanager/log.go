package alertmanager

import (
	"fmt"

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
		newLogger := log.With(logger, keyvals...)
		al := &alertingLogger{kitLogger: newLogger}
		return al
	}
}

func (l *alertingLogger) New(ctx ...any) alertingLogging.Logger {
	fmt.Println("alertingLogger.New() called")
	return &alertingLogger{log.With(l.kitLogger, ctx...)}
}

func (l *alertingLogger) Log(keyvals ...any) error {
	fmt.Println("alertingLogger.Log() called")
	return l.kitLogger.Log(keyvals...)
}

func (l *alertingLogger) Debug(msg string, ctx ...any) {
	fmt.Println("alertingLogger.Debug() called")
	args := buildArgs(msg, ctx...)
	level.Debug(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Info(msg string, ctx ...any) {
	fmt.Println("alertingLogger.Info() called")
	args := buildArgs(msg, ctx...)
	level.Info(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Warn(msg string, ctx ...any) {
	fmt.Println("alertingLogger.Warn() called")
	args := buildArgs(msg, ctx...)
	level.Warn(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Error(msg string, ctx ...any) {
	fmt.Println("alertingLogger.Error() called")
	args := buildArgs(msg, ctx...)
	level.Error(l.kitLogger).Log(args...)
}

// buildArgs builds the keyvals for the log message.
// It adds "msg" and the message string as the first two elements.
func buildArgs(msg string, ctx ...any) []any {
	return append([]any{"msg", msg}, ctx...)
}
