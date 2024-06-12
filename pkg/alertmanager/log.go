// SPDX-License-Identifier: AGPL-3.0-only

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

// newLoggerFactory returns a function that implements the alertingLogging.LoggerFactory interface.
func newLoggerFactory(logger log.Logger) alertingLogging.LoggerFactory {
	return func(loggerName string, ctx ...any) alertingLogging.Logger {
		keyvals := append([]any{"logger", loggerName}, ctx...)
		return &alertingLogger{kitLogger: log.With(logger, keyvals...)}
	}
}

func (l *alertingLogger) New(ctx ...any) alertingLogging.Logger {
	return &alertingLogger{log.With(l.kitLogger, ctx...)}
}

func (l *alertingLogger) Log(keyvals ...any) error {
	return l.kitLogger.Log(keyvals...)
}

func (l *alertingLogger) Debug(msg string, ctx ...any) {
	args := buildKeyvals(msg, ctx)
	level.Debug(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Info(msg string, ctx ...any) {
	args := buildKeyvals(msg, ctx)
	level.Info(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Warn(msg string, ctx ...any) {
	args := buildKeyvals(msg, ctx)
	level.Warn(l.kitLogger).Log(args...)
}

func (l *alertingLogger) Error(msg string, ctx ...any) {
	args := buildKeyvals(msg, ctx)
	level.Error(l.kitLogger).Log(args...)
}

// buildKeyvals builds the keyvals for the log message.
// It adds "msg" and the message string as the first two elements.
func buildKeyvals(msg string, ctx []any) []any {
	return append([]any{"msg", msg}, ctx...)
}
