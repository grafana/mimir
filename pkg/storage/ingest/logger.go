package ingest

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaLogger struct {
	logger log.Logger
	level  kgo.LogLevel
}

func newKafkaLogger(logger log.Logger, level kgo.LogLevel) *kafkaLogger {
	return &kafkaLogger{
		logger: log.With(logger, "component", "kafka"),
		level:  level,
	}
}

func (l *kafkaLogger) Level() kgo.LogLevel {
	return l.level
}

func (l *kafkaLogger) Log(lev kgo.LogLevel, msg string, keyvals ...any) {
	keyvals = append([]any{"msg", msg}, keyvals...)
	switch lev {
	case kgo.LogLevelDebug:
		level.Debug(l.logger).Log(keyvals...)
	case kgo.LogLevelInfo:
		level.Info(l.logger).Log(keyvals...)
	case kgo.LogLevelWarn:
		level.Warn(l.logger).Log(keyvals...)
	case kgo.LogLevelError:
		level.Error(l.logger).Log(keyvals...)
	}
}
